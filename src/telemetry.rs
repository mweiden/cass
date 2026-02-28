use std::error::Error;

use opentelemetry::{Context, KeyValue, global, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    error::OTelSdkResult, propagation::TraceContextPropagator, resource::Resource,
    trace::SdkTracerProvider,
};
use tonic::codegen::http::HeaderMap;
use tonic::{
    Request, Status,
    metadata::{Ascii, KeyRef, MetadataKey, MetadataMap, MetadataValue},
    service::Interceptor,
};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// RAII guard that shuts down the tracer provider when dropped.
pub struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
}

impl TelemetryGuard {
    /// Shutdown the underlying tracer provider, flushing any pending spans.
    pub fn shutdown(mut self) -> OTelSdkResult {
        if let Some(provider) = self.provider.take() {
            provider.shutdown()
        } else {
            Ok(())
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            if let Err(err) = provider.shutdown() {
                tracing::error!(?err, "failed to shutdown tracer provider");
            }
        }
    }
}

/// Initialize the global tracing subscriber with stdout and OTLP exporters.
///
/// The OTLP collector endpoint defaults to `http://127.0.0.1:4317` and can be
/// overridden via the `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable.
/// Similarly the service name can be set with `OTEL_SERVICE_NAME` and the
/// service instance with `OTEL_SERVICE_INSTANCE_ID`.
///
/// Returns a [`TelemetryGuard`] which must remain in scope for as long as
/// tracing should be active. Dropping the guard will attempt to flush any
/// buffered spans before shutdown.
pub fn init_tracing(
    default_service_name: &str,
    default_instance: Option<String>,
) -> Result<TelemetryGuard, Box<dyn Error>> {
    let env_filter =
        || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if tracing_disabled() {
        let fmt_layer = tracing_subscriber::fmt::layer().with_target(true);
        tracing_subscriber::registry()
            .with(env_filter())
            .with(fmt_layer)
            .try_init()?;
        return Ok(TelemetryGuard { provider: None });
    }
    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:4317".to_string());
    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| default_service_name.to_string());
    let instance = std::env::var("OTEL_SERVICE_INSTANCE_ID")
        .ok()
        .or(default_instance);

    global::set_text_map_propagator(TraceContextPropagator::new());

    let mut resource_builder = Resource::builder().with_service_name(service_name);
    if let Some(instance_id) = instance {
        resource_builder =
            resource_builder.with_attribute(KeyValue::new("service.instance.id", instance_id));
    }
    let resource = resource_builder.build();

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .build()?;

    let provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build();

    let guard = TelemetryGuard {
        provider: Some(provider.clone()),
    };
    let tracer = provider.tracer(default_service_name.to_owned());
    let _ = global::set_tracer_provider(provider);

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true);

    tracing_subscriber::registry()
        .with(env_filter())
        .with(fmt_layer)
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()?;

    Ok(guard)
}

/// Extract a remote context from HTTP headers (used by gRPC requests).
pub fn extract_remote_context_from_headers(headers: &HeaderMap) -> Context {
    global::get_text_map_propagator(|prop| prop.extract(&HeaderExtractor(headers)))
}

/// Extract a remote context from gRPC metadata.
pub fn extract_remote_context_from_metadata(metadata: &MetadataMap) -> Context {
    global::get_text_map_propagator(|prop| prop.extract(&MetadataMapExtractor(metadata)))
}

/// Inject the current span's context into gRPC metadata.
pub fn inject_context(metadata: &mut MetadataMap) {
    global::get_text_map_propagator(|prop| {
        let context = Span::current().context();
        prop.inject_context(&context, &mut MetadataMapInjector(metadata));
    });
}

/// Interceptor that propagates the current tracing context through outgoing
/// gRPC metadata.
#[derive(Clone, Default)]
pub struct PropagatingInterceptor;

impl Interceptor for PropagatingInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        inject_context(request.metadata_mut());
        Ok(request)
    }
}

struct HeaderExtractor<'a>(&'a HeaderMap);

impl<'a> opentelemetry::propagation::Extractor for HeaderExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|name| name.as_str()).collect()
    }
}

struct MetadataMapExtractor<'a>(&'a MetadataMap);

impl<'a> opentelemetry::propagation::Extractor for MetadataMapExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|key| match key {
                KeyRef::Ascii(key) => Some(key.as_str()),
                KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

struct MetadataMapInjector<'a>(&'a mut MetadataMap);

impl<'a> opentelemetry::propagation::Injector for MetadataMapInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(value) = MetadataValue::<Ascii>::try_from(value.as_str()) {
            if let Ok(key) = key.parse::<MetadataKey<Ascii>>() {
                let _ = self.0.insert(key, value);
            }
        }
    }
}

pub fn tracing_disabled() -> bool {
    matches!(
        std::env::var("CASS_DISABLE_TRACING"),
        Ok(v) if matches!(v.as_str(), "1" | "true" | "TRUE" | "True" | "yes" | "YES")
    )
}
