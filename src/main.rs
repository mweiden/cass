use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use cass::{
    Database, DatabaseOptions,
    cluster::Cluster,
    rpc::{
        FlushRequest, FlushResponse, HealthRequest, HealthResponse, LwtCommitRequest,
        LwtCommitResponse, LwtPrepareRequest, LwtPrepareResponse, LwtProposeRequest,
        LwtProposeResponse, LwtReadRequest, LwtReadResponse, PanicRequest, PanicResponse,
        QueryRequest, QueryResponse,
        cass_client::CassClient,
        cass_server::{Cass, CassServer},
        query_response,
    },
    storage::{Storage, local::LocalStorage, s3::S3Storage},
    telemetry,
    util::{print_rows, sstable_disk_usage},
    wal::WalOptions,
};
use clap::{Args, Parser, Subcommand, ValueEnum};
use hyper::{
    Body as HttpBody, Request as HttpRequest, Response as HttpResponse, Server as HyperServer,
    header::{CONTENT_TYPE, HeaderValue},
    service::{make_service_fn, service_fn},
};
use once_cell::sync::Lazy;
use prometheus::{Gauge, GaugeVec, register_gauge, register_gauge_vec};
use sysinfo::System;
use tokio::time::{Duration, sleep};
use tonic::{Request, Response, Status, transport::Server};
use tonic_prometheus_layer::{MetricsLayer, metrics as tl_metrics};
use tower_http::trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::{Level, Span, field, info};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use url::Url;

type DynStorage = Arc<dyn Storage>;

static NODE_HEALTH: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!("node_health", "Health status of peer nodes", &["peer"]).unwrap()
});
static RAM_USAGE: Lazy<Gauge> =
    Lazy::new(|| register_gauge!("ram_usage_bytes", "RAM usage in bytes").unwrap());
static CPU_USAGE: Lazy<Gauge> =
    Lazy::new(|| register_gauge!("cpu_usage_percent", "CPU usage percentage").unwrap());
static SSTABLE_DISK_USAGE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!("sstable_disk_usage_bytes", "SSTable disk usage in bytes").unwrap()
});

#[derive(Parser)]
#[command(name = "cass")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the gRPC server
    Server(ServerArgs),
    /// Broadcast a flush across the cluster via the target node
    Flush { target: String },
    /// Make the specified node unhealthy for a short period
    Panic { target: String },
    /// Start an interactive SQL REPL against the provided nodes
    Repl { nodes: Vec<String> },
}

#[derive(Args)]
struct ServerArgs {
    #[arg(long, default_value = "local", value_enum)]
    storage: StorageKind,
    #[arg(long, default_value = "/tmp/cass-data")]
    data_dir: String,
    #[arg(long)]
    bucket: Option<String>,
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    node_addr: String,
    #[arg(long)]
    peer: Vec<String>,
    #[arg(long, default_value_t = 1)]
    rf: usize,
    #[arg(long, default_value_t = 8)]
    vnodes: usize,
    /// Server-level read consistency: ONE, QUORUM, ALL
    #[arg(long, value_enum)]
    read_consistency: Option<Consistency>,
    /// Periodic commitlog fsync interval in milliseconds (0 for immediate flushes)
    #[arg(long, default_value_t = 10_000)]
    commitlog_sync_period_ms: u64,
}

#[derive(Copy, Clone, ValueEnum)]
enum StorageKind {
    Local,
    S3,
}

#[derive(Copy, Clone, ValueEnum)]
enum Consistency {
    One,
    Quorum,
    All,
}

#[derive(Clone)]
struct CassService {
    cluster: Arc<Cluster>,
}

#[tonic::async_trait]
impl Cass for CassService {
    #[tracing::instrument(skip(self, req), fields(query.sql = field::Empty))]
    async fn query(&self, req: Request<QueryRequest>) -> Result<Response<QueryResponse>, Status> {
        let parent_cx = telemetry::extract_remote_context_from_metadata(req.metadata());
        let span = Span::current();
        span.set_parent(parent_cx);
        let sql = req.get_ref().sql.clone();
        span.record("query.sql", &field::display(&sql));
        match self.cluster.execute(&sql, false).await {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(Status::invalid_argument(e.to_string())),
        }
    }

    #[tracing::instrument(skip(self, req), fields(query.sql = field::Empty, query.forwarded = true))]
    async fn internal(
        &self,
        req: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let parent_cx = telemetry::extract_remote_context_from_metadata(req.metadata());
        let span = Span::current();
        span.set_parent(parent_cx);
        let sql = req.get_ref().sql.clone();
        span.record("query.sql", &field::display(&sql));
        match self.cluster.execute(&sql, true).await {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(Status::invalid_argument(e.to_string())),
        }
    }

    #[tracing::instrument(skip(self, req))]
    async fn flush(&self, req: Request<FlushRequest>) -> Result<Response<FlushResponse>, Status> {
        let parent_cx = telemetry::extract_remote_context_from_metadata(req.metadata());
        Span::current().set_parent(parent_cx);
        self.cluster.flush_all().await.map_err(Status::internal)?;
        Ok(Response::new(FlushResponse {}))
    }

    #[tracing::instrument(skip(self, req), fields(forwarded = true))]
    async fn flush_internal(
        &self,
        req: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        let parent_cx = telemetry::extract_remote_context_from_metadata(req.metadata());
        Span::current().set_parent(parent_cx);
        self.cluster
            .flush_self()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(FlushResponse {}))
    }

    #[tracing::instrument(skip(self, req))]
    async fn panic(&self, req: Request<PanicRequest>) -> Result<Response<PanicResponse>, Status> {
        let parent_cx = telemetry::extract_remote_context_from_metadata(req.metadata());
        Span::current().set_parent(parent_cx);
        self.cluster
            .panic_for(std::time::Duration::from_secs(60))
            .await;
        let healthy = self.cluster.self_healthy().await;
        Ok(Response::new(PanicResponse { healthy }))
    }

    #[tracing::instrument(skip(self, req))]
    async fn health(
        &self,
        req: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let parent_cx = telemetry::extract_remote_context_from_metadata(req.metadata());
        Span::current().set_parent(parent_cx);
        if !self.cluster.self_healthy().await {
            return Err(Status::unavailable("unhealthy"));
        }
        Ok(Response::new(HealthResponse {
            info: self.cluster.health_info().to_string(),
        }))
    }

    #[tracing::instrument(
        skip(self, req),
        fields(namespace = field::Empty, key = field::Empty, ballot = field::Empty)
    )]
    async fn lwt_prepare(
        &self,
        req: Request<LwtPrepareRequest>,
    ) -> Result<Response<LwtPrepareResponse>, Status> {
        let parent_cx = telemetry::extract_remote_context_from_metadata(req.metadata());
        let span = Span::current();
        span.set_parent(parent_cx);
        let r = req.into_inner();
        span.record("namespace", &field::display(&r.namespace));
        span.record("key", &field::display(&r.key));
        span.record("ballot", &field::display(r.ballot));
        let (promised, ballot, value) = self
            .cluster
            .lwt_prepare(&r.namespace, &r.key, r.ballot)
            .await;
        Ok(Response::new(LwtPrepareResponse {
            promised,
            ballot,
            value,
        }))
    }

    #[tracing::instrument(
        skip(self, req),
        fields(namespace = field::Empty, key = field::Empty, ballot = field::Empty)
    )]
    async fn lwt_propose(
        &self,
        req: Request<LwtProposeRequest>,
    ) -> Result<Response<LwtProposeResponse>, Status> {
        let parent_cx = telemetry::extract_remote_context_from_metadata(req.metadata());
        let span = Span::current();
        span.set_parent(parent_cx);
        let r = req.into_inner();
        span.record("namespace", &field::display(&r.namespace));
        span.record("key", &field::display(&r.key));
        span.record("ballot", &field::display(r.ballot));
        let accepted = self
            .cluster
            .lwt_propose(&r.namespace, &r.key, r.ballot, r.value)
            .await;
        Ok(Response::new(LwtProposeResponse { accepted }))
    }

    #[tracing::instrument(
        skip(self, req),
        fields(namespace = field::Empty, key = field::Empty)
    )]
    async fn lwt_read(
        &self,
        req: Request<LwtReadRequest>,
    ) -> Result<Response<LwtReadResponse>, Status> {
        let parent_cx = telemetry::extract_remote_context_from_metadata(req.metadata());
        let span = Span::current();
        span.set_parent(parent_cx);
        let r = req.into_inner();
        span.record("namespace", &field::display(&r.namespace));
        span.record("key", &field::display(&r.key));
        let (ballot, value) = self.cluster.lwt_read(&r.namespace, &r.key).await;
        Ok(Response::new(LwtReadResponse { ballot, value }))
    }

    #[tracing::instrument(
        skip(self, req),
        fields(namespace = field::Empty, key = field::Empty)
    )]
    async fn lwt_commit(
        &self,
        req: Request<LwtCommitRequest>,
    ) -> Result<Response<LwtCommitResponse>, Status> {
        let parent_cx = telemetry::extract_remote_context_from_metadata(req.metadata());
        let span = Span::current();
        span.set_parent(parent_cx);
        let r = req.into_inner();
        span.record("namespace", &field::display(&r.namespace));
        span.record("key", &field::display(&r.key));
        self.cluster.lwt_commit(&r.namespace, &r.key, r.value).await;
        Ok(Response::new(LwtCommitResponse {}))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Server(args) => {
            let guard = telemetry::init_tracing("cass", Some(args.node_addr.clone()))?;
            run_server(args).await?;
            if let Err(err) = guard.shutdown() {
                tracing::error!(?err, "failed to shutdown tracer provider");
            }
        }
        Command::Flush { target } => {
            let guard = telemetry::init_tracing("cass-cli", None)?;
            let mut client = CassClient::connect_traced(target).await?;
            client.flush(FlushRequest {}).await?;
            if let Err(err) = guard.shutdown() {
                tracing::error!(?err, "failed to shutdown tracer provider");
            }
        }
        Command::Panic { target } => {
            let guard = telemetry::init_tracing("cass-cli", None)?;
            let mut client = CassClient::connect_traced(target).await?;
            let resp = client.panic(PanicRequest {}).await?;
            println!("healthy: {}", resp.into_inner().healthy);
            if let Err(err) = guard.shutdown() {
                tracing::error!(?err, "failed to shutdown tracer provider");
            }
        }
        Command::Repl { nodes } => {
            let guard = telemetry::init_tracing("cass-repl", None)?;
            repl(nodes).await?;
            if let Err(err) = guard.shutdown() {
                tracing::error!(?err, "failed to shutdown tracer provider");
            }
        }
    }
    Ok(())
}

async fn run_server(args: ServerArgs) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = args.data_dir.clone();
    let storage: DynStorage = match args.storage {
        StorageKind::Local => Arc::new(LocalStorage::new(&data_dir)),
        StorageKind::S3 => {
            let bucket = args.bucket.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--bucket required for s3 storage mode",
                )
            })?;
            Arc::new(S3Storage::new(&bucket).await?)
        }
    };
    let db_options = DatabaseOptions {
        wal: WalOptions {
            commitlog_sync_period: Duration::from_millis(args.commitlog_sync_period_ms),
        },
        ..DatabaseOptions::default()
    };
    let db = Arc::new(Database::new_with_options(storage, "wal.log", db_options).await?);
    let cluster = Arc::new(Cluster::new_with_consistency(
        db.clone(),
        args.node_addr.clone(),
        args.peer.clone(),
        args.vnodes,
        args.rf,
        match args.read_consistency.unwrap_or(Consistency::Quorum) {
            Consistency::One => 1,
            Consistency::Quorum => args.rf.max(1) / 2 + 1,
            Consistency::All => args.rf.max(1),
        },
    ));

    tl_metrics::try_init_settings(tl_metrics::GlobalSettings {
        registry: prometheus::default_registry().clone(),
        ..Default::default()
    })
    .ok();

    let svc = CassService {
        cluster: cluster.clone(),
    };
    let url = Url::parse(&args.node_addr)?;
    let port = url.port().unwrap_or(80);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let metrics_addr = SocketAddr::from(([0, 0, 0, 0], port + 1000));

    let metrics_layer = MetricsLayer::new();

    let cluster_metrics = cluster.clone();
    let data_dir_metrics = data_dir.clone();
    let sys_metrics = Arc::new(Mutex::new(System::new_all()));
    tokio::spawn(async move {
        loop {
            for (peer, alive) in cluster_metrics.peer_health().await {
                NODE_HEALTH
                    .with_label_values(&[peer.as_str()])
                    .set(if alive { 1.0 } else { 0.0 });
            }
            let self_addr = cluster_metrics.self_addr().to_string();
            let self_alive = cluster_metrics.self_healthy().await;
            NODE_HEALTH
                .with_label_values(&[self_addr.as_str()])
                .set(if self_alive { 1.0 } else { 0.0 });

            let sys = sys_metrics.clone();
            let dir = data_dir_metrics.clone();
            match tokio::task::spawn_blocking(move || {
                let mut sys = sys.lock().unwrap();
                sys.refresh_memory();
                sys.refresh_cpu();
                let ram = sys.used_memory() as f64;
                let cpu = sys.global_cpu_info().cpu_usage() as f64;
                let disk = sstable_disk_usage(&dir) as f64;
                (ram, cpu, disk)
            })
            .await
            {
                Ok((ram, cpu, disk)) => {
                    RAM_USAGE.set(ram);
                    CPU_USAGE.set(cpu);
                    SSTABLE_DISK_USAGE.set(disk);
                }
                Err(_) => {
                    // If the blocking task panicked or was cancelled, skip this sample.
                }
            }

            sleep(Duration::from_secs(10)).await;
        }
    });

    tokio::spawn(async move {
        let make_svc = make_service_fn(|_| async {
            Ok::<_, Infallible>(service_fn(|_req: HttpRequest<HttpBody>| async move {
                let body = tl_metrics::encode_to_string().unwrap_or_default();
                let response = HttpResponse::builder()
                    .header(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; version=0.0.4"),
                    )
                    .body(HttpBody::from(body))
                    .unwrap();
                Ok::<_, Infallible>(response)
            }))
        });

        if let Err(e) = HyperServer::bind(&metrics_addr).serve(make_svc).await {
            eprintln!("metrics server error: {e}");
        }
    });

    info!("Cass gRPC server listening on {addr}");
    if telemetry::tracing_disabled() {
        Server::builder()
            .layer(metrics_layer)
            .add_service(CassServer::new(svc))
            .serve(addr)
            .await?;
    } else {
        let trace_layer = TraceLayer::new_for_grpc()
            .make_span_with(|request: &tonic::codegen::http::Request<_>| {
                let path = request.uri().path();
                let span = tracing::debug_span!(
                    "grpc.request",
                    otel.name = %path,
                    grpc.method = %path,
                    grpc.status_code = field::Empty,
                );
                let context = telemetry::extract_remote_context_from_headers(request.headers());
                span.set_parent(context);
                span
            })
            .on_request(DefaultOnRequest::new().level(Level::DEBUG))
            .on_response(DefaultOnResponse::new().level(Level::DEBUG));
        Server::builder()
            .layer(metrics_layer)
            .layer(trace_layer)
            .add_service(CassServer::new(svc))
            .serve(addr)
            .await?;
    }
    Ok(())
}

async fn repl(nodes: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    use rustyline::{Editor, history::DefaultHistory};
    let rl = Arc::new(Mutex::new(Editor::<(), DefaultHistory>::new()?));
    loop {
        let rl_clone = rl.clone();
        let line = tokio::task::spawn_blocking(move || {
            let mut rl = rl_clone.lock().unwrap();
            let line = rl.readline("> ");
            if let Ok(ref l) = line {
                let _ = rl.add_history_entry(l.as_str());
            }
            line
        })
        .await??;

        let sql = line.trim();
        if sql.is_empty() {
            continue;
        }

        let mut last_err: Option<Status> = None;
        for node in &nodes {
            match CassClient::connect_traced(node.clone()).await {
                Ok(mut client) => match client
                    .query(QueryRequest {
                        sql: sql.to_string(),
                    })
                    .await
                {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        match resp.payload {
                            Some(query_response::Payload::Rows(rs)) => {
                                let mut out = std::io::stdout();
                                print_rows(&rs.rows, &mut out);
                            }
                            Some(query_response::Payload::Mutation(m)) => {
                                println!("{} {} {}", m.op, m.count, m.unit);
                            }
                            Some(query_response::Payload::Tables(t)) => {
                                for tbl in &t.tables {
                                    println!("{}", tbl);
                                }
                                println!("({} tables)", t.tables.len());
                            }
                            _ => println!(""),
                        }
                        last_err = None;
                        break;
                    }
                    Err(e) => last_err = Some(e),
                },
                Err(e) => last_err = Some(Status::unknown(e.to_string())),
            }
        }
        if let Some(err) = last_err {
            eprintln!("query failed: {}", err.message());
        }
    }
}
