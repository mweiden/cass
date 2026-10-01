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
        let req = req.into_inner();
        let sql = req.sql;
        let ts = req.ts;
        span.record("query.sql", field::display(&sql));
        match self.cluster.execute(&sql, false, ts).await {
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
        let req = req.into_inner();
        let sql = req.sql;
        let ts = req.ts;
        span.record("query.sql", field::display(&sql));
        match self.cluster.execute(&sql, true, ts).await {
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
        span.record("namespace", field::display(&r.namespace));
        span.record("key", field::display(&r.key));
        span.record("ballot", field::display(r.ballot));
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
        span.record("namespace", field::display(&r.namespace));
        span.record("key", field::display(&r.key));
        span.record("ballot", field::display(r.ballot));
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
        span.record("namespace", field::display(&r.namespace));
        span.record("key", field::display(&r.key));
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
        span.record("namespace", field::display(&r.namespace));
        span.record("key", field::display(&r.key));
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
                        ts: 0,
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
                            _ => println!(),
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

#[cfg(test)]
mod scan_read_tests {
    use super::*;
    use async_trait::async_trait;
    use cass::{
        query::{QueryOutput, SqlEngine},
        schema::encode_row,
        storage::{StorageError, local::LocalStorage},
    };
    use std::{
        collections::BTreeMap,
        io,
        net::{SocketAddr, TcpListener},
        path::Path,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        time::Duration,
    };
    use tempfile::TempDir;
    use tokio::{sync::oneshot, time::timeout};
    use tonic::transport::{Channel, Server};

    const SELECT_ORDERS: &str = "SELECT * FROM orders WHERE customer_id = 'nike'";
    const COUNT_ORDERS: &str = "SELECT COUNT(*) FROM orders WHERE customer_id = 'nike'";
    const SSTABLE_WITH_NEWER_ROWS: &str = "sstable_2.tbl";
    const INJECTED_READ_ERROR: &str = "injected SSTable read failure";

    struct FailingStorage {
        inner: LocalStorage,
        failed_path: &'static str,
        fail_reads: AtomicBool,
        failed_reads: AtomicUsize,
        successful_data_reads: AtomicUsize,
    }

    impl FailingStorage {
        fn new(path: &Path) -> Self {
            Self {
                inner: LocalStorage::new(path),
                failed_path: SSTABLE_WITH_NEWER_ROWS,
                fail_reads: AtomicBool::new(false),
                failed_reads: AtomicUsize::new(0),
                successful_data_reads: AtomicUsize::new(0),
            }
        }

        fn fail_reads(&self) {
            self.fail_reads.store(true, Ordering::SeqCst);
        }

        fn allow_reads(&self) {
            self.fail_reads.store(false, Ordering::SeqCst);
        }

        fn failed_read_count(&self) -> usize {
            self.failed_reads.load(Ordering::SeqCst)
        }

        fn successful_data_read_count(&self) -> usize {
            self.successful_data_reads.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Storage for FailingStorage {
        async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), StorageError> {
            self.inner.put(path, data).await
        }

        async fn get(&self, path: &str) -> Result<Vec<u8>, StorageError> {
            if path == self.failed_path {
                if self.fail_reads.load(Ordering::SeqCst) {
                    self.failed_reads.fetch_add(1, Ordering::SeqCst);
                    return Err(StorageError::Io(io::Error::other(INJECTED_READ_ERROR)));
                }
                self.successful_data_reads.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get(path).await
        }

        async fn append(&self, path: &str, data: &[u8]) -> Result<(), StorageError> {
            self.inner.append(path, data).await
        }

        fn local_path(&self) -> Option<&Path> {
            self.inner.local_path()
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.inner.list(prefix).await
        }
    }

    async fn seed_storage(storage: Arc<FailingStorage>) -> Arc<Database> {
        let db = Arc::new(Database::new(storage.clone(), "wal.log").await.unwrap());
        let engine = SqlEngine::new();
        engine
            .execute_with_ts(
                &db,
                "CREATE TABLE orders (customer_id TEXT, order_id TEXT, order_date TEXT, PRIMARY KEY(customer_id, order_id))",
                0,
                false,
            )
            .await
            .unwrap();
        db.flush().await.unwrap();

        engine
            .execute_with_ts(
                &db,
                "INSERT INTO orders VALUES ('nike','aaa','first'), ('nike','abc123','old'), ('nike','gone','present'), ('nike','memover','old')",
                1,
                false,
            )
            .await
            .unwrap();
        db.flush().await.unwrap();

        engine
            .execute_with_ts(
                &db,
                "UPDATE orders SET order_date = 'new' WHERE customer_id = 'nike' AND order_id = 'abc123'",
                2,
                false,
            )
            .await
            .unwrap();
        engine
            .execute_with_ts(
                &db,
                "DELETE FROM orders WHERE customer_id = 'nike' AND order_id = 'gone'",
                3,
                false,
            )
            .await
            .unwrap();
        engine
            .execute_with_ts(
                &db,
                "UPDATE orders SET order_date = 'disk' WHERE customer_id = 'nike' AND order_id = 'memover'",
                4,
                false,
            )
            .await
            .unwrap();
        db.flush().await.unwrap();

        let files = storage.list("sstable_").await.unwrap();
        assert!(files.iter().any(|file| file == SSTABLE_WITH_NEWER_ROWS));
        db
    }

    async fn add_memtable_overlay(db: &Database) {
        let overwritten = BTreeMap::from([("order_date".to_string(), "memtable".to_string())]);
        db.insert_ns_ts(
            "orders",
            "nike|memover".to_string(),
            encode_row(&overwritten),
            5,
        )
        .await
        .unwrap();

        let inserted = BTreeMap::from([("order_date".to_string(), "last".to_string())]);
        db.insert_ns_ts("orders", "nike|zzz".to_string(), encode_row(&inserted), 6)
            .await
            .unwrap();
        db.sync_wal().await.unwrap();
    }

    fn result_rows(output: QueryOutput) -> Vec<BTreeMap<String, String>> {
        match output {
            QueryOutput::Rows(rows) => rows,
            _ => panic!("expected row result"),
        }
    }

    fn response_rows(response: &QueryResponse) -> Option<Vec<(String, String)>> {
        let Some(query_response::Payload::Rows(rows)) = response.payload.as_ref() else {
            return None;
        };
        Some(
            rows.rows
                .iter()
                .map(|row| {
                    (
                        row.columns.get("order_id").cloned().unwrap_or_default(),
                        row.columns.get("order_date").cloned().unwrap_or_default(),
                    )
                })
                .collect(),
        )
    }

    fn row_pairs(rows: &[BTreeMap<String, String>]) -> Vec<(String, String)> {
        rows.iter()
            .map(|row| {
                (
                    row.get("order_id").cloned().unwrap_or_default(),
                    row.get("order_date").cloned().unwrap_or_default(),
                )
            })
            .collect()
    }

    fn assert_complete_rows(rows: Vec<BTreeMap<String, String>>) {
        assert_eq!(
            row_pairs(&rows),
            vec![
                ("aaa".to_string(), "first".to_string()),
                ("abc123".to_string(), "new".to_string()),
                ("memover".to_string(), "memtable".to_string()),
                ("zzz".to_string(), "last".to_string()),
            ],
            "successful scans must preserve key ordering, newest values, tombstones, and memtable overlay"
        );
    }

    fn free_addresses(count: usize) -> Vec<SocketAddr> {
        let mut addresses = Vec::with_capacity(count);
        while addresses.len() < count {
            let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
            let address = listener.local_addr().unwrap();
            drop(listener);
            if !addresses.contains(&address) {
                addresses.push(address);
            }
        }
        addresses
    }

    struct RunningServer {
        uri: String,
        shutdown: Option<oneshot::Sender<()>>,
    }

    impl Drop for RunningServer {
        fn drop(&mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
        }
    }

    async fn connect(uri: &str) -> CassClient<Channel> {
        let uri = uri.to_string();
        timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(client) = CassClient::connect(uri.clone()).await {
                    return client;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("gRPC server did not become ready")
    }

    async fn start_server(address: SocketAddr, cluster: Arc<Cluster>) -> RunningServer {
        let (shutdown, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            Server::builder()
                .add_service(CassServer::new(CassService { cluster }))
                .serve_with_shutdown(address, async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        let server = RunningServer {
            uri: format!("http://{address}"),
            shutdown: Some(shutdown),
        };
        drop(connect(&server.uri).await);
        server
    }

    struct Replica {
        _dir: TempDir,
        storage: Arc<FailingStorage>,
        cluster: Arc<Cluster>,
        address: SocketAddr,
        uri: String,
        server: Option<RunningServer>,
    }

    async fn start_replicas(read_consistency: usize) -> Vec<Replica> {
        let addresses = free_addresses(3);
        let uris: Vec<String> = addresses
            .iter()
            .map(|address| format!("http://{address}"))
            .collect();
        let mut replicas = Vec::with_capacity(addresses.len());
        for (index, address) in addresses.iter().copied().enumerate() {
            let dir = tempfile::tempdir().unwrap();
            let storage = Arc::new(FailingStorage::new(dir.path()));
            let seeded = seed_storage(storage.clone()).await;
            add_memtable_overlay(&seeded).await;
            let db = Arc::new(Database::new(storage.clone(), "wal.log").await.unwrap());
            let peers = uris
                .iter()
                .enumerate()
                .filter_map(|(peer_index, uri)| (peer_index != index).then_some(uri.clone()))
                .collect();
            let cluster = Arc::new(Cluster::new(
                db.clone(),
                uris[index].clone(),
                peers,
                1,
                3,
                read_consistency,
            ));
            replicas.push(Replica {
                _dir: dir,
                storage,
                cluster,
                address,
                uri: uris[index].clone(),
                server: None,
            });
        }

        for index in 0..replicas.len() {
            let address = replicas[index].address;
            let cluster = replicas[index].cluster.clone();
            replicas[index].server = Some(start_server(address, cluster).await);
        }

        timeout(Duration::from_secs(5), async {
            loop {
                let mut all_healthy = true;
                for replica in &replicas {
                    for uri in &uris {
                        if !replica.cluster.is_alive(uri).await {
                            all_healthy = false;
                        }
                    }
                }
                if all_healthy {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("replica cluster did not become healthy");

        replicas
    }

    fn request() -> QueryRequest {
        QueryRequest {
            sql: SELECT_ORDERS.to_string(),
            ts: 0,
        }
    }

    #[tokio::test]
    async fn one_replica_query_does_not_return_partial_rows_after_sstable_read_failure() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(FailingStorage::new(dir.path()));
        let healthy_db = seed_storage(storage.clone()).await;
        add_memtable_overlay(&healthy_db).await;

        let healthy_address = free_addresses(1)[0];
        let healthy_cluster = Arc::new(Cluster::new(
            healthy_db,
            format!("http://{healthy_address}"),
            Vec::new(),
            1,
            1,
            1,
        ));
        let healthy_server = start_server(healthy_address, healthy_cluster).await;
        let mut healthy_client = connect(&healthy_server.uri).await;
        let response = healthy_client.query(request()).await.unwrap().into_inner();
        assert_complete_rows(result_rows(match response.payload {
            Some(query_response::Payload::Rows(rows)) => QueryOutput::Rows(
                rows.rows
                    .into_iter()
                    .map(|row| row.columns.into_iter().collect())
                    .collect(),
            ),
            _ => panic!("expected rows from a complete scan"),
        }));
        let count = healthy_client
            .query(QueryRequest {
                sql: COUNT_ORDERS.to_string(),
                ts: 0,
            })
            .await
            .unwrap()
            .into_inner();
        let Some(query_response::Payload::Rows(rows)) = count.payload else {
            panic!("expected COUNT result");
        };
        assert_eq!(rows.rows[0].columns.get("count"), Some(&"4".to_string()));
        drop(healthy_server);

        // Reopen after the healthy query so this Database instance has no cached
        // schema; its schema must still be loaded from the older SSTable.
        let db = Arc::new(Database::new(storage.clone(), "wal.log").await.unwrap());
        let address = free_addresses(1)[0];
        let cluster = Arc::new(Cluster::new(
            db.clone(),
            format!("http://{address}"),
            Vec::new(),
            1,
            1,
            1,
        ));
        let server = start_server(address, cluster).await;
        let mut client = connect(&server.uri).await;
        storage.fail_reads();

        let response = client.query(request()).await;
        assert!(
            storage.failed_read_count() > 0,
            "the injected SSTable read error was not reached"
        );
        let observed = response
            .as_ref()
            .ok()
            .and_then(|response| response_rows(response.get_ref()));
        assert!(
            response.is_err(),
            "partial-key SELECT returned a successful response after an injected SSTable read error: {observed:?}"
        );
        let count = client
            .query(QueryRequest {
                sql: COUNT_ORDERS.to_string(),
                ts: 0,
            })
            .await;
        assert!(
            count.is_err(),
            "COUNT returned after an injected SSTable read error"
        );

        let internal_error = client.internal(request()).await.unwrap_err();
        assert!(
            internal_error.message().contains(INJECTED_READ_ERROR),
            "replica did not return the source-owned storage error: {}",
            internal_error.message()
        );
    }

    #[tokio::test]
    async fn quorum_requires_complete_scans_and_accepts_two_intact_replicas() {
        let replicas = start_replicas(2).await;
        replicas[0].storage.fail_reads();
        replicas[1].storage.fail_reads();

        let mut internal_results = Vec::new();
        for index in 0..2 {
            let mut client = connect(&replicas[index].uri).await;
            internal_results.push(client.internal(request()).await);
            assert!(
                replicas[index].storage.failed_read_count() > 0,
                "replica {index} did not exercise the injected SSTable error"
            );
        }

        let mut coordinator = connect(&replicas[2].uri).await;
        let result = coordinator.query(request()).await;
        assert!(
            result.is_err(),
            "QUORUM counted failed partition scans as acknowledgements when only one replica completed"
        );

        for result in internal_results {
            let error = result.expect_err("faulted replica returned a partial successful scan");
            assert!(
                error.message().contains(INJECTED_READ_ERROR),
                "replica did not preserve the source storage error: {}",
                error.message()
            );
        }

        replicas[1].storage.allow_reads();
        let node_one_before = replicas[1].storage.successful_data_read_count();
        let node_two_before = replicas[2].storage.successful_data_read_count();
        let response = coordinator.query(request()).await.unwrap().into_inner();
        assert_complete_rows(result_rows(match response.payload {
            Some(query_response::Payload::Rows(rows)) => QueryOutput::Rows(
                rows.rows
                    .into_iter()
                    .map(|row| row.columns.into_iter().collect())
                    .collect(),
            ),
            _ => panic!("expected merged rows when two replicas complete the scan"),
        }));
        assert!(
            replicas[1].storage.successful_data_read_count() > node_one_before,
            "the first intact replica did not complete a data SSTable read"
        );
        assert!(
            replicas[2].storage.successful_data_read_count() > node_two_before,
            "the second intact replica did not complete a data SSTable read"
        );

        let mut failed_replica = connect(&replicas[0].uri).await;
        let error = failed_replica.internal(request()).await.unwrap_err();
        assert!(
            error.message().contains(INJECTED_READ_ERROR),
            "the remaining failed replica did not expose its storage error"
        );
    }
}
