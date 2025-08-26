use std::{
    convert::Infallible,
    fs,
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
};

use cass::{
    Database,
    cluster::Cluster,
    rpc::{
        FlushRequest, FlushResponse, HealthRequest, HealthResponse, PanicRequest, PanicResponse,
        QueryRequest, QueryResponse, Row as RpcRow,
        cass_client::CassClient,
        cass_server::{Cass, CassServer},
        query_response,
    },
    storage::{Storage, local::LocalStorage, s3::S3Storage},
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
use tonic::{Request, Response, Status, transport::Server};
use tonic_prometheus_layer::{MetricsLayer, metrics as tl_metrics};
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::{Level, info};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
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

fn print_rows(rows: &[RpcRow]) {
    if rows.is_empty() {
        println!("(0 rows)");
        return;
    }
    let mut cols: Vec<String> = rows
        .iter()
        .flat_map(|r| r.columns.keys().cloned())
        .collect();
    cols.sort();
    cols.dedup();
    println!("{}", cols.join(" | "));
    for row in rows {
        let mut vals: Vec<String> = Vec::new();
        for c in &cols {
            vals.push(row.columns.get(c).cloned().unwrap_or_default());
        }
        println!("{}", vals.join(" | "));
    }
    println!("({} rows)", rows.len());
}

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
}

#[derive(Copy, Clone, ValueEnum)]
enum StorageKind {
    Local,
    S3,
}

#[derive(Clone)]
struct CassService {
    cluster: Arc<Cluster>,
}

#[tonic::async_trait]
impl Cass for CassService {
    async fn query(&self, req: Request<QueryRequest>) -> Result<Response<QueryResponse>, Status> {
        let sql = req.into_inner().sql;
        match self.cluster.execute(&sql, false).await {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(Status::invalid_argument(e.to_string())),
        }
    }

    async fn internal(
        &self,
        req: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let sql = req.into_inner().sql;
        match self.cluster.execute(&sql, true).await {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(Status::invalid_argument(e.to_string())),
        }
    }

    async fn flush(&self, _req: Request<FlushRequest>) -> Result<Response<FlushResponse>, Status> {
        self.cluster.flush_all().await.map_err(Status::internal)?;
        Ok(Response::new(FlushResponse {}))
    }

    async fn flush_internal(
        &self,
        _req: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        self.cluster
            .flush_self()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(FlushResponse {}))
    }

    async fn panic(&self, _req: Request<PanicRequest>) -> Result<Response<PanicResponse>, Status> {
        self.cluster
            .panic_for(std::time::Duration::from_secs(60))
            .await;
        let healthy = self.cluster.self_healthy().await;
        Ok(Response::new(PanicResponse { healthy }))
    }

    async fn health(
        &self,
        _req: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        if !self.cluster.self_healthy().await {
            return Err(Status::unavailable("unhealthy"));
        }
        Ok(Response::new(HealthResponse {
            info: self.cluster.health_info().to_string(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let cli = Cli::parse();
    match cli.command {
        Command::Server(args) => run_server(args).await?,
        Command::Flush { target } => {
            let mut client = CassClient::connect(target).await?;
            client.flush(FlushRequest {}).await?;
        }
        Command::Panic { target } => {
            let mut client = CassClient::connect(target).await?;
            let resp = client.panic(PanicRequest {}).await?;
            println!("healthy: {}", resp.into_inner().healthy);
        }
        Command::Repl { nodes } => repl(nodes).await?,
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
    let db = Arc::new(Database::new(storage, "wal.log").await?);
    let cluster = Arc::new(Cluster::new(
        db.clone(),
        args.node_addr.clone(),
        args.peer.clone(),
        args.vnodes,
        args.rf,
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
    tokio::spawn(async move {
        let make_svc = make_service_fn(move |_| {
            let cluster = cluster_metrics.clone();
            let data_dir = data_dir_metrics.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |_req: HttpRequest<HttpBody>| {
                    let cluster = cluster.clone();
                    let data_dir = data_dir.clone();
                    async move {
                        for (peer, alive) in cluster.peer_health().await {
                            NODE_HEALTH
                                .with_label_values(&[peer.as_str()])
                                .set(if alive { 1.0 } else { 0.0 });
                        }
                        let self_addr = cluster.self_addr().to_string();
                        let self_alive = cluster.self_healthy().await;
                        NODE_HEALTH
                            .with_label_values(&[self_addr.as_str()])
                            .set(if self_alive { 1.0 } else { 0.0 });
                        let mut sys = System::new_all();
                        sys.refresh_memory();
                        sys.refresh_cpu();
                        let ram = sys.used_memory() as f64 * 1024.0;
                        let cpu = sys.global_cpu_info().cpu_usage() as f64;
                        RAM_USAGE.set(ram);
                        CPU_USAGE.set(cpu);
                        let disk = sstable_disk_usage(&data_dir) as f64;
                        SSTABLE_DISK_USAGE.set(disk);

                        let body = tl_metrics::encode_to_string().unwrap_or_default();
                        let response = HttpResponse::builder()
                            .header(
                                CONTENT_TYPE,
                                HeaderValue::from_static("text/plain; version=0.0.4"),
                            )
                            .body(HttpBody::from(body))
                            .unwrap();
                        Ok::<_, Infallible>(response)
                    }
                }))
            }
        });

        if let Err(e) = HyperServer::bind(&metrics_addr).serve(make_svc).await {
            eprintln!("metrics server error: {e}");
        }
    });

    info!("Cass gRPC server listening on {addr}");
    let trace_layer = TraceLayer::new_for_grpc()
        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
        .on_request(DefaultOnRequest::new().level(Level::INFO))
        .on_response(DefaultOnResponse::new().level(Level::INFO));
    Server::builder()
        .layer(metrics_layer)
        .layer(trace_layer)
        .add_service(CassServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}

fn sstable_disk_usage(dir: &str) -> u64 {
    fn visit(path: &Path) -> u64 {
        let mut size = 0;
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    size += visit(&p);
                } else if p.extension().and_then(|e| e.to_str()) == Some("sst") {
                    if let Ok(meta) = entry.metadata() {
                        size += meta.len();
                    }
                }
            }
        }
        size
    }
    visit(Path::new(dir))
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
            match CassClient::connect(node.clone()).await {
                Ok(mut client) => match client
                    .query(QueryRequest {
                        sql: sql.to_string(),
                    })
                    .await
                {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        match resp.payload {
                            Some(query_response::Payload::Rows(rs)) => print_rows(&rs.rows),
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
