use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use cass::{
    rpc::{QueryRequest, cass_client::CassClient},
    telemetry::{self, PropagatingInterceptor},
};
use clap::Parser;
use futures::{
    future::try_join_all,
    stream::{FuturesUnordered, StreamExt},
};
use statrs::statistics::Statistics;
use tokio::{fs, time::timeout};
use tonic::{Status, codegen::InterceptedService, transport::Channel};
use url::Url;

#[derive(Parser)]
struct Args {
    /// gRPC endpoint of the cass node
    #[clap(long, default_value = "http://127.0.0.1:8080")]
    node: String,
    /// Number of write/read operations to issue
    #[clap(long, default_value_t = 1000)]
    ops: usize,
    /// Number of concurrent client threads/tasks to use
    #[clap(long, default_value_t = 1)]
    threads: usize,
    /// Max in-flight RPCs per thread/task
    #[clap(long, default_value_t = 1)]
    inflight: usize,
    /// Timeout in milliseconds for each RPC (0 disables the deadline)
    #[clap(long, default_value_t = 5000)]
    timeout_ms: u64,
    /// Optional path to write the node's Prometheus metrics after the run
    #[clap(long)]
    metrics_out: Option<String>,
}

const MAX_LOGGED_FAILURES: usize = 5;

#[derive(Clone)]
enum PerfClient {
    Traced(CassClient<InterceptedService<Channel, PropagatingInterceptor>>),
    Plain(CassClient<Channel>),
}

impl PerfClient {
    async fn connect(node: String) -> Result<Self, tonic::transport::Error> {
        if telemetry::tracing_disabled() {
            CassClient::connect(node).await.map(PerfClient::Plain)
        } else {
            CassClient::connect_traced(node)
                .await
                .map(PerfClient::Traced)
        }
    }

    async fn query(
        &mut self,
        request: QueryRequest,
    ) -> Result<tonic::Response<cass::rpc::QueryResponse>, Status> {
        match self {
            PerfClient::Traced(client) => client.query(request).await,
            PerfClient::Plain(client) => client.query(request).await,
        }
    }
}

#[derive(Default)]
struct PhaseStats {
    errors: usize,
    timeouts: usize,
    messages: Vec<String>,
}

impl PhaseStats {
    fn total_failures(&self) -> usize {
        self.errors + self.timeouts
    }

    fn has_failures(&self) -> bool {
        self.total_failures() > 0
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    // Ensure table exists via a single client
    {
        let mut client = PerfClient::connect(args.node.clone()).await?;
        client
            .query(QueryRequest {
                sql: "CREATE TABLE IF NOT EXISTS perf (k INT PRIMARY KEY, v INT)".into(),
            })
            .await?;
    }

    let rpc_timeout = if args.timeout_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(args.timeout_ms))
    };

    // Writes with concurrency
    let (write_lat_ms, write_dur, write_stats) = run_phase(
        args.node.clone(),
        args.ops,
        args.threads,
        args.inflight,
        true,
        rpc_timeout,
    )
    .await?;
    // Reads with concurrency
    let (read_lat_ms, read_dur, read_stats) = run_phase(
        args.node.clone(),
        args.ops,
        args.threads,
        args.inflight,
        false,
        rpc_timeout,
    )
    .await?;

    // Report in a style similar to cassandra-stress
    report("WRITE", args.ops, write_dur.as_secs_f64(), &write_lat_ms);
    report("READ", args.ops, read_dur.as_secs_f64(), &read_lat_ms);
    log_phase_errors("WRITE", &write_stats);
    log_phase_errors("READ", &read_stats);

    if let Some(out) = args.metrics_out {
        // Derive metrics URL from the node address: gRPC port + 1000.
        let metrics_url = match Url::parse(&args.node) {
            Ok(mut url) => {
                let port = url.port().unwrap_or(80);
                url.set_port(Some(port + 1000)).ok();
                url.set_path("/metrics");
                url.to_string()
            }
            Err(_) => {
                // Fallback: assume http on localhost if parse fails
                "http://127.0.0.1:9080/metrics".to_string()
            }
        };
        let body = reqwest::get(&metrics_url).await?.text().await?;
        fs::write(out, body).await?;
    }

    Ok(())
}

async fn run_phase(
    node: String,
    ops: usize,
    threads: usize,
    inflight: usize,
    write: bool,
    rpc_timeout: Option<Duration>,
) -> Result<(Vec<f64>, Duration, PhaseStats), Box<dyn std::error::Error>> {
    if ops == 0 {
        return Ok((Vec::new(), Duration::from_secs(0), PhaseStats::default()));
    }

    let workers = threads.max(1).min(ops.max(1));
    let start = Instant::now();
    let base = ops / workers;
    let rem = ops % workers;

    let clients: Vec<PerfClient> =
        try_join_all((0..workers).map(|_| PerfClient::connect(node.clone()))).await?;

    let mut handles = Vec::with_capacity(workers);
    let concurrency = inflight.max(1);
    for (idx, client) in clients.into_iter().enumerate() {
        let count = if idx < rem { base + 1 } else { base };
        let offset = idx * base + idx.min(rem);
        let rpc_timeout = rpc_timeout;
        handles.push(tokio::spawn(async move {
            run_worker(client, offset, count, write, concurrency, rpc_timeout).await
        }));
    }

    let mut latencies = Vec::with_capacity(ops);
    let mut stats = PhaseStats::default();
    for handle in handles {
        let worker = handle
            .await
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
        latencies.extend(worker.latencies);
        stats.errors += worker.errors;
        stats.timeouts += worker.timeouts;
        if stats.messages.len() < MAX_LOGGED_FAILURES {
            let remaining = MAX_LOGGED_FAILURES - stats.messages.len();
            stats
                .messages
                .extend(worker.messages.into_iter().take(remaining));
        }
    }

    Ok((latencies, start.elapsed(), stats))
}

struct WorkerStats {
    latencies: Vec<f64>,
    errors: usize,
    timeouts: usize,
    messages: Vec<String>,
}

enum RequestOutcome {
    Ok,
    Error { key: usize, detail: String },
    Timeout { key: usize },
}

async fn run_worker(
    client: PerfClient,
    offset: usize,
    count: usize,
    write: bool,
    inflight: usize,
    rpc_timeout: Option<Duration>,
) -> WorkerStats {
    if count == 0 {
        return WorkerStats {
            latencies: Vec::new(),
            errors: 0,
            timeouts: 0,
            messages: Vec::new(),
        };
    }

    let concurrency = inflight.max(1);
    let mut latencies = Vec::with_capacity(count);
    let mut errors = 0;
    let mut timeouts = 0;
    let mut messages = Vec::new();

    let mut pool: VecDeque<PerfClient> = VecDeque::with_capacity(concurrency);
    pool.push_back(client);
    if concurrency > 1 {
        let template = pool.front().expect("client pool").clone();
        for _ in 1..concurrency {
            pool.push_back(template.clone());
        }
    }

    let mut issued = 0usize;
    let mut inflight_futures = FuturesUnordered::new();

    while issued < count || !inflight_futures.is_empty() {
        while issued < count && !pool.is_empty() {
            let client = pool.pop_front().expect("available client");
            let key = offset + issued;
            let sql = if write {
                format!("INSERT INTO perf VALUES ({}, {})", key, key)
            } else {
                format!("SELECT * FROM perf WHERE k = {}", key)
            };
            let deadline = rpc_timeout;
            inflight_futures.push(async move {
                let mut client = client;
                let op_start = Instant::now();
                let outcome = if let Some(limit) = deadline {
                    match timeout(limit, client.query(QueryRequest { sql })).await {
                        Ok(Ok(_)) => RequestOutcome::Ok,
                        Ok(Err(status)) => RequestOutcome::Error {
                            key,
                            detail: format!("{} {}", status.code(), status.message()),
                        },
                        Err(_) => RequestOutcome::Timeout { key },
                    }
                } else {
                    match client.query(QueryRequest { sql }).await {
                        Ok(_) => RequestOutcome::Ok,
                        Err(status) => RequestOutcome::Error {
                            key,
                            detail: format!("{} {}", status.code(), status.message()),
                        },
                    }
                };
                let elapsed = op_start.elapsed().as_secs_f64() * 1000.0;
                (client, elapsed, outcome)
            });
            issued += 1;
        }

        if let Some((client, latency, outcome)) = inflight_futures.next().await {
            latencies.push(latency);
            match outcome {
                RequestOutcome::Ok => {}
                RequestOutcome::Error { key, detail } => {
                    errors += 1;
                    if messages.len() < MAX_LOGGED_FAILURES {
                        messages.push(format!(
                            "{} key {} failed: {}",
                            if write { "WRITE" } else { "READ" },
                            key,
                            detail
                        ));
                    }
                }
                RequestOutcome::Timeout { key } => {
                    timeouts += 1;
                    if messages.len() < MAX_LOGGED_FAILURES {
                        if let Some(limit) = rpc_timeout {
                            messages.push(format!(
                                "{} key {} timed out after {} ms",
                                if write { "WRITE" } else { "READ" },
                                key,
                                limit.as_millis()
                            ));
                        } else {
                            messages.push(format!(
                                "{} key {} timed out",
                                if write { "WRITE" } else { "READ" },
                                key
                            ));
                        }
                    }
                }
            }
            pool.push_back(client);
        }
    }

    WorkerStats {
        latencies,
        errors,
        timeouts,
        messages,
    }
}

fn log_phase_errors(kind: &str, stats: &PhaseStats) {
    if !stats.has_failures() {
        return;
    }

    eprintln!(
        "[{}] {} RPCs failed ({} timeouts)",
        kind,
        stats.total_failures(),
        stats.timeouts
    );
    for msg in &stats.messages {
        eprintln!("    {}", msg);
    }
}

fn report(kind: &str, ops: usize, total_secs: f64, lat_ms: &[f64]) {
    let mut v = lat_ms.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let rate = (ops as f64) / total_secs;
    let mean = (&v[..]).mean();
    let median = percentile_sorted(&v, 50.0);
    let p95 = percentile_sorted(&v, 95.0);
    let p99 = percentile_sorted(&v, 99.0);
    let p999 = percentile_sorted(&v, 99.9);
    let max = *v.last().unwrap_or(&0.0);

    println!(
        "Op rate                   : {:>8} op/s  [{}]",
        fmt_f(rate),
        kind
    );
    println!(
        "Partition rate            : {:>8} pk/s  [{}]",
        fmt_f(rate),
        kind
    );
    println!(
        "Row rate                  : {:>8} row/s [{}]",
        fmt_f(rate),
        kind
    );
    println!(
        "Latency mean              : {:>7} ms [{}]",
        fmt_f(mean),
        kind
    );
    println!(
        "Latency median            : {:>7} ms [{}]",
        fmt_f(median),
        kind
    );
    println!(
        "Latency 95th percentile   : {:>7} ms [{}]",
        fmt_f(p95),
        kind
    );
    println!(
        "Latency 99th percentile   : {:>7} ms [{}]",
        fmt_f(p99),
        kind
    );
    println!(
        "Latency 99.9th percentile : {:>7} ms [{}]",
        fmt_f(p999),
        kind
    );
    println!(
        "Latency max               : {:>7} ms [{}]",
        fmt_f(max),
        kind
    );
}

fn percentile_sorted(v: &[f64], pct: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    let rank = ((pct / 100.0) * (v.len() as f64 - 1.0)).round() as usize;
    v[rank]
}

fn fmt_f(x: f64) -> String {
    if x >= 100.0 {
        format!("{:.0}", x)
    } else if x >= 10.0 {
        format!("{:.1}", x)
    } else {
        format!("{:.2}", x)
    }
}
