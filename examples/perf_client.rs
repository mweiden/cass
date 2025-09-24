use std::time::Instant;

use cass::rpc::{QueryRequest, cass_client::CassClient};
use clap::Parser;
use futures::stream::{self, StreamExt};
use statrs::statistics::Statistics;
use tokio::fs;
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
    /// Optional path to write the node's Prometheus metrics after the run
    #[clap(long)]
    metrics_out: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    // Ensure table exists via a single client
    {
        let mut client = CassClient::connect_traced(args.node.clone()).await?;
        client
            .query(QueryRequest {
                sql: "CREATE TABLE IF NOT EXISTS perf (k INT PRIMARY KEY, v INT)".into(),
            })
            .await?;
    }

    // Writes with concurrency
    let (write_lat_ms, write_dur) = run_phase(
        args.node.clone(),
        args.ops,
        args.threads,
        args.inflight,
        true,
    )
    .await?;
    // Reads with concurrency
    let (read_lat_ms, read_dur) = run_phase(
        args.node.clone(),
        args.ops,
        args.threads,
        args.inflight,
        false,
    )
    .await?;

    // Report in a style similar to cassandra-stress
    report("WRITE", args.ops, write_dur.as_secs_f64(), &write_lat_ms);
    report("READ", args.ops, read_dur.as_secs_f64(), &read_lat_ms);

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
) -> Result<(Vec<f64>, std::time::Duration), Box<dyn std::error::Error>> {
    let threads = threads.max(1);
    let start = Instant::now();

    // Partition ops across threads as evenly as possible
    let base = ops / threads;
    let rem = ops % threads;
    let mut handles = Vec::with_capacity(threads);
    // Establish a base client once; clones share the underlying HTTP/2 channel
    let base_client = std::sync::Arc::new(CassClient::connect_traced(node.clone()).await?);
    for t in 0..threads {
        let base_client = base_client.clone();
        let count = if t < rem { base + 1 } else { base };
        let offset = t * base + t.min(rem);
        handles.push(tokio::spawn(async move {
            let inflight = inflight.max(1);
            if count == 0 {
                return Vec::<f64>::new();
            }
            let futs = (0..count).map(|i| {
                // clone a client for this op; clones share the channel
                let mut client = (*base_client).clone();
                let k = offset + i;
                let sql = if write {
                    format!("INSERT INTO perf VALUES ({}, {})", k, k)
                } else {
                    format!("SELECT * FROM perf WHERE k = {}", k)
                };
                async move {
                    let op_start = Instant::now();
                    let _ = client.query(QueryRequest { sql }).await;
                    op_start.elapsed().as_secs_f64() * 1000.0
                }
            });
            let lats: Vec<f64> = stream::iter(futs)
                .buffer_unordered(inflight)
                .collect()
                .await;
            lats
        }));
    }
    let mut all = Vec::with_capacity(ops);
    for h in handles {
        if let Ok(v) = h.await {
            all.extend(v);
        }
    }
    let dur = start.elapsed();
    Ok((all, dur))
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
