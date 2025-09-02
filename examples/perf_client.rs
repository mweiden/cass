use std::time::Instant;

use cass::rpc::{QueryRequest, cass_client::CassClient};
use clap::Parser;
use tokio::fs;

#[derive(Parser)]
struct Args {
    /// gRPC endpoint of the cass node
    #[clap(long, default_value = "http://127.0.0.1:8080")]
    node: String,
    /// Number of write/read operations to issue
    #[clap(long, default_value_t = 1000)]
    ops: usize,
    /// Metrics endpoint for the node
    #[clap(long, default_value = "http://127.0.0.1:9090/metrics")]
    metrics_endpoint: String,
    /// Optional path to write Prometheus metrics after the run
    #[clap(long)]
    metrics_out: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut client = CassClient::connect(args.node.clone()).await?;
    client
        .query(QueryRequest {
            sql: "CREATE TABLE IF NOT EXISTS perf (k INT PRIMARY KEY, v INT)".into(),
        })
        .await?;

    let start = Instant::now();
    for i in 0..args.ops {
        let sql = format!("INSERT INTO perf VALUES ({}, {})", i, i);
        client.query(QueryRequest { sql }).await?;
    }
    let write_dur = start.elapsed();

    let start = Instant::now();
    for i in 0..args.ops {
        let sql = format!("SELECT * FROM perf WHERE k = {}", i);
        client.query(QueryRequest { sql }).await?;
    }
    let read_dur = start.elapsed();

    println!(
        "cass writes: ops={} ms={} ops/sec={:.2}",
        args.ops,
        write_dur.as_millis(),
        (args.ops as f64) / write_dur.as_secs_f64()
    );
    println!(
        "cass reads: ops={} ms={} ops/sec={:.2}",
        args.ops,
        read_dur.as_millis(),
        (args.ops as f64) / read_dur.as_secs_f64()
    );

    if let Some(out) = args.metrics_out {
        let body = reqwest::get(&args.metrics_endpoint).await?.text().await?;
        fs::write(out, body).await?;
    }

    Ok(())
}
