use cass::rpc::{QueryRequest, cass_client::CassClient, query_response};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tokio::time::sleep;

mod common;
use common::CassProcess;

#[tokio::test]
async fn grpc_query_roundtrip() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let base = "http://127.0.0.1:8080";
    let _child = CassProcess::spawn([
        "server",
        "--node-addr",
        base,
        "--data-dir",
        tmp_dir.path().to_str().unwrap(),
    ]);

    for _ in 0..10 {
        if CassClient::connect(base).await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    let mut client = CassClient::connect(base).await.unwrap();
    client
        .query(QueryRequest {
            sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
        })
        .await
        .unwrap();

    client
        .query(QueryRequest {
            sql: "INSERT INTO kv (id, val) VALUES ('foo','bar')".into(),
        })
        .await
        .unwrap();

    let res = client
        .query(QueryRequest {
            sql: "SELECT val FROM kv WHERE id = 'foo'".into(),
        })
        .await
        .unwrap()
        .into_inner();
    match res.payload {
        Some(query_response::Payload::Rows(rs)) => {
            assert_eq!(rs.rows.len(), 1);
            assert_eq!(rs.rows[0].columns.get("val"), Some(&"bar".to_string()));
        }
        _ => panic!("unexpected response"),
    }

    let count = client
        .query(QueryRequest {
            sql: "SELECT COUNT(*) FROM kv WHERE id = 'foo'".into(),
        })
        .await
        .unwrap()
        .into_inner();
    match count.payload {
        Some(query_response::Payload::Rows(rs)) => {
            assert_eq!(rs.rows.len(), 1);
            assert_eq!(rs.rows[0].columns.get("count"), Some(&"1".to_string()));
        }
        _ => panic!("unexpected count response"),
    }
}

#[tokio::test]
async fn repl_interaction_executes_queries() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let base = "http://127.0.0.1:8090";
    let _server = CassProcess::spawn([
        "server",
        "--node-addr",
        base,
        "--data-dir",
        tmp_dir.path().to_str().unwrap(),
    ]);

    for _ in 0..10 {
        if CassClient::connect(base).await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    let mut repl = Command::new(env!("CARGO_BIN_EXE_cass"))
        .arg("repl")
        .arg(base)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to spawn repl");

    let mut stdin = repl.stdin.take().unwrap();
    let mut stdout = BufReader::new(repl.stdout.take().unwrap());

    writeln!(
        stdin,
        "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))"
    )
    .unwrap();
    writeln!(stdin, "INSERT INTO kv (id, val) VALUES ('foo','bar')").unwrap();
    writeln!(stdin, "SELECT val FROM kv WHERE id='foo'").unwrap();

    let start = Instant::now();
    let mut line = String::new();
    let mut found = false;
    while start.elapsed() < Duration::from_secs(2) {
        line.clear();
        if stdout.read_line(&mut line).unwrap() == 0 {
            break;
        }
        if line.contains("bar") {
            found = true;
            break;
        }
    }
    assert!(found);

    let _ = repl.kill();
}
