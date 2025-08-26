use std::{
    process::{Command, Stdio},
    thread,
    time::Duration,
};

use cass::rpc::{HealthRequest, QueryRequest, cass_client::CassClient};
use serde_json::Value;

#[tokio::test]
async fn health_endpoint_reports_tokens() {
    let base = "http://127.0.0.1:18085";
    let dir = tempfile::tempdir().unwrap();
    let bin = env!("CARGO_BIN_EXE_cass");
    let mut child = Command::new(bin)
        .args([
            "server",
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--node-addr",
            base,
            "--rf",
            "1",
            "--vnodes",
            "4",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    for _ in 0..20 {
        if CassClient::connect(base.to_string()).await.is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    let mut client = CassClient::connect(base.to_string()).await.unwrap();
    let body = client
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner()
        .info;
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["node"], base);
    assert_eq!(v["tokens"].as_array().unwrap().len(), 4);

    child.kill().unwrap();
}

#[tokio::test]
async fn errors_when_not_enough_healthy_replicas() {
    let base1 = "http://127.0.0.1:18091";
    let base2 = "http://127.0.0.1:18092";
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let bin = env!("CARGO_BIN_EXE_cass");

    let mut child1 = Command::new(bin)
        .args([
            "server",
            "--data-dir",
            dir1.path().to_str().unwrap(),
            "--node-addr",
            base1,
            "--peer",
            base2,
            "--rf",
            "2",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut child2 = Command::new(bin)
        .args([
            "server",
            "--data-dir",
            dir2.path().to_str().unwrap(),
            "--node-addr",
            base2,
            "--peer",
            base1,
            "--rf",
            "2",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    for _ in 0..20 {
        let ok1 = CassClient::connect(base1.to_string()).await.is_ok();
        let ok2 = CassClient::connect(base2.to_string()).await.is_ok();
        if ok1 && ok2 {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    let mut c1 = CassClient::connect(base1.to_string()).await.unwrap();
    c1.query(QueryRequest {
        sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
    })
    .await
    .unwrap();

    child2.kill().unwrap();

    thread::sleep(Duration::from_secs(1));

    let res = c1
        .query(QueryRequest {
            sql: "INSERT INTO kv (id, val) VALUES ('x','1')".into(),
        })
        .await;
    assert!(
        res.unwrap_err()
            .message()
            .contains("not enough healthy replicas")
    );

    child1.kill().unwrap();
}
