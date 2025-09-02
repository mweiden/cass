use std::time::{Duration, Instant};
use tokio::time::sleep;

use cass::rpc::{HealthRequest, QueryRequest, cass_client::CassClient};
use serde_json::Value;

mod common;
use common::CassProcess;

/// The health endpoint reports the node's tokens.
#[tokio::test]
async fn health_endpoint_reports_tokens() {
    let base = "http://127.0.0.1:18085";
    let dir = tempfile::tempdir().unwrap();
    let _child = CassProcess::spawn([
        "server",
        "--data-dir",
        dir.path().to_str().unwrap(),
        "--node-addr",
        base,
        "--rf",
        "1",
        "--vnodes",
        "4",
    ]);

    for _ in 0..20 {
        if CassClient::connect(base.to_string()).await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
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
}

/// Queries fail when the required number of healthy replicas is not met.
#[tokio::test]
async fn errors_when_not_enough_healthy_replicas() {
    let base1 = "http://127.0.0.1:18091";
    let base2 = "http://127.0.0.1:18092";
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let mut child1 = CassProcess::spawn([
        "server",
        "--data-dir",
        dir1.path().to_str().unwrap(),
        "--node-addr",
        base1,
        "--peer",
        base2,
        "--rf",
        "2",
    ]);
    let mut child2 = CassProcess::spawn([
        "server",
        "--data-dir",
        dir2.path().to_str().unwrap(),
        "--node-addr",
        base2,
        "--peer",
        base1,
        "--rf",
        "2",
    ]);

    for _ in 0..20 {
        let ok1 = CassClient::connect(base1.to_string()).await.is_ok();
        let ok2 = CassClient::connect(base2.to_string()).await.is_ok();
        if ok1 && ok2 {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let mut c1 = CassClient::connect(base1.to_string()).await.unwrap();
    c1.query(QueryRequest {
        sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
    })
    .await
    .unwrap();
    c1.query(QueryRequest {
        sql: "INSERT INTO kv (id, val) VALUES ('x','1')".into(),
    })
    .await
    .unwrap();

    child2.kill();

    let start = Instant::now();
    loop {
        let res = c1
            .query(QueryRequest {
                sql: "SELECT val FROM kv WHERE id = 'x'".into(),
            })
            .await;
        if let Err(e) = res {
            assert!(e.message().contains("not enough healthy replicas"));
            break;
        }
        if start.elapsed() > Duration::from_secs(2) {
            panic!("replica did not report unhealthy in time");
        }
        sleep(Duration::from_millis(50)).await;
    }

    child1.kill();
}

/// Lowering the read consistency allows queries to succeed with fewer healthy replicas.
#[tokio::test]
async fn read_succeeds_with_lower_consistency() {
    let base1 = "http://127.0.0.1:18101";
    let base2 = "http://127.0.0.1:18102";
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let mut child1 = CassProcess::spawn([
        "server",
        "--data-dir",
        dir1.path().to_str().unwrap(),
        "--node-addr",
        base1,
        "--peer",
        base2,
        "--rf",
        "2",
        "--read-consistency",
        "1",
    ]);
    let mut child2 = CassProcess::spawn([
        "server",
        "--data-dir",
        dir2.path().to_str().unwrap(),
        "--node-addr",
        base2,
        "--peer",
        base1,
        "--rf",
        "2",
        "--read-consistency",
        "1",
    ]);

    for _ in 0..20 {
        let ok1 = CassClient::connect(base1.to_string()).await.is_ok();
        let ok2 = CassClient::connect(base2.to_string()).await.is_ok();
        if ok1 && ok2 {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let mut c1 = CassClient::connect(base1.to_string()).await.unwrap();
    c1.query(QueryRequest {
        sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
    })
    .await
    .unwrap();
    c1.query(QueryRequest {
        sql: "INSERT INTO kv (id, val) VALUES ('a','1')".into(),
    })
    .await
    .unwrap();

    child2.kill();

    let start = Instant::now();
    loop {
        let res = c1
            .query(QueryRequest {
                sql: "SELECT val FROM kv WHERE id = 'a'".into(),
            })
            .await;
        if let Ok(resp) = res {
            if let Some(cass::rpc::query_response::Payload::Rows(rs)) = resp.into_inner().payload {
                assert_eq!(rs.rows[0].columns.get("val"), Some(&"1".to_string()));
                break;
            }
        }
        if start.elapsed() > Duration::from_secs(2) {
            panic!("read did not succeed in time");
        }
        sleep(Duration::from_millis(50)).await;
    }

    child1.kill();
}
