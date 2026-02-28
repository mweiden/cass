use std::time::{Duration, Instant};
use tokio::time::sleep;

use cass::rpc::{HealthRequest, QueryRequest, cass_client::CassClient};
use serde_json::Value;

mod common;
use common::{CassProcess, free_http_addr};

/// The health endpoint reports the node's tokens.
#[tokio::test]
async fn health_endpoint_reports_tokens() {
    let base = free_http_addr();
    let dir = tempfile::tempdir().unwrap();
    let _child = CassProcess::spawn([
        "server",
        "--data-dir",
        dir.path().to_str().unwrap(),
        "--node-addr",
        &base,
        "--rf",
        "1",
        "--vnodes",
        "4",
    ]);

    for _ in 0..20 {
        if CassClient::connect(base.clone()).await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let mut client = CassClient::connect(base.clone()).await.unwrap();
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
    let base1 = free_http_addr();
    let base2 = free_http_addr();
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let mut child1 = CassProcess::spawn([
        "server",
        "--data-dir",
        dir1.path().to_str().unwrap(),
        "--node-addr",
        &base1,
        "--peer",
        &base2,
        "--rf",
        "2",
    ]);
    let mut child2 = CassProcess::spawn([
        "server",
        "--data-dir",
        dir2.path().to_str().unwrap(),
        "--node-addr",
        &base2,
        "--peer",
        &base1,
        "--rf",
        "2",
    ]);

    for _ in 0..20 {
        let ok1 = CassClient::connect(base1.clone()).await.is_ok();
        let ok2 = CassClient::connect(base2.clone()).await.is_ok();
        if ok1 && ok2 {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let mut c1 = CassClient::connect(base1.clone()).await.unwrap();
    c1.query(QueryRequest {
        sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
        ts: 0,
    })
    .await
    .unwrap();
    c1.query(QueryRequest {
        sql: "INSERT INTO kv (id, val) VALUES ('x','1')".into(),
        ts: 0,
    })
    .await
    .unwrap();

    child2.kill();

    let start = Instant::now();
    loop {
        let res = c1
            .query(QueryRequest {
                sql: "SELECT val FROM kv WHERE id = 'x'".into(),
                ts: 0,
            })
            .await;
        if let Err(e) = res {
            let msg = e.message();
            assert!(
                msg.contains("not enough healthy replicas")
                    || msg.contains("not enough replicas acknowledged"),
                "unexpected error message: {}",
                msg
            );
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
    let base1 = free_http_addr();
    let base2 = free_http_addr();
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let mut child1 = CassProcess::spawn([
        "server",
        "--data-dir",
        dir1.path().to_str().unwrap(),
        "--node-addr",
        &base1,
        "--peer",
        &base2,
        "--rf",
        "2",
        "--read-consistency",
        "one",
    ]);
    let mut child2 = CassProcess::spawn([
        "server",
        "--data-dir",
        dir2.path().to_str().unwrap(),
        "--node-addr",
        &base2,
        "--peer",
        &base1,
        "--rf",
        "2",
        "--read-consistency",
        "one",
    ]);

    for _ in 0..20 {
        let ok1 = CassClient::connect(base1.clone()).await.is_ok();
        let ok2 = CassClient::connect(base2.clone()).await.is_ok();
        if ok1 && ok2 {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let mut c1 = CassClient::connect(base1.clone()).await.unwrap();
    c1.query(QueryRequest {
        sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
        ts: 0,
    })
    .await
    .unwrap();
    c1.query(QueryRequest {
        sql: "INSERT INTO kv (id, val) VALUES ('a','1')".into(),
        ts: 0,
    })
    .await
    .unwrap();

    child2.kill();

    let start = Instant::now();
    loop {
        let res = c1
            .query(QueryRequest {
                sql: "SELECT val FROM kv WHERE id = 'a'".into(),
                ts: 0,
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
