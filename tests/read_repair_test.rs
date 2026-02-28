use std::time::Duration;
use tokio::time::sleep;

use cass::rpc::{QueryRequest, cass_client::CassClient, query_response};

mod common;
use common::CassProcess;

/// A read triggers repair of replicas with stale data.
#[tokio::test]
async fn read_repairs_stale_replicas() {
    let base1 = "http://127.0.0.1:18121";
    let base2 = "http://127.0.0.1:18122";
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let _child1 = CassProcess::spawn([
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
    let _child2 = CassProcess::spawn([
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
    let mut c2 = CassClient::connect(base2.to_string()).await.unwrap();
    c1.query(QueryRequest {
        sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
        ts: 0,
    })
    .await
    .unwrap();

    c1.internal(QueryRequest {
        sql: "INSERT INTO kv (id, val) VALUES ('r','new')".into(),
        ts: 2,
    })
    .await
    .unwrap();
    c2.internal(QueryRequest {
        sql: "INSERT INTO kv (id, val) VALUES ('r','old')".into(),
        ts: 1,
    })
    .await
    .unwrap();

    c1.query(QueryRequest {
        sql: "SELECT val FROM kv WHERE id = 'r'".into(),
        ts: 0,
    })
    .await
    .unwrap();

    let resp = c2
        .query(QueryRequest {
            sql: "SELECT val FROM kv WHERE id = 'r'".into(),
            ts: 0,
        })
        .await
        .unwrap()
        .into_inner();
    if let Some(query_response::Payload::Rows(rs)) = resp.payload {
        assert_eq!(rs.rows[0].columns.get("val"), Some(&"new".to_string()));
    } else {
        panic!("unexpected response");
    }
}

/// A replica missing a row receives it via read repair on demand.
#[tokio::test]
async fn read_repair_populates_missing_replicas() {
    let base1 = "http://127.0.0.1:18123";
    let base2 = "http://127.0.0.1:18124";
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let _child1 = CassProcess::spawn([
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
    let _child2 = CassProcess::spawn([
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
    let mut c2 = CassClient::connect(base2.to_string()).await.unwrap();
    c1.query(QueryRequest {
        sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
        ts: 0,
    })
    .await
    .unwrap();

    // Write only to node1 so node2 lacks the row.
    c1.internal(QueryRequest {
        sql: "INSERT INTO kv (id, val) VALUES ('missing','present')".into(),
        ts: 5,
    })
    .await
    .unwrap();

    // Coordinated read should trigger repair for node2.
    c1.query(QueryRequest {
        sql: "SELECT val FROM kv WHERE id = 'missing'".into(),
        ts: 0,
    })
    .await
    .unwrap();

    let resp = c2
        .query(QueryRequest {
            sql: "SELECT val FROM kv WHERE id = 'missing'".into(),
            ts: 0,
        })
        .await
        .unwrap()
        .into_inner();
    if let Some(query_response::Payload::Rows(rs)) = resp.payload {
        assert_eq!(rs.rows[0].columns.get("val"), Some(&"present".to_string()));
    } else {
        panic!("unexpected response");
    }
}
