use std::time::{Duration, Instant};
use tokio::time::sleep;

use cass::rpc::{QueryRequest, cass_client::CassClient, query_response};

mod common;
use common::CassProcess;

#[tokio::test]
async fn hinted_handoff_replays_when_node_recovers() {
    let base1 = "http://127.0.0.1:18111";
    let base2 = "http://127.0.0.1:18112";
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

    child2.kill();

    c1.query(QueryRequest {
        sql: "INSERT INTO kv (id, val) VALUES ('h','1')".into(),
    })
    .await
    .unwrap();

    child2 = CassProcess::spawn([
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
        if CassClient::connect(base2.to_string()).await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let start = Instant::now();
    loop {
        let _ = c1
            .query(QueryRequest {
                sql: "SELECT val FROM kv WHERE id = 'h'".into(),
            })
            .await;
        let mut c2 = CassClient::connect(base2.to_string()).await.unwrap();
        let resp = c2
            .query(QueryRequest {
                sql: "SELECT val FROM kv WHERE id = 'h'".into(),
            })
            .await
            .unwrap()
            .into_inner();
        if let Some(query_response::Payload::Rows(rs)) = resp.payload {
            if rs.rows.get(0).and_then(|r| r.columns.get("val")) == Some(&"1".to_string()) {
                break;
            }
        }
        if start.elapsed() > Duration::from_secs(4) {
            panic!("hint not delivered");
        }
        sleep(Duration::from_millis(100)).await;
    }

    child1.kill();
    child2.kill();
}
