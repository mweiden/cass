use cass::Database;
use cass::cluster::Cluster;
use cass::rpc::{QueryRequest, cass_client::CassClient, query_response};
use cass::storage::local::LocalStorage;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::time::{Duration, sleep};

mod common;
use common::{CassProcess, free_http_addr};

async fn build_cluster(peers: Vec<String>, self_addr: &str) -> Cluster {
    let dir = tempdir().unwrap();
    let storage = Arc::new(LocalStorage::new(dir.path()));
    let db = Arc::new(Database::new(storage, "wal.log").await.unwrap());
    let peer_list = peers.clone();
    let cluster = Cluster::new(db, self_addr.to_string(), peers, 1, 2, 1, 1);
    for _ in 0..100 {
        let mut all_healthy = true;
        for peer in &peer_list {
            if !cluster.is_alive(peer).await {
                all_healthy = false;
                break;
            }
        }
        if all_healthy {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    cluster
}

fn applied(resp: &cass::rpc::QueryResponse) -> Option<String> {
    match &resp.payload {
        Some(query_response::Payload::Rows(rs)) => rs
            .rows
            .get(0)
            .and_then(|r| r.columns.get("[applied]").cloned()),
        _ => None,
    }
}

#[tokio::test]
async fn flush_all_calls_remote_nodes() {
    let remote_addr = free_http_addr();
    let dir_remote = tempdir().unwrap();
    let _remote = CassProcess::spawn([
        "server",
        "--data-dir",
        dir_remote.path().to_str().unwrap(),
        "--node-addr",
        &remote_addr,
    ]);

    let mut ready = false;
    for _ in 0..100 {
        // up to ~5s
        if CassClient::connect(remote_addr.clone()).await.is_ok() {
            ready = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(ready, "remote server did not become ready: {}", remote_addr);

    let cluster = build_cluster(vec![remote_addr.clone()], &free_http_addr()).await;
    cluster.flush_all().await.unwrap();
}

#[tokio::test]
async fn execute_lwt_remote_branches() {
    let remote_addr = free_http_addr();
    let dir_remote = tempdir().unwrap();
    let _remote = CassProcess::spawn([
        "server",
        "--data-dir",
        dir_remote.path().to_str().unwrap(),
        "--node-addr",
        &remote_addr,
    ]);

    let mut ready = false;
    for _ in 0..100 {
        // up to ~5s
        if CassClient::connect(remote_addr.clone()).await.is_ok() {
            ready = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(ready, "remote server did not become ready: {}", remote_addr);

    let cluster = build_cluster(vec![remote_addr.clone()], &free_http_addr()).await;
    cluster
        .execute("CREATE TABLE t (id TEXT, val TEXT, PRIMARY KEY(id))", false)
        .await
        .unwrap();

    let resp1 = cluster
        .execute(
            "INSERT INTO t (id, val) VALUES ('a','1') IF NOT EXISTS",
            false,
        )
        .await
        .unwrap();
    assert_eq!(applied(&resp1), Some("true".to_string()));

    let resp2 = cluster
        .execute(
            "INSERT INTO t (id, val) VALUES ('a','2') IF NOT EXISTS",
            false,
        )
        .await
        .unwrap();
    assert_eq!(applied(&resp2), Some("false".to_string()));

    let mut client = CassClient::connect(remote_addr.clone()).await.unwrap();
    let res = client
        .query(QueryRequest {
            sql: "SELECT val FROM t WHERE id='a'".into(),
        })
        .await
        .unwrap()
        .into_inner();
    if let Some(query_response::Payload::Rows(rs)) = res.payload {
        assert_eq!(rs.rows[0].columns.get("val"), Some(&"1".to_string()));
    } else {
        panic!("unexpected response");
    }
}
