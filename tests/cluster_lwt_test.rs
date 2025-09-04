use cass::cluster::Cluster;
use cass::query::QueryError;
use cass::rpc::{query_response, QueryResponse};
use cass::storage::local::LocalStorage;
use cass::Database;
use std::sync::Arc;
use tempfile::tempdir;

async fn build_cluster(rf: usize, self_addr: &str) -> Cluster {
    let dir = tempdir().unwrap();
    let storage = Arc::new(LocalStorage::new(dir.path()));
    let db = Arc::new(Database::new(storage, "wal.log").await.unwrap());
    Cluster::new(db, self_addr.to_string(), Vec::new(), 1, rf, rf)
}

fn applied(resp: &QueryResponse) -> Option<String> {
    match &resp.payload {
        Some(query_response::Payload::Rows(rs)) => rs
            .rows
            .get(0)
            .and_then(|r| r.columns.get("[applied]").cloned()),
        _ => None,
    }
}

fn column(resp: &QueryResponse, name: &str) -> Option<String> {
    match &resp.payload {
        Some(query_response::Payload::Rows(rs)) => {
            rs.rows.get(0).and_then(|r| r.columns.get(name).cloned())
        }
        _ => None,
    }
}

#[tokio::test]
async fn execute_lwt_insert_and_update_paths() {
    let addr = "http://127.0.0.1:6100";
    let cluster = build_cluster(1, addr).await;
    cluster
        .execute(
            "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))",
            false,
        )
        .await
        .unwrap();

    let resp = cluster
        .execute(
            "INSERT INTO kv (id, val) VALUES ('a','1') IF NOT EXISTS",
            false,
        )
        .await
        .unwrap();
    assert_eq!(applied(&resp), Some("true".to_string()));

    let resp = cluster
        .execute(
            "INSERT INTO kv (id, val) VALUES ('a','2') IF NOT EXISTS",
            false,
        )
        .await
        .unwrap();
    assert_eq!(applied(&resp), Some("false".to_string()));

    let resp = cluster
        .execute("UPDATE kv SET val='3' WHERE id='a' IF val='1'", false)
        .await
        .unwrap();
    assert_eq!(applied(&resp), Some("true".to_string()));

    let resp = cluster
        .execute("UPDATE kv SET val='4' WHERE id='a' IF val='1'", false)
        .await
        .unwrap();
    assert_eq!(applied(&resp), Some("false".to_string()));
    assert_eq!(column(&resp, "val"), Some("3".to_string()));
}

#[tokio::test]
async fn execute_lwt_errors_when_insufficient_replicas() {
    let addr = "http://127.0.0.1:6200";
    let cluster = build_cluster(2, addr).await; // rf=2 but only one node present
    cluster
        .execute("CREATE TABLE t (id TEXT, val TEXT, PRIMARY KEY(id))", false)
        .await
        .unwrap();

    let err = cluster
        .execute(
            "INSERT INTO t (id, val) VALUES ('a','1') IF NOT EXISTS",
            false,
        )
        .await
        .unwrap_err();
    match err {
        QueryError::Other(msg) => assert_eq!(msg, "not enough healthy replicas"),
        _ => panic!("unexpected error: {:?}", err),
    }
}
