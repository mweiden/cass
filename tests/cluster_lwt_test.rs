use cass::Database;
use cass::cluster::Cluster;
use cass::query::QueryError;
use cass::rpc::{QueryResponse, query_response};
use cass::storage::local::LocalStorage;
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
            .rows.first()
            .and_then(|r| r.columns.get("[applied]").cloned()),
        _ => None,
    }
}

fn column(resp: &QueryResponse, name: &str) -> Option<String> {
    match &resp.payload {
        Some(query_response::Payload::Rows(rs)) => {
            rs.rows.first().and_then(|r| r.columns.get(name).cloned())
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
            0,
        )
        .await
        .unwrap();

    let resp = cluster
        .execute(
            "INSERT INTO kv (id, val) VALUES ('a','1') IF NOT EXISTS",
            false,
            0,
        )
        .await
        .unwrap();
    assert_eq!(applied(&resp), Some("true".to_string()));

    let resp = cluster
        .execute(
            "INSERT INTO kv (id, val) VALUES ('a','2') IF NOT EXISTS",
            false,
            0,
        )
        .await
        .unwrap();
    assert_eq!(applied(&resp), Some("false".to_string()));

    let resp = cluster
        .execute("UPDATE kv SET val='3' WHERE id='a' IF val='1'", false, 0)
        .await
        .unwrap();
    assert_eq!(applied(&resp), Some("true".to_string()));

    let resp = cluster
        .execute("UPDATE kv SET val='4' WHERE id='a' IF val='1'", false, 0)
        .await
        .unwrap();
    assert_eq!(applied(&resp), Some("false".to_string()));
    assert_eq!(column(&resp, "val"), Some("3".to_string()));
}

#[tokio::test]
async fn concurrent_lwt_inserts_apply_exactly_once() {
    let addr = "http://127.0.0.1:6300";
    let cluster = Arc::new(build_cluster(1, addr).await);
    cluster
        .execute(
            "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))",
            false,
            0,
        )
        .await
        .unwrap();

    // Fire a batch of competing IF NOT EXISTS inserts for the same key in
    // parallel; the Paxos rounds now fan out to replicas concurrently and
    // exactly one contender may win.
    let mut handles = Vec::new();
    for i in 0..8 {
        let c = cluster.clone();
        handles.push(tokio::spawn(async move {
            let sql = format!("INSERT INTO kv (id, val) VALUES ('race','{i}') IF NOT EXISTS");
            c.execute(&sql, false, 0).await
        }));
    }
    let mut wins = 0usize;
    for h in handles {
        if let Ok(Ok(resp)) = h.await
            && applied(&resp) == Some("true".to_string())
        {
            wins += 1;
        }
    }
    assert_eq!(wins, 1, "exactly one concurrent LWT insert may be applied");
}

#[tokio::test]
async fn execute_lwt_errors_when_insufficient_replicas() {
    let addr = "http://127.0.0.1:6200";
    let cluster = build_cluster(2, addr).await; // rf=2 but only one node present
    cluster
        .execute(
            "CREATE TABLE t (id TEXT, val TEXT, PRIMARY KEY(id))",
            false,
            0,
        )
        .await
        .unwrap();

    let err = cluster
        .execute(
            "INSERT INTO t (id, val) VALUES ('a','1') IF NOT EXISTS",
            false,
            0,
        )
        .await
        .unwrap_err();
    match err {
        QueryError::Other(msg) => assert_eq!(msg, "not enough healthy replicas"),
        _ => panic!("unexpected error: {:?}", err),
    }
}
