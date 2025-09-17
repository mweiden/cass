use cass::Database;
use cass::cluster::Cluster;
use cass::query::QueryError;
use cass::rpc::{QueryResponse, query_response};
use cass::storage::local::LocalStorage;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

async fn build_cluster(peers: Vec<String>, vnodes: usize, rf: usize, self_addr: &str) -> Cluster {
    let dir = tempdir().unwrap();
    let storage = Arc::new(LocalStorage::new(dir.path()));
    let db = Arc::new(Database::new(storage, "wal.log").await.unwrap());
    Cluster::new(db, self_addr.to_string(), peers, vnodes, rf, rf, rf)
}

fn first_val(resp: QueryResponse, col: &str) -> Option<String> {
    match resp.payload {
        Some(query_response::Payload::Rows(rs)) => {
            rs.rows.get(0).and_then(|row| row.columns.get(col).cloned())
        }
        _ => None,
    }
}

#[tokio::test]
async fn forwarded_insert_executes() {
    let addr = "http://127.0.0.1:6000";
    let cluster = build_cluster(Vec::new(), 1, 1, addr).await;
    cluster
        .execute(
            "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))",
            false,
        )
        .await
        .unwrap();
    cluster
        .execute("--ts:5\nINSERT INTO kv (id, val) VALUES ('a','x')", true)
        .await
        .unwrap();
    let resp = cluster
        .execute("SELECT val FROM kv WHERE id='a'", false)
        .await
        .unwrap();
    assert_eq!(first_val(resp, "val"), Some("x".to_string()));
}

#[tokio::test]
async fn insert_errors_when_no_healthy_replicas() {
    let addr = "http://127.0.0.1:6001";
    let cluster = build_cluster(Vec::new(), 1, 1, addr).await;
    cluster
        .execute("CREATE TABLE t (id TEXT, val TEXT, PRIMARY KEY(id))", false)
        .await
        .unwrap();
    cluster.panic_for(Duration::from_secs(2)).await;
    let err = cluster
        .execute("INSERT INTO t (id, val) VALUES ('a','1')", false)
        .await
        .unwrap_err();
    match err {
        QueryError::Other(msg) => assert_eq!(msg, "no healthy replicas"),
        _ => panic!("unexpected error"),
    }
}

#[tokio::test]
async fn select_errors_when_all_replicas_unhealthy() {
    let addr = "http://127.0.0.1:6002";
    let cluster = build_cluster(Vec::new(), 1, 1, addr).await;
    cluster
        .execute(
            "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))",
            false,
        )
        .await
        .unwrap();
    cluster
        .execute("INSERT INTO kv (id, val) VALUES ('a','v')", false)
        .await
        .unwrap();
    cluster.panic_for(Duration::from_secs(2)).await;
    let err = cluster
        .execute("SELECT val FROM kv WHERE id='a'", false)
        .await
        .unwrap_err();
    match err {
        QueryError::Other(msg) => assert_eq!(msg, "not enough healthy replicas"),
        _ => panic!("unexpected error"),
    }
}

#[tokio::test]
async fn show_tables_succeeds_with_unhealthy_peer() {
    let self_addr = "http://127.0.0.1:6004";
    let peer = "http://127.0.0.1:6005".to_string();
    let cluster = build_cluster(vec![peer], 1, 2, self_addr).await;
    cluster
        .execute(
            "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))",
            false,
        )
        .await
        .unwrap();
    let resp = cluster.execute("SHOW TABLES", false).await.unwrap();
    if let Some(query_response::Payload::Tables(t)) = resp.payload {
        assert_eq!(t.tables, vec!["kv".to_string()]);
    } else {
        panic!("expected table list");
    }
}

#[tokio::test]
async fn delete_removes_row() {
    let addr = "http://127.0.0.1:6006";
    let cluster = build_cluster(Vec::new(), 1, 1, addr).await;
    cluster
        .execute(
            "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))",
            false,
        )
        .await
        .unwrap();
    cluster
        .execute("INSERT INTO kv (id, val) VALUES ('a','x')", false)
        .await
        .unwrap();
    let resp = cluster
        .execute("DELETE FROM kv WHERE id='a'", false)
        .await
        .unwrap();
    if let Some(query_response::Payload::Mutation(m)) = resp.payload {
        assert_eq!(m.op, "DELETE");
        assert_eq!(m.count, 1);
    } else {
        panic!("expected mutation response");
    }
    let resp = cluster
        .execute("SELECT val FROM kv WHERE id='a'", false)
        .await
        .unwrap();
    assert_eq!(first_val(resp, "val"), None);
}

#[tokio::test]
async fn drop_table_removes_schema() {
    let addr = "http://127.0.0.1:6007";
    let cluster = build_cluster(Vec::new(), 1, 1, addr).await;
    cluster
        .execute(
            "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))",
            false,
        )
        .await
        .unwrap();
    let resp = cluster.execute("DROP TABLE kv", false).await.unwrap();
    if let Some(query_response::Payload::Mutation(m)) = resp.payload {
        assert_eq!(m.op, "DROP TABLE");
        assert_eq!(m.count, 1);
    } else {
        panic!("expected mutation response");
    }
    let resp = cluster.execute("SHOW TABLES", false).await.unwrap();
    if let Some(query_response::Payload::Tables(t)) = resp.payload {
        assert!(t.tables.is_empty());
    } else {
        panic!("expected table list");
    }
}

#[tokio::test]
async fn select_with_in_returns_rows() {
    let addr = "http://127.0.0.1:6008";
    let cluster = build_cluster(Vec::new(), 1, 1, addr).await;
    cluster
        .execute(
            "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))",
            false,
        )
        .await
        .unwrap();
    for (id, val) in [("a", "1"), ("b", "2"), ("c", "3")].iter() {
        cluster
            .execute(
                &format!("INSERT INTO kv (id, val) VALUES ('{}','{}')", id, val),
                false,
            )
            .await
            .unwrap();
    }
    let resp = cluster
        .execute("SELECT val FROM kv WHERE id IN ('a','c')", false)
        .await
        .unwrap();
    if let Some(query_response::Payload::Rows(rs)) = resp.payload {
        let mut vals: Vec<String> = rs
            .rows
            .iter()
            .filter_map(|r| r.columns.get("val").cloned())
            .collect();
        vals.sort();
        assert_eq!(vals, vec!["1".to_string(), "3".to_string()]);
    } else {
        panic!("expected rows response");
    }
}
