use cass::storage::{Storage, local::LocalStorage};
use cass::{Database, SqlEngine, query::QueryOutput};
use std::sync::Arc;

#[tokio::test]
async fn select_reads_from_mem_and_disk() {
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(dir.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();

    engine
        .execute(
            &db,
            "CREATE TABLE logs (user_id TEXT, ts TEXT, val TEXT, PRIMARY KEY(user_id, ts))",
        )
        .await
        .unwrap();

    engine
        .execute(
            &db,
            "INSERT INTO logs (user_id, ts, val) VALUES ('u1','1','a')",
        )
        .await
        .unwrap();
    db.flush().await.unwrap();
    engine
        .execute(
            &db,
            "INSERT INTO logs (user_id, ts, val) VALUES ('u1','2','b')",
        )
        .await
        .unwrap();

    let res = engine
        .execute(&db, "SELECT val FROM logs WHERE user_id='u1'")
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => {
            let mut vals: Vec<String> = rows.iter().filter_map(|r| r.get("val").cloned()).collect();
            vals.sort();
            assert_eq!(vals, vec!["a".to_string(), "b".to_string()]);
        }
        _ => panic!("unexpected"),
    }
}

#[tokio::test]
async fn count_reads_from_mem_and_disk() {
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(dir.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();

    engine
        .execute(
            &db,
            "CREATE TABLE logs (user_id TEXT, ts TEXT, val TEXT, PRIMARY KEY(user_id, ts))",
        )
        .await
        .unwrap();

    engine
        .execute(
            &db,
            "INSERT INTO logs (user_id, ts, val) VALUES ('u1','1','a')",
        )
        .await
        .unwrap();
    db.flush().await.unwrap();
    engine
        .execute(
            &db,
            "INSERT INTO logs (user_id, ts, val) VALUES ('u1','2','b')",
        )
        .await
        .unwrap();

    let res = engine
        .execute(&db, "SELECT count(*) FROM logs WHERE user_id='u1'")
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => {
            assert_eq!(rows[0].get("count"), Some(&"2".to_string()));
        }
        _ => panic!("unexpected"),
    }
}
