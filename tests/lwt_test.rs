use cass::storage::{Storage, local::LocalStorage};
use cass::{Database, SqlEngine, query::QueryOutput};
use std::sync::Arc;

#[tokio::test]
async fn insert_if_not_exists() {
    let tmp = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();

    engine
        .execute(&db, "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();

    let res = engine
        .execute(
            &db,
            "INSERT INTO kv (id, val) VALUES ('a','1') IF NOT EXISTS",
        )
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => assert_eq!(rows[0].get("[applied]"), Some(&"true".to_string())),
        _ => panic!("unexpected"),
    }

    let res = engine
        .execute(
            &db,
            "INSERT INTO kv (id, val) VALUES ('a','1') IF NOT EXISTS",
        )
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => assert_eq!(rows[0].get("[applied]"), Some(&"false".to_string())),
        _ => panic!("unexpected"),
    }

    // After flushing the memtable, the row only exists on disk. A subsequent
    // conditional insert should still detect the existing data and refuse to
    // overwrite it.
    db.flush().await.unwrap();
    let res = engine
        .execute(
            &db,
            "INSERT INTO kv (id, val) VALUES ('a','1') IF NOT EXISTS",
        )
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => assert_eq!(rows[0].get("[applied]"), Some(&"false".to_string())),
        _ => panic!("unexpected"),
    }
}

#[tokio::test]
async fn update_if_equals() {
    let tmp = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();

    engine
        .execute(&db, "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO kv (id, val) VALUES ('a','1')")
        .await
        .unwrap();

    let res = engine
        .execute(&db, "UPDATE kv SET val='2' WHERE id='a' IF val='1'")
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => assert_eq!(rows[0].get("[applied]"), Some(&"true".to_string())),
        _ => panic!("unexpected"),
    }

    let res = engine
        .execute(&db, "UPDATE kv SET val='3' WHERE id='a' IF val='1'")
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => {
            assert_eq!(rows[0].get("[applied]"), Some(&"false".to_string()));
            assert_eq!(rows[0].get("val"), Some(&"2".to_string()));
        }
        _ => panic!("unexpected"),
    }
}
