use cass::storage::{Storage, local::LocalStorage};
use cass::{Database, SqlEngine, query::QueryOutput};
use std::sync::Arc;

#[tokio::test]
async fn insert_if_not_exists_and_update_if() {
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
        QueryOutput::Mutation { count, .. } => assert_eq!(count, 1),
        _ => panic!("unexpected"),
    }

    // second insert should do nothing
    let res = engine
        .execute(
            &db,
            "INSERT INTO kv (id, val) VALUES ('a','1') IF NOT EXISTS",
        )
        .await
        .unwrap();
    match res {
        QueryOutput::Mutation { count, .. } => assert_eq!(count, 0),
        _ => panic!("unexpected"),
    }

    // conditional update succeeds
    let res = engine
        .execute(&db, "UPDATE kv SET val='2' WHERE id='a' IF val='1'")
        .await
        .unwrap();
    match res {
        QueryOutput::Mutation { count, .. } => assert_eq!(count, 1),
        _ => panic!("unexpected"),
    }

    // verify update took effect
    let res = engine
        .execute(&db, "SELECT val FROM kv WHERE id='a'")
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => assert_eq!(rows[0].get("val"), Some(&"2".to_string())),
        _ => panic!("unexpected"),
    }

    // mismatch condition fails
    let res = engine
        .execute(&db, "UPDATE kv SET val='3' WHERE id='a' IF val='1'")
        .await
        .unwrap();
    match res {
        QueryOutput::Mutation { count, .. } => assert_eq!(count, 0),
        _ => panic!("unexpected"),
    }
}
