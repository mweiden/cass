use cass::storage::{Storage, local::LocalStorage};
use cass::{Database, SqlEngine, query::QueryOutput};
use std::sync::Arc;

#[tokio::test]
async fn update_delete_and_count() {
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
    engine
        .execute(&db, "UPDATE kv SET val = '2' WHERE id = 'a'")
        .await
        .unwrap();
    let res = engine
        .execute(&db, "SELECT val FROM kv WHERE id = 'a'")
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => assert_eq!(rows[0].get("val"), Some(&"2".to_string())),
        _ => panic!("unexpected"),
    }

    engine
        .execute(&db, "DELETE FROM kv WHERE id = 'a'")
        .await
        .unwrap();
    let res = engine
        .execute(&db, "SELECT val FROM kv WHERE id = 'a'")
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => assert!(rows.is_empty()),
        _ => panic!("unexpected"),
    }

    engine
        .execute(&db, "INSERT INTO kv (id, val) VALUES ('b','3')")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO kv (id, val) VALUES ('c','4')")
        .await
        .unwrap();

    let res = engine
        .execute(&db, "SELECT COUNT(*) FROM kv")
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => {
            assert_eq!(rows[0].get("count"), Some(&"2".to_string()));
        }
        _ => panic!("unexpected"),
    }
    let res = engine
        .execute(&db, "SELECT COUNT(*) FROM kv WHERE id = 'b'")
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => {
            assert_eq!(rows[0].get("count"), Some(&"1".to_string()));
        }
        _ => panic!("unexpected"),
    }

    let res = engine
        .execute(&db, "SELECT COUNT(*) FROM kv WHERE val = '3'")
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => {
            assert_eq!(rows[0].get("count"), Some(&"1".to_string()));
        }
        _ => panic!("unexpected"),
    }
}

#[tokio::test]
async fn table_names_and_cast() {
    let tmp = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();

    engine
        .execute(&db, "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO kv (id, val) VALUES ('a','001')")
        .await
        .unwrap();

    let tables = engine.execute(&db, "SHOW TABLES").await.unwrap();
    match tables {
        QueryOutput::Tables(t) => assert_eq!(t, vec!["kv".to_string()]),
        _ => panic!("unexpected"),
    }

    let cast = engine
        .execute(&db, "SELECT CAST(val AS INT) FROM kv WHERE id = 'a'")
        .await
        .unwrap();
    match cast {
        QueryOutput::Rows(rows) => {
            assert_eq!(rows[0].get("val"), Some(&"1".to_string()));
        }
        _ => panic!("unexpected"),
    }
}

#[tokio::test]
async fn multi_row_insert_count() {
    let tmp = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();

    engine
        .execute(&db, "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();
    let res = engine
        .execute(&db, "INSERT INTO kv (id, val) VALUES ('a','1'),('b','2')")
        .await
        .unwrap();
    match res {
        QueryOutput::Mutation { op, count, .. } => {
            assert_eq!(op, "INSERT");
            assert_eq!(count, 2);
        }
        _ => panic!("unexpected"),
    }
}
