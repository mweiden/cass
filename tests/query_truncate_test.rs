use cass::storage::{Storage, local::LocalStorage};
use cass::{Database, SqlEngine, query::QueryOutput};
use std::sync::Arc;

#[tokio::test]
async fn truncate_table_removes_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();

    engine
        .execute(&db, "CREATE TABLE t (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();

    engine
        .execute(&db, "INSERT INTO t (id, val) VALUES ('1','a')")
        .await
        .unwrap();

    let before = engine
        .execute(&db, "SELECT * FROM t WHERE id='1'")
        .await
        .unwrap();
    let rows = match before {
        QueryOutput::Rows(r) => r,
        _ => panic!("unexpected"),
    };
    assert_eq!(rows.len(), 1);

    engine.execute(&db, "TRUNCATE TABLE t").await.unwrap();

    let after = engine
        .execute(&db, "SELECT * FROM t WHERE id='1'")
        .await
        .unwrap();
    match after {
        QueryOutput::Rows(r) => assert!(r.is_empty()),
        _ => panic!("unexpected"),
    }
}
