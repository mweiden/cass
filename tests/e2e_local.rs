use cass::{
    Database, SqlEngine,
    query::QueryOutput,
    storage::{Storage, local::LocalStorage},
};
use std::sync::Arc;

#[tokio::test]
async fn e2e_insert_select() {
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(dir.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();
    engine
        .execute(&db, "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO kv (id, val) VALUES ('foo','bar')")
        .await
        .unwrap();
    let res = engine
        .execute(&db, "SELECT val FROM kv WHERE id='foo'")
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => {
            assert_eq!(rows[0].get("val"), Some(&"bar".to_string()));
        }
        _ => panic!("unexpected"),
    }
}
