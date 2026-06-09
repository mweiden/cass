use cass::{
    Database, SqlEngine,
    query::QueryOutput,
    storage::{Storage, local::LocalStorage},
};
use std::sync::Arc;

async fn setup() -> (Database, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(dir.path()));
    // keep the tempdir alive for the duration of the test
    std::mem::forget(dir);
    let db = Database::new(storage, "wal.log").await.unwrap();
    (db, SqlEngine::new())
}

#[tokio::test]
async fn corrupt_row_surfaces_error_instead_of_empty_row() {
    let (db, engine) = setup().await;
    engine
        .execute(&db, "CREATE TABLE t (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO t (id, val) VALUES ('1', 'a')")
        .await
        .unwrap();

    // Overwrite the stored row with bytes that are not valid JSON,
    // simulating on-disk corruption.
    db.insert_ns("t", "1".to_string(), b"{not json".to_vec())
        .await
        .unwrap();

    let res = engine.execute(&db, "SELECT * FROM t WHERE id = '1'").await;
    assert!(
        res.is_err(),
        "corrupt row data must produce an error, not a silently empty row"
    );
}

#[tokio::test]
async fn deleted_row_still_reads_as_absent() {
    let (db, engine) = setup().await;
    engine
        .execute(&db, "CREATE TABLE t (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO t (id, val) VALUES ('1', 'a')")
        .await
        .unwrap();
    engine
        .execute(&db, "DELETE FROM t WHERE id = '1'")
        .await
        .unwrap();

    // A tombstone (empty payload) is a legitimate empty row, not corruption.
    match engine
        .execute(&db, "SELECT * FROM t WHERE id = '1'")
        .await
        .unwrap()
    {
        QueryOutput::Rows(rows) => assert!(rows.is_empty()),
        _ => panic!("expected row output"),
    }
}
