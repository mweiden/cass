use cass::{
    Database,
    storage::{Storage, local::LocalStorage},
};
use std::sync::Arc;

#[tokio::test]
async fn flush_and_query_from_sstable() {
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(dir.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();

    db.insert("k1".to_string(), b"v1".to_vec()).await.unwrap();
    db.insert("k2".to_string(), b"v2".to_vec()).await.unwrap();
    // manually flush current memtable to disk
    db.flush().await.unwrap();

    // data should be retrievable even after memtable cleared
    let v1 = db.get("k1").await.map(|b| b[8..].to_vec());
    let v2 = db.get("k2").await.map(|b| b[8..].to_vec());
    assert_eq!(v1, Some(b"v1".to_vec()));
    assert_eq!(v2, Some(b"v2".to_vec()));
    assert_eq!(db.get("missing").await, None);
}
