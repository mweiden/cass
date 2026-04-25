use cass::{
    Database,
    storage::{Storage, local::LocalStorage},
};
use std::sync::Arc;

#[tokio::test]
async fn queries_check_storage_in_reverse_chronological_order() {
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(dir.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();

    // Oldest value persisted in first SSTable
    db.insert("k".to_string(), b"v1".to_vec()).await.unwrap();
    db.flush().await.unwrap();

    // Newer value persisted in second SSTable
    db.insert("k".to_string(), b"v2".to_vec()).await.unwrap();
    db.flush().await.unwrap();

    // Latest value remains in memtable
    db.insert("k".to_string(), b"v3".to_vec()).await.unwrap();

    // Memtable should take precedence over SSTables
    let v = db.get("k").await.map(|b| b[8..].to_vec());
    assert_eq!(v, Some(b"v3".to_vec()));

    // After flushing, newest SSTable should be consulted before older ones
    db.flush().await.unwrap();
    let v = db.get("k").await.map(|b| b[8..].to_vec());
    assert_eq!(v, Some(b"v3".to_vec()));
}
