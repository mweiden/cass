use cass::{
    Database,
    storage::{Storage, local::LocalStorage},
};
use std::sync::Arc;

#[tokio::test]
async fn wal_recovery_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let wal = "wal.log";
    {
        let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(&path));
        let db = Database::new(storage, wal).await.unwrap();
        db.insert("k1".to_string(), b"v1".to_vec()).await.unwrap();
        db.sync_wal().await.unwrap();
    }
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(&path));
    let db = Database::new(storage, wal).await.unwrap();
    let v1 = db.get("k1").await.map(|b| b[8..].to_vec());
    assert_eq!(v1, Some(b"v1".to_vec()));
}

#[tokio::test]
async fn wal_recovery_survives_torn_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let wal = "wal.log";
    {
        let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(&path));
        let db = Database::new(storage.clone(), wal).await.unwrap();
        db.insert("k1".to_string(), b"v1".to_vec()).await.unwrap();
        db.sync_wal().await.unwrap();
        // Simulate a crash mid-append: a partial record with truncated
        // base64 and no trailing newline.
        storage.append(wal, b"k2\tdjE").await.unwrap();
    }
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(&path));
    let db = Database::new(storage, wal).await.unwrap();
    let v1 = db.get("k1").await.map(|b| b[8..].to_vec());
    assert_eq!(v1, Some(b"v1".to_vec()));
    assert_eq!(db.get("k2").await, None);
}
