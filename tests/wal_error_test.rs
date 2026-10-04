use std::sync::Arc;

use async_trait::async_trait;
use cass::{
    Database,
    storage::{Storage, StorageError},
};

/// Storage backend whose WAL contains a mix of corrupted and valid records.
struct CorruptWalStorage;

#[async_trait]
impl Storage for CorruptWalStorage {
    async fn put(&self, _path: &str, _data: Vec<u8>) -> Result<(), StorageError> {
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        if path == "wal.log" {
            // invalid base64, missing separator, invalid UTF-8 key, then a
            // valid record ("good" -> base64("v1"))
            let mut buf = Vec::new();
            buf.extend_from_slice(b"bad\t@@\n");
            buf.extend_from_slice(b"no-separator\n");
            buf.extend_from_slice(b"\xff\xfe\tdjE=\n");
            buf.extend_from_slice(b"good\tdjE=\n");
            Ok(buf)
        } else {
            Err(StorageError::Io(std::io::Error::other("missing")))
        }
    }

    async fn append(&self, _path: &str, _data: &[u8]) -> Result<(), StorageError> {
        Ok(())
    }

    async fn list(&self, _prefix: &str) -> Result<Vec<String>, StorageError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn database_recovers_past_corrupted_wal_records() {
    let storage: Arc<dyn Storage> = Arc::new(CorruptWalStorage);
    let db = Database::new(storage, "wal.log")
        .await
        .expect("corrupted WAL records must not prevent startup");
    // The valid record after the corrupted ones is still replayed.
    assert_eq!(db.get("good").await, Some(b"v1".to_vec()));
    // The corrupted records are dropped rather than partially applied.
    assert_eq!(db.get("bad").await, None);
    assert_eq!(db.get("no-separator").await, None);
}
