use std::sync::Arc;

use async_trait::async_trait;
use cass::{
    Database,
    storage::{Storage, StorageError},
};

struct BadStorage;

#[async_trait]
impl Storage for BadStorage {
    async fn put(&self, _path: &str, _data: Vec<u8>) -> Result<(), StorageError> {
        Ok(())
    }

    async fn get(&self, _path: &str) -> Result<Vec<u8>, StorageError> {
        // return invalid WAL data to trigger a parse error
        Ok(b"bad\t@@\n".to_vec())
    }

    async fn append(&self, _path: &str, _data: &[u8]) -> Result<(), StorageError> {
        Ok(())
    }

    async fn list(&self, _prefix: &str) -> Result<Vec<String>, StorageError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn database_new_propagates_wal_error() {
    let storage: Arc<dyn Storage> = Arc::new(BadStorage);
    let res = Database::new(storage, "wal.log").await;
    assert!(res.is_err());
}
