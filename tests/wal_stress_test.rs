use async_trait::async_trait;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex;

use cass::storage::{Storage, StorageError};
use cass::wal::{Wal, WalOptions};

#[derive(Default)]
struct CountingStorage {
    data: Mutex<Vec<u8>>,
    bytes: AtomicUsize,
}

#[async_trait]
impl Storage for CountingStorage {
    async fn put(&self, _path: &str, data: Vec<u8>) -> Result<(), StorageError> {
        let mut buf = self.data.lock().await;
        *buf = data.clone();
        self.bytes.fetch_add(data.len(), Ordering::SeqCst);
        Ok(())
    }

    async fn get(&self, _path: &str) -> Result<Vec<u8>, StorageError> {
        Ok(self.data.lock().await.clone())
    }

    async fn append(&self, _path: &str, data: &[u8]) -> Result<(), StorageError> {
        let mut buf = self.data.lock().await;
        buf.extend_from_slice(data);
        self.bytes.fetch_add(data.len(), Ordering::SeqCst);
        Ok(())
    }

    async fn list(&self, _prefix: &str) -> Result<Vec<String>, StorageError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn wal_append_stress_no_bloat() {
    let storage = Arc::new(CountingStorage::default());
    let options = WalOptions {
        commitlog_sync_period: Duration::ZERO,
    };
    let wal = Wal::new_with_options(storage.clone(), "wal.log", options)
        .await
        .unwrap()
        .0;
    for _ in 0..1000 {
        wal.append(b"abc").await.unwrap();
    }
    // each append writes "abc\n" -> 4 bytes
    assert_eq!(storage.bytes.load(Ordering::SeqCst), 4 * 1000);
}

#[tokio::test]
async fn wal_periodic_flushes_entries() {
    let storage = Arc::new(CountingStorage::default());
    let options = WalOptions {
        commitlog_sync_period: Duration::from_millis(200),
    };
    let wal = Wal::new_with_options(storage.clone(), "wal.log", options)
        .await
        .unwrap()
        .0;

    wal.append(b"abc").await.unwrap();
    assert_eq!(storage.bytes.load(Ordering::SeqCst), 0);

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(storage.bytes.load(Ordering::SeqCst), 0);

    tokio::time::sleep(Duration::from_millis(250)).await;
    let flushed = storage.bytes.load(Ordering::SeqCst);
    assert!(
        flushed >= 4,
        "expected at least one periodic flush, observed {} bytes",
        flushed
    );
}
