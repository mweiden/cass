pub mod bloom;
pub mod cluster;
pub mod memtable;
pub mod query;
pub mod schema;
pub mod sstable;
pub mod storage;
pub mod wal;
pub mod zonemap;

pub mod rpc {
    tonic::include_proto!("cass");
}

use base64::Engine;
pub use query::SqlEngine;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Core database type combining an in-memory memtable with a persistent
/// storage layer.
pub struct Database {
    storage: Arc<dyn storage::Storage>,
    memtable: memtable::MemTable,
    sstables: tokio::sync::RwLock<Vec<sstable::SsTable>>,
    max_memtable_size: usize,
    next_id: std::sync::atomic::AtomicUsize,
    wal: wal::Wal,
}

impl Database {
    /// Create a new database instance backed by the provided storage
    /// implementation.
    pub async fn new(
        storage: Arc<dyn storage::Storage>,
        wal_path: impl Into<String>,
    ) -> std::io::Result<Self> {
        let wal_path = wal_path.into();
        let (wal, entries) = wal::Wal::new(storage.clone(), wal_path).await?;
        let memtable = memtable::MemTable::new();
        for (k, v) in entries {
            memtable.insert(k, v).await;
        }
        let files = storage.list("sstable_").await.map_err(|e| match e {
            storage::StorageError::Io(e) => e,
            _ => std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
        })?;
        let mut pairs = Vec::new();
        let mut next_id = 0usize;
        for f in files {
            if let Some(id_str) = f
                .strip_prefix("sstable_")
                .and_then(|s| s.strip_suffix(".tbl"))
            {
                if let Ok(id) = id_str.parse::<usize>() {
                    let table = sstable::SsTable::load(&f, storage.as_ref()).await.map_err(
                        |e| match e {
                            storage::StorageError::Io(e) => e,
                            _ => std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
                        },
                    )?;
                    if id >= next_id {
                        next_id = id + 1;
                    }
                    pairs.push((id, table));
                }
            }
        }
        pairs.sort_by_key(|(id, _)| *id);
        let sstables = pairs.into_iter().map(|(_, t)| t).collect();
        Ok(Self {
            storage,
            memtable,
            sstables: tokio::sync::RwLock::new(sstables),
            // default threshold before automatically flushing to disk
            max_memtable_size: 1024,
            next_id: std::sync::atomic::AtomicUsize::new(next_id),
            wal,
        })
    }

    /// Return a reference to the configured storage backend.
    pub fn storage(&self) -> &Arc<dyn storage::Storage> {
        &self.storage
    }

    /// Return a reference to the in-memory memtable used for writes.
    pub fn memtable(&self) -> &memtable::MemTable {
        &self.memtable
    }

    /// Current timestamp in microseconds since Unix epoch.
    fn now_ts() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_micros() as u64
    }

    async fn insert_internal(&self, key: String, value: Vec<u8>) {
        // best effort to log the write ahead of applying it
        let mut rec = key.clone().into_bytes();
        rec.push(b'\t');
        let enc = base64::engine::general_purpose::STANDARD.encode(&value);
        rec.extend_from_slice(enc.as_bytes());
        let _ = self.wal.append(&rec).await;

        self.memtable.insert(key, value).await;
        if self.memtable.len().await >= self.max_memtable_size {
            // best-effort flush; ignore errors for now
            let _ = self.flush().await;
        }
    }

    /// Insert a key/value pair with an explicit timestamp.
    pub async fn insert_ts(&self, key: String, value: Vec<u8>, ts: u64) {
        let mut data = ts.to_be_bytes().to_vec();
        data.extend_from_slice(&value);
        self.insert_internal(key, data).await;
    }

    /// Insert a key/value pair into the database using the current time.
    pub async fn insert(&self, key: String, value: Vec<u8>) {
        let ts = Self::now_ts();
        self.insert_ts(key, value, ts).await;
    }

    /// Retrieve the value associated with `key`, if it exists.
    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        if let Some(val) = self.memtable.get(key).await {
            return Some(val);
        }
        let tables = self.sstables.read().await;
        for table in tables.iter().rev() {
            if let Ok(Some(v)) = table.get(key, self.storage.as_ref()).await {
                return Some(v);
            }
        }
        None
    }

    /// Delete a key from the database.
    pub async fn delete(&self, key: &str) {
        self.memtable.delete(key).await;
    }

    /// Return all key/value pairs currently stored.
    pub async fn scan(&self) -> Vec<(String, Vec<u8>)> {
        self.memtable.scan().await
    }

    /// Remove all data from the in-memory table.
    pub async fn clear(&self) {
        self.memtable.clear().await;
    }

    /// Insert a key/value pair into the provided namespace with explicit timestamp.
    pub async fn insert_ns_ts(&self, ns: &str, key: String, value: Vec<u8>, ts: u64) {
        let namespaced = format!("{}:{}", ns, key);
        self.insert_ts(namespaced, value, ts).await;
    }

    /// Insert a key/value pair into the provided namespace using the current time.
    pub async fn insert_ns(&self, ns: &str, key: String, value: Vec<u8>) {
        let ts = Self::now_ts();
        self.insert_ns_ts(ns, key, value, ts).await;
    }

    /// Retrieve a value from the given namespace.
    pub async fn get_ns(&self, ns: &str, key: &str) -> Option<Vec<u8>> {
        let namespaced = format!("{}:{}", ns, key);
        self.get(&namespaced).await
    }

    /// Delete a key within the specified namespace.
    pub async fn delete_ns(&self, ns: &str, key: &str) {
        let namespaced = format!("{}:{}", ns, key);
        self.memtable.delete(&namespaced).await;
    }

    /// Scan all key/value pairs for a namespace, stripping the prefix.
    pub async fn scan_ns(&self, ns: &str) -> Vec<(String, Vec<u8>)> {
        let prefix = format!("{}:", ns);
        self.memtable
            .scan()
            .await
            .into_iter()
            .filter_map(|(k, v)| k.strip_prefix(&prefix).map(|rest| (rest.to_string(), v)))
            .collect()
    }

    /// Clear all data for a namespace.
    pub async fn clear_ns(&self, ns: &str) {
        let prefix = format!("{}:", ns);
        self.memtable.delete_prefix(&prefix).await;
    }

    /// Manually flush the current memtable to an on-disk [`SsTable`].
    pub async fn flush(&self) -> Result<(), storage::StorageError> {
        let entries = self.memtable.scan().await;
        if entries.is_empty() {
            return Ok(());
        }
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let path = format!("sstable_{id}.tbl");
        let table = sstable::SsTable::create(&path, &entries, self.storage.as_ref()).await?;
        self.memtable.clear().await;
        self.sstables.write().await.push(table);
        // reset WAL since its contents are now persisted in the SSTable
        self.wal.clear().await.map_err(storage::StorageError::Io)?;
        Ok(())
    }
}
