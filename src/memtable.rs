use std::{collections::BTreeMap, io::Cursor};

use murmur3::murmur3_32;
use tokio::sync::RwLock;

struct InnerTable {
    map: BTreeMap<String, Vec<u8>>,
    size_bytes: usize,
}

impl InnerTable {
    fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            size_bytes: 0,
        }
    }
}

struct Shard {
    data: RwLock<InnerTable>,
}

impl Shard {
    fn new() -> Self {
        Self {
            data: RwLock::new(InnerTable::new()),
        }
    }
}

/// Thread-safe, sharded in-memory memtable.
pub struct MemTable {
    shards: Vec<Shard>,
}

impl MemTable {
    /// Create a new, empty [`MemTable`].
    pub fn new() -> Self {
        let shard_count = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .clamp(1, 64);
        let shards = (0..shard_count).map(|_| Shard::new()).collect();
        Self { shards }
    }

    fn shard_for(&self, key: &str) -> usize {
        if self.shards.len() == 1 {
            return 0;
        }
        let mut cursor = Cursor::new(key.as_bytes());
        let hash = murmur3_32(&mut cursor, 0).unwrap_or(0);
        (hash as usize) % self.shards.len()
    }

    /// Insert a `key`/`value` pair into the table.
    pub async fn insert(&self, key: String, value: Vec<u8>) {
        let shard_idx = self.shard_for(&key);
        let key_len = key.len();
        let value_len = value.len();
        let mut guard = self.shards[shard_idx].data.write().await;
        match guard.map.insert(key, value) {
            Some(prev) => {
                guard.size_bytes = guard.size_bytes.saturating_sub(prev.len());
                guard.size_bytes += value_len;
            }
            None => {
                guard.size_bytes += key_len + value_len;
            }
        }
    }

    /// Insert `key` with `value` only if it does not already exist.
    pub async fn insert_if_absent(&self, key: String, value: Vec<u8>) -> bool {
        let shard_idx = self.shard_for(&key);
        let key_len = key.len();
        let value_len = value.len();
        let mut guard = self.shards[shard_idx].data.write().await;
        if guard.map.contains_key(&key) {
            false
        } else {
            guard.size_bytes += key_len + value_len;
            guard.map.insert(key, value);
            true
        }
    }

    /// Retrieve the value for `key` if it exists.
    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let shard_idx = self.shard_for(key);
        self.shards[shard_idx]
            .data
            .read()
            .await
            .map
            .get(key)
            .cloned()
    }

    /// Remove a `key` from the table.
    pub async fn delete(&self, key: &str) {
        let shard_idx = self.shard_for(key);
        let mut guard = self.shards[shard_idx].data.write().await;
        if let Some(prev) = guard.map.remove(key) {
            guard.size_bytes = guard.size_bytes.saturating_sub(key.len() + prev.len());
        }
    }

    /// Return all entries currently stored in the table.
    pub async fn scan(&self) -> Vec<(String, Vec<u8>)> {
        let mut out = Vec::new();
        for shard in &self.shards {
            let guard = shard.data.read().await;
            out.extend(guard.map.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Return the number of entries in the table.
    pub async fn len(&self) -> usize {
        let mut total = 0usize;
        for shard in &self.shards {
            total += shard.data.read().await.map.len();
        }
        total
    }

    /// Remove all entries from the table.
    pub async fn clear(&self) {
        for shard in &self.shards {
            let mut guard = shard.data.write().await;
            guard.map.clear();
            guard.size_bytes = 0;
        }
    }

    /// Delete every key that begins with `prefix`.
    pub async fn delete_prefix(&self, prefix: &str) {
        for shard in &self.shards {
            let mut guard = shard.data.write().await;
            let keys: Vec<String> = guard
                .map
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            for key in keys {
                if let Some(prev) = guard.map.remove(&key) {
                    guard.size_bytes = guard.size_bytes.saturating_sub(key.len() + prev.len());
                }
            }
        }
    }

    /// Return the total number of bytes occupied by keys and values.
    pub async fn size_bytes(&self) -> usize {
        let mut total = 0usize;
        for shard in &self.shards {
            total += shard.data.read().await.size_bytes;
        }
        total
    }
}
