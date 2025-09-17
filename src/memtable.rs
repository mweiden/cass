use std::collections::BTreeMap;
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

/// Thread-safe in-memory table backed by a `BTreeMap`.
pub struct MemTable {
    /// Protected map storing raw key/value pairs alongside accumulated size.
    data: RwLock<InnerTable>,
}

impl MemTable {
    /// Create a new, empty [`MemTable`].
    pub fn new() -> Self {
        Self {
            data: RwLock::new(InnerTable::new()),
        }
    }

    /// Insert a `key`/`value` pair into the table.
    pub async fn insert(&self, key: String, value: Vec<u8>) {
        let key_len = key.len();
        let value_len = value.len();
        let mut guard = self.data.write().await;
        match guard.map.insert(key, value) {
            Some(prev) => {
                // Key already accounted for; just adjust for value delta.
                guard.size_bytes = guard.size_bytes.saturating_sub(prev.len());
                guard.size_bytes += value_len;
            }
            None => {
                guard.size_bytes += key_len + value_len;
            }
        }
    }

    /// Insert `key` with `value` only if it does not already exist.
    ///
    /// Returns `true` if the value was inserted, `false` if the key was
    /// already present. This helper enables simple compare-and-set semantics
    /// for lightweight transactions where we need to ensure a row is only
    /// written once.
    pub async fn insert_if_absent(&self, key: String, value: Vec<u8>) -> bool {
        let key_len = key.len();
        let value_len = value.len();
        let mut guard = self.data.write().await;
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
        self.data.read().await.map.get(key).cloned()
    }

    /// Remove a `key` from the table.
    pub async fn delete(&self, key: &str) {
        let mut guard = self.data.write().await;
        if let Some(prev) = guard.map.remove(key) {
            guard.size_bytes = guard.size_bytes.saturating_sub(key.len() + prev.len());
        }
    }

    /// Return all entries currently stored in the table.
    pub async fn scan(&self) -> Vec<(String, Vec<u8>)> {
        self.data
            .read()
            .await
            .map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Return the number of entries in the table.
    pub async fn len(&self) -> usize {
        self.data.read().await.map.len()
    }

    /// Remove all entries from the table.
    pub async fn clear(&self) {
        let mut guard = self.data.write().await;
        guard.map.clear();
        guard.size_bytes = 0;
    }

    /// Delete every key that begins with `prefix`.
    pub async fn delete_prefix(&self, prefix: &str) {
        let mut guard = self.data.write().await;
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

    /// Return the total number of bytes occupied by keys and values.
    pub async fn size_bytes(&self) -> usize {
        self.data.read().await.size_bytes
    }
}
