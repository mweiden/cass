use std::collections::BTreeMap;
use tokio::sync::RwLock;

/// Thread-safe in-memory table backed by a `BTreeMap`.
pub struct MemTable {
    /// Protected map storing raw key/value pairs.
    data: RwLock<BTreeMap<String, Vec<u8>>>,
}

impl MemTable {
    /// Create a new, empty [`MemTable`].
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
        }
    }

    /// Insert a `key`/`value` pair into the table.
    pub async fn insert(&self, key: String, value: Vec<u8>) {
        self.data.write().await.insert(key, value);
    }

    /// Retrieve the value for `key` if it exists.
    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.data.read().await.get(key).cloned()
    }

    /// Remove a `key` from the table.
    pub async fn delete(&self, key: &str) {
        self.data.write().await.remove(key);
    }

    /// Return all entries currently stored in the table.
    pub async fn scan(&self) -> Vec<(String, Vec<u8>)> {
        self.data
            .read()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Return the number of entries in the table.
    pub async fn len(&self) -> usize {
        self.data.read().await.len()
    }

    /// Atomically compare the existing value for `key` with `expected` and set
    /// it to `value` if they match.
    pub async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> bool {
        let mut map = self.data.write().await;
        match expected {
            Some(exp) => match map.get(key) {
                Some(cur) if *cur == exp => {
                    map.insert(key.to_string(), value);
                    true
                }
                _ => false,
            },
            None => {
                if map.contains_key(key) {
                    false
                } else {
                    map.insert(key.to_string(), value);
                    true
                }
            }
        }
    }

    /// Remove all entries from the table.
    pub async fn clear(&self) {
        self.data.write().await.clear();
    }

    /// Delete every key that begins with `prefix`.
    pub async fn delete_prefix(&self, prefix: &str) {
        self.data
            .write()
            .await
            .retain(|k, _| !k.starts_with(prefix));
    }
}
