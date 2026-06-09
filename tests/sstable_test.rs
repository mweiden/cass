use cass::{
    sstable::SsTable,
    storage::{Storage, local::LocalStorage},
};

#[tokio::test]
async fn sstable_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let entries = vec![
        ("b".to_string(), b"2".to_vec()),
        ("a".to_string(), b"1".to_vec()),
        ("c".to_string(), b"3".to_vec()),
    ];
    let table = SsTable::create("table", &entries, &storage).await.unwrap();
    assert_eq!(table.get("a", &storage).await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(table.get("b", &storage).await.unwrap(), Some(b"2".to_vec()));
    let raw = storage.get("table").await.unwrap();
    let text = String::from_utf8(raw).unwrap();
    let keys: Vec<&str> = text
        .lines()
        .map(|l| l.split('\t').next().unwrap())
        .collect();
    assert_eq!(keys, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn out_of_bounds_index_offset_does_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let entries = vec![
        ("a".to_string(), b"1".to_vec()),
        ("b".to_string(), b"2".to_vec()),
    ];
    let mut table = SsTable::create("table", &entries, &storage).await.unwrap();
    // Simulate a corrupt/stale sparse index pointing past the end of the
    // data file. Lookups must not panic and must still find existing keys
    // by falling back to a full scan.
    table.index = vec![("a".to_string(), u64::MAX), ("b".to_string(), 1 << 40)];
    assert_eq!(table.get("a", &storage).await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(table.get("b", &storage).await.unwrap(), Some(b"2".to_vec()));
    assert_eq!(table.get("zz", &storage).await.unwrap(), None);
}

#[tokio::test]
async fn out_of_bounds_index_offset_remote_storage_does_not_panic() {
    struct NoLocal(LocalStorage);

    #[async_trait::async_trait]
    impl Storage for NoLocal {
        async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), cass::storage::StorageError> {
            self.0.put(path, data).await
        }
        async fn get(&self, path: &str) -> Result<Vec<u8>, cass::storage::StorageError> {
            self.0.get(path).await
        }
        async fn append(&self, path: &str, data: &[u8]) -> Result<(), cass::storage::StorageError> {
            self.0.append(path, data).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, cass::storage::StorageError> {
            self.0.list(prefix).await
        }
    }

    let dir = tempfile::tempdir().unwrap();
    // Hide the local path so `get` exercises the remote-storage fallback.
    let storage = NoLocal(LocalStorage::new(dir.path()));
    let entries = vec![("a".to_string(), b"1".to_vec())];
    let mut table = SsTable::create("table", &entries, &storage).await.unwrap();
    table.index = vec![("a".to_string(), u64::MAX)];
    assert_eq!(table.get("a", &storage).await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(table.get("zz", &storage).await.unwrap(), None);
}
