use cass::{
    Database, SqlEngine,
    sstable::SsTable,
    storage::{Storage, StorageError, local::LocalStorage},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// Storage wrapper that counts reads of the table file and hides the local
/// path so the remote-storage code path is exercised.
struct CountingStorage {
    inner: LocalStorage,
    tbl_reads: AtomicUsize,
}

#[async_trait::async_trait]
impl Storage for CountingStorage {
    async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), StorageError> {
        self.inner.put(path, data).await
    }
    async fn get(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        if path.ends_with(".tbl") || !path.contains('.') {
            self.tbl_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(path).await
    }
    async fn append(&self, path: &str, data: &[u8]) -> Result<(), StorageError> {
        self.inner.append(path, data).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.inner.list(prefix).await
    }
}

#[tokio::test]
async fn scan_prefix_returns_only_matching_entries() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let entries = vec![
        ("a:1".to_string(), b"v1".to_vec()),
        ("a:2".to_string(), b"v2".to_vec()),
        ("ab:1".to_string(), b"x".to_vec()),
        ("b:1".to_string(), b"y".to_vec()),
    ];
    let table = SsTable::create("table", &entries, &storage).await.unwrap();

    let got = table.scan_prefix("a:", &storage).await.unwrap();
    assert_eq!(
        got,
        vec![
            ("a:1".to_string(), b"v1".to_vec()),
            ("a:2".to_string(), b"v2".to_vec()),
        ]
    );
    assert!(table.scan_prefix("zz:", &storage).await.unwrap().is_empty());
}

#[tokio::test]
async fn scan_prefix_skips_tables_outside_zone_map_without_io() {
    let dir = tempfile::tempdir().unwrap();
    let storage = CountingStorage {
        inner: LocalStorage::new(dir.path()),
        tbl_reads: AtomicUsize::new(0),
    };
    let entries = vec![
        ("b:1".to_string(), b"v1".to_vec()),
        ("b:2".to_string(), b"v2".to_vec()),
    ];
    let table = SsTable::create("table", &entries, &storage).await.unwrap();
    storage.tbl_reads.store(0, Ordering::SeqCst);

    // "a:" sorts entirely below the table's key range and "c:" entirely
    // above; both must be answered from the zone map alone.
    assert!(table.scan_prefix("a:", &storage).await.unwrap().is_empty());
    assert!(table.scan_prefix("c:", &storage).await.unwrap().is_empty());
    assert_eq!(
        storage.tbl_reads.load(Ordering::SeqCst),
        0,
        "zone-map-excluded prefixes must not read the table file"
    );

    // A matching prefix still reads the file.
    let got = table.scan_prefix("b:", &storage).await.unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(storage.tbl_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn scan_ns_merges_sstables_and_memtable() {
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(dir.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();
    engine
        .execute(&db, "CREATE TABLE t (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();
    engine
        .execute(&db, "CREATE TABLE other (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();

    // first generation, flushed to an SSTable
    engine
        .execute(&db, "INSERT INTO t (id, val) VALUES ('1', 'old')")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO t (id, val) VALUES ('2', 'keep')")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO other (id, val) VALUES ('9', 'zzz')")
        .await
        .unwrap();
    db.flush().await.unwrap();

    // second generation: overwrite one row in a newer SSTable
    engine
        .execute(&db, "INSERT INTO t (id, val) VALUES ('1', 'new')")
        .await
        .unwrap();
    db.flush().await.unwrap();

    // third generation stays in the memtable
    engine
        .execute(&db, "INSERT INTO t (id, val) VALUES ('3', 'mem')")
        .await
        .unwrap();

    let rows = db.scan_ns("t").await;
    let keys: Vec<&str> = rows.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, vec!["1", "2", "3"]);
    // newer SSTable wins over older
    let v1 = &rows[0].1;
    assert!(String::from_utf8_lossy(&v1[8..]).contains("new"));
    // rows from the other namespace are not included
    assert!(!keys.contains(&"9"));
}
