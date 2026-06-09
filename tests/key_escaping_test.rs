use cass::{
    Database, SqlEngine,
    query::QueryOutput,
    sstable::SsTable,
    storage::{Storage, local::LocalStorage},
};
use std::sync::Arc;

const HOSTILE_KEYS: &[&str] = &[
    "with\ttab",
    "with\nnewline",
    "with\\backslash",
    "tab\tand\nnewline\\mix",
];

#[tokio::test]
async fn wal_recovery_preserves_keys_with_delimiters() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(&path));
        let db = Database::new(storage, "wal.log").await.unwrap();
        for (i, key) in HOSTILE_KEYS.iter().enumerate() {
            db.insert(key.to_string(), format!("v{i}").into_bytes())
                .await
                .unwrap();
        }
        db.insert("normal".to_string(), b"vn".to_vec()).await.unwrap();
        db.sync_wal().await.unwrap();
    }
    // Reopen: WAL replay must reconstruct the exact keys, and the record
    // following a hostile key must stay intact.
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(&path));
    let db = Database::new(storage, "wal.log").await.unwrap();
    for (i, key) in HOSTILE_KEYS.iter().enumerate() {
        assert_eq!(
            db.get(key).await.map(|b| b[8..].to_vec()),
            Some(format!("v{i}").into_bytes()),
            "key {key:?} lost or corrupted in WAL recovery"
        );
    }
    assert_eq!(
        db.get("normal").await.map(|b| b[8..].to_vec()),
        Some(b"vn".to_vec())
    );
    // No phantom rows from mis-split records.
    assert_eq!(db.scan().await.len(), HOSTILE_KEYS.len() + 1);
}

#[tokio::test]
async fn sstable_roundtrip_preserves_keys_with_delimiters() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let mut entries: Vec<(String, Vec<u8>)> = HOSTILE_KEYS
        .iter()
        .enumerate()
        .map(|(i, k)| (k.to_string(), format!("v{i}").into_bytes()))
        .collect();
    entries.push(("normal".to_string(), b"vn".to_vec()));

    let table = SsTable::create("table", &entries, &storage).await.unwrap();
    for (k, v) in &entries {
        assert_eq!(
            table.get(k, &storage).await.unwrap().as_ref(),
            Some(v),
            "key {k:?} not found after SSTable create"
        );
    }

    // Rebuild from the raw file (no meta) and via normal load.
    std::fs::remove_file(dir.path().join("table.meta")).unwrap();
    let reloaded = SsTable::load("table", &storage).await.unwrap();
    for (k, v) in &entries {
        assert_eq!(
            reloaded.get(k, &storage).await.unwrap().as_ref(),
            Some(v),
            "key {k:?} not found after SSTable reload"
        );
    }
}

#[tokio::test]
async fn flush_and_query_preserve_partition_keys_with_delimiters() {
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(dir.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();
    engine
        .execute(&db, "CREATE TABLE t (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();
    // A partition key value containing both delimiters.
    engine
        .execute(&db, "INSERT INTO t (id, val) VALUES ('a\tb\nc', 'x')")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO t (id, val) VALUES ('plain', 'y')")
        .await
        .unwrap();
    db.flush().await.unwrap();

    let out = engine
        .execute(&db, "SELECT val FROM t WHERE id = 'a\tb\nc'")
        .await
        .unwrap();
    match out {
        QueryOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["val"], "x");
        }
        _ => panic!("expected rows"),
    }

    // The neighbouring record must be unaffected.
    let out = engine
        .execute(&db, "SELECT val FROM t WHERE id = 'plain'")
        .await
        .unwrap();
    match out {
        QueryOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["val"], "y");
        }
        _ => panic!("expected rows"),
    }
}
