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

fn absent_key_false_positives(table: &SsTable) -> usize {
    (0..1_000)
        .filter(|i| table.bloom.may_contain(&format!("absent-{i}")))
        .count()
}

#[tokio::test]
async fn bloom_filter_scales_with_entry_count() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let entries: Vec<(String, Vec<u8>)> = (0..10_000)
        .map(|i| (format!("key-{i:05}"), b"v".to_vec()))
        .collect();
    let table = SsTable::create("table", &entries, &storage).await.unwrap();

    // With the previous fixed 1024-bit filter nearly every absent key was a
    // false positive, defeating the filter entirely.
    let fp = absent_key_false_positives(&table);
    assert!(fp < 200, "bloom filter saturated: {fp}/1000 false positives");

    // The rebuilt filter (load without a meta file) must be sized the same
    // way.
    std::fs::remove_file(dir.path().join("table.meta")).unwrap();
    let reloaded = SsTable::load("table", &storage).await.unwrap();
    for i in 0..10_000 {
        assert!(reloaded.bloom.may_contain(&format!("key-{i:05}")));
    }
    let fp = absent_key_false_positives(&reloaded);
    assert!(
        fp < 200,
        "rebuilt bloom filter saturated: {fp}/1000 false positives"
    );
}
