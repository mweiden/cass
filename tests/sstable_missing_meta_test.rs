use cass::{sstable::SsTable, storage::local::LocalStorage};

#[tokio::test]
async fn reconstruct_metadata_when_meta_missing() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let entries = (0..20)
        .map(|i| (format!("k{:02}", i), i.to_string().into_bytes()))
        .collect::<Vec<_>>();
    let table = SsTable::create("data.tbl", &entries, &storage)
        .await
        .unwrap();

    // remove the metadata file to trigger fallback reconstruction
    std::fs::remove_file(dir.path().join("data.meta")).unwrap();

    let loaded = SsTable::load("data.tbl", &storage).await.unwrap();

    assert_eq!(loaded.bloom.to_bytes(), table.bloom.to_bytes());
    assert_eq!(loaded.zone_map.min, table.zone_map.min);
    assert_eq!(loaded.zone_map.max, table.zone_map.max);
    assert_eq!(loaded.index, table.index);
    assert_eq!(
        loaded.get("k00", &storage).await.unwrap(),
        Some(b"0".to_vec())
    );
    assert_eq!(
        loaded.get("k19", &storage).await.unwrap(),
        Some(b"19".to_vec())
    );
}
