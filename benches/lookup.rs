use base64::{Engine, engine::general_purpose::STANDARD};
use cass::{
    sstable::SsTable,
    storage::{Storage, StorageError, local::LocalStorage},
};
use criterion::{Criterion, criterion_group, criterion_main};

const SEP: u8 = b'\t';
const NL: u8 = b'\n';

async fn old_lookup(
    path: &str,
    storage: &LocalStorage,
    key: &str,
) -> Result<Option<Vec<u8>>, StorageError> {
    let raw = storage.get(path).await?;
    let lines: Vec<&[u8]> = raw
        .split(|b| *b == NL)
        .filter(|line| !line.is_empty())
        .collect();
    let mut lo = 0;
    let mut hi = lines.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        let line = lines[mid];
        if let Some(pos) = line.iter().position(|b| *b == SEP) {
            match line[..pos].cmp(key.as_bytes()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let val = STANDARD
                        .decode(&line[pos + 1..])
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                    return Ok(Some(val));
                }
            }
        } else {
            break;
        }
    }
    Ok(None)
}

fn bench_lookup(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let entries: Vec<(String, Vec<u8>)> = (0..1000)
        .map(|i| (format!("k{:04}", i), vec![b'x'; 8]))
        .collect();
    let table = rt
        .block_on(SsTable::create("table.tbl", &entries, &storage))
        .unwrap();
    let key = "k0500";
    c.bench_function("lookup_indexed", |b| {
        b.iter(|| {
            rt.block_on(table.get(key, &storage)).unwrap();
        })
    });
    c.bench_function("lookup_old", |b| {
        b.iter(|| {
            rt.block_on(old_lookup(&table.path, &storage, key)).unwrap();
        })
    });
}

criterion_group!(benches, bench_lookup);
criterion_main!(benches);
