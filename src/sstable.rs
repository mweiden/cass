use crate::bloom::{BloomFilter, BloomProto};
use crate::storage::{Storage, StorageError};
use crate::zonemap::{ZoneMap, ZoneMapProto};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use prost::Message;
use std::fs::File;

use memmap2::MmapOptions;

// field delimiters within on-disk table files
const SEP: u8 = b'\t';
const NL: u8 = b'\n';

// build an index entry every N keys
const INDEX_INTERVAL: usize = 16;

fn meta_path(path: &str) -> String {
    if let Some(base) = path.strip_suffix(".tbl") {
        format!("{}.meta", base)
    } else {
        format!("{}.meta", path)
    }
}

/// Simplified on-disk sorted string table used for persisting flushed
/// memtable data.
pub struct SsTable {
    /// Path to the file storing the SSTable contents.
    pub path: String,
    /// Bloom filter over the keys to quickly rule out non-existent lookups.
    pub bloom: BloomFilter,
    /// Zone map storing min/max keys for coarse filtering.
    pub zone_map: ZoneMap,
    /// Sparse index mapping keys to approximate offsets in the table file.
    pub index: Vec<(String, u64)>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct TableMeta {
    #[prost(message, optional, tag = "1")]
    bloom: Option<BloomProto>,
    #[prost(message, optional, tag = "2")]
    zone_map: Option<ZoneMapProto>,
    #[prost(message, repeated, tag = "3")]
    index: Vec<IndexEntryProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct IndexEntryProto {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(uint64, tag = "2")]
    offset: u64,
}

impl SsTable {
    /// Create an empty [`SsTable`] with default bloom filter and zone map.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            bloom: BloomFilter::new(1024),
            zone_map: ZoneMap::default(),
            index: Vec::new(),
        }
    }

    /// Write a new table to disk from the provided `entries` and return
    /// the constructed [`SsTable`] metadata.
    pub async fn create<S: Storage + Sync + Send + ?Sized>(
        path: impl Into<String>,
        entries: &[(String, Vec<u8>)],
        storage: &S,
    ) -> Result<Self, StorageError> {
        let path = path.into();
        let mut sorted = entries.to_vec();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let mut bloom = BloomFilter::new(1024);
        let mut zone_map = ZoneMap::default();
        let mut data = Vec::new();
        let mut index = Vec::new();
        let mut offset: u64 = 0;
        for (i, (k, v)) in sorted.iter().enumerate() {
            if i % INDEX_INTERVAL == 0 {
                index.push((k.clone(), offset));
            }
            // update auxiliary structures
            bloom.insert(k);
            zone_map.update(k);
            // write "key\tbase64(value)\n" lines
            data.extend_from_slice(k.as_bytes());
            data.push(SEP);
            let enc = STANDARD.encode(v);
            data.extend_from_slice(enc.as_bytes());
            data.push(NL);
            offset = data.len() as u64;
        }
        storage.put(&path, data).await?;
        let meta_path = meta_path(&path);
        let meta = TableMeta {
            bloom: Some(bloom.to_proto()),
            zone_map: Some(zone_map.to_proto()),
            index: index
                .iter()
                .map(|(k, o)| IndexEntryProto {
                    key: k.clone(),
                    offset: *o,
                })
                .collect(),
        };
        let mut buf = Vec::new();
        meta.encode(&mut buf).unwrap();
        storage.put(&meta_path, buf).await?;
        Ok(Self {
            path,
            bloom,
            zone_map,
            index,
        })
    }

    /// Load an existing table from storage rebuilding auxiliary metadata.
    pub async fn load<S: Storage + Sync + Send + ?Sized>(
        path: impl Into<String>,
        storage: &S,
    ) -> Result<Self, StorageError> {
        let path = path.into();
        let meta_path = meta_path(&path);
        if let Ok(bytes) = storage.get(&meta_path).await {
            if let Ok(meta) = TableMeta::decode(bytes.as_ref()) {
                let bloom = meta
                    .bloom
                    .map(BloomFilter::from_proto)
                    .unwrap_or_else(|| BloomFilter::new(1024));
                let zone_map = meta.zone_map.map(ZoneMap::from_proto).unwrap_or_default();
                let index = meta.index.into_iter().map(|e| (e.key, e.offset)).collect();
                return Ok(Self {
                    path,
                    bloom,
                    zone_map,
                    index,
                });
            }
        }
        let raw = storage.get(&path).await?;
        let mut bloom = BloomFilter::new(1024);
        let mut zone_map = ZoneMap::default();
        let mut index = Vec::new();
        let mut offset: u64 = 0;
        let mut i = 0;
        for line in raw.split(|b| *b == NL).filter(|l| !l.is_empty()) {
            if let Some(pos) = line.iter().position(|b| *b == SEP) {
                let key = std::str::from_utf8(&line[..pos])
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                bloom.insert(key);
                zone_map.update(key);
                if i % INDEX_INTERVAL == 0 {
                    index.push((key.to_string(), offset));
                }
                offset += line.len() as u64 + 1; // include newline
                i += 1;
            }
        }
        Ok(Self {
            path,
            bloom,
            zone_map,
            index,
        })
    }

    /// Retrieve a value from the table.
    ///
    /// The bloom filter and zone map are consulted first to avoid unnecessary
    /// I/O. If they indicate the key may exist, the SSTable is read and a
    /// binary search over the sorted entries is performed to locate the key.
    pub async fn get<S: Storage + Sync + Send + ?Sized>(
        &self,
        key: &str,
        storage: &S,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        if !self.zone_map.contains(key) || !self.bloom.may_contain(key) {
            return Ok(None);
        }
        let offset = self.seek_offset(key);
        if let Some(root) = storage.local_path() {
            let path = root.join(&self.path);
            let file = File::open(path)?;
            // memory-map the file for efficient slicing
            let mmap = unsafe { MmapOptions::new().map(&file)? };
            if let Some(enc) = Self::scan_from(&mmap[offset as usize..], key) {
                let val = STANDARD
                    .decode(enc)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                return Ok(Some(val));
            }
            return Ok(None);
        }
        // fallback to reading entire file from storage
        let raw = storage.get(&self.path).await?;
        if let Some(enc) = Self::scan_from(&raw[offset as usize..], key) {
            let val = STANDARD
                .decode(enc)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            return Ok(Some(val));
        }
        Ok(None)
    }

    /// Perform a binary search over `lines` to find `key`.
    ///
    /// Each entry in `lines` must be of the form `key\tvalue` and the slice of
    /// lines must be sorted by key in ascending order. When the target key is
    /// found the encoded value slice (the bytes after the separator) is
    /// returned. If the key is not present `None` is returned.

    /// Find the starting byte offset for a given key using the sparse index.
    fn seek_offset(&self, key: &str) -> u64 {
        if self.index.is_empty() {
            return 0;
        }
        let mut lo = 0;
        let mut hi = self.index.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.index[mid].0.as_str().cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return self.index[mid].1,
            }
        }
        if lo == 0 { 0 } else { self.index[lo - 1].1 }
    }

    /// Scan forward from `slice` searching for `key` and return the encoded
    /// value slice when found.
    fn scan_from<'a>(mut slice: &'a [u8], key: &str) -> Option<&'a [u8]> {
        while !slice.is_empty() {
            let nl_pos = match slice.iter().position(|b| *b == NL) {
                Some(p) => p,
                None => return None,
            };
            let line = &slice[..nl_pos];
            if let Some(pos) = line.iter().position(|b| *b == SEP) {
                let key_bytes = &line[..pos];
                match key_bytes.cmp(key.as_bytes()) {
                    std::cmp::Ordering::Less => {
                        slice = &slice[nl_pos + 1..];
                        continue;
                    }
                    std::cmp::Ordering::Greater => return None,
                    std::cmp::Ordering::Equal => {
                        return Some(&line[pos + 1..]);
                    }
                }
            } else {
                return None;
            }
        }
        None
    }
}
