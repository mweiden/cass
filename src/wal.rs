use base64::{Engine, engine::general_purpose::STANDARD};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::storage::{Storage, StorageError};

/// Basic write-ahead log persisted via the configured storage backend.
pub struct Wal {
    path: String,
    storage: Arc<dyn Storage>,
    buf: Mutex<Vec<u8>>,
}

fn parse_entries(data: &[u8]) -> std::io::Result<Vec<(String, Vec<u8>)>> {
    let mut res = Vec::new();
    for line in data.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        if let Some(pos) = line.iter().position(|b| *b == b'\t') {
            let key = std::str::from_utf8(&line[..pos])
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
                .to_string();
            let val = STANDARD
                .decode(&line[pos + 1..])
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            res.push((key, val));
        }
    }
    Ok(res)
}

fn map_err(e: StorageError) -> std::io::Error {
    match e {
        StorageError::Io(e) => e,
        StorageError::Unimplemented => {
            std::io::Error::new(std::io::ErrorKind::Other, "unimplemented")
        }
    }
}

impl Wal {
    /// Create or open a log at `path`, returning the WAL instance and any
    /// existing entries to be replayed into the memtable.
    pub async fn new(
        storage: Arc<dyn Storage>,
        path: impl Into<String>,
    ) -> std::io::Result<(Self, Vec<(String, Vec<u8>)>)> {
        let path = path.into();
        let data = storage.get(&path).await.unwrap_or_default();
        let entries = parse_entries(&data)?;
        Ok((
            Self {
                path,
                storage,
                buf: Mutex::new(data),
            },
            entries,
        ))
    }

    /// Append a line of `data` to the log and persist it through the storage
    /// backend.
    pub async fn append(&self, data: &[u8]) -> std::io::Result<()> {
        let mut buf = self.buf.lock().await;
        let start = buf.len();
        buf.extend_from_slice(data);
        buf.push(b'\n');
        self.storage
            .append(&self.path, &buf[start..])
            .await
            .map_err(map_err)?;
        Ok(())
    }

    /// Remove all data from the log by truncating the underlying storage
    /// object.
    pub async fn clear(&self) -> std::io::Result<()> {
        let mut buf = self.buf.lock().await;
        buf.clear();
        self.storage
            .put(&self.path, Vec::new())
            .await
            .map_err(map_err)?;
        Ok(())
    }
}
