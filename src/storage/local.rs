use super::{Storage, StorageError};
use async_trait::async_trait;
use std::{collections::HashMap, path::{Path, PathBuf}};
use tokio::{io::AsyncWriteExt, sync::Mutex};

pub struct LocalStorage {
    root: PathBuf,
    // Cache of open append handles per path to avoid reopen on every append
    append_handles: Mutex<HashMap<PathBuf, std::sync::Arc<Mutex<tokio::fs::File>>>>,
}

impl LocalStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            append_handles: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), StorageError> {
        let p = self.root.join(path);
        // Invalidate any cached append handle for this path to ensure consistent state
        {
            let mut map = self.append_handles.lock().await;
            map.remove(&p);
        }
        if let Some(parent) = p.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(p, data).await?;
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        let p = self.root.join(path);
        Ok(tokio::fs::read(p).await?)
    }

    async fn append(&self, path: &str, data: &[u8]) -> Result<(), StorageError> {
        use tokio::fs::OpenOptions;
        let p = self.root.join(path);
        if let Some(parent) = p.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let handle_arc = {
            let mut map = self.append_handles.lock().await;
            if let Some(h) = map.get(&p) {
                h.clone()
            } else {
                let file = OpenOptions::new().create(true).append(true).open(&p).await?;
                let arc = std::sync::Arc::new(Mutex::new(file));
                map.insert(p.clone(), arc.clone());
                arc
            }
        };
        let mut file = handle_arc.lock().await;
        file.write_all(data).await?;
        Ok(())
    }

    fn local_path(&self) -> Option<&Path> {
        Some(&self.root)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let mut out = Vec::new();
        if !tokio::fs::try_exists(&self.root).await? {
            return Ok(out);
        }
        let mut dir = tokio::fs::read_dir(&self.root).await?;
        while let Some(entry) = dir.next_entry().await? {
            let name = entry.file_name();
            let name = name.to_string_lossy().to_string();
            if name.starts_with(prefix) {
                out.push(name);
            }
        }
        Ok(out)
    }
}
