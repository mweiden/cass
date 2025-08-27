use super::{Storage, StorageError};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

pub struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), StorageError> {
        let p = self.root.join(path);
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
        let p = self.root.join(path);
        if let Some(parent) = p.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)
            .await?;
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
