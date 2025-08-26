use async_trait::async_trait;
use std::{path::Path, sync::Arc};

#[async_trait]
pub trait Storage: Send + Sync {
    async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), StorageError>;
    async fn get(&self, path: &str) -> Result<Vec<u8>, StorageError>;

    fn local_path(&self) -> Option<&Path> {
        None
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError>;
}

#[async_trait]
impl Storage for Box<dyn Storage> {
    async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), StorageError> {
        (**self).put(path, data).await
    }

    async fn get(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        (**self).get(path).await
    }

    fn local_path(&self) -> Option<&Path> {
        (**self).local_path()
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        (**self).list(prefix).await
    }
}

#[async_trait]
impl Storage for Arc<dyn Storage> {
    async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), StorageError> {
        (**self).put(path, data).await
    }

    async fn get(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        (**self).get(path).await
    }

    fn local_path(&self) -> Option<&Path> {
        (**self).local_path()
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        (**self).list(prefix).await
    }
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("unimplemented")]
    Unimplemented,
}

pub mod local;
pub mod s3;
