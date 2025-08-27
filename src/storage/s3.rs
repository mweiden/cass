use super::{Storage, StorageError};
use async_trait::async_trait;
use s3::Bucket;
use s3::Region;
use s3::creds::Credentials;
use std::io::{Error, ErrorKind};

pub struct S3Storage {
    bucket: Box<Bucket>,
}

impl S3Storage {
    pub async fn new(bucket: &str) -> Result<Self, StorageError> {
        let creds = Credentials::from_env().map_err(|e: s3::creds::error::CredentialsError| {
            StorageError::Io(Error::new(ErrorKind::Other, e.to_string()))
        })?;
        let region = Region::from_env("AWS_REGION", Some("AWS_ENDPOINT")).map_err(
            |e: s3::region::error::RegionError| {
                StorageError::Io(Error::new(ErrorKind::Other, e.to_string()))
            },
        )?;
        let bucket = Bucket::new(bucket, region, creds)
            .map_err(|e: s3::error::S3Error| {
                StorageError::Io(Error::new(ErrorKind::Other, e.to_string()))
            })?
            .with_path_style();
        Ok(Self { bucket })
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), StorageError> {
        self.bucket
            .put_object(path, &data)
            .await
            .map_err(|e| StorageError::Io(Error::new(ErrorKind::Other, e.to_string())))?;
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        let resp = self
            .bucket
            .get_object(path)
            .await
            .map_err(|e| StorageError::Io(Error::new(ErrorKind::Other, e.to_string())))?;
        if resp.status_code() != 200 {
            return Err(StorageError::Io(Error::new(
                ErrorKind::Other,
                format!("status {}", resp.status_code()),
            )));
        }
        Ok(resp.bytes().to_vec())
    }

    async fn append(&self, path: &str, data: &[u8]) -> Result<(), StorageError> {
        // S3 does not support appends; fall back to read-modify-write.
        let mut existing = self.get(path).await.unwrap_or_default();
        existing.extend_from_slice(data);
        self.put(path, existing).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let results = self
            .bucket
            .list(prefix.to_string(), None)
            .await
            .map_err(|e| StorageError::Io(Error::new(ErrorKind::Other, e.to_string())))?;
        let mut out = Vec::new();
        for res in results {
            for obj in res.contents {
                out.push(obj.key);
            }
        }
        Ok(out)
    }
}
