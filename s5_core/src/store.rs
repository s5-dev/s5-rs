use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;

use crate::blob::location::BlobLocation;

pub type StoreResult<T> = anyhow::Result<T>;

/// Abstract key-value store used by S5 components.
///
/// `Store` is a low-level, path-based storage layer that higher-level
/// components such as `BlobStore` build on. Implementations may use
/// local filesystems, cloud object stores, databases, etc.
#[async_trait]
pub trait Store: std::fmt::Debug + Send + Sync + 'static {
    async fn put_stream(
        &self,
        path: &str,
        stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<()>;

    fn features(&self) -> StoreFeatures;

    async fn exists(&self, path: &str) -> StoreResult<bool>;

    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()>;

    async fn open_read_stream(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>>;

    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes>;

    /// Returns the total size of the object at the given path.
    async fn size(&self, path: &str) -> StoreResult<u64>;

    /// Returns a stream of all object paths in the store.
    async fn list(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>>;

    async fn delete(&self, path: &str) -> StoreResult<()>;

    async fn rename(&self, old_path: &str, new_path: &str) -> StoreResult<()>;

    async fn provide(&self, path: &str) -> StoreResult<Vec<BlobLocation>>;

    /// Stores a stream to a temporary location and returns the path.
    ///
    /// The default implementation generates a random path in a `.tmp` directory.
    async fn put_temp(
        &self,
        stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<String> {
        let path = format!(".tmp/{}", uuid::Uuid::new_v4());
        self.put_stream(&path, stream).await?;
        Ok(path)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StoreFeatures {
    pub supports_rename: bool,
    pub case_sensitive: bool,
    pub recommended_max_dir_size: u64,
}
