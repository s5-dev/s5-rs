use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;

use crate::blob::location::BlobLocation;

pub type StoreResult<T, E = anyhow::Error> = std::result::Result<T, E>;

#[async_trait]
pub trait Store: std::fmt::Debug + Send + Sync + 'static {
    async fn put_stream(
        &self,
        path: &str,
        stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<PutResponse>;

    fn features(&self) -> StoreFeatures;

    async fn exists(&self, path: &str) -> StoreResult<bool>;

    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<PutResponse>;

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

    async fn delete(&self, path: &str) -> StoreResult<()>;

    async fn rename(&self, old_path: &str, new_path: &str) -> StoreResult<()>;

    async fn provide(&self, path: &str) -> StoreResult<Vec<BlobLocation>>;
}

pub type PutResponse = ();

pub struct StoreFeatures {
    pub supports_rename: bool,
    pub case_sensitive: bool,
    pub recommended_max_dir_size: u64,
}