use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::{self, Stream, TryStreamExt};
use s5_core::{
    blob::location::BlobLocation,
    store::{PutResponse, StoreError, StoreFeatures, StoreResult},
};

use std::io;

#[derive(Debug)]
pub struct MemoryStore {
    files: DashMap<String, Bytes>,
}

impl MemoryStore {
    /// Creates a new, empty `MemoryStore`.
    pub fn new() -> Self {
        Self {
            files: DashMap::new(),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl s5_core::store::Store for MemoryStore {
    /// Consumes a stream of bytes and stores the concatenated result at the given path.
    async fn put_stream(
        &self,
        path: &str,
        stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<PutResponse> {
        let chunks: Vec<Bytes> = stream
            .try_collect()
            .await
            .map_err(|e| StoreError::Other(e.into()))?;
        let bytes = Bytes::from(chunks.concat());
        self.files.insert(path.to_string(), bytes);
        Ok(())
    }

    /// Returns the features supported by this store.
    fn features(&self) -> StoreFeatures {
        StoreFeatures {
            supports_rename: true,
            case_sensitive: true,
            recommended_max_dir_size: u64::MAX,
        }
    }

    /// Checks if an object exists at the given path.
    async fn exists(&self, path: &str) -> StoreResult<bool> {
        Ok(self.files.contains_key(path))
    }

    /// Stores a `Bytes` object at the given path.
    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<PutResponse> {
        self.files.insert(path.to_string(), bytes);
        Ok(())
    }

    /// Returns a stream that yields the bytes of the object at the given path.
    async fn open_read_stream(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>>
    {
        let bytes = self.open_read_bytes(path, offset, max_len).await?;
        let future = Box::pin(async { Ok(bytes) });
        let stream = stream::once(future);
        Ok(Box::new(stream))
    }

    /// Returns the bytes of the object at the given path.
    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        let file = self.files.get(path).ok_or(StoreError::NotFound)?;
        let file_len = file.len();
        let start = offset as usize;

        if start >= file_len {
            return Ok(Bytes::new());
        }

        let remaining = file_len - start;
        let len = match max_len {
            Some(max) => std::cmp::min(remaining, max as usize),
            None => remaining,
        };
        let end = start + len;

        Ok(file.slice(start..end))
    }

    /// Returns the total size of the object at the given path.
    async fn size(&self, path: &str) -> StoreResult<u64> {
        let file = self.files.get(path).ok_or(StoreError::NotFound)?;
        Ok(file.len() as u64)
    }

    /// Returns a stream of all object paths in the store.
    async fn list(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>>
    {
        let keys: Vec<Result<String, io::Error>> = self
            .files
            .iter()
            .map(|entry| Ok(entry.key().clone()))
            .collect();
        let stream = stream::iter(keys);
        Ok(Box::new(stream))
    }

    /// Deletes the object at the given path.
    async fn delete(&self, path: &str) -> StoreResult<()> {
        self.files.remove(path).ok_or(StoreError::NotFound)?;
        Ok(())
    }

    /// Renames an object from an old path to a new path.
    async fn rename(&self, old_path: &str, new_path: &str) -> StoreResult<()> {
        if old_path == new_path {
            return Ok(());
        }
        let (_key, value) = self.files.remove(old_path).ok_or(StoreError::NotFound)?;
        self.files.insert(new_path.to_string(), value);
        Ok(())
    }

    /// Returns locations for a blob. For an in-memory store, this is always empty.
    async fn provide(&self, path: &str) -> StoreResult<Vec<BlobLocation>> {
        if !self.files.contains_key(path) {
            return Err(StoreError::NotFound.into());
        }
        Ok(vec![])
    }
}
