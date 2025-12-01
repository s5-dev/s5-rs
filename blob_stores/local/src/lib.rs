use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use s5_core::blob::location::BlobLocation;
use s5_core::store::{StoreFeatures, StoreResult};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::{ReaderStream, StreamReader};
use walkdir::WalkDir;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct LocalStoreConfig {
    pub base_path: String,
}

#[derive(Debug, Clone)]
pub struct LocalStore {
    base_path: PathBuf,
    // TODO copy_files: bool,
}

impl LocalStore {
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        LocalStore {
            base_path: base_path.into(),
        }
    }

    pub fn to_blob_store(self) -> s5_core::BlobStore {
        s5_core::BlobStore::new(self)
    }

    pub fn create(config: LocalStoreConfig) -> Self {
        LocalStore {
            base_path: config.base_path.into(),
            // TODO copy_files: config.copy_files,
        }
    }
    fn resolve_path(&self, path: &str) -> StoreResult<PathBuf> {
        if path.contains("..") || path.starts_with('/') {
            return Err(anyhow!(
                "Invalid path: '{}'. Must be a relative path without '..'.",
                path
            ));
        }
        Ok(self.base_path.join(path))
    }
}

#[async_trait::async_trait]
impl s5_core::store::Store for LocalStore {
    /// Writes a stream of bytes to a file.
    async fn put_stream(
        &self,
        path: &str,
        stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<()> {
        let full_path = self.resolve_path(path)?;
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut file = File::create(&full_path).await?;
        let mut stream_reader = StreamReader::new(stream);
        tokio::io::copy(&mut stream_reader, &mut file).await?;
        Ok(())
    }

    /// Returns the features of this store.
    fn features(&self) -> StoreFeatures {
        StoreFeatures {
            case_sensitive: false,
            recommended_max_dir_size: 1024,
            supports_rename: true,
        }
    }

    /// Checks if a file exists at the given path.
    async fn exists(&self, path: &str) -> StoreResult<bool> {
        let full_path = self.resolve_path(path)?;
        tokio::fs::try_exists(&full_path).await.map_err(Into::into)
    }

    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()> {
        let full_path = self.resolve_path(path)?;
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(&full_path, &bytes).await?;
        Ok(())
    }

    async fn open_read_stream(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>>
    {
        let full_path = self.resolve_path(path)?;
        let mut file = File::open(&full_path).await?;

        if offset > 0 {
            file.seek(std::io::SeekFrom::Start(offset)).await?;
        }

        let reader: Box<dyn tokio::io::AsyncRead + Send + Unpin> = if let Some(len) = max_len {
            Box::new(file.take(len))
        } else {
            Box::new(file)
        };

        let stream = ReaderStream::new(reader);

        Ok(Box::new(stream))
    }

    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        let full_path = self.resolve_path(path)?;
        let mut file = File::open(&full_path).await?;
        let file_len = file.metadata().await?.len();

        if offset >= file_len {
            return Ok(Bytes::new());
        }

        file.seek(std::io::SeekFrom::Start(offset)).await?;

        let len_to_read = match max_len {
            Some(len) => std::cmp::min(len, file_len - offset),
            None => file_len - offset,
        };

        let capacity =
            usize::try_from(len_to_read).context("file segment too large to read into memory")?;
        let mut buffer = Vec::with_capacity(capacity);

        file.take(len_to_read).read_to_end(&mut buffer).await?;

        Ok(Bytes::from(buffer))
    }

    async fn delete(&self, path: &str) -> StoreResult<()> {
        let full_path = self.resolve_path(path)?;
        match tokio::fs::metadata(&full_path).await {
            Ok(_metadata) => {
                tokio::fs::remove_file(&full_path).await?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    async fn rename(&self, old_path: &str, new_path: &str) -> StoreResult<()> {
        let old_full_path = self.resolve_path(old_path)?;
        let new_full_path = self.resolve_path(new_path)?;

        if let Some(parent) = new_full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::rename(&old_full_path, &new_full_path).await?;
        Ok(())
    }

    async fn provide(&self, _path: &str) -> StoreResult<Vec<BlobLocation>> {
        Ok(vec![])
    }

    async fn size(&self, path: &str) -> StoreResult<u64> {
        Ok(std::fs::metadata(&self.resolve_path(path)?)?.len())
    }

    async fn list(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>>
    {
        let base_path = self.base_path.clone();
        let walker = WalkDir::new(&base_path).into_iter();
        let stream = futures::stream::iter(walker).filter_map(move |entry| {
            futures::future::ready(match entry {
                Ok(entry) => {
                    if entry.file_type().is_file() {
                        let path = entry.path();

                        let relative_path = path.strip_prefix(&base_path).unwrap();
                        let key = relative_path.to_string_lossy().into_owned();
                        Some(Ok(key))
                    } else {
                        None
                    }
                }
                Err(e) => Some(Err(e.into())),
            })
        });

        Ok(Box::new(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s5_core::testutil::StoreTests;

    #[tokio::test]
    async fn test_local_store() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = LocalStore::new(temp_dir.path());
        StoreTests::new(&store).run_all().await.unwrap();
    }
}
