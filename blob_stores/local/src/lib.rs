use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use s5_core::blob::location::BlobLocation;
use s5_core::blob::store::BlobStore;
use s5_core::store::{StoreFeatures, StoreResult};
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::{ReaderStream, StreamReader};
use walkdir::WalkDir;

pub mod prune;

/// Atomic-write staging subdir (the tmp+rename target of `put_bytes`).
/// One source of truth: `put_bytes` writes here, `list_hashes` excludes
/// it, and `prune` never evicts from it — in-flight uploads are not
/// cached blobs and racing them breaks the atomic-write contract.
pub(crate) const TMP_SUBDIR: &str = ".tmp";

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

    pub fn to_blob_store(self) -> BlobStore {
        BlobStore::new(self)
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
            supports_reflink: true,
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

        // Atomic write via tmp+rename: write to a sibling `.tmp/blob/`
        // directory (same filesystem, so rename is atomic), then move
        // into the final key path. Keeps the CAS directory pristine —
        // no orphaned tmp files from interrupted uploads leak into
        // listings or tiered sweeps. `.tmp/` is the standard Linux
        // pattern (git, rsync, et al).
        //
        // Why tmp+rename at all: a direct `tokio::fs::write` opens with
        // O_TRUNC then streams bytes, leaving the file empty/partial
        // in the window a concurrent reader could land in. That
        // window is what made `StoreRegistry::set` (which calls `get`
        // → reads the file) observe "insufficient bytes for
        // deserialization" under concurrent publishes — the registry
        // entry was mid-rewrite. Tmp+rename closes the window:
        // readers see either the old bytes or the new bytes, never
        // partial.
        //
        // The pid+counter suffix combines cross-process uniqueness
        // (pid) with intra-process uniqueness (atomic counter — two
        // concurrent `put_bytes` calls in the same process otherwise
        // race on a shared tmp path and the second's rename fails
        // with ENOENT after the first's rename consumed the tmp).
        static TMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let counter = TMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp_dir = self.base_path.join(TMP_SUBDIR).join("blob");
        let file_name = Path::new(path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("blob");
        let tmp_path = tmp_dir.join(format!(
            "{}.tmp.{}.{}",
            file_name,
            std::process::id(),
            counter
        ));
        tokio::fs::create_dir_all(&tmp_dir).await?;
        tokio::fs::write(&tmp_path, &bytes).await?;
        tokio::fs::rename(&tmp_path, &full_path).await?;
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

    async fn modified(&self, path: &str) -> StoreResult<Option<std::time::SystemTime>> {
        Ok(Some(
            std::fs::metadata(&self.resolve_path(path)?)?.modified()?,
        ))
    }

    async fn list(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>>
    {
        let base_path = self.base_path.clone();
        let walker = WalkDir::new(&base_path).into_iter();
        let stream = futures::stream::iter(walker).filter_map(move |entry| {
            let item = match entry {
                Ok(entry) if entry.file_type().is_file() => {
                    let path = entry.path();
                    let relative_path = path.strip_prefix(&base_path).unwrap();

                    // Skip the `.tmp/` directory (atomic-write staging).
                    if relative_path.starts_with(TMP_SUBDIR) {
                        None
                    } else {
                        let key = relative_path.to_string_lossy().into_owned();
                        Some(Ok(key))
                    }
                }
                Ok(_) => None,
                Err(e) => Some(Err(std::io::Error::other(e.to_string()))),
            };
            futures::future::ready(item)
        });

        Ok(Box::new(stream))
    }

    async fn reflink_file_to(&self, source: &std::path::Path, dest_path: &str) -> StoreResult<()> {
        let full_dest = self.resolve_path(dest_path)?;
        if let Some(parent) = full_dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let source = source.to_path_buf();
        tokio::task::spawn_blocking(move || try_reflink(&source, &full_dest)).await?
    }
}

/// Attempt a reflink (FICLONE) copy from `src` to `dst`.
///
/// FICLONE is a Linux ioctl that creates a copy-on-write clone of a file.
/// On XFS with `reflink=1`, this is instant and uses zero extra disk space
/// until either file is modified. If FICLONE is not supported (wrong FS,
/// cross-device, etc.) this returns an error and the caller should fall back
/// to a regular copy.
#[cfg(target_os = "linux")]
fn try_reflink(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    // FICLONE ioctl number: _IOW(0x94, 9, int) = 0x40049409
    const FICLONE: libc::c_ulong = 0x40049409;

    let src_file = std::fs::File::open(src)
        .with_context(|| format!("reflink: failed to open source {:?}", src))?;
    let dst_file = std::fs::File::create(dst)
        .with_context(|| format!("reflink: failed to create dest {:?}", dst))?;

    let ret = unsafe { libc::ioctl(dst_file.as_raw_fd(), FICLONE, src_file.as_raw_fd()) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        // Clean up the empty destination file on failure
        let _ = std::fs::remove_file(dst);
        return Err(err)
            .with_context(|| format!("FICLONE ioctl failed for {:?} -> {:?}", src, dst));
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn try_reflink(_src: &std::path::Path, _dst: &std::path::Path) -> Result<()> {
    anyhow::bail!("reflink not supported on this platform")
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
