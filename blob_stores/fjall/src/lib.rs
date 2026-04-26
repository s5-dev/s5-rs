//! Fjall LSM-tree blob store for S5.
//!
//! Uses a single fjall `Database` with **KV separation** enabled so that
//! large blob values (>1 KiB) are stored in dedicated blob files rather than
//! inline in the LSM tree.  This eliminates the catastrophic write
//! amplification that occurs when multi-KB values are rewritten at every
//! compaction level.
//!
//! Writes use regular `Keyspace::insert()` for immediate read-after-write
//! visibility — no queues, no bulk ingestion, no sharding.

use std::io;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use fjall::{Keyspace, KeyspaceCreateOptions, KvSeparationOptions, PersistMode};
use futures::stream::{self, Stream};
use s5_core::blob::location::BlobLocation;
use s5_core::store::{Store, StoreFeatures, StoreResult};

/// A blob store backed by fjall with KV separation.
///
/// Values larger than 1 KiB are stored in separate blob files so that LSM
/// compaction only rewrites the small keys, not the data.  This gives
/// near-optimal on-disk size for content-addressed storage workloads.
pub struct FjallStore {
    db: Arc<fjall::Database>,
    blobs: Keyspace,
}

impl std::fmt::Debug for FjallStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallStore")
            .field("approximate_len", &self.blobs.approximate_len())
            .field("disk_space", &self.blobs.disk_space())
            .finish()
    }
}

impl FjallStore {
    /// Open or create a FjallStore at the given path with default 256 MiB cache.
    pub fn open<P: AsRef<Path>>(path: P) -> StoreResult<Self> {
        Self::open_with_cache(path, 256 * 1024 * 1024)
    }

    /// Open or create a FjallStore with a custom block cache size (bytes).
    pub fn open_with_cache<P: AsRef<Path>>(path: P, cache_bytes: u64) -> StoreResult<Self> {
        let db = fjall::Database::builder(path.as_ref())
            .cache_size(cache_bytes)
            .manual_journal_persist(true)
            .open()?;

        let blobs = db.keyspace("blobs", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;

        Ok(Self {
            db: Arc::new(db),
            blobs,
        })
    }

    /// Approximate number of stored blobs.
    pub fn approximate_len(&self) -> usize {
        self.blobs.approximate_len()
    }

    /// Disk space used (bytes).
    pub fn disk_space(&self) -> u64 {
        self.blobs.disk_space()
    }
}

#[async_trait]
impl Store for FjallStore {
    async fn put_stream(
        &self,
        path: &str,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<()> {
        use futures::TryStreamExt;
        let chunks: Vec<Bytes> = stream.try_collect().await?;
        let bytes = Bytes::from(chunks.concat());
        self.put_bytes(path, bytes).await
    }

    fn features(&self) -> StoreFeatures {
        StoreFeatures {
            supports_rename: true,
            case_sensitive: true,
            recommended_max_dir_size: u64::MAX,
            ..Default::default()
        }
    }

    async fn exists(&self, path: &str) -> StoreResult<bool> {
        let blobs = self.blobs.clone();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || blobs.contains_key(path.as_bytes()).map_err(Into::into))
            .await?
    }

    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()> {
        let blobs = self.blobs.clone();
        let db = self.db.clone();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || {
            blobs.insert(path.as_bytes(), bytes.as_ref())?;
            // `manual_journal_persist(true)` means inserts only land in the
            // in-memory write buffer by default; flush the journal to the OS
            // after each write so a hard crash doesn't lose acknowledged
            // blobs. Uses `Buffer` (fdatasync-equivalent flush to page cache)
            // rather than `SyncAll` (fsync) — adequate for content-addressed
            // blobs where a lost write can be re-uploaded.
            db.persist(PersistMode::Buffer)?;
            Ok(())
        })
        .await?
    }

    async fn open_read_stream(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static>>
    {
        let bytes = self.open_read_bytes(path, offset, max_len).await?;
        Ok(Box::new(stream::iter(vec![Ok(bytes)])))
    }

    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        let blobs = self.blobs.clone();
        let path = path.to_string();

        tokio::task::spawn_blocking(move || {
            let value = blobs.get(path.as_bytes())?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("blob not found: {path}"))
            })?;

            let data = value.as_ref();
            let start = offset as usize;

            if start >= data.len() {
                return Ok(Bytes::new());
            }

            let remaining = data.len() - start;
            let len = match max_len {
                Some(max) => std::cmp::min(remaining, max as usize),
                None => remaining,
            };

            Ok(Bytes::copy_from_slice(&data[start..start + len]))
        })
        .await?
    }

    async fn size(&self, path: &str) -> StoreResult<u64> {
        let blobs = self.blobs.clone();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || {
            let size = blobs.size_of(path.as_bytes())?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("blob not found: {path}"))
            })?;
            Ok(size as u64)
        })
        .await?
    }

    async fn list(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, io::Error>> + Send + Unpin + 'static>>
    {
        let blobs = self.blobs.clone();
        let keys: Vec<String> = tokio::task::spawn_blocking(move || {
            let mut keys = Vec::new();
            for entry in blobs.iter() {
                let key = entry.key().map_err(|e| io::Error::other(e.to_string()))?;
                keys.push(String::from_utf8_lossy(&key).into_owned());
            }
            StoreResult::Ok(keys)
        })
        .await??;

        Ok(Box::new(stream::iter(keys.into_iter().map(Ok))))
    }

    async fn delete(&self, path: &str) -> StoreResult<()> {
        let blobs = self.blobs.clone();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || {
            blobs.remove(path.as_bytes())?;
            Ok(())
        })
        .await?
    }

    async fn rename(&self, old_path: &str, new_path: &str) -> StoreResult<()> {
        if old_path == new_path {
            return Ok(());
        }

        let blobs = self.blobs.clone();
        let old = old_path.to_string();
        let new = new_path.to_string();

        tokio::task::spawn_blocking(move || {
            let value = blobs.get(old.as_bytes())?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("blob not found: {old}"))
            })?;
            let data = Bytes::copy_from_slice(value.as_ref());
            drop(value);

            blobs.insert(new.as_bytes(), data.as_ref())?;
            blobs.remove(old.as_bytes())?;
            Ok(())
        })
        .await?
    }

    async fn provide(&self, _path: &str) -> StoreResult<Vec<BlobLocation>> {
        // Local-only store — nothing to provide for remote fetch.
        Ok(Vec::new())
    }

    async fn sync(&self) -> StoreResult<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.persist(PersistMode::SyncAll)?;
            Ok(())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_read_write() {
        let dir = tempfile::tempdir().unwrap();
        let store = FjallStore::open(dir.path().join("blobs.fjall")).unwrap();

        let data = Bytes::from("hello world");
        store.put_bytes("test", data.clone()).await.unwrap();

        // Immediate read-after-write (no sync needed!)
        let read = store.open_read_bytes("test", 0, None).await.unwrap();
        assert_eq!(read, data);

        // Partial read
        let partial = store.open_read_bytes("test", 6, None).await.unwrap();
        assert_eq!(partial, Bytes::from("world"));

        // Size
        assert_eq!(store.size("test").await.unwrap(), 11);
    }

    #[tokio::test]
    async fn test_exists() {
        let dir = tempfile::tempdir().unwrap();
        let store = FjallStore::open(dir.path().join("blobs.fjall")).unwrap();

        assert!(!store.exists("missing").await.unwrap());
        store
            .put_bytes("present", Bytes::from("data"))
            .await
            .unwrap();
        assert!(store.exists("present").await.unwrap());
    }

    #[tokio::test]
    async fn test_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = FjallStore::open(dir.path().join("blobs.fjall")).unwrap();

        store
            .put_bytes("to-delete", Bytes::from("data"))
            .await
            .unwrap();
        assert!(store.exists("to-delete").await.unwrap());

        store.delete("to-delete").await.unwrap();
        assert!(!store.exists("to-delete").await.unwrap());
    }

    #[tokio::test]
    async fn test_rename() {
        let dir = tempfile::tempdir().unwrap();
        let store = FjallStore::open(dir.path().join("blobs.fjall")).unwrap();

        let data = Bytes::from("test data");
        store.put_bytes("old-path", data.clone()).await.unwrap();

        store.rename("old-path", "new-path").await.unwrap();

        assert!(!store.exists("old-path").await.unwrap());
        assert!(store.exists("new-path").await.unwrap());

        let read = store.open_read_bytes("new-path", 0, None).await.unwrap();
        assert_eq!(read, data);
    }

    #[tokio::test]
    async fn test_list() {
        let dir = tempfile::tempdir().unwrap();
        let store = FjallStore::open(dir.path().join("blobs.fjall")).unwrap();

        for i in 0..10 {
            store
                .put_bytes(&format!("blob-{i:03}"), Bytes::from(format!("data-{i}")))
                .await
                .unwrap();
        }

        use futures::StreamExt;
        let listed: Vec<String> = store
            .list()
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(listed.len(), 10);
    }

    #[tokio::test]
    async fn test_many_writes() {
        let dir = tempfile::tempdir().unwrap();
        let store = FjallStore::open(dir.path().join("blobs.fjall")).unwrap();

        for i in 0..1000 {
            store
                .put_bytes(&format!("blob-{i:05}"), Bytes::from(format!("data-{i}")))
                .await
                .unwrap();
        }

        // Immediate visibility — no sync needed
        for i in 0..1000 {
            assert!(store.exists(&format!("blob-{i:05}")).await.unwrap());
        }

        assert!(store.approximate_len() >= 1000);
    }

    #[tokio::test]
    async fn test_sync_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blobs.fjall");

        {
            let store = FjallStore::open(&path).unwrap();
            store
                .put_bytes("durable", Bytes::from("important"))
                .await
                .unwrap();
            store.sync().await.unwrap();
        }

        // Reopen and verify data survived
        let store = FjallStore::open(&path).unwrap();
        let read = store.open_read_bytes("durable", 0, None).await.unwrap();
        assert_eq!(read, Bytes::from("important"));
    }

    #[tokio::test]
    async fn test_large_values_kv_separation() {
        let dir = tempfile::tempdir().unwrap();
        let store = FjallStore::open(dir.path().join("blobs.fjall")).unwrap();

        // Write values larger than the 1 KiB separation threshold
        let large = Bytes::from(vec![42u8; 8192]);
        store.put_bytes("large-blob", large.clone()).await.unwrap();

        let read = store.open_read_bytes("large-blob", 0, None).await.unwrap();
        assert_eq!(read, large);

        // Partial read of large value
        let partial = store
            .open_read_bytes("large-blob", 4096, Some(1024))
            .await
            .unwrap();
        assert_eq!(partial.len(), 1024);
        assert!(partial.iter().all(|&b| b == 42));
    }
}
