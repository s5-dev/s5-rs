use std::any::Any;

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

    /// Ensures all pending writes are durably persisted to storage.
    ///
    /// For stores backed by persistent storage (databases, filesystems), this
    /// ensures that all previously written data will survive a crash or power
    /// loss. For in-memory stores, this is a no-op.
    ///
    /// # When to call
    ///
    /// Call `sync()` at critical points where data loss would be unacceptable:
    /// - Before creating snapshots
    /// - On graceful shutdown
    /// - After completing a batch of important writes
    ///
    /// Avoid calling after every write in performance-critical code - batch
    /// writes first, then sync once.
    ///
    /// # Performance
    ///
    /// This operation may be slow as it typically involves flushing buffers
    /// and calling fsync(). The default implementation is a no-op for stores
    /// that don't support or need explicit syncing.
    async fn sync(&self) -> StoreResult<()> {
        Ok(())
    }

    /// The substrate this store's data physically lives on, for cheap
    /// cross-store migration ([`migrate`]). `None` (the default) means there is
    /// no by-reference path, so [`migrate`] falls back to client-mediated byte
    /// copy. See [`Substrate`].
    fn migration_substrate(&self) -> Option<Substrate> {
        None
    }

    /// If this store can migrate **by reference** (no byte movement) to a store
    /// on the same [`Substrate`], a handle that drives it; otherwise `None` (the
    /// default). Consulted by [`migrate`] together with
    /// [`migration_substrate`](Store::migration_substrate).
    fn as_reference_migrate(&self) -> Option<&dyn ReferenceMigrate> {
        None
    }

    /// Create a reflink (copy-on-write) clone of a source file into the store.
    ///
    /// This is used by `BlobStore::import_file` when `supports_reflink` is true.
    /// The store creates a COW copy of `source` at `dest_path` (a store-relative
    /// path). The caller (BlobStore) handles hashing and finalization.
    ///
    /// Only available on native targets. The default implementation returns
    /// an error — override only in stores that support FICLONE or similar.
    #[cfg(not(target_arch = "wasm32"))]
    async fn reflink_file_to(
        &self,
        _source: &std::path::Path,
        _dest_path: &str,
    ) -> StoreResult<()> {
        Err(anyhow::anyhow!("reflink not supported by this store"))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StoreFeatures {
    pub supports_rename: bool,
    pub case_sensitive: bool,
    pub recommended_max_dir_size: u64,
    /// Whether the store supports reflink (FICLONE) file copies.
    /// When true, `BlobStore::import_file` will try `Store::reflink_file_to`
    /// before falling back to the TeeStream path.
    pub supports_reflink: bool,
}

/// Identifies the storage substrate a [`Store`]'s bytes physically live on, so
/// two stores that share one can [`migrate`] data **by reference** (no byte
/// movement) instead of routing every byte through the client.
///
/// The substrate is the *shared medium*, not the store's address: two different
/// Sia indexers are different stores but the same [`SiaHosts`](Substrate::SiaHosts)
/// substrate — the sectors live on the host network, reachable by either — so
/// one can re-pin the other's objects without moving bytes. This is the
/// "your data outlives any single provider" property of a decentralized
/// substrate, made into an interface.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Substrate {
    /// Sia hosts on a given network (e.g. `"sia-mainnet"`). Any indexer on the
    /// same network can reference the same sectors.
    SiaHosts { network: String },
}

impl Substrate {
    /// Can a store on `self`'s substrate transfer **by reference** to a store on
    /// `other`'s? Currently exact-match; refine per-variant as substrates are
    /// added (e.g. S3 same-region copy, mutual SFTP reachability).
    pub fn can_reference_migrate_to(&self, other: &Substrate) -> bool {
        self == other
    }
}

/// By-reference migration between two stores on the same [`Substrate`] — the
/// cheap path [`migrate`] uses. The exported handle is type-erased and
/// substrate-specific (e.g. an opened Sia object holding the data key, kept
/// strictly in-process) and is consumed by `import_ref` on a same-substrate
/// store. Stores expose this via [`Store::as_reference_migrate`].
#[async_trait]
pub trait ReferenceMigrate: Send + Sync {
    /// Export the entry at `path` as an opaque, substrate-specific handle.
    async fn export_ref(&self, path: &str) -> StoreResult<Box<dyn Any + Send>>;

    /// Import a handle exported by a same-substrate store, recording it at
    /// `path`. An unexpected handle type is an error (the substrates were
    /// mislabeled).
    async fn import_ref(&self, path: &str, handle: Box<dyn Any + Send>) -> StoreResult<()>;
}

/// Outcome of [`migrate`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MigrationReport {
    /// Entries moved **by reference** (no byte copy).
    pub by_reference: u64,
    /// Entries moved by client-mediated byte copy (the fallback).
    pub by_copy: u64,
}

/// Migrate every entry from `src` to `dst`.
///
/// If both stores sit on the same migratable [`Substrate`] (and both expose
/// [`Store::as_reference_migrate`]), entries move **by reference** — no byte
/// movement (e.g. two Sia indexers re-pinning the same sectors on their hosts).
/// Otherwise each entry is **copied through the client** (a streamed read +
/// write), which always works but routes every byte through the caller.
pub async fn migrate(src: &dyn Store, dst: &dyn Store) -> StoreResult<MigrationReport> {
    use futures::StreamExt;

    let by_ref = match (src.migration_substrate(), dst.migration_substrate()) {
        (Some(s), Some(t)) if s.can_reference_migrate_to(&t) => {
            src.as_reference_migrate().zip(dst.as_reference_migrate())
        }
        _ => None,
    };

    let mut report = MigrationReport::default();
    let mut paths = src.list().await?;
    while let Some(path) = paths.next().await {
        let path = path?;
        match by_ref {
            Some((s, d)) => {
                let handle = s.export_ref(&path).await?;
                d.import_ref(&path, handle).await?;
                report.by_reference += 1;
            }
            None => {
                let reader = src.open_read_stream(&path, 0, None).await?;
                dst.put_stream(&path, reader).await?;
                report.by_copy += 1;
            }
        }
    }
    Ok(report)
}
