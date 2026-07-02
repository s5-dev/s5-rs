pub mod cached;
pub mod fallback;
pub mod identifier;
pub mod import;
pub mod location;
pub mod paths;
pub mod read;
pub mod store;
pub mod tee;
pub mod verify;

pub use identifier::BlobId;
pub use location::BlobLocation;
pub use store::BlobStore;
pub use verify::{VerifyingReader, verify_bytes};

use crate::Hash;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::io;
use tokio::io::AsyncRead;

/// Stream of reachable hashes consumed by `BlobsDelete::blob_retain`.
pub type ReachableStream = Box<dyn Stream<Item = Hash> + Send + Unpin>;

/// Point-in-time staging gauges for a write-buffering backend.
///
/// A packing store buffers small writes in a staging WAL and folds them into
/// durable packs on a flush loop, so bytes returned by `blob_upload_*` are not
/// yet durable. This is the honesty surface for that gap — `vup status` /
/// `vup doctor` show staged-but-not-durable bytes rather than implying every
/// accepted write reached durable storage.
///
/// [`BlobsWrite::staging_stats`] returns `None` for a backend with no staging
/// layer (a direct path store): its writes are durable the moment they return,
/// so there is nothing to report.
#[derive(Clone, Copy, Debug)]
pub struct StagingStats {
    /// Bytes sitting in the staging WAL, not yet inside a durable pack.
    pub staged_bytes: u64,
    /// Seconds since the last pack flush completed successfully (or since the
    /// store opened, if none has yet).
    pub since_last_flush_secs: u64,
    /// A pack upload is currently in flight.
    pub inflight: bool,
}

#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

pub type BlobResult<T> = anyhow::Result<T>;

/// High-level async read interface for content-addressed blobs.
///
/// # Integrity contract
///
/// Implementations MUST guarantee that a **full** blob read —
/// [`blob_download`](Self::blob_download) and
/// [`blob_read`](Self::blob_read) — never completes successfully with
/// bytes that do not BLAKE3-hash to the requested [`Hash`]. Buffered
/// impls use [`verify::verify_bytes`]; streaming impls wrap the reader
/// in [`verify::VerifyingReader`] (the mismatch surfaces as
/// `InvalidData` at EOF). Wrappers/combinators that delegate to another
/// `BlobsRead` inherit the guarantee.
///
/// [`blob_download_slice`](Self::blob_download_slice) is **exempt** for
/// partial ranges (a slice can't be checked without the whole blob);
/// callers that need verified partial reads must verify at a higher
/// layer (e.g. bao). Path-addressed [`Store`](crate::store::Store)
/// implementations make no content-address promise at all — their
/// integrity is signature-based. This split is decided in
/// `docs/reference/architecture-decisions-2026-07-01.md` (D4).
#[async_trait]
pub trait BlobsRead: Sync + Send {
    /// Returns true if the blob exists.
    async fn blob_contains(&self, hash: Hash) -> BlobResult<bool>;

    /// Returns the size of the blob in bytes.
    async fn blob_get_size(&self, hash: Hash) -> BlobResult<u64>;

    /// Downloads a full blob into memory, verified against `hash`
    /// (see the trait-level integrity contract).
    async fn blob_download(&self, hash: Hash) -> BlobResult<Bytes>;

    /// Downloads a slice of a blob to memory, starting from `offset`
    /// (inclusive) with optional maximum length `max_len`.
    ///
    /// Partial ranges are NOT content-verified (see the trait-level
    /// integrity contract); a full-range call (`offset == 0`,
    /// `max_len == None`) is equivalent to `blob_download` and is.
    async fn blob_download_slice(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> BlobResult<Bytes>;

    /// Returns an async reader for the blob contents, verified against
    /// `hash` by EOF (see the trait-level integrity contract).
    async fn blob_read(&self, hash: Hash) -> BlobResult<Box<dyn AsyncRead + Send + Unpin>>;
}

/// High-level async write interface for content-addressed blobs.
#[async_trait]
pub trait BlobsWrite: Sync + Send {
    /// Upload a small blob of bytes.
    async fn blob_upload_bytes(&self, bytes: Bytes) -> BlobResult<BlobId>;

    /// Upload a blob from a reader.
    ///
    /// Not available through `dyn BlobsWrite` (requires `Self: Sized`).
    async fn blob_upload_reader<R, F>(
        &self,
        hash: Hash,
        size: u64,
        reader: R,
        on_progress: F,
    ) -> BlobResult<BlobId>
    where
        Self: Sized,
        R: AsyncRead + Send + Unpin + 'static,
        F: Fn(u64) -> io::Result<()> + Send + Sync + 'static;

    /// Upload a blob from a streaming source of bytes.
    ///
    /// Not available through `dyn BlobsWrite` (requires `Self: Sized`).
    async fn blob_upload_stream<S>(&self, stream: S) -> BlobResult<BlobId>
    where
        Self: Sized,
        S: Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static;

    /// Upload a local file as a blob.
    #[cfg(not(target_arch = "wasm32"))]
    async fn blob_upload_file(&self, path: PathBuf) -> BlobResult<BlobId>;

    /// Flush pending writes to durable storage.
    ///
    /// The default implementation is a no-op. Backends that buffer writes
    /// (e.g. local-disk stores) should override this.
    async fn blob_sync(&self) -> BlobResult<()> {
        Ok(())
    }

    /// Point-in-time staging gauges, when the backend buffers writes before
    /// they reach durable storage (a packing store). `None` (the default)
    /// means writes are durable on return — there is no staging layer to
    /// report. Surfaced by `vup status` / `vup doctor`; see [`StagingStats`].
    fn staging_stats(&self) -> Option<StagingStats> {
        None
    }
}

/// Combined read + write interface for content-addressed blobs.
///
/// This is the primary trait for code that needs to both read and write
/// blobs by hash without caring about storage layout (paths, directories,
/// etc.).  Any type that implements both `BlobsRead` and `BlobsWrite`
/// automatically implements `BlobsReadWrite`.
pub trait BlobsReadWrite: BlobsRead + BlobsWrite {}

impl<T: BlobsRead + BlobsWrite> BlobsReadWrite for T {}

/// Deletion interface for content-addressed blobs.
///
/// Kept separate from `BlobsRead`/`BlobsWrite` because deletion has very
/// different semantics: it is irreversible, GC-flavored, and has retention
/// implications. Most components (e.g. `s5_fs_v2`) only care about reading
/// and writing — reclamation is a layer/concern initiated externally by
/// checking what is still reachable.
#[async_trait]
pub trait BlobsDelete: Sync + Send {
    /// Hint to the store that this hash is no longer referenced and may
    /// be reclaimed. Idempotent; not-found is `Ok(())`. Whether the
    /// underlying storage is freed immediately or deferred is
    /// store-specific (e.g. a packing store may append to a deletion log
    /// and reclaim later when whole packs become dead).
    async fn blob_delete(&self, hash: Hash) -> BlobResult<()>;

    /// Optional batch reachability hint: "the set of hashes I still
    /// consider reachable is this — everything else may be reclaimed."
    ///
    /// Stores that can do efficient bulk reachability sweeps implement
    /// this. The default falls back to per-hash `blob_delete` calls, but
    /// only after collecting an inventory of what the store knows about,
    /// so the default impl returns `NotSupported` to make the absence
    /// loud — implementers should override.
    async fn blob_retain(&self, _reachable: ReachableStream) -> BlobResult<()> {
        Err(anyhow::anyhow!("blob_retain not supported by this store"))
    }
}

/// Lazily-yielded stream of every stored blob hash, returned by
/// [`BlobsList::list_hashes`]. Each item is a `BlobResult` so a backend that
/// fails partway through enumeration surfaces the error rather than silently
/// truncating the inventory — mirrors [`Store::list`](crate::store::Store::list).
pub type HashStream = Box<dyn Stream<Item = BlobResult<Hash>> + Send + Unpin>;

/// Enumeration capability for blob stores — streaming every stored hash.
///
/// Separate from read/write because it is fundamentally a *backend* property:
/// a content-addressed, path-based store (`BlobStore` over a `Store`) can walk
/// its `blob3/` prefix, but a store that keeps only truncated keys (e.g. a
/// `PackingStore`, 12-byte prefixes) cannot recover full hashes and so does not
/// implement this. Used by the cold-store GC to inventory what exists.
///
/// It **streams** rather than returning a `Vec` so a full inventory (millions
/// of hashes at TiB scale) never has to materialize in memory at once — the GC
/// can process and reclaim each hash as it arrives, holding only the reachable
/// set. `BlobStore` keeps an inherent `list_hashes() -> Vec<Hash>` convenience
/// that simply collects this one stream, for callers that want the whole set.
#[async_trait]
pub trait BlobsList: Sync + Send {
    /// Stream every blob hash currently stored; order is backend-defined.
    async fn list_hashes(&self) -> BlobResult<HashStream>;
}

/// The vault-facing blob interface: read + write + delete by hash, as a single
/// trait object. A thin combination so `Arc<dyn Blobs>` works — any
/// `BlobsReadWrite + BlobsDelete` is a `Blobs` automatically. This is what a
/// vault holds for its store, so a `BlobStore` (over a `Store`) and a
/// `PackingStore` (content-addressed packs) are interchangeable behind it.
pub trait Blobs: BlobsReadWrite + BlobsDelete {}

impl<T: BlobsReadWrite + BlobsDelete + ?Sized> Blobs for T {}
