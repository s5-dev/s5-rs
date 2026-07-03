//! Immutable prolly tree snapshot — the main runtime type for FS5 V2.
//!
//! [`Snapshot`] is the single runtime type that combines:
//! - A prolly tree root (hash + plaintext hash)
//! - A blob store reference for fetching/uploading
//! - An on-wire [`TraversalContext`](crate::node::TraversalContext) carrying
//!   encryption keys and blob processing pipelines
//!
//! It implements [`ReadableLayer`] for tree queries, and provides methods
//! for node loading, context derivation, file import/export, and recursive
//! namespace traversal.
//!
//! # Tree Structure
//!
//! - Level 0 (leaf): entries are actual data (`NodeEntry`s)
//! - Level 1+ (internal): entries are `Link`s to child nodes, keyed by
//!   the first key in each child.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use s5_core::{BlobId, BlobsRead, BlobsWrite, Hash};

use crate::context::{self, KDF_META};
use crate::layer::ReadableLayer;
use crate::node::{
    BlobPipeline, CompressionStrategy, ContentRef, EncryptionStrategy, Node, NodeEntry, NodeKind,
    PaddingStrategy, SemanticMeta, Structural, TraversalContext,
};
use crate::pipeline::NodeCache;

/// Master secret key slot in the on-wire `TraversalContext.keys` map.
const KEY_SLOT_MASTER: u8 = 0x0e;

/// Leaf blob encryption key slot (file content / chunk data).
pub const KEY_SLOT_LEAF: u8 = 0x10;

/// Node blob encryption key slot (serialized metadata).
pub const KEY_SLOT_NODE: u8 = 0x11;

/// Recovery seed slot (vault root only). Holds the per-vault
/// `recovery_secret` from which `vault_id` and `recovery_signing_key`
/// are derived. No blob pipeline references this slot — it is pure
/// derivation material, not an encryption key. See
/// `docs/reference/snapshot-publication.md` § Vault ID derivation.
pub const KEY_SLOT_RECOVERY: u8 = 0x12;

/// Per-blob inlined leaf key slot used by the D21 `copy` mechanism
/// ([`crate::copy`]). A shallow copy stores each reused leaf's *per-blob*
/// ChaCha20 key here — NOT the source master — inside the entry's
/// `child_context`, referenced by an
/// [`ExplicitKeyChaCha20`](crate::node::EncryptionStrategy::ExplicitKeyChaCha20)
/// leaf pipeline. Deliberately DISTINCT from [`KEY_SLOT_LEAF`] so the
/// destination vault's own leaf master is never touched or overwritten when
/// this slot is merged into a child context.
pub const KEY_SLOT_EXPLICIT_LEAF: u8 = 0x13;

/// Default padding block size (4 KiB).
///
/// After compression and before encryption, blob bytes are zero-padded up to
/// the next multiple of 4096. This is the universal filesystem block size
/// (ext4/XFS/Btrfs/NTFS/APFS/ZFS ashift) — aligned blobs avoid partial-block
/// writes, read-modify-write overhead, and extent-map fragmentation on every
/// major filesystem. An observer sees the padded length on the wire; the
/// plaintext length is unknowable without the decryption key.
///
/// 4 KiB is chosen because:
///
/// - At ~64 KiB average CDC chunks (see [`chunking`](crate::chunking)),
///   4 KiB alignment adds ~2.6 % padding waste on random data and ~1.5 % on
///   structured data — negligible at any scale.
/// - Every modern filesystem allocates in 4 KiB blocks minimum. Blobs that
///   aren't 4 KiB-aligned waste the tail of the last block and add extent
///   fragmentation. Aligned blobs enable direct I/O.
/// - Coarser alignments (16/64 KiB) were benchmarked and rejected: they
///   add 6–42 % overhead with no meaningful privacy gain once download order
///   is randomized per file.
/// - Download order is randomized per file, so an observer watching
///   sequential chunk requests cannot correlate chunk-size sequences to
///   specific known files — making precise size fingerprinting moot.
///
/// Internal prolly-tree node blobs inherit this same padding from the node
/// pipeline. Node blobs (typically 1–8 KiB) pay a higher relative overhead
/// but this is negligible against total vault storage.
const DEFAULT_PAD_BLOCK_SIZE: u32 = 4096;

/// Internal traversal event used by `walk_inner` to drive both
/// `walk()` (entries-only) and `walk_hashes()` (hashes-only) without
/// duplicating tree-traversal logic. Not part of the public API —
/// public consumers see one stream type or the other, never this
/// enum's branches.
#[allow(clippy::large_enum_variant)]
enum WalkEvent {
    Hash(Hash),
    Entry(String, NodeEntry),
}

/// An immutable snapshot backed by a prolly tree in the blob store.
///
/// This is the main runtime type — it holds everything needed to load,
/// decrypt, traverse, import, and export FS5 data.
pub struct Snapshot {
    /// Root hash of the prolly tree (CAS address).
    root: Hash,
    /// Plaintext hash of the root node (needed for encrypted metadata KDF).
    root_plaintext_hash: Option<[u8; 32]>,
    /// Blob store for fetching and uploading.
    store: Arc<dyn BlobsRead>,
    /// On-wire context: keys and blob processing pipelines. Includes
    /// the per-pipeline `skip_when_unhelpful` policy — there is no
    /// runtime flag duplicating it on `Snapshot`.
    ctx: TraversalContext,
    /// Decoded node cache — avoids repeated blob_download + decrypt +
    /// decompress + CBOR parse for the same prolly tree node. Shared
    /// across clones (and with derived `Pipeline`s via
    /// [`Pipeline::with_node_cache`]) so concurrent `is_changed()`
    /// calls and pipeline loads see each other's hits.
    node_cache: Arc<NodeCache>,
    /// Max leaf chunks encoded + uploaded concurrently by
    /// [`Self::import_stream_with_prev`]. Bounds the in-flight
    /// `spawn_blocking` encode + upload window (the CDC chunker itself
    /// stays serial); working set is ~`import_concurrency × chunk_size`.
    /// Override via [`Self::with_import_concurrency`].
    import_concurrency: usize,
}

/// Default chunk-pipeline depth: ~1/3 of available cores (min 1), so the
/// encode+upload pipeline overlaps a few chunks without monopolising the
/// machine or blowing the `depth × chunk_size` memory window.
fn default_import_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get() / 3)
        .unwrap_or(0)
        .max(1)
}

impl Snapshot {
    // =====================================================================
    // Constructors
    // =====================================================================

    /// Creates a snapshot from its parts.
    ///
    /// `root_plaintext_hash` is needed when metadata is encrypted — it comes
    /// from the parent entry that links to this snapshot's root.
    /// Pass `None` for unencrypted trees or the top-level root.
    pub fn new(
        root: Hash,
        store: Arc<dyn BlobsRead>,
        ctx: TraversalContext,
        root_plaintext_hash: Option<[u8; 32]>,
    ) -> Self {
        Self {
            root,
            root_plaintext_hash,
            store,
            ctx,
            node_cache: Arc::new(NodeCache::new()),
            import_concurrency: default_import_concurrency(),
        }
    }

    /// Creates an unencrypted, uncompressed snapshot.
    pub fn new_plain(root: Hash, store: Arc<dyn BlobsRead>) -> Self {
        Self::new(root, store, TraversalContext::default(), None)
    }

    /// Creates an empty snapshot (no root in the store yet).
    ///
    /// Used for first-time backup where there is no previous tree to diff
    /// against. `load_root()` returns an empty `Node` for empty snapshots.
    pub fn empty(store: Arc<dyn BlobsRead>, ctx: TraversalContext) -> Self {
        Self {
            root: Hash::from([0u8; 32]),
            root_plaintext_hash: None,
            store,
            ctx,
            node_cache: Arc::new(NodeCache::new()),
            import_concurrency: default_import_concurrency(),
        }
    }

    /// Creates an empty, unencrypted snapshot.
    pub fn empty_plain(store: Arc<dyn BlobsRead>) -> Self {
        Self::empty(store, TraversalContext::default())
    }

    /// Creates an empty PLAINTEXT-tree snapshot that nonetheless carries a
    /// `KEY_SLOT_RECOVERY` slot in its root context.
    ///
    /// The tree stays plaintext — no leaf/node encryption keys or pipelines, so
    /// blobs are written as plaintext CBOR readable by anonymous consumers. The
    /// ONLY addition over [`Self::empty_plain`] is `recovery_secret` in
    /// `KEY_SLOT_RECOVERY`: pure derivation material for `vault_id` + the
    /// recovery signing key (no pipeline references it). The publish path
    /// REQUIRES that slot, so a plaintext vault that will be PUBLISHED
    /// (`plaintext_published_tn`) must use this rather than [`Self::empty_plain`]
    /// — otherwise its root has no recovery slot and every publish fails with
    /// "vault root TraversalContext has no KEY_SLOT_RECOVERY slot". The caller
    /// generates `recovery_secret` securely (e.g. `rand::rngs::OsRng`).
    pub fn empty_plain_with_recovery(store: Arc<dyn BlobsRead>, recovery_secret: [u8; 32]) -> Self {
        let mut keys = BTreeMap::new();
        keys.insert(KEY_SLOT_RECOVERY, recovery_secret);
        let ctx = TraversalContext {
            keys: Some(keys),
            // No leaf/node pipelines → tree stays plaintext; the recovery slot is
            // unreferenced, derivation-only material for vault_id.
            ..Default::default()
        };
        Self::empty(store, ctx)
    }

    /// Creates an empty, encrypted snapshot with default pipelines.
    ///
    /// Both leaf and node pipelines use Zstd compression, 4KiB padding,
    /// and DeterministicChaCha20 encryption with the given master secret.
    /// Compression is automatically skipped for blobs where it yields no
    /// storage savings after padding.
    pub fn empty_encrypted(store: Arc<dyn BlobsRead>, master_secret: [u8; 32]) -> Self {
        // The skip-when-unhelpful policy is set on the leaf pipeline
        // inside `encrypted_context` — no runtime flag to flip here.
        Self::empty(store, encrypted_context(master_secret))
    }

    /// Creates an empty, encrypted snapshot with separate leaf and node keys
    /// plus a recovery seed (vault root shape).
    ///
    /// Both pipelines use Zstd compression, 4 KiB padding, and
    /// DeterministicChaCha20 encryption — but with independent keys so
    /// that metadata and content are cryptographically separated.
    /// Compression is automatically skipped for blobs where it yields no
    /// storage savings after padding.
    ///
    /// `recovery_secret` is stored in `KEY_SLOT_RECOVERY` and is the seed
    /// from which `vault_id` and `recovery_signing_key` are derived.
    ///
    /// The caller is responsible for generating all three values
    /// securely (e.g. `rand::rngs::OsRng`).
    pub fn empty_encrypted_split(
        store: Arc<dyn BlobsRead>,
        leaf_key: [u8; 32],
        node_key: [u8; 32],
        recovery_secret: [u8; 32],
    ) -> Self {
        // The skip-when-unhelpful policy is set on the leaf pipeline
        // inside `encrypted_split_context` — no runtime flag to flip here.
        Self::empty(
            store,
            encrypted_split_context(leaf_key, node_key, recovery_secret),
        )
    }

    /// Creates an encrypted snapshot with default pipelines.
    ///
    /// Both leaf and node pipelines use Zstd compression, 4KiB padding,
    /// and DeterministicChaCha20 encryption with the given master secret.
    pub fn new_encrypted(
        root: Hash,
        store: Arc<dyn BlobsRead>,
        master_secret: [u8; 32],
        root_plaintext_hash: Option<[u8; 32]>,
    ) -> Self {
        Self::new(
            root,
            store,
            encrypted_context(master_secret),
            root_plaintext_hash,
        )
    }

    // =====================================================================
    // Accessors
    // =====================================================================

    /// Returns the root hash.
    pub fn root(&self) -> Hash {
        self.root
    }

    /// Returns true if this is an empty snapshot (no tree in the store).
    pub fn is_empty(&self) -> bool {
        self.root == Hash::from([0u8; 32])
    }

    /// Returns the root plaintext hash.
    pub fn root_plaintext_hash(&self) -> Option<&[u8; 32]> {
        self.root_plaintext_hash.as_ref()
    }

    /// Returns the on-wire traversal context.
    pub fn context(&self) -> &TraversalContext {
        &self.ctx
    }

    /// Returns the blob store reference.
    pub fn store(&self) -> &Arc<dyn BlobsRead> {
        &self.store
    }

    /// Resolves the master secret from the key map (slot 0x0e).
    pub fn master_secret(&self) -> Option<&[u8; 32]> {
        self.ctx
            .keys
            .as_ref()
            .and_then(|keys| keys.get(&KEY_SLOT_MASTER))
    }

    /// Set the leaf-pipeline `skip_when_unhelpful` policy in
    /// `self.ctx` (builder style). Equivalent to mutating
    /// `self.ctx.leaf.skip_when_unhelpful` directly. Has no effect if
    /// `self.ctx.leaf` is `None` (no leaf pipeline to set the policy
    /// on, e.g. the unencrypted/uncompressed default context).
    pub fn with_skip_unhelpful_compression(mut self, skip: bool) -> Self {
        if let Some(leaf) = self.ctx.leaf.as_mut() {
            leaf.skip_when_unhelpful = Some(skip);
        }
        self
    }

    /// Sets the in-flight chunk-pipeline depth used by
    /// [`Self::import_stream_with_prev`] (and the override/prev variants
    /// that delegate to it). `1` restores fully-serial import (one chunk
    /// encoded + uploaded at a time, the original working-set ceiling).
    /// Values above the host core count rarely help once uploads overlap.
    /// Clamped to `>= 1`. Carried across [`Self::child`]/`with_ctx`
    /// derivations so a configured depth applies to nested imports too.
    pub fn with_import_concurrency(mut self, concurrency: usize) -> Self {
        self.import_concurrency = concurrency.max(1);
        self
    }

    /// Returns the leaf blob pipeline.
    pub fn leaf_pipeline(&self) -> Option<&BlobPipeline> {
        self.ctx.leaf.as_ref()
    }

    /// Returns the node blob pipeline.
    pub fn node_pipeline(&self) -> Option<&BlobPipeline> {
        self.ctx.node.as_ref()
    }

    /// Build a [`Pipeline`](crate::pipeline::Pipeline) view of this
    /// snapshot's blob-level state — the read store, traversal context,
    /// and decoded-node cache. The cache is shared (`Arc::clone`), so
    /// loads via the pipeline and via the snapshot's own internal
    /// callers see each other's hits. Public so layered consumers
    /// (e.g. a writable FUSE adapter) can hold a `Pipeline` instead of
    /// a full `Snapshot`.
    pub fn as_pipeline(&self) -> crate::pipeline::Pipeline {
        crate::pipeline::Pipeline::with_node_cache(
            Arc::clone(&self.store),
            self.ctx.clone(),
            Arc::clone(&self.node_cache),
        )
    }

    // =====================================================================
    // Context Derivation
    // =====================================================================

    /// Returns a clone of this snapshot with a different `TraversalContext`.
    ///
    /// Used by [`Snapshot::import_stream_with_override`] to encode a
    /// single file under a per-call merged context without mutating the
    /// caller's snapshot. The cloned snapshot shares store, root, and
    /// node cache with the original — only `ctx` differs.
    pub(crate) fn with_ctx(&self, ctx: TraversalContext) -> Self {
        Self {
            root: self.root,
            root_plaintext_hash: self.root_plaintext_hash,
            store: self.store.clone(),
            ctx,
            node_cache: self.node_cache.clone(),
            import_concurrency: self.import_concurrency,
        }
    }

    /// Derives a child snapshot from a [`NodeEntry`].
    ///
    /// The child inherits this snapshot's context, with the entry's
    /// `child_context` taking priority where present. The child is
    /// rooted at `entry.hash()`.
    ///
    /// Panics if the entry is a tombstone (no content).
    pub fn child(&self, entry: &NodeEntry) -> Self {
        let content = entry
            .content
            .as_ref()
            .expect("child() called on tombstone entry");

        let child_ctx = match entry.child_context.as_ref() {
            Some(child_tcx) => merge_contexts(&self.ctx, child_tcx),
            None => self.ctx.clone(),
        };

        Self {
            root: content.hash(),
            root_plaintext_hash: content.plaintext_hash,
            store: self.store.clone(),
            ctx: child_ctx,
            node_cache: self.node_cache.clone(),
            import_concurrency: self.import_concurrency,
        }
    }

    // =====================================================================
    // Node Loading
    // =====================================================================

    /// Loads and decodes a [`Node`] from the blob store. Delegates to
    /// [`Pipeline::load`](crate::pipeline::Pipeline::load) — see that
    /// for the cache + decode details.
    pub async fn load(
        &self,
        hash: Hash,
        plaintext_hash: Option<&[u8; 32]>,
    ) -> anyhow::Result<Arc<Node>> {
        self.as_pipeline().load(hash, plaintext_hash).await
    }

    /// Loads the root node of this snapshot.
    ///
    /// Returns an empty `Node` for empty snapshots (no tree in the store).
    pub async fn load_root(&self) -> anyhow::Result<Arc<Node>> {
        if self.is_empty() {
            return Ok(Arc::new(Node::new()));
        }
        self.load(self.root, self.root_plaintext_hash.as_ref())
            .await
    }

    // =====================================================================
    // Node Writing
    // =====================================================================

    /// Serializes, compresses, encrypts, and uploads a [`Node`].
    ///
    /// Returns the `BlobId` (hash + size) of the uploaded blob, plus
    /// the plaintext hash needed by the parent entry.
    ///
    /// External callers should use [`merge_and_persist`](Self::merge_and_persist)
    /// instead — this is an internal building block.
    #[allow(dead_code)] // Kept for tests and future internal callers.
    pub(crate) async fn write_node(
        &self,
        node: &Node,
        store: &dyn BlobsWrite,
    ) -> anyhow::Result<(BlobId, [u8; 32])> {
        let cbor = node
            .to_bytes()
            .map_err(|e| anyhow::anyhow!("encoding Node: {e}"))?;

        let plaintext_hash: [u8; 32] = *blake3::hash(&cbor).as_bytes();

        let result = context::pipeline_encode(
            &cbor,
            self.ctx.node.as_ref(),
            &plaintext_hash,
            KDF_META,
            self.ctx.keys.as_ref(),
        )?;

        let blob_id = store
            .blob_upload_bytes(result.bytes)
            .await
            .map_err(|e| anyhow::anyhow!("uploading Node: {e}"))?;

        Ok((blob_id, plaintext_hash))
    }

    // =====================================================================
    // File Import
    // =====================================================================

    /// Reads an async stream, splits it into leaf-blob chunks per the
    /// strategy in `self.ctx.chunking`, and uploads blobs.
    ///
    /// Strategy dispatch (`TraversalContext.chunking`):
    /// - `None` (the `Option`): default to Xet CDC — backwards compatible
    ///   with the original chunking-only-supports-CDC behaviour.
    /// - `Some(FileChunkingStrategy::DataCdc { .. })`: Xet gearhash CDC,
    ///   ~64 KiB target. The `params` field is currently informational —
    ///   the runtime always uses the spec-fixed Xet constants
    ///   (`MIN_CHUNK_SIZE` / `MAX_CHUNK_SIZE` / `MASK`) so chunk
    ///   boundaries stay reproducible across implementations.
    /// - `Some(FileChunkingStrategy::Fixed { chunk_size })`: fixed-size
    ///   slicing. Each chunk is exactly `chunk_size` bytes except the
    ///   last (= remainder; no padding).
    /// - `Some(FileChunkingStrategy::None)`: shorthand for
    ///   `Fixed { chunk_size: u32::MAX }` — single blob per file (up to
    ///   the 4 GiB u32 ceiling).
    ///
    /// If the stream is empty, returns a zero-length leaf entry. If only
    /// one chunk is produced, returns a `Leaf` `NodeEntry`. Otherwise
    /// builds a `ByteStream` prolly tree of leaves.
    pub async fn import_stream<R: tokio::io::AsyncRead + std::marker::Unpin>(
        &self,
        stream: R,
        store: &dyn BlobsWrite,
        semantic: Option<SemanticMeta>,
    ) -> anyhow::Result<NodeEntry> {
        self.import_stream_with_prev(stream, store, semantic, &[])
            .await
    }

    /// Walk a chunked-file entry's ByteStream tree and return its chunks
    /// in order. Caller uses the result as the `prev_chunks` argument to
    /// [`import_stream_with_prev`] for per-chunk dedup.
    ///
    /// Returns an empty Vec for entries that are not `Structural::Link`
    /// (single-leaf files, directories, symlinks, tombstones).
    pub async fn collect_byte_stream_chunks(
        &self,
        entry: &NodeEntry,
    ) -> anyhow::Result<Vec<NodeEntry>> {
        let Some(content) = entry.content.as_ref() else {
            return Ok(Vec::new());
        };
        if content.structural != Structural::Link {
            return Ok(Vec::new());
        }
        let pipe = self.as_pipeline();
        let mut stream = pipe.walk_byte_stream(content.hash(), content.plaintext_hash);
        let mut chunks = Vec::new();
        while let Some(item) = stream.next().await {
            chunks.push(item?);
        }
        Ok(chunks)
    }

    /// Like [`import_stream`], but consults `prev_chunks` (in chunk
    /// order) for per-chunk dedup. When a new chunk's plaintext BLAKE3
    /// matches the prev chunk at the same position, the prev
    /// [`NodeEntry`] is reused as-is — skipping compression, encryption,
    /// and the blob-store upload.
    ///
    /// Pass `&[]` to disable dedup (equivalent to [`import_stream`]).
    ///
    /// Per-chunk dedup applies universally: every chunk encodes
    /// independently, so a positionally-matched chunk is always safe to
    /// reuse. (The `ZstdDictFromPrecedingEntry` dictionary-chunk scheme
    /// that once forced a fallback here was purged — see git history.)
    ///
    /// TODO (append-aware / "Layer 2"): for the dominant append-only
    /// workload (feedy's `.seg`/`.eseg`/`.ril` files), we still pay
    /// O(file_size) of read + CDC + BLAKE3 even when only a few KB
    /// were appended. The chunker can start at the previous EOF
    /// boundary, dedup the unchanged prefix via the existing
    /// `prev_chunks` API, and only chunk the appended tail. Estimated
    /// reduction: ~16 s steady-state cycle → sub-second on appended
    /// files. Needs a new `import_stream_append(stream, store, semantic,
    /// prev_chunks, resume_from_offset)` variant and a way for the
    /// caller to know "this file only grew."
    pub async fn import_stream_with_prev<R: tokio::io::AsyncRead + std::marker::Unpin>(
        &self,
        stream: R,
        store: &dyn BlobsWrite,
        semantic: Option<SemanticMeta>,
        prev_chunks: &[NodeEntry],
    ) -> anyhow::Result<NodeEntry> {
        // Dispatch on the requested chunking strategy. We wrap both
        // chunkers in an enum so the rest of `import_stream` stays
        // chunker-agnostic — only `next_chunk()` is exercised.
        enum LeafChunker<R> {
            Xet(crate::chunking::XetChunker<R>),
            Fixed(crate::chunking::FixedChunker<R>),
        }
        impl<R: tokio::io::AsyncRead + std::marker::Unpin> LeafChunker<R> {
            async fn next_chunk(&mut self) -> std::io::Result<Option<bytes::Bytes>> {
                match self {
                    Self::Xet(c) => c.next_chunk().await,
                    Self::Fixed(c) => c.next_chunk().await,
                }
            }
        }

        let mut chunker = match self.ctx.chunking.clone() {
            None | Some(crate::node::FileChunkingStrategy::DataCdc { .. }) => {
                LeafChunker::Xet(crate::chunking::XetChunker::new(stream))
            }
            Some(crate::node::FileChunkingStrategy::Fixed { chunk_size }) => LeafChunker::Fixed(
                crate::chunking::FixedChunker::new(stream, chunk_size as usize),
            ),
            Some(crate::node::FileChunkingStrategy::None) => LeafChunker::Fixed(
                crate::chunking::FixedChunker::new(stream, u32::MAX as usize),
            ),
        };

        // Peek first chunk to see if the file is tiny.
        let t_read = std::time::Instant::now();
        let first_chunk = match chunker.next_chunk().await {
            Ok(Some(c)) => c,
            Ok(None) => {
                // Empty file.
                return self.import_bytes(&[], store, semantic).await;
            }
            Err(e) => return Err(anyhow::anyhow!("chunker error: {e}")),
        };
        crate::import_stats::add_read(t_read.elapsed().as_nanos() as u64, first_chunk.len() as u64);

        let t_read = std::time::Instant::now();
        let second_opt = chunker
            .next_chunk()
            .await
            .map_err(|e| anyhow::anyhow!("chunker error: {e}"))?;
        if let Some(ref c) = second_opt {
            crate::import_stats::add_read(t_read.elapsed().as_nanos() as u64, c.len() as u64);
        }
        let Some(second_chunk) = second_opt else {
            // Single-chunk new file. If the prev was also single-chunk
            // (one entry in `prev_chunks`) and the plaintext hash
            // matches, reuse the prev entry's content directly. See the
            // multi-chunk loop below for the plain-vs-transformed
            // plaintext-hash derivation.
            //
            // In practice this branch rarely fires: `collect_byte_stream_chunks`
            // returns empty for `Structural::Leaf` entries (single-chunk
            // prev files), and Link-shaped prev_chunks always have ≥2
            // entries. Kept defensive.
            let plaintext_hash: [u8; 32] = *blake3::hash(&first_chunk).as_bytes();
            if let [prev] = prev_chunks
                && let Some(prev_content) = prev.content.as_ref()
                && prev_content.plaintext_hash.unwrap_or(prev_content.hash) == plaintext_hash
            {
                return Ok(NodeEntry {
                    content: prev.content.clone(),
                    semantic,
                    child_context: prev.child_context.clone(),
                    tombstone: None,
                });
            }
            return self.import_bytes(&first_chunk, store, semantic).await;
        };

        // Multi-chunk file. The CDC chunker stays serial (boundary
        // finding is sequential), but each fresh chunk's CPU-bound encode
        // runs on a `spawn_blocking` thread and its upload overlaps the
        // next chunk's work. `FuturesOrdered` yields results in offset
        // order, so the stored bytes/CIDs/tree are byte-identical to a
        // serial import — completion order only changes *when* an entry is
        // emitted. At most `import_concurrency` chunks are in flight
        // (working set ~`import_concurrency × chunk_size`; `1` restores
        // the original single-chunk ceiling). Per-chunk dedup is decided
        // here in the serial producer; a hit skips encode + upload.
        let dedup_enabled = !prev_chunks.is_empty();
        let mut dedup_hits: u64 = 0;
        let mut dedup_bytes_saved: u64 = 0;

        let concurrency = self.import_concurrency.max(1);
        let cfg = Arc::new(crate::pipeline::LeafEncodeConfig::from_ctx(&self.ctx));

        // One chunk job, unified into a single enum so `FuturesOrdered`'s
        // element type stays concrete (no boxing). Allow the size gap:
        // one job exists at a time, so boxing the `Dedup` arm would only
        // add an alloc on the hot unchanged-chunk path.
        #[allow(clippy::large_enum_variant)]
        enum ChunkJob {
            Dedup(NodeEntry),
            Encode(Bytes),
        }

        let mut entries: Vec<(String, NodeEntry)> = Vec::new();
        let mut inflight = stream::FuturesOrdered::new();
        let mut offset: u64 = 0;
        let mut chunk_index: usize = 0;

        // The first two chunks were already read above (to disambiguate
        // empty / single-leaf / multi-chunk). Process them in order,
        // then continue pulling from the chunker until EOF.
        let prefix = [first_chunk, second_chunk];
        let mut prefix_iter = prefix.into_iter();

        loop {
            // Bound the in-flight window *before* pulling/encoding more,
            // so at most `concurrency` chunk buffers are alive at once.
            while inflight.len() >= concurrency {
                if let Some(res) = inflight.next().await {
                    entries.push(res?);
                }
            }

            let chunk = match prefix_iter.next() {
                Some(c) => c,
                None => {
                    let t_read = std::time::Instant::now();
                    let next = chunker
                        .next_chunk()
                        .await
                        .map_err(|e| anyhow::anyhow!("chunker error: {e}"))?;
                    match next {
                        Some(c) => {
                            crate::import_stats::add_read(
                                t_read.elapsed().as_nanos() as u64,
                                c.len() as u64,
                            );
                            c
                        }
                        None => break,
                    }
                }
            };

            let chunk_len = chunk.len() as u64;
            let key = format!("{:016x}", offset);

            // Dedup against prev file's chunk at this position (cheap,
            // serial). The reference hash depends on whether the prev
            // chunk went through any pipeline transforms when written:
            //   - has_transforms (compression/encryption): the prev's
            //     plaintext BLAKE3 is stored separately in
            //     `content.plaintext_hash`.
            //   - no transforms (plain passthrough — feedy's case):
            //     `plaintext_hash` is `None` because the blob CID itself
            //     IS BLAKE3(plaintext). Compare against `content.hash`.
            let job = if dedup_enabled {
                let t_hash = std::time::Instant::now();
                let plaintext_hash: [u8; 32] = *blake3::hash(&chunk).as_bytes();
                crate::import_stats::add_hash(t_hash.elapsed().as_nanos() as u64);
                match prev_chunks.get(chunk_index).and_then(|prev| {
                    let prev_content = prev.content.as_ref()?;
                    let prev_pt_hash = prev_content.plaintext_hash.unwrap_or(prev_content.hash);
                    (prev_pt_hash == plaintext_hash).then_some(prev)
                }) {
                    Some(prev) => {
                        dedup_hits += 1;
                        dedup_bytes_saved += chunk_len;
                        crate::import_stats::add_dedup_hit(chunk_len);
                        ChunkJob::Dedup(prev.clone())
                    }
                    None => ChunkJob::Encode(chunk),
                }
            } else {
                ChunkJob::Encode(chunk)
            };

            let cfg = Arc::clone(&cfg);
            inflight.push_back(async move {
                let entry = match job {
                    ChunkJob::Dedup(entry) => entry,
                    ChunkJob::Encode(chunk) => {
                        let t_enc = std::time::Instant::now();
                        let enc = tokio::task::spawn_blocking(move || {
                            crate::pipeline::encode_leaf(&cfg, &chunk)
                        })
                        .await
                        .map_err(|e| anyhow::anyhow!("chunk encode task panicked: {e}"))??;
                        let blob_id = store
                            .blob_upload_bytes(enc.bytes.clone())
                            .await
                            .map_err(|e| anyhow::anyhow!("uploading chunk blob: {e}"))?;
                        let assembled = crate::pipeline::assemble_leaf_entry(
                            &enc,
                            *blob_id.hash.as_bytes(),
                            None,
                        );
                        crate::import_stats::add_encode(t_enc.elapsed().as_nanos() as u64);
                        assembled
                    }
                };
                Ok::<(String, NodeEntry), anyhow::Error>((key, entry))
            });

            offset += chunk_len;
            chunk_index += 1;
        }

        // Drain the remaining in-flight chunks (in submission order).
        while let Some(res) = inflight.next().await {
            entries.push(res?);
        }

        let total_size = offset;
        // Dedup counters consumed by callers via the surface; reserved
        // for an `ImportStats` return type in a follow-up. For now silence
        // unused-variable warnings while keeping the increments live so a
        // follow-up reads them with no further surgery.
        let _ = (dedup_hits, dedup_bytes_saved);

        // We build a `NodeKind::ByteStream` tree from the chunks.
        // We use a default mask of 0x3F (64 entries per node on average).
        let mask = 0x3F;
        let leaf_nodes =
            crate::persist::chunk_entries(&entries, mask, &crate::node::NodeKind::ByteStream, 0);

        let mut stats = crate::persist::MergeStats::default();
        let t_tree = std::time::Instant::now();
        let (root_hash, root_plaintext_hash) = self
            .as_pipeline()
            .build_tree_dedup(
                leaf_nodes,
                store,
                &crate::node::NodeKind::ByteStream,
                mask,
                &mut stats,
            )
            .await?;
        crate::import_stats::add_tree(t_tree.elapsed().as_nanos() as u64);

        // The returned NodeEntry points to the root of the ByteStream tree.
        Ok(NodeEntry {
            content: Some(ContentRef {
                structural: Structural::Link,
                hash: *root_hash.as_bytes(),
                size: total_size,
                plaintext_hash: Some(root_plaintext_hash),
                stored_blocks: None, // Omit for Link, or we could aggregate from stats
            }),
            semantic,
            child_context: None,
            tombstone: None,
        })
    }

    /// Same as [`Self::import_stream`], but encodes the file under
    /// `merge_contexts(self.ctx, override_ctx)` and stamps
    /// `entry.child_context = Some(override_ctx.clone())` on the
    /// returned entry so future reads cascade the same merged context.
    ///
    /// The override is what gets *stamped* on the entry — not the
    /// merged write context — because cascade-via-`merge_contexts` at
    /// read time will reconstruct the same merged ctx from
    /// (parent_snapshot.ctx + entry.child_context). Stamping only the
    /// override keeps the override compact (no echo of inherited
    /// fields) and keeps the merge semantics symmetric across writes
    /// and reads.
    ///
    /// If the entry already has a `child_context` set by a downstream
    /// codec hook (e.g. `skip_when_unhelpful` flipping
    /// `compression: Uncompressed`), the override is *merged into* it
    /// here rather than replacing it — so a route forcing
    /// `chunking: Fixed(8 MiB)` composes cleanly with an inner
    /// auto-Uncompressed flip.
    ///
    /// Used by `s5_fs_local::backup` when the ingest task carries a
    /// per-route pipeline override (see `NodeConfigVault.pipelines`).
    pub async fn import_stream_with_override<R: tokio::io::AsyncRead + std::marker::Unpin>(
        &self,
        stream: R,
        store: &dyn BlobsWrite,
        semantic: Option<SemanticMeta>,
        override_ctx: &TraversalContext,
    ) -> anyhow::Result<NodeEntry> {
        self.import_stream_with_override_and_prev(stream, store, semantic, override_ctx, &[])
            .await
    }

    /// Like [`import_stream_with_override`], but with per-chunk dedup
    /// against `prev_chunks` (see [`import_stream_with_prev`]).
    pub async fn import_stream_with_override_and_prev<
        R: tokio::io::AsyncRead + std::marker::Unpin,
    >(
        &self,
        stream: R,
        store: &dyn BlobsWrite,
        semantic: Option<SemanticMeta>,
        override_ctx: &TraversalContext,
        prev_chunks: &[NodeEntry],
    ) -> anyhow::Result<NodeEntry> {
        let merged = merge_contexts(&self.ctx, override_ctx);
        let with_override = self.with_ctx(merged);
        let mut entry = with_override
            .import_stream_with_prev(stream, store, semantic, prev_chunks)
            .await?;
        entry.child_context = Some(Box::new(match entry.child_context.take() {
            Some(existing) => merge_contexts(override_ctx, &existing),
            None => override_ctx.clone(),
        }));
        Ok(entry)
    }

    /// Append-aware import for FIXED-chunked, APPEND-ONLY files (#3).
    ///
    /// Fixed chunk `i` is always bytes `[i·chunk_size, (i+1)·chunk_size)`,
    /// independent of any later content — so for a file that only ever GROWS
    /// (interner_packs, ledger `.ril`, sealed segments), every leading FULL
    /// chunk is byte-identical to the previous import. We REUSE the maximal
    /// full-chunk prefix from `prev_chunks` by reference (no read, no BLAKE3,
    /// no upload) and only `seek` to the first non-full prev chunk and chunk
    /// the tail. The 1.4 GB `dids_plc.bin` re-read every publish (the bulk of
    /// the snap's `walk_ms`, measured 2026-06-17 s5) drops to ~one chunk.
    ///
    /// CALLER MUST GUARANTEE the file is append-only (the leading bytes never
    /// change) — gated on a per-route `append_only` hint. The result is
    /// byte-identical to a full [`import_stream_with_prev`] of the same bytes
    /// (same chunk entries → same `chunk_entries` tree → same root); proven by
    /// `append_import_matches_full_oracle` and re-checked live by the
    /// `S5_STRUCTURAL_MERGE_VERIFY` soak's root comparison.
    ///
    /// Falls back to the full read path whenever the fast path can't safely
    /// apply: non-Fixed chunking, no reusable full-chunk prefix, a file at or
    /// below one chunk (the single-leaf vs Link shape would differ), or a file
    /// shorter than the reusable prefix (shrank ⇒ not append-only).
    ///
    /// Returns `(entry, bytes_read)` — `bytes_read` is the bytes physically
    /// read from disk this import (the appended tail on the fast path, the
    /// whole file on a fallback). It is the #3 validation signal: for an
    /// append-only file `bytes_read ≪ entry.content.size`.
    pub async fn import_file_append(
        &self,
        path: &std::path::Path,
        store: &dyn BlobsWrite,
        semantic: Option<SemanticMeta>,
        prev_chunks: &[NodeEntry],
    ) -> anyhow::Result<(NodeEntry, u64)> {
        use tokio::io::AsyncSeekExt;

        // Whole-file read → bytes_read = the file's content size.
        let whole = |e: NodeEntry| {
            let n = e.content.as_ref().map(|c| c.size).unwrap_or(0);
            (e, n)
        };

        // Only Fixed chunking is positional (prefix-stable). Anything else
        // (CDC/None) → canonical full path.
        let chunk_size = match self.ctx.chunking.clone() {
            Some(crate::node::FileChunkingStrategy::Fixed { chunk_size }) => chunk_size as usize,
            _ => {
                let f = tokio::fs::File::open(path).await?;
                return Ok(whole(
                    self.import_stream_with_prev(f, store, semantic, prev_chunks)
                        .await?,
                ));
            }
        };

        // Maximal prefix of FULL prev chunks (size == chunk_size, leaf). A
        // partial (last) chunk can grow, so it is NOT reused.
        let mut k = 0usize;
        for c in prev_chunks {
            match c.content.as_ref() {
                Some(cr) if cr.size as usize == chunk_size && cr.structural == Structural::Leaf => {
                    k += 1
                }
                _ => break,
            }
        }

        let resume = (k as u64) * (chunk_size as u64);
        let mut file = tokio::fs::File::open(path).await?;
        let file_len = file.metadata().await?.len();

        // Fall back when the fast path can't safely apply:
        //  - k == 0: nothing reusable (single-leaf prev / first chunk partial).
        //  - file_len <= chunk_size: full import yields a single Leaf, not a
        //    Link tree — our Link-from-entries shape would diverge.
        //  - file_len < resume: file shrank below the prefix ⇒ not append-only.
        if k == 0 || file_len <= chunk_size as u64 || file_len < resume {
            file.seek(std::io::SeekFrom::Start(0)).await?;
            return Ok(whole(
                self.import_stream_with_prev(file, store, semantic, prev_chunks)
                    .await?,
            ));
        }

        file.seek(std::io::SeekFrom::Start(resume)).await?;

        // Reuse the full-chunk prefix verbatim (key = offset hex, exactly as
        // `import_stream_with_prev` keys its chunks).
        let mut entries: Vec<(String, NodeEntry)> = Vec::with_capacity(k + 4);
        for (i, c) in prev_chunks.iter().take(k).enumerate() {
            entries.push((
                format!("{:016x}", (i as u64) * (chunk_size as u64)),
                c.clone(),
            ));
        }

        // Chunk the tail [resume, EOF) serially (it's small) and dedup against
        // the remaining prev chunks — identical hash logic to the full path.
        let cfg = Arc::new(crate::pipeline::LeafEncodeConfig::from_ctx(&self.ctx));
        let mut chunker = crate::chunking::FixedChunker::new(file, chunk_size);
        let mut offset = resume;
        let mut tail_index = 0usize;
        loop {
            let t_read = std::time::Instant::now();
            let Some(chunk) = chunker
                .next_chunk()
                .await
                .map_err(|e| anyhow::anyhow!("chunker error: {e}"))?
            else {
                break;
            };
            let chunk_len = chunk.len() as u64;
            crate::import_stats::add_read(t_read.elapsed().as_nanos() as u64, chunk_len);
            let key = format!("{:016x}", offset);
            let t_hash = std::time::Instant::now();
            let plaintext_hash: [u8; 32] = *blake3::hash(&chunk).as_bytes();
            crate::import_stats::add_hash(t_hash.elapsed().as_nanos() as u64);
            let reuse = prev_chunks.get(k + tail_index).and_then(|p| {
                let pc = p.content.as_ref()?;
                (pc.plaintext_hash.unwrap_or(pc.hash) == plaintext_hash).then_some(p)
            });
            let entry = match reuse {
                Some(p) => {
                    crate::import_stats::add_dedup_hit(chunk_len);
                    p.clone()
                }
                None => {
                    let t_enc = std::time::Instant::now();
                    let cfg = Arc::clone(&cfg);
                    let chunk2 = chunk.clone();
                    let enc = tokio::task::spawn_blocking(move || {
                        crate::pipeline::encode_leaf(&cfg, &chunk2)
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("chunk encode task panicked: {e}"))??;
                    let blob_id = store
                        .blob_upload_bytes(enc.bytes.clone())
                        .await
                        .map_err(|e| anyhow::anyhow!("uploading chunk blob: {e}"))?;
                    let assembled =
                        crate::pipeline::assemble_leaf_entry(&enc, *blob_id.hash.as_bytes(), None);
                    crate::import_stats::add_encode(t_enc.elapsed().as_nanos() as u64);
                    assembled
                }
            };
            entries.push((key, entry));
            offset += chunk_len;
            tail_index += 1;
        }
        let total_size = offset;

        // Identical tree build to `import_stream_with_prev`.
        let mask = 0x3F;
        let leaf_nodes =
            crate::persist::chunk_entries(&entries, mask, &crate::node::NodeKind::ByteStream, 0);
        let mut stats = crate::persist::MergeStats::default();
        let t_tree = std::time::Instant::now();
        let (root_hash, root_plaintext_hash) = self
            .as_pipeline()
            .build_tree_dedup(
                leaf_nodes,
                store,
                &crate::node::NodeKind::ByteStream,
                mask,
                &mut stats,
            )
            .await?;
        crate::import_stats::add_tree(t_tree.elapsed().as_nanos() as u64);
        // bytes_read = the appended tail only (the prefix was reused by ref).
        let bytes_read = total_size.saturating_sub(resume);
        let content = ContentRef {
            structural: Structural::Link,
            hash: *root_hash.as_bytes(),
            size: total_size,
            plaintext_hash: Some(root_plaintext_hash),
            stored_blocks: None,
        };

        // #3 VERIFY SOAK (S5_APPEND_IMPORT_VERIFY): the structural-merge verify
        // CANNOT catch a bad append import — both merge paths consume the SAME
        // diff produced here, so a wrong NodeEntry yields matching roots and
        // slips through. This is #3's dedicated safety net: re-read the file,
        // assert the fast-path root matches a full import, and publish the
        // proven FULL result on mismatch. Drop the flag after a clean soak.
        //
        // CRITICAL (2026-06-17 s5): read EXACTLY `total_size` bytes (`.take`),
        // NOT to the current EOF. The append-only files this path serves
        // (dids_plc.bin, .ril) are appended to CONTINUOUSLY by the live writer,
        // so a fresh full read races ahead of the append's tail read and sees
        // EXTRA bytes — diverging by exactly the appended entries (a 16 B
        // dids_plc.bin entry was the observed delta). That is a TOCTOU artifact
        // of comparing two reads of a moving target, NOT an append bug (proven
        // live: the diverging leaf was always the tail chunk, off by one entry).
        // `.take(total_size)` pins the comparison to the SAME byte range the
        // append captured — the bytes below `total_size` are already durable
        // (append-only ⇒ immutable), so this is now a valid same-bytes check.
        if append_import_verify_enabled() {
            use tokio::io::AsyncReadExt as _;
            let f = tokio::fs::File::open(path).await?;
            let full = self
                .import_stream_with_prev(f.take(total_size), store, semantic.clone(), prev_chunks)
                .await?;
            let full_hash = full.content.as_ref().map(|c| c.hash);
            if full_hash != Some(content.hash) {
                tracing::error!(
                    path = %path.display(),
                    fast = ?content.hash,
                    full = ?full_hash,
                    k, resume, total_size, file_len,
                    prev_chunks = prev_chunks.len(),
                    append_entries = entries.len(),
                    "S5_APPEND_IMPORT_VERIFY: MISMATCH — append import diverged from full read; publishing the full result (append-only assumption violated?)"
                );
                // Name the diverging field: walk BOTH result trees and log the
                // first leaf whose stored representation differs. This is the
                // ground-truth probe the offline repro couldn't trigger
                // (2026-06-17 s5 #3 hunt) — it runs against the real reloaded
                // prev + real route ctx on the live writer.
                let append_entry = NodeEntry {
                    content: Some(content.clone()),
                    semantic: semantic.clone(),
                    child_context: None,
                    tombstone: None,
                };
                if let (Ok(al), Ok(ol)) = (
                    self.collect_byte_stream_chunks(&append_entry).await,
                    self.collect_byte_stream_chunks(&full).await,
                ) {
                    tracing::error!(
                        append_leaves = al.len(),
                        full_leaves = ol.len(),
                        "S5_APPEND_IMPORT_VERIFY: leaf-count compare"
                    );
                    let mut found = false;
                    for (i, (a, o)) in al.iter().zip(ol.iter()).enumerate() {
                        let af = format!("{:?}|{:?}", a.content, a.child_context);
                        let of = format!("{:?}|{:?}", o.content, o.child_context);
                        if af != of {
                            tracing::error!(
                                idx = i,
                                append_leaf = %af,
                                full_leaf = %of,
                                "S5_APPEND_IMPORT_VERIFY: first diverging leaf"
                            );
                            found = true;
                            break;
                        }
                    }
                    if !found && al.len() != ol.len() {
                        let n = al.len().min(ol.len());
                        tracing::error!(
                            shared = n,
                            "S5_APPEND_IMPORT_VERIFY: leaves identical up to min len — divergence is leaf COUNT (tree geometry), not content"
                        );
                    }
                }
                let n = full.content.as_ref().map(|c| c.size).unwrap_or(0);
                return Ok((full, n));
            }
            tracing::debug!(path = %path.display(), "append-import verify OK");
        }

        Ok((
            NodeEntry {
                content: Some(content),
                semantic,
                child_context: None,
                tombstone: None,
            },
            bytes_read,
        ))
    }

    /// [`import_file_append`] under a per-route `override_ctx`, mirroring
    /// [`import_stream_with_override_and_prev`]: encode under the merged ctx
    /// and stamp `child_context` so reads cascade the same context.
    pub async fn import_file_append_with_override(
        &self,
        path: &std::path::Path,
        store: &dyn BlobsWrite,
        semantic: Option<SemanticMeta>,
        override_ctx: &TraversalContext,
        prev_chunks: &[NodeEntry],
    ) -> anyhow::Result<(NodeEntry, u64)> {
        let merged = merge_contexts(&self.ctx, override_ctx);
        let with_override = self.with_ctx(merged);
        let (mut entry, bytes_read) = with_override
            .import_file_append(path, store, semantic, prev_chunks)
            .await?;
        entry.child_context = Some(Box::new(match entry.child_context.take() {
            Some(existing) => merge_contexts(override_ctx, &existing),
            None => override_ctx.clone(),
        }));
        Ok((entry, bytes_read))
    }

    /// Import in-memory bytes into the blob store as a leaf entry.
    /// Delegates to
    /// [`Pipeline::import_bytes`](crate::pipeline::Pipeline::import_bytes).
    pub async fn import_bytes(
        &self,
        plaintext: &[u8],
        store: &dyn BlobsWrite,
        semantic: Option<SemanticMeta>,
    ) -> anyhow::Result<NodeEntry> {
        self.as_pipeline()
            .import_bytes(plaintext, store, semantic)
            .await
    }

    // =====================================================================
    // File Export
    // =====================================================================

    /// Download, decrypt, decompress, and verify a leaf entry's
    /// content. Delegates to
    /// [`Pipeline::export_bytes`](crate::pipeline::Pipeline::export_bytes),
    /// which handles both single-leaf and chunked-file (`Structural::Link`)
    /// cases.
    pub async fn export_bytes(&self, entry: &NodeEntry) -> anyhow::Result<Bytes> {
        self.as_pipeline().export_bytes(entry).await
    }

    /// Recursively walk the snapshot tree, yielding `(path, NodeEntry)` for
    /// every user-visible entry (files, directories, symlinks).
    ///
    /// Descends through Transparent roots and internal prolly tree levels,
    /// yielding only leaf-level Namespace entries. ByteStream (chunked file)
    /// trees are NOT surfaced as path/entry pairs — they're delivered as
    /// `Link` entries for the consumer to handle via `export_bytes`.
    ///
    /// Tombstones are skipped.
    /// Paths are built by joining namespace segments with `/`.
    pub fn walk(&self) -> BoxStream<'_, anyhow::Result<(String, NodeEntry)>> {
        let stream = self.walk_inner(self.root, self.root_plaintext_hash, String::new());
        Box::pin(async_stream::try_stream! {
            let mut s = std::pin::pin!(stream);
            while let Some(ev) = s.next().await {
                if let WalkEvent::Entry(path, entry) = ev? {
                    yield (path, entry);
                }
            }
        })
    }

    /// Walk the snapshot once and collect every reachable blob hash,
    /// truncated to 16 bytes — the per-vault reachable-set the
    /// per-blob ACL consults on `BlobsServer::handle_download`.
    ///
    /// Truncation rationale (per `docs/reference/iroh-inspirations.md`
    /// and the architecture-directions step-3 discussion): collision
    /// probability at 16 B is `N²/2¹²⁸` — negligible at 10⁹ blobs;
    /// halves memory vs full 32 B hashes; a false positive only lets a
    /// peer fetch one age-encrypted blob whose hash collides into
    /// their authorised set, still gibberish without the keys.
    ///
    /// TODO(step 3b-2): persist as a content-addressed
    /// `DerivedComputation` blob keyed by `(snapshot_root, "reachable-set", v1)`
    /// so daemons coming up cold can serve reads without re-walking.
    /// TODO(step 3b-3): incremental update via prolly-tree diff between
    /// old and new snapshots — `O(diff_size)` instead of `O(tree_size)`
    /// per publish; only viable when both snapshots are loadable
    /// (i.e. keys are warm at publish time, which they always are).
    pub async fn collect_reachable_chunks(
        &self,
    ) -> anyhow::Result<std::collections::HashSet<[u8; 16]>> {
        use futures::StreamExt;
        let mut set = std::collections::HashSet::new();
        let mut s = self.walk_hashes();
        while let Some(h) = s.next().await {
            let h = h?;
            let bytes = h.as_bytes();
            let mut t = [0u8; 16];
            t.copy_from_slice(&bytes[..16]);
            set.insert(t);
        }
        Ok(set)
    }

    /// Yield every blob hash the snapshot references — Transparent
    /// root, Namespace internal/leaf nodes, ByteStream subtree
    /// internal nodes, and chunk leaves.
    ///
    /// Drives the per-blob reachable-set ACL: a peer authorised for
    /// vault V is allowed to fetch hash H iff H appears in
    /// `walk_hashes()` for V's current snapshot. Built once per snap
    /// and invalidated when the registry HEAD advances.
    ///
    /// Shares its internal traversal with [`walk`] — both call the
    /// same recursive `walk_inner`; this method projects out the
    /// hash events while [`walk`] projects out the entry events.
    pub fn walk_hashes(&self) -> BoxStream<'_, anyhow::Result<Hash>> {
        let stream = self.walk_inner(self.root, self.root_plaintext_hash, String::new());
        Box::pin(async_stream::try_stream! {
            let mut s = std::pin::pin!(stream);
            while let Some(ev) = s.next().await {
                if let WalkEvent::Hash(h) = ev? {
                    yield h;
                }
            }
        })
    }

    fn walk_inner(
        &self,
        hash: Hash,
        plaintext_hash: Option<[u8; 32]>,
        prefix: String,
    ) -> BoxStream<'_, anyhow::Result<WalkEvent>> {
        Box::pin(async_stream::try_stream! {
            // Every visited blob — a peer needs this hash to traverse
            // the tree, so it goes in the reachable set.
            yield WalkEvent::Hash(hash);
            let node = self.load(hash, plaintext_hash.as_ref()).await?;

            match node.header.kind {
                NodeKind::Transparent => {
                    if let Some(entry) = node.transparent_entry()
                        && entry.is_link()
                    {
                        let content = entry.content.as_ref().expect("is_link implies content");
                        let child = self.child(entry);
                        let mut s = std::pin::pin!(child.walk_inner(
                            content.hash(),
                            content.plaintext_hash,
                            prefix,
                        ));
                        while let Some(item) = s.next().await {
                            yield item?;
                        }
                    } else {
                        // Transparent node without a link entry — skip.
                    }
                }
                NodeKind::Namespace => {
                    if node.header.level > 0 {
                        // Internal prolly tree node — descend to children.
                        for entry in node.entries.values() {
                            if entry.is_link() {
                                let content = entry.content.as_ref().expect("is_link implies content");
                                let mut s = std::pin::pin!(self.walk_inner(
                                    content.hash(),
                                    content.plaintext_hash,
                                    prefix.clone(),
                                ));
                                while let Some(item) = s.next().await {
                                    yield item?;
                                }
                            }
                        }
                    } else {
                        // Leaf prolly node — iterate actual namespace entries.
                        for (name, entry) in &node.entries {
                            // Skip tombstones.
                            if entry.is_tombstone() {
                                continue;
                            }

                            let path = if prefix.is_empty() {
                                name.clone()
                            } else {
                                format!("{prefix}/{name}")
                            };

                            // Yield the user-visible entry first (preserves
                            // existing `walk()` semantics).
                            yield WalkEvent::Entry(path.clone(), entry.clone());

                            // Then surface every hash reachable from
                            // this entry's content — for chunked files
                            // this descends the ByteStream subtree, for
                            // single-chunk leaves it's just one hash.
                            if let Some(content) = entry.content.as_ref() {
                                if entry.is_link() {
                                    // Link → recurse into the subtree
                                    // (ByteStream, or a sub-Namespace).
                                    let child = self.child(entry);
                                    let mut s = std::pin::pin!(child.walk_inner(
                                        content.hash(),
                                        content.plaintext_hash,
                                        path,
                                    ));
                                    while let Some(item) = s.next().await {
                                        yield item?;
                                    }
                                } else if entry.is_leaf() {
                                    // Single raw chunk — emit its hash
                                    // without trying to load it as a Node.
                                    yield WalkEvent::Hash(content.hash());
                                }
                            }
                        }
                    }
                }
                NodeKind::ByteStream => {
                    if node.header.level > 0 {
                        // Internal byte-stream node — recurse into each
                        // link child (next level / leaf chunks).
                        for entry in node.entries.values() {
                            if entry.is_link()
                                && let Some(content) = entry.content.as_ref()
                            {
                                let child = self.child(entry);
                                let mut s = std::pin::pin!(child.walk_inner(
                                    content.hash(),
                                    content.plaintext_hash,
                                    prefix.clone(),
                                ));
                                while let Some(item) = s.next().await {
                                    yield item?;
                                }
                            }
                        }
                    } else {
                        // Leaf chunk node — emit each chunk's content
                        // hash. These are the actual file-content blobs
                        // that peers fetch via `export_bytes`.
                        for entry in node.entries.values() {
                            if let Some(content) = entry.content.as_ref() {
                                yield WalkEvent::Hash(content.hash());
                            }
                        }
                    }
                }
            }
        })
    }

    // =====================================================================
    // ReadableLayer Helpers (private)
    // =====================================================================

    /// Walks the tree from root to the leaf containing `key`.
    async fn walk_to_leaf(&self, key: &str) -> anyhow::Result<Option<Arc<Node>>> {
        let mut node = self.load_root().await?;

        loop {
            if node.is_leaf_node() {
                return Ok(Some(node));
            }

            let child_entry = node
                .entries
                .range::<String, _>(..=key.to_owned())
                .next_back()
                .map(|(_, entry)| entry.clone());

            let Some(entry) = child_entry else {
                return Ok(None);
            };

            let child = self.child(&entry);
            node = child.load_root().await?;
        }
    }

    /// Recursively collects all leaf entries within `[start, end)` bounds.
    async fn collect_range(
        &self,
        node: &Node,
        start: &Bound<String>,
        end: &Bound<String>,
    ) -> anyhow::Result<Vec<(String, NodeEntry)>> {
        if node.is_leaf_node() {
            let entries: Vec<_> = node
                .entries
                .range::<String, _>((start.clone(), end.clone()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            return Ok(entries);
        }

        let child_keys: Vec<_> = node.entries.keys().collect();
        let mut result = Vec::new();

        for (i, child_key) in child_keys.iter().enumerate() {
            let next_key = child_keys.get(i + 1).map(|k| k.as_str());
            if let Some(next) = next_key
                && !range_start_before(start, next)
            {
                continue;
            }
            if !range_end_after(end, child_key) {
                break;
            }

            let entry = &node.entries[*child_key];
            let child = self.child(entry);
            let child_node = child.load_root().await?;
            let entries = Box::pin(self.collect_range(&child_node, start, end)).await?;
            result.extend(entries);
        }

        Ok(result)
    }
}

impl Clone for Snapshot {
    fn clone(&self) -> Self {
        Self {
            root: self.root,
            root_plaintext_hash: self.root_plaintext_hash,
            store: self.store.clone(),
            ctx: self.ctx.clone(),
            node_cache: self.node_cache.clone(),
            import_concurrency: self.import_concurrency,
        }
    }
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("root", &self.root)
            .field("encrypted", &self.master_secret().is_some())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// ReadableLayer
// ---------------------------------------------------------------------------

#[async_trait]
impl ReadableLayer for Snapshot {
    async fn get(&self, key: &str) -> anyhow::Result<Option<NodeEntry>> {
        let Some(leaf) = self.walk_to_leaf(key).await? else {
            return Ok(None);
        };
        match leaf.get(key) {
            Some(entry) if entry.is_tombstone() => Ok(None),
            Some(entry) => Ok(Some(entry.clone())),
            None => Ok(None),
        }
    }

    async fn get_raw(&self, key: &str) -> anyhow::Result<Option<NodeEntry>> {
        let Some(leaf) = self.walk_to_leaf(key).await? else {
            return Ok(None);
        };
        Ok(leaf.get(key).cloned())
    }

    fn scan(
        &self,
        start: Bound<String>,
        end: Bound<String>,
    ) -> BoxStream<'_, anyhow::Result<(String, NodeEntry)>> {
        let fut = {
            let start = start.clone();
            let end = end.clone();
            async move {
                let root = self.load_root().await?;
                let entries = self.collect_range(&root, &start, &end).await?;
                Ok::<_, anyhow::Error>(stream::iter(entries.into_iter().map(Ok)))
            }
        };

        stream::once(fut)
            .map(|result| match result {
                Ok(inner) => inner.boxed(),
                Err(e) => stream::once(async move { Err(e) }).boxed(),
            })
            .flatten()
            .boxed()
    }

    /// Snapshot's chunk mask comes from the root node's `BuildContext`
    /// — it's the only thing tied to this concrete tree's shape. An
    /// empty / not-yet-loaded snapshot falls back to the workspace
    /// default. Layers wrapping a `Snapshot` (e.g. `MergedView`,
    /// `WritableOverlay`) inherit this via their base.
    async fn chunk_mask(&self) -> u32 {
        if !self.is_empty()
            && let Ok(root) = self.load_root().await
            && let Some(build) = &root.header.build
            && let Some(crate::node::MetaChunkingStrategy::ProllyBlake3 {
                expected_entries_per_node,
            }) = &build.meta_chunking
        {
            return expected_entries_per_node.wrapping_sub(1);
        }
        crate::persist::DEFAULT_ENTRIES_PER_NODE - 1
    }
}

// ---------------------------------------------------------------------------
// Context Construction
// ---------------------------------------------------------------------------

/// Whether append-aware import (`import_file_append`) re-reads the whole file
/// and asserts byte-identity with a full import before publishing (#3 verify
/// soak). Read from `S5_APPEND_IMPORT_VERIFY` (`1`/`true`/`yes`/`on`). Needed
/// because the structural-merge verify can't catch a bad append import (both
/// merge paths share the diff). Drop after a clean soak.
fn append_import_verify_enabled() -> bool {
    std::env::var("S5_APPEND_IMPORT_VERIFY")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Creates the default encrypted `TraversalContext`.
///
/// Both leaf and node pipelines use plain Zstd compression, 4 KiB padding,
/// and DeterministicChaCha20 encryption.
fn encrypted_context(master_secret: [u8; 32]) -> TraversalContext {
    let mut keys = BTreeMap::new();
    keys.insert(KEY_SLOT_MASTER, master_secret);

    let leaf_pipeline = BlobPipeline {
        compression: Some(CompressionStrategy::Zstd), // plain Zstd, no dict
        padding: Some(PaddingStrategy {
            block_size: DEFAULT_PAD_BLOCK_SIZE,
        }),
        encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_MASTER)),
        // Encrypted leaves default to padding-aware skip-on-no-gain;
        // see BlobPipeline::skip_when_unhelpful docs.
        skip_when_unhelpful: Some(true),
    };

    let node_pipeline = BlobPipeline {
        compression: Some(CompressionStrategy::Zstd),
        padding: Some(PaddingStrategy {
            block_size: DEFAULT_PAD_BLOCK_SIZE,
        }),
        encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_MASTER)),
        // Tree-node encoding doesn't benefit from skip — node blobs are
        // small and consistent, and we always want to compress them.
        skip_when_unhelpful: None,
    };

    TraversalContext {
        keys: Some(keys),
        leaf: Some(leaf_pipeline),
        node: Some(node_pipeline),
        // Gearhash CDC (Xet spec) is set on the vault root at creation, just like
        // the encryption config above — every file inherits it unless a route
        // overrides. See `FileChunkingStrategy::default`.
        chunking: Some(crate::node::FileChunkingStrategy::default()),
    }
}

/// Creates a vault-root-shaped encrypted `TraversalContext` with
/// separate leaf and node encryption keys plus a recovery seed slot.
///
/// Pipelines get their own key slots (`KEY_SLOT_LEAF` / `KEY_SLOT_NODE`)
/// so that file content and metadata are encrypted with independent keys.
/// `recovery_secret` lives in `KEY_SLOT_RECOVERY` — no pipeline references
/// it; it's pure derivation material for `vault_id` and the recovery
/// registry signing key (see `docs/reference/snapshot-publication.md`
/// § Vault ID derivation).
fn encrypted_split_context(
    leaf_key: [u8; 32],
    node_key: [u8; 32],
    recovery_secret: [u8; 32],
) -> TraversalContext {
    let mut keys = BTreeMap::new();
    keys.insert(KEY_SLOT_LEAF, leaf_key);
    keys.insert(KEY_SLOT_NODE, node_key);
    keys.insert(KEY_SLOT_RECOVERY, recovery_secret);

    let leaf_pipeline = BlobPipeline {
        compression: Some(CompressionStrategy::Zstd),
        padding: Some(PaddingStrategy {
            block_size: DEFAULT_PAD_BLOCK_SIZE,
        }),
        encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_LEAF)),
        skip_when_unhelpful: Some(true),
    };

    let node_pipeline = BlobPipeline {
        compression: Some(CompressionStrategy::Zstd),
        padding: Some(PaddingStrategy {
            block_size: DEFAULT_PAD_BLOCK_SIZE,
        }),
        encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_NODE)),
        skip_when_unhelpful: None,
    };

    TraversalContext {
        keys: Some(keys),
        leaf: Some(leaf_pipeline),
        node: Some(node_pipeline),
        // Gearhash CDC (Xet spec) set on the vault root at creation, alongside
        // the encryption config — inherited by every file unless a route
        // overrides. See `FileChunkingStrategy::default`.
        chunking: Some(crate::node::FileChunkingStrategy::default()),
    }
}

// ---------------------------------------------------------------------------
// Context Merging
// ---------------------------------------------------------------------------

/// Merges parent context with child overrides.
///
/// Child fields take priority where `Some`; parent values are inherited
/// where child is `None`. Keys are merged (child keys override parent keys
/// with the same slot). Pipelines are merged field-by-field.
///
/// `pub(crate)` so [`crate::pipeline::Pipeline::child_for`] can reuse
/// the same merge semantics; the function itself isn't part of the
/// crate's external API.
pub(crate) fn merge_contexts(
    parent: &TraversalContext,
    child: &TraversalContext,
) -> TraversalContext {
    let keys = match (&parent.keys, &child.keys) {
        (Some(pk), Some(ck)) => {
            let mut merged = pk.clone();
            merged.extend(ck);
            Some(merged)
        }
        (None, Some(ck)) => Some(ck.clone()),
        (Some(pk), None) => Some(pk.clone()),
        (None, None) => None,
    };

    TraversalContext {
        keys,
        leaf: merge_pipelines(parent.leaf.as_ref(), child.leaf.as_ref()),
        node: merge_pipelines(parent.node.as_ref(), child.node.as_ref()),
        chunking: child.chunking.clone().or_else(|| parent.chunking.clone()),
    }
}

/// Merges two optional pipelines. Child fields override parent fields.
fn merge_pipelines(
    parent: Option<&BlobPipeline>,
    child: Option<&BlobPipeline>,
) -> Option<BlobPipeline> {
    match (parent, child) {
        (None, None) => None,
        (Some(p), None) => Some(p.clone()),
        (None, Some(c)) => Some(c.clone()),
        (Some(p), Some(c)) => Some(BlobPipeline {
            compression: c.compression.clone().or(p.compression.clone()),
            padding: c.padding.clone().or(p.padding.clone()),
            encryption: c.encryption.clone().or(p.encryption.clone()),
            skip_when_unhelpful: c.skip_when_unhelpful.or(p.skip_when_unhelpful),
        }),
    }
}

// ---------------------------------------------------------------------------
// Range Bound Helpers
// ---------------------------------------------------------------------------

/// Returns true if `start` bound is before `value` (i.e., `value` could be in range).
fn range_start_before(start: &Bound<String>, value: &str) -> bool {
    match start {
        Bound::Unbounded => true,
        Bound::Included(s) => value >= s.as_str(),
        Bound::Excluded(s) => value > s.as_str(),
    }
}

/// Returns true if `end` bound is after `value` (i.e., `value` could be in range).
fn range_end_after(end: &Bound<String>, value: &str) -> bool {
    match end {
        Bound::Unbounded => true,
        Bound::Included(e) => value <= e.as_str(),
        Bound::Excluded(e) => value < e.as_str(),
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use s5_core::blob::BlobStore;
    use s5_store_memory::MemoryStore;

    fn test_rw_store() -> Arc<BlobStore> {
        Arc::new(BlobStore::new(MemoryStore::new()))
    }

    /// Round-trip: import_stream → export_bytes for a small (single-chunk) file.
    #[tokio::test]
    async fn import_export_single_chunk() {
        let store = test_rw_store();
        let master = [42u8; 32];
        let snap = Snapshot::empty_encrypted(store.clone(), master);

        // 1 KB of text — well below the ~8KB min chunk size
        let data = b"hello world! ".repeat(80);
        let reader = tokio::io::BufReader::new(&data[..]);

        let entry = snap
            .import_stream(reader, store.as_ref(), None)
            .await
            .unwrap();

        // Should be a Leaf (single chunk, not a Link tree)
        let content = entry.content.as_ref().unwrap();
        assert_eq!(content.structural, Structural::Leaf);
        assert_eq!(content.size, data.len() as u64);

        let restored = snap.export_bytes(&entry).await.unwrap();
        assert_eq!(&restored[..], &data[..], "single-chunk round-trip mismatch");
    }

    /// Round-trip: import_stream → export_bytes for a large (multi-chunk) file.
    #[tokio::test]
    async fn import_export_multi_chunk() {
        let store = test_rw_store();
        let master = [42u8; 32];
        let snap = Snapshot::empty_encrypted(store.clone(), master);

        // ~512 KB of compressible data — should produce multiple CDC chunks
        let data: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
        let reader = tokio::io::BufReader::new(&data[..]);

        let entry = snap
            .import_stream(reader, store.as_ref(), None)
            .await
            .unwrap();

        // Should be a Link (multi-chunk ByteStream tree)
        let content = entry.content.as_ref().unwrap();
        assert_eq!(
            content.structural,
            Structural::Link,
            "expected multi-chunk Link"
        );
        assert_eq!(content.size, data.len() as u64);

        let restored = snap.export_bytes(&entry).await.unwrap();
        assert_eq!(restored.len(), data.len(), "restored length mismatch");
        assert_eq!(&restored[..], &data[..], "multi-chunk round-trip mismatch");
    }

    /// Round-trip: import_stream → export_bytes for a large file with
    /// pseudo-random (incompressible) data.
    #[tokio::test]
    async fn import_export_multi_chunk_random() {
        let store = test_rw_store();
        let master = [42u8; 32];
        let snap = Snapshot::empty_encrypted(store.clone(), master);

        // ~256 KB of pseudo-random data (uses a simple PRNG, not truly random)
        let mut data = vec![0u8; 256 * 1024];
        let mut state: u64 = 0xDEADBEEF;
        for byte in data.iter_mut() {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (state >> 33) as u8;
        }
        let reader = tokio::io::BufReader::new(&data[..]);

        let entry = snap
            .import_stream(reader, store.as_ref(), None)
            .await
            .unwrap();
        let content = entry.content.as_ref().unwrap();
        assert_eq!(content.size, data.len() as u64);

        let restored = snap.export_bytes(&entry).await.unwrap();
        assert_eq!(restored.len(), data.len(), "restored length mismatch");
        assert_eq!(&restored[..], &data[..], "random-data round-trip mismatch");
    }

    /// The concurrent chunk pipeline must produce a byte-identical result
    /// regardless of `import_concurrency`. Each leaf is content-addressed
    /// from its own plaintext alone — no chunk reads a neighbour (the
    /// `ZstdDictFromPrecedingEntry` D-chunk scheme that once did was
    /// purged) — and chunk position is carried by the byte-offset key,
    /// not completion order. Importing the same bytes at depth 1 vs 16
    /// through the full encrypted+compressed+padded pipeline must yield
    /// the same root CID; with convergent encryption, any order-dependent
    /// key/nonce/compression would change it.
    #[tokio::test]
    async fn import_concurrency_does_not_change_root() {
        use crate::node::FileChunkingStrategy;

        let master = [7u8; 32];
        // 4 MiB at fixed 64 KiB → 64 leaves, so chunks finish out of
        // order under concurrency.
        let data: Vec<u8> = (0..4 * 1024 * 1024)
            .map(|i| ((i / 257) ^ (i * 3)) as u8)
            .collect();

        async fn import_at(master: [u8; 32], data: &[u8], concurrency: usize) -> NodeEntry {
            let store = test_rw_store();
            let mut ctx = encrypted_context(master);
            ctx.chunking = Some(FileChunkingStrategy::Fixed {
                chunk_size: 64 * 1024,
            });
            let snap = Snapshot::empty(store.clone() as Arc<dyn s5_core::BlobsRead>, ctx)
                .with_import_concurrency(concurrency);
            let entry = snap
                .import_stream(std::io::Cursor::new(data), store.as_ref(), None)
                .await
                .unwrap();
            assert_eq!(
                entry.content.as_ref().unwrap().structural,
                Structural::Link,
                "test data must produce a multi-chunk ByteStream (the path under test)"
            );
            entry
        }

        let serial = import_at(master, &data, 1).await;
        let parallel = import_at(master, &data, 16).await;

        let s = serial.content.as_ref().unwrap();
        let p = parallel.content.as_ref().unwrap();
        assert_eq!(
            s.hash, p.hash,
            "root CID changed with import_concurrency — chunk processing is not order-independent"
        );
        assert_eq!(
            s.plaintext_hash, p.plaintext_hash,
            "plaintext hash diverged"
        );
        assert_eq!(s.size, p.size, "size diverged");
    }

    /// Stream-import a file large enough to produce many chunks and
    /// verify it round-trips correctly. Exercises the streaming-import
    /// path under realistic chunk-count pressure (which the smaller
    /// multi-chunk test doesn't reach).
    ///
    /// The 32 MiB at Fixed{256 KiB} = 128 chunks — enough to go
    /// through the prefix-iter path (first 2) plus 126 chunker pulls
    /// + the prolly-tree spine assembly.
    #[tokio::test]
    async fn import_export_many_chunks_streamed() {
        use crate::node::{BlobPipeline, FileChunkingStrategy};

        let store = test_rw_store();
        // Plain ctx with explicit Fixed chunking forces many chunks
        // without depending on Xet CDC's threshold heuristic.
        let ctx = TraversalContext {
            keys: None,
            leaf: Some(BlobPipeline {
                compression: Some(CompressionStrategy::Zstd),
                padding: None,
                encryption: None,
                skip_when_unhelpful: None,
            }),
            node: None,
            chunking: Some(FileChunkingStrategy::Fixed {
                chunk_size: 256 * 1024,
            }),
        };
        let snap = Snapshot::empty(store.clone() as Arc<dyn s5_core::BlobsRead>, ctx);

        // 32 MiB of structured (compressible) bytes — produces 128
        // 256-KiB leaves through the streaming loop.
        let data: Vec<u8> = (0..32 * 1024 * 1024)
            .map(|i| ((i / 251) ^ i) as u8)
            .collect();
        let reader = std::io::Cursor::new(&data[..]);

        let entry = snap
            .import_stream(reader, store.as_ref(), None)
            .await
            .unwrap();

        let content = entry.content.as_ref().unwrap();
        assert_eq!(
            content.structural,
            Structural::Link,
            "expected multi-chunk Link entry"
        );
        assert_eq!(content.size, data.len() as u64);

        let restored = snap.export_bytes(&entry).await.unwrap();
        assert_eq!(
            restored.len(),
            data.len(),
            "restored length mismatch (streaming-import vs collect-then-build)"
        );
        assert_eq!(&restored[..], &data[..], "byte-for-byte mismatch");
    }

    /// The export read path must fetch chunk blobs CONCURRENTLY, not one at a
    /// time. A probe store counts simultaneous downloads; with a multi-chunk file
    /// it must reach `EXPORT_CONCURRENCY`, and the bytes must still round-trip.
    /// Guards the perf property — a regression to serial fetch drops max to 1.
    #[tokio::test]
    async fn export_fetches_chunks_concurrently() {
        use crate::node::{BlobPipeline, FileChunkingStrategy};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct ConcurrencyProbe {
            inner: Arc<BlobStore>,
            cur: AtomicUsize,
            max: AtomicUsize,
        }
        #[async_trait::async_trait]
        impl s5_core::BlobsRead for ConcurrencyProbe {
            async fn blob_contains(&self, hash: Hash) -> anyhow::Result<bool> {
                self.inner.blob_contains(hash).await
            }
            async fn blob_get_size(&self, hash: Hash) -> anyhow::Result<u64> {
                self.inner.blob_get_size(hash).await
            }
            async fn blob_download(&self, hash: Hash) -> anyhow::Result<bytes::Bytes> {
                let n = self.cur.fetch_add(1, Ordering::SeqCst) + 1;
                self.max.fetch_max(n, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                let r = self.inner.blob_download(hash).await;
                self.cur.fetch_sub(1, Ordering::SeqCst);
                r
            }
            async fn blob_download_slice(
                &self,
                hash: Hash,
                offset: u64,
                max_len: Option<u64>,
            ) -> anyhow::Result<bytes::Bytes> {
                self.inner.blob_download_slice(hash, offset, max_len).await
            }
            async fn blob_read(
                &self,
                hash: Hash,
            ) -> anyhow::Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
                self.inner.blob_read(hash).await
            }
        }

        let store = test_rw_store();
        let ctx = TraversalContext {
            keys: None,
            leaf: Some(BlobPipeline {
                compression: None,
                padding: None,
                encryption: None,
                skip_when_unhelpful: None,
            }),
            node: None,
            chunking: Some(FileChunkingStrategy::Fixed {
                chunk_size: 64 * 1024,
            }),
        };

        // Import a 16-chunk file (writes; no read delay here).
        let snap = Snapshot::empty(store.clone() as Arc<dyn s5_core::BlobsRead>, ctx.clone());
        let data: Vec<u8> = (0..16 * 64 * 1024).map(|i| (i * 7) as u8).collect();
        let entry = snap
            .import_stream(std::io::Cursor::new(&data[..]), store.as_ref(), None)
            .await
            .unwrap();
        assert_eq!(
            entry.content.as_ref().unwrap().structural,
            Structural::Link,
            "expected a multi-chunk file"
        );

        // Export through the probing read store and check overlap + correctness.
        let probe = Arc::new(ConcurrencyProbe {
            inner: store.clone(),
            cur: AtomicUsize::new(0),
            max: AtomicUsize::new(0),
        });
        let read_snap = Snapshot::empty(probe.clone() as Arc<dyn s5_core::BlobsRead>, ctx);
        let restored = read_snap.export_bytes(&entry).await.unwrap();

        assert_eq!(
            &restored[..],
            &data[..],
            "byte-for-byte mismatch after concurrent export"
        );
        let max = probe.max.load(Ordering::SeqCst);
        assert_eq!(
            max,
            crate::pipeline::EXPORT_CONCURRENCY,
            "export should fetch EXPORT_CONCURRENCY chunks at once; observed max {max}"
        );
    }

    /// Verify that incompressible data gets a child_context override (Uncompressed)
    /// and round-trips correctly through import_bytes → export_bytes.
    #[tokio::test]
    async fn compression_skip_sets_child_context() {
        let store = test_rw_store();
        let master = [42u8; 32];
        let snap = Snapshot::empty_encrypted(store.clone(), master);

        // Random data that won't compress well.
        let mut data = vec![0u8; 4096];
        let mut state: u64 = 0xCAFEBABE;
        for byte in data.iter_mut() {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (state >> 33) as u8;
        }

        let entry = snap
            .import_bytes(&data, store.as_ref(), None)
            .await
            .unwrap();

        // Incompressible data should have a child_context override.
        assert!(
            entry.child_context.is_some(),
            "expected child_context override for incompressible data"
        );
        let child_ctx = entry.child_context.as_ref().unwrap();
        let leaf = child_ctx
            .leaf
            .as_ref()
            .expect("expected leaf pipeline override");
        assert_eq!(
            leaf.compression,
            Some(CompressionStrategy::Uncompressed),
            "expected Uncompressed override"
        );

        // Round-trip: export should still produce the original data.
        let restored = snap.export_bytes(&entry).await.unwrap();
        assert_eq!(
            &restored[..],
            &data[..],
            "incompressible round-trip mismatch"
        );
    }

    /// `export_byte_chunks` yields per-chunk plaintext that concatenates
    /// to exactly `export_bytes` (which is now defined in terms of it),
    /// and the optional `ChunkCache` is populated on a miss and served
    /// on the next pass — without corrupting the bytes.
    #[tokio::test]
    async fn export_byte_chunks_concats_and_uses_cache() {
        use crate::node::{BlobPipeline, FileChunkingStrategy};
        use crate::pipeline::ChunkCache;
        use futures::StreamExt;
        use std::collections::HashMap;
        use std::sync::Mutex;

        let store = test_rw_store();
        let ctx = TraversalContext {
            keys: None,
            leaf: Some(BlobPipeline {
                compression: Some(CompressionStrategy::Zstd),
                padding: None,
                encryption: None,
                skip_when_unhelpful: None,
            }),
            node: None,
            chunking: Some(FileChunkingStrategy::Fixed {
                chunk_size: 256 * 1024,
            }),
        };
        let snap = Snapshot::empty(store.clone() as Arc<dyn s5_core::BlobsRead>, ctx);

        // 4 MiB structured (compressible) bytes → many 256-KiB chunks.
        let data: Vec<u8> = (0..4 * 1024 * 1024)
            .map(|i| ((i / 251) ^ i) as u8)
            .collect();
        let reader = std::io::Cursor::new(&data[..]);
        let entry = snap
            .import_stream(reader, store.as_ref(), None)
            .await
            .unwrap();
        assert_eq!(
            entry.content.as_ref().unwrap().structural,
            Structural::Link,
            "expected a multi-chunk Link entry"
        );

        let pipe = snap.as_pipeline();

        // Uncached: per-chunk stream concatenates to the original, and
        // matches `export_bytes` (now implemented via this method).
        let mut chunks: Vec<Bytes> = Vec::new();
        {
            let mut s = pipe.export_byte_chunks(&entry, None);
            while let Some(r) = s.next().await {
                chunks.push(r.unwrap());
            }
        }
        assert!(
            chunks.len() > 1,
            "expected multiple chunks, got {}",
            chunks.len()
        );
        let concat: Vec<u8> = chunks.iter().flat_map(|b| b.to_vec()).collect();
        assert_eq!(concat, data, "export_byte_chunks concat != original");
        assert_eq!(
            &pipe.export_bytes(&entry).await.unwrap()[..],
            &data[..],
            "export_bytes (now via export_byte_chunks) != original"
        );

        #[derive(Default)]
        struct StubCache {
            map: Mutex<HashMap<[u8; 32], Bytes>>,
            hits: Mutex<usize>,
            inserts: Mutex<usize>,
        }
        impl ChunkCache for StubCache {
            fn get(&self, k: &[u8; 32]) -> Option<Bytes> {
                let v = self.map.lock().unwrap().get(k).cloned();
                if v.is_some() {
                    *self.hits.lock().unwrap() += 1;
                }
                v
            }
            fn insert(&self, k: [u8; 32], v: Bytes) {
                self.map.lock().unwrap().insert(k, v);
                *self.inserts.lock().unwrap() += 1;
            }
        }
        let cache = StubCache::default();
        let n = chunks.len();

        // Pass 1: every chunk a miss → inserted, no hits.
        {
            let mut s = pipe.export_byte_chunks(&entry, Some(&cache));
            while let Some(r) = s.next().await {
                r.unwrap();
            }
        }
        assert_eq!(
            *cache.inserts.lock().unwrap(),
            n,
            "pass 1 should insert every chunk"
        );
        assert_eq!(*cache.hits.lock().unwrap(), 0, "pass 1 has no hits");

        // Pass 2: every chunk a hit, no new inserts, bytes still correct.
        let mut chunks2: Vec<Bytes> = Vec::new();
        {
            let mut s = pipe.export_byte_chunks(&entry, Some(&cache));
            while let Some(r) = s.next().await {
                chunks2.push(r.unwrap());
            }
        }
        assert_eq!(
            *cache.hits.lock().unwrap(),
            n,
            "pass 2 should hit every chunk"
        );
        assert_eq!(
            *cache.inserts.lock().unwrap(),
            n,
            "pass 2 must add no new inserts"
        );
        let concat2: Vec<u8> = chunks2.iter().flat_map(|b| b.to_vec()).collect();
        assert_eq!(concat2, data, "cached-pass bytes != original");
    }

    /// `export_byte_chunks_at` downloads ONLY the requested chunk indices
    /// (verified via the cache insert count) and yields their plaintext,
    /// byte-identical to the full `export_byte_chunks` at those positions.
    #[tokio::test]
    async fn export_byte_chunks_at_fetches_only_wanted() {
        use crate::node::{BlobPipeline, FileChunkingStrategy};
        use crate::pipeline::ChunkCache;
        use futures::StreamExt;
        use std::collections::HashMap;
        use std::sync::Mutex;

        let store = test_rw_store();
        let ctx = TraversalContext {
            keys: None,
            leaf: Some(BlobPipeline {
                compression: Some(CompressionStrategy::Zstd),
                padding: None,
                encryption: None,
                skip_when_unhelpful: None,
            }),
            node: None,
            chunking: Some(FileChunkingStrategy::Fixed {
                chunk_size: 256 * 1024,
            }),
        };
        let snap = Snapshot::empty(store.clone() as Arc<dyn s5_core::BlobsRead>, ctx);
        let data: Vec<u8> = (0..4 * 1024 * 1024)
            .map(|i| ((i / 251) ^ i) as u8)
            .collect();
        let reader = std::io::Cursor::new(&data[..]);
        let entry = snap
            .import_stream(reader, store.as_ref(), None)
            .await
            .unwrap();
        assert_eq!(entry.content.as_ref().unwrap().structural, Structural::Link);
        let pipe = snap.as_pipeline();

        // Ground truth: all chunks, in order.
        let mut all: Vec<Bytes> = Vec::new();
        {
            let mut s = pipe.export_byte_chunks(&entry, None);
            while let Some(r) = s.next().await {
                all.push(r.unwrap());
            }
        }
        let n = all.len();
        assert!(n >= 6, "need several chunks, got {n}");

        #[derive(Default)]
        struct CountCache {
            map: Mutex<HashMap<[u8; 32], Bytes>>,
            inserts: Mutex<usize>,
        }
        impl ChunkCache for CountCache {
            fn get(&self, k: &[u8; 32]) -> Option<Bytes> {
                self.map.lock().unwrap().get(k).cloned()
            }
            fn insert(&self, k: [u8; 32], v: Bytes) {
                self.map.lock().unwrap().insert(k, v);
                *self.inserts.lock().unwrap() += 1;
            }
        }
        let cache = CountCache::default();
        let wanted: Vec<usize> = vec![1, 3, n - 1];

        let mut got: Vec<(usize, Bytes)> = Vec::new();
        {
            let mut s = pipe.export_byte_chunks_at(&entry, &wanted, Some(&cache));
            while let Some(r) = s.next().await {
                got.push(r.unwrap());
            }
        }
        let got_idx: Vec<usize> = got.iter().map(|(i, _)| *i).collect();
        assert_eq!(
            got_idx, wanted,
            "yields exactly the wanted indices in order"
        );
        for (i, b) in &got {
            assert_eq!(b, &all[*i], "chunk {i} bytes match the full export");
        }
        // The whole point: only the wanted chunk blobs were downloaded.
        assert_eq!(
            *cache.inserts.lock().unwrap(),
            wanted.len(),
            "fetched ONLY the wanted chunks, not the whole file"
        );
    }

    /// `import_stream_with_override` stamps the supplied override onto
    /// `entry.child_context` and the bytes round-trip via the parent
    /// snapshot (which has a *different* default context — proving the
    /// cascade is what makes the read work).
    #[tokio::test]
    async fn import_with_override_stamps_and_round_trips() {
        use crate::node::{BlobPipeline, FileChunkingStrategy};

        let store = test_rw_store();
        // Parent snapshot is plain — no compression, no chunking strategy.
        let snap = Snapshot::empty(store.clone(), TraversalContext::default());

        // Override forces zstd + fixed chunking (analogous to a per-route
        // pipeline on a feedy `segments/**/*.seg` rule).
        let override_ctx = TraversalContext {
            keys: None,
            leaf: Some(BlobPipeline {
                compression: Some(CompressionStrategy::Zstd),
                padding: None,
                encryption: None,
                skip_when_unhelpful: None,
            }),
            node: None,
            chunking: Some(FileChunkingStrategy::Fixed {
                chunk_size: 1 << 12,
            }),
        };

        // 32 KiB of repeating bytes — compresses well, and at chunk_size
        // = 4 KiB produces 8 leaves so we exercise the multi-chunk Link
        // path.
        let data = vec![0xAB; 32 * 1024];
        let cursor = std::io::Cursor::new(data.clone());

        let entry = snap
            .import_stream_with_override(cursor, store.as_ref(), None, &override_ctx)
            .await
            .unwrap();

        // Override stamped onto the entry verbatim (compression + chunking).
        let cc = entry
            .child_context
            .as_ref()
            .expect("override should be stamped");
        let leaf = cc.leaf.as_ref().expect("override leaf pipeline missing");
        assert_eq!(leaf.compression, Some(CompressionStrategy::Zstd));
        assert!(matches!(
            cc.chunking,
            Some(FileChunkingStrategy::Fixed { chunk_size: 4096 })
        ));

        // Tree was actually built (multi-chunk path).
        assert_eq!(
            entry.content.as_ref().unwrap().structural,
            crate::node::Structural::Link
        );

        // Round-trip via the parent snapshot — only works if the read-side
        // cascade merges entry.child_context into snap.ctx.
        let restored = snap.export_bytes(&entry).await.unwrap();
        assert_eq!(&restored[..], &data[..]);
    }

    /// When `import_stream_with_override` is called and an *inner* codec
    /// hook (`skip_when_unhelpful`) also wants to stamp child_context
    /// (e.g. flips compression to Uncompressed for incompressible bytes),
    /// the override and the inner stamp compose — both end up on the
    /// returned entry's child_context.
    #[tokio::test]
    async fn override_composes_with_skip_when_unhelpful() {
        use crate::node::{BlobPipeline, FileChunkingStrategy};

        let store = test_rw_store();
        let snap = Snapshot::empty(store.clone(), TraversalContext::default());

        // Override sets chunking only (single chunk via large size, so we
        // hit the *single-leaf* path — that's where skip_when_unhelpful
        // actually triggers via import_bytes → pipeline.import_bytes).
        let override_ctx = TraversalContext {
            keys: None,
            leaf: Some(BlobPipeline {
                compression: Some(CompressionStrategy::Zstd),
                padding: None,
                encryption: None,
                skip_when_unhelpful: Some(true),
            }),
            node: None,
            chunking: Some(FileChunkingStrategy::Fixed {
                chunk_size: 1 << 20,
            }),
        };

        // Random data — incompressible — triggers skip-when-unhelpful's
        // Uncompressed flip.
        let mut data = vec![0u8; 4096];
        let mut state: u64 = 0xDEADBEEF;
        for byte in data.iter_mut() {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (state >> 33) as u8;
        }
        let cursor = std::io::Cursor::new(data.clone());

        let entry = snap
            .import_stream_with_override(cursor, store.as_ref(), None, &override_ctx)
            .await
            .unwrap();

        let cc = entry
            .child_context
            .as_ref()
            .expect("composed override should be stamped");
        // Inner skip_when_unhelpful flipped compression to Uncompressed,
        // and the merge in import_stream_with_override preserves chunking
        // from the route override (since the inner stamp only touches
        // leaf.compression).
        assert_eq!(
            cc.leaf.as_ref().unwrap().compression,
            Some(CompressionStrategy::Uncompressed),
            "inner skip-when-unhelpful flip should win on compression"
        );
        assert!(
            matches!(
                cc.chunking,
                Some(FileChunkingStrategy::Fixed {
                    chunk_size: 1048576
                })
            ),
            "outer route's chunking should survive the merge"
        );

        // Round-trip.
        let restored = snap.export_bytes(&entry).await.unwrap();
        assert_eq!(&restored[..], &data[..]);
    }

    /// Append vs `import_stream_with_prev` (the S5_APPEND_IMPORT_VERIFY
    /// comparison, NOT the fresh `import_stream` the first oracle used).
    ///
    /// ⚠️ OPEN BUG (2026-06-17 s5): in PROD, S5_APPEND_IMPORT_VERIFY caught
    /// `import_file_append` diverging from the full import on live
    /// interner_packs/dids_plc.bin + ledger .ril (7 mismatches / 3 cycles;
    /// prod was safe — the verify published the full result, and #3 is now
    /// DISABLED at the route level pending this fix). This test does NOT yet
    /// reproduce that divergence — same-process, Fixed+uncompressed, multi-level
    /// (200-chunk) tree, unchanged AND grown, append == import_stream_with_prev.
    /// So the live trigger is a factor NOT covered here: the per-route
    /// OVERRIDE-ctx merge (import_file_append_with_override) and/or a RELOADED
    /// prev Snapshot (prev_chunks walked from the persisted tree, not an
    /// in-process import). Next debugging step: reconstruct a Snapshot from a
    /// persisted root + drive the override path, then bisect import_file_append.
    /// Kept as a passing guard for the same-process case + a pointer to the hunt.
    #[tokio::test]
    async fn append_matches_import_with_prev_repro() {
        let store = test_rw_store();
        let base = Snapshot::empty_plain(store.clone() as Arc<dyn s5_core::BlobsRead>);
        // Small chunk_size + many chunks → a MULTI-LEVEL ByteStream tree
        // (>64 chunks crosses the 0x3F mask into internal nodes), like the
        // real 175-chunk dids_plc.bin — the single-node case passed.
        let chunk_size = 4096usize;
        let mut ctx = base.context().clone();
        ctx.chunking = Some(crate::node::FileChunkingStrategy::Fixed {
            chunk_size: chunk_size as u32,
        });
        let snap = base.with_ctx(ctx);

        let path = std::env::temp_dir().join(format!(
            "s5_append_repro_{}_{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let head_len = 200 * chunk_size + 1234; // 200 full chunks + partial
        let head: Vec<u8> = (0..head_len).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &head).unwrap();
        let entry_head = {
            let f = tokio::fs::File::open(&path).await.unwrap();
            snap.import_stream(f, store.as_ref(), None).await.unwrap()
        };
        let prev_chunks = snap.collect_byte_stream_chunks(&entry_head).await.unwrap();

        for tail_len in [0usize, 1234, 64 * 1024 + 7] {
            let mut grown = head.clone();
            grown.extend((0..tail_len).map(|i| ((i * 7 + 3) % 197) as u8));
            std::fs::write(&path, &grown).unwrap();

            let (entry_append, _read) = snap
                .import_file_append(&path, store.as_ref(), None, &prev_chunks)
                .await
                .unwrap();
            // The VERIFY's comparison: import_stream_with_prev, not fresh.
            let entry_with_prev = {
                let f = tokio::fs::File::open(&path).await.unwrap();
                snap.import_stream_with_prev(f, store.as_ref(), None, &prev_chunks)
                    .await
                    .unwrap()
            };
            assert_eq!(
                entry_append.content.as_ref().unwrap().hash,
                entry_with_prev.content.as_ref().unwrap().hash,
                "append MUST match import_stream_with_prev (tail_len={tail_len})"
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    /// #3 OPEN-BUG HUNT (2026-06-17 s5): reproduce the live
    /// `S5_APPEND_IMPORT_VERIFY` divergence that the same-process plain-ctx
    /// repro above misses. The untested axis is the per-route OVERRIDE ctx
    /// carrying a `leaf` pipeline (interner = Uncompressed, segments = Zstd):
    /// that flips `has_transforms` true, so leaves carry `plaintext_hash` and
    /// (for skip-when-unhelpful) a `child_context` — none of which the
    /// `leaf=None` repro exercises. We drive the EXACT prod calls
    /// (`import_file_append_with_override` vs `import_stream_with_override_and_prev`)
    /// with prev_chunks walked back from the persisted root (reload), and on
    /// mismatch dump the first diverging leaf so the field is named, not guessed.
    #[tokio::test]
    async fn append_diverges_on_override_ctx_repro() {
        let store = test_rw_store();
        let chunk_size = 4096usize;

        // Two route shapes that diverged live: interner (Uncompressed) +
        // segment (Zstd L9). Both set a leaf pipeline ⇒ has_transforms=true.
        let shapes: [(&str, crate::node::CompressionStrategy, Option<bool>); 2] = [
            (
                "interner",
                crate::node::CompressionStrategy::Uncompressed,
                None,
            ),
            (
                "segment",
                crate::node::CompressionStrategy::ZstdLevel { level: 9 },
                Some(true),
            ),
        ];

        for (name, compression, skip) in shapes {
            let base = Snapshot::empty_plain(store.clone() as Arc<dyn s5_core::BlobsRead>);
            // The per-route override exactly as `compile_pipeline_routes` builds it.
            let mut override_ctx = base.context().clone();
            override_ctx.chunking = Some(crate::node::FileChunkingStrategy::Fixed {
                chunk_size: chunk_size as u32,
            });
            override_ctx.leaf = Some(crate::node::BlobPipeline {
                compression: Some(compression),
                padding: None,
                encryption: None,
                skip_when_unhelpful: skip,
            });

            let path = std::env::temp_dir().join(format!(
                "s5_override_repro_{}_{}_{}.bin",
                name,
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            // 200 full chunks + a partial last chunk (like a real growing file).
            let head_len = 200 * chunk_size + 1234;
            let head: Vec<u8> = (0..head_len).map(|i| (i % 251) as u8).collect();
            std::fs::write(&path, &head).unwrap();

            // First cycle: import via the prod override path, then RELOAD the
            // prev from JUST the persisted root content (no in-process leakage).
            let entry_head = {
                let f = tokio::fs::File::open(&path).await.unwrap();
                base.import_stream_with_override_and_prev(
                    f,
                    store.as_ref(),
                    None,
                    &override_ctx,
                    &[],
                )
                .await
                .unwrap()
            };
            let reloaded_head = NodeEntry {
                content: entry_head.content.clone(),
                semantic: None,
                child_context: None,
                tombstone: None,
            };
            let prev_chunks = base
                .collect_byte_stream_chunks(&reloaded_head)
                .await
                .unwrap();

            for tail_len in [0usize, 1234, 64 * 1024 + 7] {
                let mut grown = head.clone();
                grown.extend((0..tail_len).map(|i| ((i * 7 + 3) % 197) as u8));
                std::fs::write(&path, &grown).unwrap();

                let (entry_append, _read) = base
                    .import_file_append_with_override(
                        &path,
                        store.as_ref(),
                        None,
                        &override_ctx,
                        &prev_chunks,
                    )
                    .await
                    .unwrap();
                let entry_oracle = {
                    let f = tokio::fs::File::open(&path).await.unwrap();
                    base.import_stream_with_override_and_prev(
                        f,
                        store.as_ref(),
                        None,
                        &override_ctx,
                        &prev_chunks,
                    )
                    .await
                    .unwrap()
                };

                let ah = entry_append.content.as_ref().unwrap().hash;
                let oh = entry_oracle.content.as_ref().unwrap().hash;
                if ah != oh {
                    // Name the field: walk both result trees and diff leaves.
                    let al = base
                        .collect_byte_stream_chunks(&entry_append)
                        .await
                        .unwrap();
                    let ol = base
                        .collect_byte_stream_chunks(&entry_oracle)
                        .await
                        .unwrap();
                    eprintln!(
                        "DIVERGE shape={name} tail_len={tail_len}: append_leaves={} oracle_leaves={}",
                        al.len(),
                        ol.len()
                    );
                    for (i, (a, o)) in al.iter().zip(ol.iter()).enumerate() {
                        let af = format!("{:?}|{:?}", a.content, a.child_context);
                        let of = format!("{:?}|{:?}", o.content, o.child_context);
                        if af != of {
                            eprintln!(
                                "  leaf[{i}] APPEND  content={:?} child_ctx={:?}",
                                a.content, a.child_context
                            );
                            eprintln!(
                                "  leaf[{i}] ORACLE  content={:?} child_ctx={:?}",
                                o.content, o.child_context
                            );
                            break;
                        }
                    }
                }
                assert_eq!(
                    ah, oh,
                    "shape={name} tail_len={tail_len}: append MUST match import_stream_with_override_and_prev"
                );
            }
            let _ = std::fs::remove_file(&path);
        }
    }

    /// #3 ROOT CAUSE (2026-06-17 s5): the live S5_APPEND_IMPORT_VERIFY mismatch
    /// is a TOCTOU artifact, NOT an append bug. The append path reads the tail
    /// at instant T1 (EOF=E1); the verify's fresh full read happens at T2>T1,
    /// and because dids_plc.bin/.ril are appended to CONTINUOUSLY by the live
    /// writer, the full read sees E2>E1 — diverging by exactly the appended
    /// bytes (observed live: the tail leaf off by one 16 B dids entry). This
    /// test simulates that growth-between-reads and proves (a) the naive
    /// full-read DIVERGES, and (b) `.take(E1)` (the fix) makes it MATCH — the
    /// append result is a correct snapshot of the file at E1.
    #[tokio::test]
    async fn append_verify_toctou_take_fix() {
        use tokio::io::AsyncReadExt as _;
        let store = test_rw_store();
        let chunk_size = 4096usize;
        let base = Snapshot::empty_plain(store.clone() as Arc<dyn s5_core::BlobsRead>);
        let mut override_ctx = base.context().clone();
        override_ctx.chunking = Some(crate::node::FileChunkingStrategy::Fixed {
            chunk_size: chunk_size as u32,
        });
        override_ctx.leaf = Some(crate::node::BlobPipeline {
            compression: Some(crate::node::CompressionStrategy::Uncompressed),
            padding: None,
            encryption: None,
            skip_when_unhelpful: None,
        });

        let path = std::env::temp_dir().join(format!(
            "s5_toctou_{}_{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        // E1: 100 full chunks + a partial tail (like a growing interner pack).
        let e1_len = 100 * chunk_size + 1234;
        let head: Vec<u8> = (0..e1_len).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &head).unwrap();

        let entry_head = {
            let f = tokio::fs::File::open(&path).await.unwrap();
            base.import_stream_with_override_and_prev(f, store.as_ref(), None, &override_ctx, &[])
                .await
                .unwrap()
        };
        let reloaded_head = NodeEntry {
            content: entry_head.content.clone(),
            semantic: None,
            child_context: None,
            tombstone: None,
        };
        let prev_chunks = base
            .collect_byte_stream_chunks(&reloaded_head)
            .await
            .unwrap();

        // Append-import a NEWLY-grown file (E1 + a bit), capturing the tree for
        // the state the append path read.
        let grown1: Vec<u8> = {
            let mut v = head.clone();
            v.extend((0..500).map(|i| ((i * 3 + 1) % 197) as u8));
            v
        };
        std::fs::write(&path, &grown1).unwrap();
        let append_total = grown1.len() as u64;
        let (entry_append, _read) = base
            .import_file_append_with_override(
                &path,
                store.as_ref(),
                None,
                &override_ctx,
                &prev_chunks,
            )
            .await
            .unwrap();
        let append_hash = entry_append.content.as_ref().unwrap().hash;

        // The live writer appends MORE between the append read and the verify.
        let mut grown2 = grown1.clone();
        grown2.extend((0..16).map(|i| (i as u8).wrapping_add(7)));
        std::fs::write(&path, &grown2).unwrap();

        // Both full reads MUST use the SAME (merged override) ctx the append
        // path used — a plain-ctx read differs by leaf plaintext_hash alone and
        // would confound the TOCTOU test. import_stream_with_override_and_prev's
        // content.hash equals the verify's self.import_stream_with_prev(merged).

        // (a) Naive full read (current EOF = E2) DIVERGES — the TOCTOU.
        let naive = {
            let f = tokio::fs::File::open(&path).await.unwrap();
            base.import_stream_with_override_and_prev(
                f,
                store.as_ref(),
                None,
                &override_ctx,
                &prev_chunks,
            )
            .await
            .unwrap()
        };
        assert_ne!(
            naive.content.as_ref().unwrap().hash,
            append_hash,
            "growth-between-reads MUST make the naive full read diverge (reproduces the live mismatch)"
        );

        // (b) `.take(append_total)` (the fix) reads the SAME bytes the append
        // captured → MATCHES. The append result is a correct snapshot at E1.
        let fixed = {
            let f = tokio::fs::File::open(&path).await.unwrap();
            base.import_stream_with_override_and_prev(
                f.take(append_total),
                store.as_ref(),
                None,
                &override_ctx,
                &prev_chunks,
            )
            .await
            .unwrap()
        };
        assert_eq!(
            fixed.content.as_ref().unwrap().hash,
            append_hash,
            ".take(total_size) MUST match the append result — same byte range, no TOCTOU"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// Plain-pipeline (no transforms — feedy's case) per-chunk dedup:
    /// re-importing the same multi-chunk file with `prev_chunks` reuses
    /// every chunk's NodeEntry from the prev tree, yielding a root
    /// content hash identical to the original import. This exercises
    /// the `plaintext_hash.unwrap_or(content.hash)` fallback because
    /// plain pipelines don't store plaintext_hash on chunks.
    #[tokio::test]
    async fn plain_dedup_reuses_chunks_unchanged_file() {
        let store = test_rw_store();
        let snap = Snapshot::empty_plain(store.clone());

        // Multi-chunk: ~512 KB, deterministic content so CDC boundaries
        // are reproducible across imports.
        let data: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
        let reader_a = tokio::io::BufReader::new(&data[..]);
        let entry_a = snap
            .import_stream(reader_a, store.as_ref(), None)
            .await
            .unwrap();
        let content_a = entry_a.content.as_ref().unwrap();
        assert_eq!(content_a.structural, Structural::Link);

        // Walk the prev chunks. For a Link entry this returns the leaf
        // chunk NodeEntries in order. Each prev chunk should be plain
        // (no plaintext_hash on the chunk's ContentRef — that's the
        // condition that exercises the `unwrap_or(content.hash)`
        // fallback in the dedup path). NOTE: the root Link entry's
        // ContentRef does carry a plaintext_hash (the tree-root
        // plaintext hash), so we only check chunks here.
        let prev_chunks = snap.collect_byte_stream_chunks(&entry_a).await.unwrap();
        assert!(prev_chunks.len() >= 2, "expected multi-chunk prev");
        for c in &prev_chunks {
            assert!(
                c.content.as_ref().unwrap().plaintext_hash.is_none(),
                "plain-pipeline chunk should have plaintext_hash=None"
            );
        }

        // Re-import the same bytes with prev_chunks. Every chunk should
        // dedup against the prev tree at the matching offset, yielding
        // a NodeEntry whose content.hash equals the original entry's
        // content.hash (because the resulting tree is byte-identical).
        let reader_b = tokio::io::BufReader::new(&data[..]);
        let entry_b = snap
            .import_stream_with_prev(reader_b, store.as_ref(), None, &prev_chunks)
            .await
            .unwrap();
        let content_b = entry_b.content.as_ref().unwrap();
        assert_eq!(content_b.structural, Structural::Link);
        assert_eq!(content_b.size, content_a.size);
        assert_eq!(
            content_b.hash, content_a.hash,
            "dedup re-import should produce identical root hash for unchanged content"
        );

        // Sanity: contents round-trip after dedup.
        let restored = snap.export_bytes(&entry_b).await.unwrap();
        assert_eq!(&restored[..], &data[..]);
    }

    /// Plain-pipeline dedup for appended content: re-import with prev
    /// chunks preserves the leading chunks' identities and only emits
    /// new NodeEntries for the appended tail. The final root differs
    /// from the prev root (because the file grew), but the leading
    /// chunks dedup correctly.
    #[tokio::test]
    async fn plain_dedup_appended_file_reuses_leading_chunks() {
        let store = test_rw_store();
        let snap = Snapshot::empty_plain(store.clone());

        let head: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
        let mut grown = head.clone();
        // Append enough bytes to create at least one extra chunk.
        grown.extend((0..256 * 1024).map(|i| (i % 197) as u8));

        let entry_head = snap
            .import_stream(tokio::io::BufReader::new(&head[..]), store.as_ref(), None)
            .await
            .unwrap();
        let prev_chunks = snap.collect_byte_stream_chunks(&entry_head).await.unwrap();

        let entry_grown = snap
            .import_stream_with_prev(
                tokio::io::BufReader::new(&grown[..]),
                store.as_ref(),
                None,
                &prev_chunks,
            )
            .await
            .unwrap();

        // Tree size matches the grown file.
        assert_eq!(
            entry_grown.content.as_ref().unwrap().size,
            grown.len() as u64
        );
        // Root hash MUST differ (file content actually changed).
        assert_ne!(
            entry_grown.content.as_ref().unwrap().hash,
            entry_head.content.as_ref().unwrap().hash
        );
        // Round-trip the grown file.
        let restored = snap.export_bytes(&entry_grown).await.unwrap();
        assert_eq!(&restored[..], &grown[..]);
    }

    /// Verify that compressible data does NOT get a child_context override.
    #[tokio::test]
    async fn compression_skip_not_set_for_compressible() {
        let store = test_rw_store();
        let master = [42u8; 32];
        let snap = Snapshot::empty_encrypted(store.clone(), master);

        // Highly compressible data: 16 KB zeros.
        // Compressed+padded (4096) < uncompressed+padded (16384) →
        // compression wins → no child_context override.
        let data = vec![0u8; 16384];

        let entry = snap
            .import_bytes(&data, store.as_ref(), None)
            .await
            .unwrap();

        // Compressible data should NOT have a child_context override.
        assert!(
            entry.child_context.is_none(),
            "expected no child_context for compressible data"
        );

        // Round-trip.
        let restored = snap.export_bytes(&entry).await.unwrap();
        assert_eq!(&restored[..], &data[..], "compressible round-trip mismatch");
    }

    /// #3 oracle: `import_file_append` (append-aware, reads only the tail)
    /// MUST produce a NodeEntry byte-identical to a full import of the same
    /// grown file — same root content hash, size, and round-trip. This is the
    /// safety net for reusing the unchanged-prefix chunks on append-only files
    /// (interner/ledger) without reading them. Covers partial-tail growth,
    /// exact-multiple heads, several-chunk growth, and the no-growth
    /// (idempotent) case.
    #[tokio::test]
    async fn append_import_matches_full_oracle() {
        let store = test_rw_store();
        // Plain (uncompressed, unencrypted — feedy's interner/ledger case) +
        // Fixed chunking, so the append fast path engages (not the fallback).
        let base = Snapshot::empty_plain(store.clone() as Arc<dyn s5_core::BlobsRead>);
        let chunk_size = 64 * 1024usize;
        let mut ctx = base.context().clone();
        ctx.chunking = Some(crate::node::FileChunkingStrategy::Fixed {
            chunk_size: chunk_size as u32,
        });
        let snap = base.with_ctx(ctx);

        // Unique temp path (no tempfile dev-dep in this crate).
        let path = std::env::temp_dir().join(format!(
            "s5_append_oracle_{}_{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        // (head_len, tail_len) shapes: partial-tail head growing into new
        // chunks; exact-multiple head + tiny append; no growth; multi-chunk
        // growth crossing a boundary.
        for (head_len, tail_len) in [
            (3 * chunk_size + 1234, 150 * 1024),
            (4 * chunk_size, 7),
            (2 * chunk_size + 10, 0),
            (5 * chunk_size + 9, 64 * 1024 + 3),
        ] {
            let head: Vec<u8> = (0..head_len).map(|i| (i % 251) as u8).collect();
            std::fs::write(&path, &head).unwrap();
            let entry_head = {
                let f = tokio::fs::File::open(&path).await.unwrap();
                snap.import_stream(f, store.as_ref(), None).await.unwrap()
            };
            assert_eq!(
                entry_head.content.as_ref().unwrap().structural,
                Structural::Link,
                "head must be multi-chunk (head_len={head_len})"
            );
            let prev_chunks = snap.collect_byte_stream_chunks(&entry_head).await.unwrap();

            // Grow the file (append-only).
            let mut grown = head.clone();
            grown.extend((0..tail_len).map(|i| ((i * 7 + 3) % 197) as u8));
            std::fs::write(&path, &grown).unwrap();

            // Append-aware import (reads only the tail) vs full fresh import
            // (the canonical tree — independent of prev; dedup only affects
            // upload, not shape/hash).
            let (entry_append, bytes_read) = snap
                .import_file_append(&path, store.as_ref(), None, &prev_chunks)
                .await
                .unwrap();
            let entry_full = {
                let f = tokio::fs::File::open(&path).await.unwrap();
                snap.import_stream(f, store.as_ref(), None).await.unwrap()
            };

            let ca = entry_append.content.as_ref().unwrap();
            let cf = entry_full.content.as_ref().unwrap();
            assert_eq!(
                ca.hash, cf.hash,
                "append root MUST equal full import (head={head_len} tail={tail_len})"
            );
            // The whole point: we read only the tail, never the full file. The
            // tail is (head's partial last chunk) + the appended bytes, bounded
            // well under the full size for these multi-chunk heads.
            assert!(
                bytes_read < ca.size,
                "must read less than the whole file (read={bytes_read} size={} head={head_len} tail={tail_len})",
                ca.size
            );
            assert_eq!(
                ca.size, cf.size,
                "size mismatch (head={head_len} tail={tail_len})"
            );
            assert_eq!(ca.size, grown.len() as u64);
            let restored = snap.export_bytes(&entry_append).await.unwrap();
            assert_eq!(
                &restored[..],
                &grown[..],
                "append round-trip (head={head_len} tail={tail_len})"
            );
        }
        let _ = std::fs::remove_file(&path);
    }
}
