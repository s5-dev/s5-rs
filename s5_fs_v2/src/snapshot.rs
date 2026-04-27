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

/// Default padding block size (1 KiB).
///
/// After compression and before encryption, blob bytes are zero-padded up to
/// the next multiple of this size. An observer sees the padded length on the
/// wire (the plaintext length is unknowable without the decryption key); the
/// padding bucket width determines how much size-fingerprint information
/// leaks per blob.
///
/// 1 KiB is chosen because:
///
/// - At a ~64 KiB average chunk size (see [`chunking`](crate::chunking)),
///   1 KiB padding adds at most ~1.5 % storage overhead per leaf chunk
///   (worst case: 1023 bytes of padding on a 1024-byte-aligned blob).
/// - 1 024-byte bucket granularity gives enough size obfuscation that size
///   alone is a weak identifier for mid-to-large files (the typical target of
///   a content-addressed store). It does not defend against adversaries who
///   already have strong content-hash or filename signals.
/// - Larger padding (e.g. 4 KiB) would give coarser buckets for modest extra
///   obfuscation but adds ~4–6 % storage overhead on small/medium chunks,
///   which is a poor trade for a general-purpose FS.
///
/// Internal prolly-tree node blobs inherit this same padding from the node
/// pipeline and pay a higher relative overhead (node blobs are typically
/// 1–8 KiB). A different `node.padding` policy is a reasonable future tweak
/// but is not currently differentiated.
const DEFAULT_PAD_BLOCK_SIZE: u32 = 1024;

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
        }
    }

    /// Creates an empty, unencrypted snapshot.
    pub fn empty_plain(store: Arc<dyn BlobsRead>) -> Self {
        Self::empty(store, TraversalContext::default())
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
            None, // nodes never use dictionary compression
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

    /// Reads an async stream, applies CDC chunking for large streams, and uploads blobs.
    ///
    /// If the total stream is under 64KiB, it is uploaded as a single `Leaf`.
    /// Otherwise, it is chunked using Content-Defined Chunking (CDC) into
    /// a series of blobs, and a `ByteStream` prolly tree is created and
    /// uploaded to `store`.
    ///
    /// Returns a `NodeEntry` pointing either to the single leaf blob or the
    /// root of the `ByteStream` chunk tree.
    pub async fn import_stream<R: tokio::io::AsyncRead + std::marker::Unpin>(
        &self,
        stream: R,
        store: &dyn BlobsWrite,
        semantic: Option<SemanticMeta>,
    ) -> anyhow::Result<NodeEntry> {
        let mut chunker = crate::chunking::XetChunker::new(stream);

        // Peek first chunk to see if the file is tiny.
        let first_chunk = match chunker.next_chunk().await {
            Ok(Some(c)) => c,
            Ok(None) => {
                // Empty file — no dictionary needed.
                return self.import_bytes(&[], store, semantic, None).await;
            }
            Err(e) => return Err(anyhow::anyhow!("cdc error: {e}")),
        };

        let second_chunk = chunker
            .next_chunk()
            .await
            .map_err(|e| anyhow::anyhow!("cdc error: {e}"))?;

        if second_chunk.is_none() {
            // Single-chunk file — no dictionary needed (always a D-chunk).
            return self.import_bytes(&first_chunk, store, semantic, None).await;
        }

        // Multi-chunk file — apply D-chunk dictionary compression.
        //
        // TODO(high-priority, OOM on large files): this collects every chunk
        // of the file into `all_chunks` before building the tree. For a
        // 10 GiB VM image that is ~10 GiB of `Bytes` handles plus the
        // plaintext in memory, which kills the entire process on any
        // realistic mobile/laptop target. The contract of this function
        // needs to be "bounded memory regardless of file size":
        //
        //   - stream each chunk through `import_bytes` as it arrives (the
        //     upload/dedup path already supports that per-chunk),
        //   - emit `(key, NodeEntry)` tuples into a bounded channel,
        //   - let `chunk_entries` + `build_tree_dedup` consume incrementally,
        //   - keep at most ~N chunks buffered to preserve the dict-chunk-
        //     lookback window (N ~= 1/mask + small constant).
        //
        // Until this is fixed, blocker for VM images, DB dumps, long video,
        // ML weight files — the exact "huge file" workloads `import_stream`
        // is advertised to handle. Was flagged as Critical in the internal
        // code review; commit-message honesty about the current limitation
        // is in c8d9938 (feat: implement streaming CDC chunking for file
        // import).
        let mut all_chunks = vec![first_chunk];
        if let Some(c) = second_chunk {
            all_chunks.push(c);
        }

        while let Some(chunk) = chunker
            .next_chunk()
            .await
            .map_err(|e| anyhow::anyhow!("cdc error: {e}"))?
        {
            all_chunks.push(chunk);
        }

        // Extract D-chunk mask from the leaf compression strategy.
        let dict_mask = self.ctx.leaf.as_ref().and_then(|p| match &p.compression {
            Some(CompressionStrategy::ZstdDictFromPrecedingEntry { mask }) => Some(*mask),
            _ => None,
        });

        let mut entries = Vec::with_capacity(all_chunks.len());
        let mut offset: u64 = 0;
        let mut total_size: u64 = 0;
        // Current D-chunk content (used as dictionary for subsequent chunks).
        let mut dict_content: Option<Vec<u8>> = None;

        for (i, chunk) in all_chunks.iter().enumerate() {
            let plaintext_hash: [u8; 32] = *blake3::hash(chunk).as_bytes();

            // Determine if this chunk is a D-chunk:
            // - First chunk of a file is always a D-chunk
            // - Any chunk where plaintext_hash[0] & mask == 0
            let is_d_chunk = match dict_mask {
                Some(mask) => i == 0 || (plaintext_hash[0] & mask) == 0,
                None => true, // no dict compression — every chunk is independent
            };

            let dictionary = if is_d_chunk {
                None // D-chunks use plain Zstd (no dictionary)
            } else {
                dict_content.as_deref()
            };

            let chunk_entry = self.import_bytes(chunk, store, None, dictionary).await?;

            // Update dictionary: D-chunks become the new dictionary.
            if is_d_chunk && dict_mask.is_some() {
                dict_content = Some(chunk.to_vec());
            }

            let key = format!("{:016x}", offset);
            entries.push((key, chunk_entry));
            offset += chunk.len() as u64;
            total_size += chunk.len() as u64;
        }

        // We build a `NodeKind::ByteStream` tree from the chunks.
        // We use a default mask of 0x3F (64 entries per node on average).
        let mask = 0x3F;
        let leaf_nodes =
            crate::persist::chunk_entries(&entries, mask, &crate::node::NodeKind::ByteStream, 0);

        let mut stats = crate::persist::MergeStats::default();
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

    /// Import in-memory bytes into the blob store as a leaf entry.
    /// Delegates to
    /// [`Pipeline::import_bytes`](crate::pipeline::Pipeline::import_bytes).
    pub async fn import_bytes(
        &self,
        plaintext: &[u8],
        store: &dyn BlobsWrite,
        semantic: Option<SemanticMeta>,
        dictionary: Option<&[u8]>,
    ) -> anyhow::Result<NodeEntry> {
        self.as_pipeline()
            .import_bytes(plaintext, store, semantic, dictionary)
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
    /// trees are NOT descended — they are yielded as Link entries for the
    /// consumer to handle via `export_bytes`.
    ///
    /// Tombstones are skipped.
    /// Paths are built by joining namespace segments with `/`.
    pub fn walk(&self) -> BoxStream<'_, anyhow::Result<(String, NodeEntry)>> {
        self.walk_inner(self.root, self.root_plaintext_hash, String::new())
    }

    fn walk_inner(
        &self,
        hash: Hash,
        plaintext_hash: Option<[u8; 32]>,
        prefix: String,
    ) -> BoxStream<'_, anyhow::Result<(String, NodeEntry)>> {
        Box::pin(async_stream::try_stream! {
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

                            // Leaf-level namespace entries are always yielded
                            // (files, dirs, symlinks, chunked files). The consumer
                            // calls `export_bytes` which handles Link→ByteStream
                            // trees internally.
                            yield (path, entry.clone());
                        }
                    }
                }
                NodeKind::ByteStream => {
                    // ByteStream nodes are file content chunks — not walked
                    // during namespace traversal.
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

/// Creates the default encrypted `TraversalContext`.
///
/// Both leaf and node pipelines use plain Zstd compression, 1KiB padding,
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
    /// random (incompressible) data to stress dictionary compression.
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
            .import_bytes(&data, store.as_ref(), None, None)
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

    /// Verify that compressible data does NOT get a child_context override.
    #[tokio::test]
    async fn compression_skip_not_set_for_compressible() {
        let store = test_rw_store();
        let master = [42u8; 32];
        let snap = Snapshot::empty_encrypted(store.clone(), master);

        // Highly compressible data (all zeros).
        let data = vec![0u8; 4096];

        let entry = snap
            .import_bytes(&data, store.as_ref(), None, None)
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
}
