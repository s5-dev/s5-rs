//! Per-blob and tree-traversal pipeline operations, factored out of
//! [`Snapshot`](crate::snapshot::Snapshot) so consumers can use them
//! without holding a snapshot.
//!
//! A [`Pipeline`] bundles the things every blob-level operation needs:
//! a read store (for downloads + dedup), a [`TraversalContext`] (the
//! encryption keys + leaf/node `BlobPipeline`s), a per-pipeline node
//! cache (so repeated `load`s on the same hash are an `Arc` bump), and
//! a per-pipeline `skip_when_unhelpful` flag (carried inside the
//! `BlobPipeline`s in [`TraversalContext`]).
//!
//! `Snapshot` now carries an `Arc<Pipeline>` internally. Its per-blob
//! methods (`load`, `import_bytes`, `export_bytes`, `child`, …) are
//! thin delegates onto the `Pipeline`. Tree-shaped operations
//! (`merge_and_persist` and friends) still live on `Snapshot` because
//! they need a concrete root + chunk-mask source — see
//! [`crate::persist`].
//!
//! Holding the pipeline as a separate type lets layered consumers
//! (e.g. a writable FUSE mount that wraps a base layer in a
//! `WritableOverlay`) reach for the encryption + storage machinery
//! without committing to a particular `Snapshot` identity.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use futures::stream::{BoxStream, FuturesOrdered, StreamExt};
use s5_core::{BlobsRead, BlobsWrite, Hash};

use crate::context::{self, KDF_LEAF, KDF_META};
use crate::node::{
    BlobPipeline, CompressionStrategy, ContentRef, Node, NodeEntry, NodeKind, SemanticMeta,
    Structural, TraversalContext,
};
use crate::snapshot::merge_contexts;

/// Leaf blobs fetched concurrently when exporting a chunked file
/// ([`Pipeline::export_byte_chunks`]). Reads are network-latency-bound (a Sia
/// download needs only `data_shards` hosts), so overlapping fetches recovers the
/// throughput a serial walk left on the table. Bounded so the working set is
/// ~`EXPORT_CONCURRENCY × chunk_size`; chunks are small, so this is phone-safe.
pub(crate) const EXPORT_CONCURRENCY: usize = 8;

/// Decoded-node cache shared between a [`Snapshot`](crate::snapshot::Snapshot)
/// and any [`Pipeline`]s derived from it. Keyed by CAS hash; values
/// are `Arc<Node>` so cache hits return a cheap pointer bump instead
/// of cloning the whole `BTreeMap` inside each `Node`.
///
/// Lifted to its own type so `Snapshot` and `Pipeline` can share one
/// by `Arc::clone` without the previous `Pipeline::with_cache`
/// constructor that existed only to thread the cache through the
/// `Snapshot::as_pipeline` seam.
#[derive(Debug, Default)]
pub struct NodeCache {
    inner: RwLock<HashMap<Hash, Arc<Node>>>,
}

impl NodeCache {
    /// New empty cache. Wrap in `Arc` for sharing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Cache hit returns a cheap `Arc::clone`; miss returns `None`.
    pub fn get(&self, hash: &Hash) -> Option<Arc<Node>> {
        self.inner.read().unwrap().get(hash).cloned()
    }

    /// Insert (or replace) a decoded node. Idempotent.
    pub fn insert(&self, hash: Hash, node: Arc<Node>) {
        self.inner.write().unwrap().insert(hash, node);
    }
}

/// Optional per-chunk plaintext cache for
/// [`Pipeline::export_byte_chunks`]. Keyed by a chunk's *ciphertext*
/// (CAS blob) hash. The pipeline consults `get` before decoding a chunk
/// and calls `insert` after a miss; the host owns the policy (byte
/// budget, eviction, thread-safety). Distinct from [`NodeCache`], which
/// caches decoded chunk-tree *nodes* — this caches decoded leaf
/// *plaintext*.
pub trait ChunkCache: Send + Sync {
    fn get(&self, key: &[u8; 32]) -> Option<Bytes>;
    fn insert(&self, key: [u8; 32], value: Bytes);
}

/// Owned, `Send + 'static` leaf-encode config, so the CPU-bound encode
/// can run on a `spawn_blocking` thread without borrowing the `Pipeline`.
#[derive(Clone)]
pub(crate) struct LeafEncodeConfig {
    leaf: Option<BlobPipeline>,
    keys: Option<BTreeMap<u8, [u8; 32]>>,
}

impl LeafEncodeConfig {
    pub(crate) fn from_ctx(ctx: &TraversalContext) -> Self {
        Self {
            leaf: ctx.leaf.clone(),
            keys: ctx.keys.clone(),
        }
    }
}

/// Output of the CPU-bound leaf encode (compress/pad/encrypt), before
/// upload — everything [`assemble_leaf_entry`] needs once the blob hash
/// is known. `has_transforms` is whether the leaf pipeline transforms at
/// all (drives whether `plaintext_hash`/`stored_blocks` are recorded).
pub(crate) struct LeafEncoded {
    pub bytes: Bytes,
    pub plaintext_hash: [u8; 32],
    pub plaintext_size: u64,
    pub stored_blocks: u64,
    pub compression_skipped: bool,
    pub has_transforms: bool,
}

/// Pure CPU stage: hash + compress + pad + encrypt. No I/O and no `self`
/// borrow, so it is safe to run inside [`tokio::task::spawn_blocking`].
pub(crate) fn encode_leaf(cfg: &LeafEncodeConfig, plaintext: &[u8]) -> anyhow::Result<LeafEncoded> {
    let plaintext_size = plaintext.len() as u64;
    let plaintext_hash: [u8; 32] = *blake3::hash(plaintext).as_bytes();
    let result = context::pipeline_encode(
        plaintext,
        cfg.leaf.as_ref(),
        &plaintext_hash,
        KDF_LEAF,
        cfg.keys.as_ref(),
    )?;
    Ok(LeafEncoded {
        bytes: result.bytes,
        plaintext_hash,
        plaintext_size,
        stored_blocks: result.stored_blocks,
        compression_skipped: result.compression_skipped,
        has_transforms: cfg.leaf.is_some(),
    })
}

/// Assemble the `NodeEntry` for a freshly-uploaded leaf blob. Mirrors
/// the tail of [`Pipeline::import_bytes`]; shared with the concurrent
/// chunk-import pipeline so both produce byte-identical entries.
pub(crate) fn assemble_leaf_entry(
    enc: &LeafEncoded,
    blob_hash: [u8; 32],
    semantic: Option<SemanticMeta>,
) -> NodeEntry {
    let (pt_hash, blocks) = if enc.has_transforms {
        (Some(enc.plaintext_hash), Some(enc.stored_blocks))
    } else {
        (None, None)
    };

    // When compression was skipped, record a per-entry override so the
    // decoder knows this blob is stored uncompressed despite the default
    // pipeline specifying Zstd.
    let child_context = if enc.compression_skipped {
        Some(Box::new(TraversalContext {
            keys: None,
            leaf: Some(BlobPipeline {
                compression: Some(CompressionStrategy::Uncompressed),
                padding: None,
                encryption: None,
                skip_when_unhelpful: None,
            }),
            node: None,
            chunking: None,
        }))
    } else {
        None
    };

    NodeEntry {
        content: Some(ContentRef {
            structural: Structural::Leaf,
            hash: blob_hash,
            size: enc.plaintext_size,
            plaintext_hash: pt_hash,
            stored_blocks: blocks,
        }),
        semantic,
        child_context,
        tombstone: None,
    }
}

/// Per-blob and per-node pipeline machinery.
///
/// All state needed to read, write, and decode blobs/nodes against a
/// blob store using a particular [`TraversalContext`]. Cheap to clone
/// (just `Arc` and field clones); intended to be wrapped in `Arc` and
/// shared.
pub struct Pipeline {
    store: Arc<dyn BlobsRead>,
    ctx: TraversalContext,
    /// Shared decoded-node cache — same `Arc` as the originating
    /// [`Snapshot::node_cache`](crate::snapshot::Snapshot::node_cache),
    /// so loads via either side see each other's cache hits.
    node_cache: Arc<NodeCache>,
}

impl Pipeline {
    /// Build a pipeline with a fresh node cache. For sharing a cache
    /// with an existing snapshot, pass `Arc::clone(snap.node_cache())`
    /// in via the field-style ctor [`Pipeline::with_node_cache`].
    pub fn new(store: Arc<dyn BlobsRead>, ctx: TraversalContext) -> Self {
        Self {
            store,
            ctx,
            node_cache: Arc::new(NodeCache::new()),
        }
    }

    /// Build a pipeline sharing the given node cache. Used by
    /// `Snapshot::as_pipeline` so the resulting pipeline hits the
    /// same cache as the snapshot's own internal loads.
    pub fn with_node_cache(
        store: Arc<dyn BlobsRead>,
        ctx: TraversalContext,
        node_cache: Arc<NodeCache>,
    ) -> Self {
        Self {
            store,
            ctx,
            node_cache,
        }
    }

    /// Underlying decoded-node cache. Clone the `Arc` to share it
    /// with another pipeline.
    pub fn node_cache(&self) -> &Arc<NodeCache> {
        &self.node_cache
    }

    /// Read store backing this pipeline.
    pub fn store(&self) -> &Arc<dyn BlobsRead> {
        &self.store
    }

    /// Traversal context (keys + leaf/node `BlobPipeline`s).
    pub fn context(&self) -> &TraversalContext {
        &self.ctx
    }

    /// Returns the leaf blob pipeline.
    pub fn leaf_pipeline(&self) -> Option<&BlobPipeline> {
        self.ctx.leaf.as_ref()
    }

    /// Returns the node blob pipeline.
    pub fn node_pipeline(&self) -> Option<&BlobPipeline> {
        self.ctx.node.as_ref()
    }

    /// Derive a child pipeline from an entry, merging the entry's
    /// `child_context` overrides into this pipeline's context. The
    /// node cache is shared with the parent (Arc clone). Used when
    /// descending into a sub-tree that may have its own per-blob
    /// pipeline overrides (e.g. an `Uncompressed` leaf strategy).
    pub fn child_for(&self, entry: &NodeEntry) -> Self {
        let child_ctx = match entry.child_context.as_ref() {
            Some(child_tcx) => merge_contexts(&self.ctx, child_tcx),
            None => self.ctx.clone(),
        };
        Self {
            store: Arc::clone(&self.store),
            ctx: child_ctx,
            node_cache: Arc::clone(&self.node_cache),
        }
    }

    // =====================================================================
    // Node load / cache
    // =====================================================================

    /// Loads and decodes a [`Node`] from the blob store.
    ///
    /// Uses the node pipeline for decryption/decompression. `plaintext_hash`
    /// is needed for encrypted nodes — it comes from the parent
    /// `ContentRef.plaintext_hash`.
    ///
    /// Results are cached by hash — repeated loads of the same node skip
    /// blob download, decryption, decompression, and CBOR parsing entirely.
    pub async fn load(
        &self,
        hash: Hash,
        plaintext_hash: Option<&[u8; 32]>,
    ) -> anyhow::Result<Arc<Node>> {
        // Fast path: return cached decoded node (cheap Arc bump).
        if let Some(node) = self.node_cache.get(&hash) {
            return Ok(node);
        }

        let raw_bytes = self
            .store
            .blob_download(hash)
            .await
            .map_err(|e| anyhow::anyhow!("loading Node {hash}: {e}"))?;

        // Verify content-addressing integrity before decode.
        // `blob_download` returns bytes keyed by hash; if the storage
        // layer is corrupted (bit rot, partial write, fjall bug),
        // this catches it with a clear error instead of a cryptic
        // CBOR decode failure.
        //
        // Defense-in-depth note: `s5_core::BlobStore::blob_download`
        // already verifies the hash before returning. This check is
        // intentional duplication at the FS layer because Node decode
        // is the most security-sensitive consumer (prolly tree
        // structure, keys, child contexts), and the cost is one
        // BLAKE3 over a typically-KB-scale node — negligible.
        let actual = Hash::from(*blake3::hash(&raw_bytes).as_bytes());
        if actual != hash {
            anyhow::bail!(
                "integrity check failed for Node {hash}: \
                 stored bytes hash to {actual} (corrupted blob)"
            );
        }

        // Node blobs are always compressed (zstd) so the decompressor
        // ignores trailing padding. plaintext_size = 0 is a sentinel
        // here — pipeline_decode handles it.
        let plaintext_size = 0;

        let decoded = context::pipeline_decode(
            raw_bytes,
            self.ctx.node.as_ref(),
            plaintext_hash,
            plaintext_size,
            KDF_META,
            self.ctx.keys.as_ref(),
        )?;

        let node =
            Node::from_bytes(&decoded).map_err(|e| anyhow::anyhow!("decoding Node {hash}: {e}"))?;

        let node = Arc::new(node);
        self.node_cache.insert(hash, Arc::clone(&node));
        Ok(node)
    }

    // =====================================================================
    // Leaf import / export
    // =====================================================================

    /// Encrypt + compress + upload a leaf, returning a `NodeEntry`
    /// pointing at the resulting blob.
    pub async fn import_bytes(
        &self,
        plaintext: &[u8],
        store: &dyn BlobsWrite,
        semantic: Option<SemanticMeta>,
    ) -> anyhow::Result<NodeEntry> {
        let t = std::time::Instant::now();
        let cfg = LeafEncodeConfig::from_ctx(&self.ctx);
        let enc = encode_leaf(&cfg, plaintext)?;

        let blob_id = store
            .blob_upload_bytes(enc.bytes.clone())
            .await
            .map_err(|e| anyhow::anyhow!("uploading blob: {e}"))?;

        let entry = assemble_leaf_entry(&enc, *blob_id.hash.as_bytes(), semantic);
        crate::import_stats::add_import_bytes(t.elapsed().as_nanos() as u64);
        Ok(entry)
    }

    /// Download, decrypt, decompress, and verify a leaf entry's
    /// content, materialising the full plaintext in memory. For chunked
    /// files (`Structural::Link`) the chunk tree is walked and the
    /// chunks concatenated in order.
    ///
    /// This is exactly [`export_byte_chunks`](Self::export_byte_chunks)
    /// collected and concatenated (uncached) — one walk implementation,
    /// two consumers, so the two can never diverge.
    pub async fn export_bytes(&self, entry: &NodeEntry) -> anyhow::Result<Bytes> {
        let size_hint = entry.content.as_ref().map(|c| c.size as usize).unwrap_or(0);
        let mut all_bytes = bytes::BytesMut::with_capacity(size_hint);
        let mut stream = self.export_byte_chunks(entry, None);
        while let Some(res) = stream.next().await {
            all_bytes.extend_from_slice(&res?);
        }
        Ok(all_bytes.freeze())
    }

    /// Stream a leaf entry's plaintext **one chunk at a time** — the
    /// per-chunk sibling of [`export_bytes`](Self::export_bytes).
    ///
    /// A single-leaf entry yields exactly one item (the whole blob); a
    /// chunked file (`Structural::Link`) yields one plaintext `Bytes`
    /// per chunk, in order. `cache`, if supplied, is consulted by chunk
    /// *ciphertext* hash before decoding and populated on a miss (the
    /// host owns the policy; the pipeline only does get/insert). Pass
    /// `None` for an uncached pass.
    ///
    /// The recursion is boxed inside
    /// [`walk_byte_stream`](Self::walk_byte_stream).
    pub fn export_byte_chunks<'a>(
        &'a self,
        entry: &'a NodeEntry,
        cache: Option<&'a dyn ChunkCache>,
    ) -> BoxStream<'a, anyhow::Result<Bytes>> {
        Box::pin(async_stream::try_stream! {
            let content = entry
                .content
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("cannot export tombstone entry"))?;

            if content.structural != Structural::Link {
                // Single leaf entry — one chunk.
                yield self.export_leaf(entry).await?;
            } else {
                // Chunked file: load the chunk-tree node, walk it,
                // decode (or serve from `cache`) each chunk in order.
                let node = self
                    .load(content.hash(), content.plaintext_hash.as_ref())
                    .await?;
                if node.header.kind != NodeKind::ByteStream {
                    Err(anyhow::anyhow!(
                        "cannot export structural link of kind {:?}",
                        node.header.kind
                    ))?;
                }

                // Fetch up to EXPORT_CONCURRENCY leaf blobs at once, yielding
                // them IN ORDER via `FuturesOrdered` — output is byte-identical
                // to a serial export, but the per-host download latency that
                // dominates Sia reads is overlapped. `cache` is consulted before
                // a fetch is scheduled and populated when the chunk is yielded,
                // same policy as the serial path.
                let child_pipe = self.child_for(entry);
                let mut walk =
                    child_pipe.walk_byte_stream(content.hash(), content.plaintext_hash);
                let mut inflight = FuturesOrdered::new();
                let mut walk_done = false;

                loop {
                    // Keep the fetch window full.
                    while !walk_done && inflight.len() < EXPORT_CONCURRENCY {
                        match walk.next().await {
                            Some(res) => {
                                let chunk_entry = res?;
                                let chunk_key = chunk_entry.content.as_ref().map(|c| c.hash);
                                let hit = match (chunk_key, cache) {
                                    (Some(key), Some(c)) => c.get(&key),
                                    _ => None,
                                };
                                let cp = &child_pipe;
                                inflight.push_back(async move {
                                    match hit {
                                        Some(b) => {
                                            Ok::<(Option<[u8; 32]>, Bytes), anyhow::Error>((None, b))
                                        }
                                        None => {
                                            let b = cp.export_leaf(&chunk_entry).await?;
                                            Ok((chunk_key, b))
                                        }
                                    }
                                });
                            }
                            None => walk_done = true,
                        }
                    }

                    match inflight.next().await {
                        Some(res) => {
                            let (to_cache, b) = res?;
                            if let (Some(key), Some(c)) = (to_cache, cache) {
                                c.insert(key, b.clone());
                            }
                            yield b;
                        }
                        None => break,
                    }
                }
            }
        })
    }

    /// Download, decrypt, decompress, and verify a single leaf blob.
    pub async fn export_leaf(&self, entry: &NodeEntry) -> anyhow::Result<Bytes> {
        let content = entry
            .content
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("cannot export tombstone entry"))?;

        let ciphertext = self
            .store
            .blob_download(content.hash())
            .await
            .map_err(|e| anyhow::anyhow!("downloading blob {}: {e}", content.hash()))?;

        // Merge per-entry leaf pipeline override (e.g. Uncompressed) if present.
        let effective_ctx = match entry.child_context.as_ref() {
            Some(child_tcx) => merge_contexts(&self.ctx, child_tcx),
            None => self.ctx.clone(),
        };

        let plaintext = context::pipeline_decode(
            ciphertext,
            effective_ctx.leaf.as_ref(),
            content.plaintext_hash.as_ref(),
            content.size,
            KDF_LEAF,
            effective_ctx.keys.as_ref(),
        )?;

        if let Some(expected_hash) = &content.plaintext_hash {
            let actual_hash = blake3::hash(&plaintext);
            if actual_hash.as_bytes() != expected_hash {
                anyhow::bail!(
                    "plaintext hash mismatch for blob {}: expected {}, got {}",
                    content.hash(),
                    Hash::from(*expected_hash),
                    actual_hash,
                );
            }
        }

        Ok(plaintext)
    }

    /// Recursive walk of a chunked-file tree, yielding leaf chunk
    /// entries in order. Used by [`export_bytes`] for `Structural::Link`
    /// leaves; descends into internal `ByteStream` levels and yields
    /// each chunk entry.
    pub fn walk_byte_stream<'a>(
        &'a self,
        hash: Hash,
        plaintext_hash: Option<[u8; 32]>,
    ) -> BoxStream<'a, anyhow::Result<NodeEntry>> {
        Box::pin(async_stream::try_stream! {
            let node = self.load(hash, plaintext_hash.as_ref()).await?;
            if node.header.kind != NodeKind::ByteStream {
                Err(anyhow::anyhow!("expected ByteStream node, found {:?}", node.header.kind))?;
            }

            if node.header.level > 0 {
                // Internal node — recurse into each link child.
                for entry in node.entries.values() {
                    if entry.is_link() {
                        let content = entry.content.as_ref().unwrap();
                        let child = self.child_for(entry);
                        let mut s = std::pin::pin!(child.walk_byte_stream(
                            content.hash(),
                            content.plaintext_hash,
                        ));
                        while let Some(item) = s.next().await {
                            let chunk_entry = item?;
                            yield chunk_entry;
                        }
                    }
                }
            } else {
                // Leaf node of chunks.
                for entry in node.entries.values() {
                    yield entry.clone();
                }
            }
        })
    }

    /// Like [`export_byte_chunks`](Self::export_byte_chunks) but downloads
    /// ONLY the chunk blobs at the `wanted` indices (which MUST be sorted
    /// ascending, no duplicates required). Walks the chunk-tree — cheap node
    /// metadata — and `export_leaf`s only the wanted chunks, stopping after
    /// the last one. Yields `(chunk_index, plaintext)` in ascending order.
    ///
    /// For sparse random access into a large append-only blob (e.g. seeking
    /// to a few records scattered through a multi-GB segment pack) this
    /// fetches a handful of chunk blobs instead of the whole file. Note:
    /// byte-ranges *within* a chunk are not possible — chunks are compressed,
    /// so the chunk is the fetch granularity. Pick a chunk size (file
    /// pipeline `Fixed { chunk_size }`) that makes this granularity useful.
    pub fn export_byte_chunks_at<'a>(
        &'a self,
        entry: &'a NodeEntry,
        wanted: &'a [usize],
        cache: Option<&'a dyn ChunkCache>,
    ) -> BoxStream<'a, anyhow::Result<(usize, Bytes)>> {
        Box::pin(async_stream::try_stream! {
            if wanted.is_empty() {
                return;
            }
            let content = entry
                .content
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("cannot export tombstone entry"))?;

            if content.structural != Structural::Link {
                // Single-leaf entry — only chunk index 0 exists.
                if wanted.contains(&0) {
                    yield (0usize, self.export_leaf(entry).await?);
                }
                return;
            }

            let node = self
                .load(content.hash(), content.plaintext_hash.as_ref())
                .await?;
            if node.header.kind != NodeKind::ByteStream {
                Err(anyhow::anyhow!(
                    "cannot export structural link of kind {:?}",
                    node.header.kind
                ))?;
            }

            let child_pipe = self.child_for(entry);
            let last_wanted = *wanted.last().unwrap();
            let mut want_pos = 0usize; // pointer into the sorted `wanted`
            let mut idx = 0usize;
            let mut stream =
                child_pipe.walk_byte_stream(content.hash(), content.plaintext_hash);
            while let Some(res) = stream.next().await {
                let chunk_entry = res?;
                if idx > last_wanted {
                    break;
                }
                // Advance past any wanted indices we've already passed
                // (tolerates duplicates and out-of-range entries).
                while want_pos < wanted.len() && wanted[want_pos] < idx {
                    want_pos += 1;
                }
                if want_pos < wanted.len() && wanted[want_pos] == idx {
                    let chunk_key = chunk_entry.content.as_ref().map(|c| c.hash);
                    let b = match (chunk_key, cache) {
                        (Some(key), Some(c)) => {
                            if let Some(hit) = c.get(&key) {
                                hit
                            } else {
                                let b = child_pipe.export_leaf(&chunk_entry).await?;
                                c.insert(key, b.clone());
                                b
                            }
                        }
                        _ => child_pipe.export_leaf(&chunk_entry).await?,
                    };
                    yield (idx, b);
                    want_pos += 1;
                }
                idx += 1;
            }
        })
    }
}
