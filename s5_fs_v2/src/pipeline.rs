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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use s5_core::{BlobsRead, BlobsWrite, Hash};

use crate::context::{self, KDF_LEAF, KDF_META};
use crate::node::{
    BlobPipeline, CompressionStrategy, ContentRef, Node, NodeEntry, NodeKind, SemanticMeta,
    Structural, TraversalContext,
};
use crate::snapshot::merge_contexts;

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

        let bytes = self
            .store
            .blob_download(hash)
            .await
            .map_err(|e| anyhow::anyhow!("loading Node {hash}: {e}"))?;

        // Node blobs are always compressed (zstd) so the decompressor
        // ignores trailing padding. plaintext_size = 0 is a sentinel
        // here — pipeline_decode handles it.
        let plaintext_size = 0;

        let decoded = context::pipeline_decode(
            bytes,
            self.ctx.node.as_ref(),
            plaintext_hash,
            plaintext_size,
            KDF_META,
            self.ctx.keys.as_ref(),
            None,
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
    /// pointing at the resulting blob. `dictionary` is the preceding
    /// D-chunk's plaintext for `ZstdDictFromPrecedingEntry`; pass
    /// `None` otherwise.
    pub async fn import_bytes(
        &self,
        plaintext: &[u8],
        store: &dyn BlobsWrite,
        semantic: Option<SemanticMeta>,
        dictionary: Option<&[u8]>,
    ) -> anyhow::Result<NodeEntry> {
        let plaintext_size = plaintext.len() as u64;
        let plaintext_hash_bytes: [u8; 32] = *blake3::hash(plaintext).as_bytes();

        let result = context::pipeline_encode(
            plaintext,
            self.ctx.leaf.as_ref(),
            &plaintext_hash_bytes,
            KDF_LEAF,
            self.ctx.keys.as_ref(),
            dictionary,
        )?;

        let blob_id = store
            .blob_upload_bytes(result.bytes)
            .await
            .map_err(|e| anyhow::anyhow!("uploading blob: {e}"))?;

        let has_transforms = self.ctx.leaf.is_some();
        let (pt_hash, blocks) = if has_transforms {
            (Some(plaintext_hash_bytes), Some(result.stored_blocks))
        } else {
            (None, None)
        };

        // When compression was skipped, record a per-entry override so
        // the decoder knows this blob is stored uncompressed despite
        // the default pipeline specifying Zstd.
        let child_context = if result.compression_skipped {
            Some(Box::new(TraversalContext {
                keys: None,
                leaf: Some(BlobPipeline {
                    compression: Some(CompressionStrategy::Uncompressed),
                    padding: None,
                    encryption: None,
                    skip_when_unhelpful: None,
                }),
                node: None,
            }))
        } else {
            None
        };

        Ok(NodeEntry {
            content: Some(ContentRef {
                structural: Structural::Leaf,
                hash: *blob_id.hash.as_bytes(),
                size: plaintext_size,
                plaintext_hash: pt_hash,
                stored_blocks: blocks,
            }),
            semantic,
            child_context,
            tombstone: None,
        })
    }

    /// Download, decrypt, decompress, and verify a leaf entry's
    /// content. For chunked files (`Structural::Link`) this walks the
    /// chunk tree, fetches each chunk, and concatenates them in order.
    /// For very large files a streaming export is preferable; this one
    /// materialises the full plaintext in memory.
    ///
    /// Plain `async fn` — no recursion to box. The internal recursion
    /// lives in [`walk_byte_stream`](Self::walk_byte_stream), which
    /// already returns a `BoxStream`.
    pub async fn export_bytes(&self, entry: &NodeEntry) -> anyhow::Result<Bytes> {
        let content = entry
            .content
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("cannot export tombstone entry"))?;

        if content.structural != Structural::Link {
            // Single leaf entry — no dictionary.
            return self.export_leaf(entry, None).await;
        }

        // Chunked file: load the chunk-tree node, walk it, export each
        // chunk (handling D-chunk / dependent-chunk dictionary
        // semantics), concatenate.
        let node = self
            .load(content.hash(), content.plaintext_hash.as_ref())
            .await?;
        if node.header.kind != NodeKind::ByteStream {
            anyhow::bail!(
                "cannot export structural link of kind {:?}",
                node.header.kind
            );
        }

        let child_pipe = self.child_for(entry);
        let dict_mask = child_pipe
            .ctx
            .leaf
            .as_ref()
            .and_then(|p| match &p.compression {
                Some(CompressionStrategy::ZstdDictFromPrecedingEntry { mask }) => Some(*mask),
                _ => None,
            });

        let mut stream = child_pipe.walk_byte_stream(content.hash(), content.plaintext_hash);

        let mut all_bytes = bytes::BytesMut::with_capacity(content.size as usize);
        let mut dict_content: Option<Vec<u8>> = None;
        let mut chunk_index: usize = 0;

        while let Some(res) = stream.next().await {
            let chunk_entry = res?;

            let is_d_chunk = match (
                dict_mask,
                chunk_entry
                    .content
                    .as_ref()
                    .and_then(|c| c.plaintext_hash.as_ref()),
            ) {
                (Some(mask), Some(ph)) => chunk_index == 0 || (ph[0] & mask) == 0,
                _ => true,
            };

            let dictionary = if is_d_chunk {
                None
            } else {
                dict_content.as_deref()
            };

            let chunk_bytes = child_pipe.export_leaf(&chunk_entry, dictionary).await?;

            if is_d_chunk && dict_mask.is_some() {
                dict_content = Some(chunk_bytes.to_vec());
            }

            all_bytes.extend_from_slice(&chunk_bytes);
            chunk_index += 1;
        }
        Ok(all_bytes.freeze())
    }

    /// Download, decrypt, decompress, and verify a single leaf blob.
    /// `dictionary` is the D-chunk content for dictionary-based
    /// decompression (`None` for D-chunks or non-dict strategies).
    pub async fn export_leaf(
        &self,
        entry: &NodeEntry,
        dictionary: Option<&[u8]>,
    ) -> anyhow::Result<Bytes> {
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
            dictionary,
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
}
