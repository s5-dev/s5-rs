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

use crate::context::{self, KDF_LEAF, KDF_META};
use crate::layer::ReadableLayer;
use crate::node::{
    BlobPipeline, CompressionStrategy, ContentRef, EncryptionStrategy, Node, NodeEntry, NodeKind,
    PaddingStrategy, SemanticMeta, Structural, TraversalContext,
};

/// Master secret key slot in the on-wire `TraversalContext.keys` map.
const KEY_SLOT_MASTER: u8 = 0x0e;

/// Leaf blob encryption key slot (file content / chunk data).
pub const KEY_SLOT_LEAF: u8 = 0x10;

/// Node blob encryption key slot (serialized metadata).
pub const KEY_SLOT_NODE: u8 = 0x11;

/// Default padding block size (4 KiB).
const DEFAULT_PAD_BLOCK_SIZE: u32 = 4096;

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
    /// On-wire context: keys and blob processing pipelines.
    ctx: TraversalContext,
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
    pub fn empty_encrypted(store: Arc<dyn BlobsRead>, master_secret: [u8; 32]) -> Self {
        Self::empty(store, encrypted_context(master_secret))
    }

    /// Creates an empty, encrypted snapshot with separate leaf and node keys.
    ///
    /// Both pipelines use Zstd compression, 4 KiB padding, and
    /// DeterministicChaCha20 encryption — but with independent keys so
    /// that metadata and content are cryptographically separated.
    ///
    /// The caller is responsible for generating the keys securely
    /// (e.g. `rand::rngs::OsRng`).
    pub fn empty_encrypted_split(
        store: Arc<dyn BlobsRead>,
        leaf_key: [u8; 32],
        node_key: [u8; 32],
    ) -> Self {
        Self::empty(store, encrypted_split_context(leaf_key, node_key))
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

    /// Returns the leaf blob pipeline.
    pub fn leaf_pipeline(&self) -> Option<&BlobPipeline> {
        self.ctx.leaf.as_ref()
    }

    /// Returns the node blob pipeline.
    pub fn node_pipeline(&self) -> Option<&BlobPipeline> {
        self.ctx.node.as_ref()
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
        }
    }

    // =====================================================================
    // Node Loading
    // =====================================================================

    /// Loads and decodes a [`Node`] from the blob store.
    ///
    /// Uses the node pipeline for decryption/decompression. `plaintext_hash`
    /// is needed for encrypted nodes — it comes from the parent
    /// `ContentRef.plaintext_hash`.
    pub async fn load(
        &self,
        hash: Hash,
        plaintext_hash: Option<&[u8; 32]>,
    ) -> anyhow::Result<Node> {
        let bytes = self
            .store
            .blob_download(hash)
            .await
            .map_err(|e| anyhow::anyhow!("loading Node {hash}: {e}"))?;

        // For node blobs we need the plaintext_size to truncate padding.
        // But we don't know it yet — that's a chicken-and-egg problem.
        // Fortunately, node blobs are always compressed (zstd), so the
        // decompressor handles trailing padding. For uncompressed nodes
        // without padding, plaintext_size doesn't matter.
        // We pass 0 as plaintext_size; the pipeline_decode handles this:
        // - If compressed: zstd frame is self-delimiting, ignores trailing zeros
        // - If uncompressed + no padding: data is passed through unchanged
        // - If uncompressed + padding: we'd need the real size (TODO: store it)
        let plaintext_size = 0; // placeholder — see comment above

        let decoded = context::pipeline_decode(
            bytes,
            self.ctx.node.as_ref(),
            plaintext_hash,
            plaintext_size,
            KDF_META,
            self.ctx.keys.as_ref(),
        )?;

        Node::from_bytes(&decoded).map_err(|e| anyhow::anyhow!("decoding Node {hash}: {e}"))
    }

    /// Loads the root node of this snapshot.
    ///
    /// Returns an empty `Node` for empty snapshots (no tree in the store).
    pub async fn load_root(&self) -> anyhow::Result<Node> {
        if self.is_empty() {
            return Ok(Node::new());
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
        mut stream: R,
        store: &dyn BlobsWrite,
        semantic: Option<SemanticMeta>,
    ) -> anyhow::Result<NodeEntry> {
        let mut chunker = crate::chunking::XetChunker::new(stream);

        // Peek first chunk to see if the file is tiny.
        let first_chunk = match chunker.next_chunk().await {
            Ok(Some(c)) => c,
            Ok(None) => {
                // Empty file
                return self.import_bytes(&[], store, semantic).await;
            }
            Err(e) => return Err(anyhow::anyhow!("cdc error: {e}")),
        };

        let second_chunk = chunker.next_chunk().await.map_err(|e| anyhow::anyhow!("cdc error: {e}"))?;

        if second_chunk.is_none() {
            // It's a single chunk! Just upload it as a Leaf.
            return self.import_bytes(&first_chunk, store, semantic).await;
        }

        // It's a multi-chunk file! We need to collect the chunk entries.
        let mut all_chunks = vec![first_chunk];
        if let Some(c) = second_chunk {
            all_chunks.push(c);
        }

        while let Some(chunk) = chunker.next_chunk().await.map_err(|e| anyhow::anyhow!("cdc error: {e}"))? {
            all_chunks.push(chunk);
        }

        let mut entries = Vec::with_capacity(all_chunks.len());
        let mut offset: u64 = 0;
        let mut total_size: u64 = 0;

        for chunk in all_chunks {
            let chunk_entry = self.import_bytes(&chunk, store, None).await?;
            // Pad the byte offset to ensure correct string-based sorting.
            let key = format!("{:016x}", offset);
            entries.push((key, chunk_entry));
            offset += chunk.len() as u64;
            total_size += chunk.len() as u64;
        }

        // We build a `NodeKind::ByteStream` tree from the chunks.
        // We use a default mask of 0x3F (64 entries per node on average).
        let mask = 0x3F;
        let leaf_nodes = crate::persist::chunk_entries(&entries, mask, &crate::node::NodeKind::ByteStream, 0);

        let mut stats = crate::persist::MergeStats::default();
        let (root_hash, root_plaintext_hash) = self
            .build_tree_dedup(leaf_nodes, store, &crate::node::NodeKind::ByteStream, &mut stats)
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
    ///
    /// Pipeline: hash plaintext → compress → pad → encrypt → upload → NodeEntry
    ///
    /// Uses the leaf pipeline from the traversal context.
    pub async fn import_bytes(
        &self,
        plaintext: &[u8],
        store: &dyn BlobsWrite,
        semantic: Option<SemanticMeta>,
    ) -> anyhow::Result<NodeEntry> {
        let plaintext_size = plaintext.len() as u64;
        let plaintext_hash_bytes: [u8; 32] = *blake3::hash(plaintext).as_bytes();

        let result = context::pipeline_encode(
            plaintext,
            self.ctx.leaf.as_ref(),
            &plaintext_hash_bytes,
            KDF_LEAF,
            self.ctx.keys.as_ref(),
        )?;

        let blob_id = store
            .blob_upload_bytes(result.bytes)
            .await
            .map_err(|e| anyhow::anyhow!("uploading blob: {e}"))?;

        // Determine if transforms were applied (need plaintext_hash + stored_blocks).
        let has_transforms = self.ctx.leaf.is_some();
        let (pt_hash, blocks) = if has_transforms {
            (Some(plaintext_hash_bytes), Some(result.stored_blocks))
        } else {
            (None, None)
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
            child_context: None,
            tombstone: None,
        })
    }

    // =====================================================================
    // File Export
    // =====================================================================

    /// Download, decrypt, decompress, and verify a leaf entry's content.
    ///
    /// For chunked files (`Structural::Link`), this recursively fetches all
    /// chunks and concatenates them into a single contiguous `Bytes`.
    /// For very large files, a streaming export should be used instead.
    pub fn export_bytes<'a>(&'a self, entry: &'a NodeEntry) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Bytes>> + Send + 'a>> {
        Box::pin(async move {
            let content = entry
                .content
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("cannot export tombstone entry"))?;

            if content.structural == Structural::Link {
                // It's a chunk tree. Load the node.
                let node = self.load(content.hash(), content.plaintext_hash.as_ref()).await?;
                if node.header.kind != NodeKind::ByteStream {
                    anyhow::bail!("cannot export structural link of kind {:?}", node.header.kind);
                }

                let child_snap = self.child(entry);
                use futures::StreamExt;
                let mut stream = child_snap.walk_byte_stream(content.hash(), content.plaintext_hash);
                
                let mut all_bytes = bytes::BytesMut::with_capacity(content.size as usize);
                while let Some(res) = stream.next().await {
                    let chunk_entry = res?;
                    let chunk_bytes = child_snap.export_bytes(&chunk_entry).await?;
                    all_bytes.extend_from_slice(&chunk_bytes);
                }
                return Ok(all_bytes.freeze());
            }

            let ciphertext = self
                .store
                .blob_download(content.hash())
                .await
                .map_err(|e| anyhow::anyhow!("downloading blob {}: {e}", content.hash()))?;

            let plaintext = context::pipeline_decode(
                ciphertext,
                self.ctx.leaf.as_ref(),
                content.plaintext_hash.as_ref(),
                content.size,
                KDF_LEAF,
                self.ctx.keys.as_ref(),
            )?;

            // Verify plaintext hash if available.
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
        })
    }

    // =====================================================================
    // Recursive Walk
    // =====================================================================

    fn walk_byte_stream<'a>(
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
                // Internal node
                for entry in node.entries.values() {
                    if entry.is_link() {
                        let content = entry.content.as_ref().unwrap();
                        let child = self.child(entry);
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
                // Leaf node of chunks
                for entry in node.entries.values() {
                    yield entry.clone();
                }
            }
        })
    }

    /// Recursively walk the snapshot tree, yielding `(path, NodeEntry)` for
    /// every leaf entry (file content).
    ///
    /// Follows Links into subdirectories, skips tombstones.
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

                            if entry.is_link() {
                                // Link entry — recurse into child snapshot.
                                let content = entry.content.as_ref().expect("is_link implies content");
                                let child = self.child(entry);
                                let mut s = std::pin::pin!(child.walk_inner(
                                    content.hash(),
                                    content.plaintext_hash,
                                    path,
                                ));
                                while let Some(item) = s.next().await {
                                    yield item?;
                                }
                            } else {
                                // Leaf entry or metadata-only entry (e.g. directory).
                                yield (path, entry.clone());
                            }
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
    async fn walk_to_leaf(&self, key: &str) -> anyhow::Result<Option<Node>> {
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
}

// ---------------------------------------------------------------------------
// Context Construction
// ---------------------------------------------------------------------------

/// Creates the default encrypted `TraversalContext`.
///
/// Both leaf and node pipelines use Zstd compression, 4KiB padding,
/// and DeterministicChaCha20 encryption.
fn encrypted_context(master_secret: [u8; 32]) -> TraversalContext {
    let mut keys = BTreeMap::new();
    keys.insert(KEY_SLOT_MASTER, master_secret);

    let pipeline = BlobPipeline {
        compression: Some(CompressionStrategy::Zstd),
        padding: Some(PaddingStrategy {
            block_size: DEFAULT_PAD_BLOCK_SIZE,
        }),
        encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_MASTER)),
    };

    TraversalContext {
        keys: Some(keys),
        leaf: Some(pipeline.clone()),
        node: Some(pipeline),
    }
}

/// Creates an encrypted `TraversalContext` with separate leaf and node keys.
///
/// Each pipeline gets its own key slot (`KEY_SLOT_LEAF` / `KEY_SLOT_NODE`)
/// so that file content and metadata are encrypted with independent keys.
fn encrypted_split_context(leaf_key: [u8; 32], node_key: [u8; 32]) -> TraversalContext {
    let mut keys = BTreeMap::new();
    keys.insert(KEY_SLOT_LEAF, leaf_key);
    keys.insert(KEY_SLOT_NODE, node_key);

    let leaf_pipeline = BlobPipeline {
        compression: Some(CompressionStrategy::Zstd),
        padding: Some(PaddingStrategy {
            block_size: DEFAULT_PAD_BLOCK_SIZE,
        }),
        encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_LEAF)),
    };

    let node_pipeline = BlobPipeline {
        compression: Some(CompressionStrategy::Zstd),
        padding: Some(PaddingStrategy {
            block_size: DEFAULT_PAD_BLOCK_SIZE,
        }),
        encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_NODE)),
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
fn merge_contexts(parent: &TraversalContext, child: &TraversalContext) -> TraversalContext {
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
