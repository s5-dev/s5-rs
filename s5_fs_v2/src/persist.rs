//! Diff-aware persist: merge changes into a prolly tree with deduplication.
//!
//! [`Snapshot::merge_and_persist`] is the primary write path. It takes a
//! [`ReadableLayer`] of changes (typically a [`WritableOverlay`](crate::overlay::WritableOverlay)),
//! merges them with the current snapshot's entries, filters tombstones,
//! and builds a new prolly tree — skipping upload of any node whose hash
//! already exists in the store (`blob_contains`).
//!
//! # Dedup Cascade
//!
//! Because chunk boundaries are content-defined (BLAKE3 of the key) and
//! encryption is deterministic, unchanged regions produce identical nodes.
//! When building the tree bottom-up:
//!
//! 1. Unchanged leaf nodes produce the same hash → `blob_contains` returns true → skip
//! 2. Internal nodes whose children all skipped also produce the same hash → skip
//! 3. Only the "spine" of changed nodes gets uploaded
//!
//! # Directory Entry Convention
//!
//! Files use their full relative path as the key (no trailing `/`):
//! `"Photos/2024/sunset.jpg"`.
//!
//! Directories use their path with a trailing `/`: `"Photos/"`, `"Photos/2024/"`.
//! All intermediate directories get explicit entries so that directory
//! metadata (timestamps, permissions, ownership) is preserved and empty
//! directories are not silently lost.
//!
//! # Chunking Strategy
//!
//! Chunk boundaries are content-defined using BLAKE3:
//!
//! ```text
//! boundary = (blake3(key)[0..4] as u32) & mask == 0
//! ```
//!
//! where `mask = expected_entries_per_node - 1` (must be power of 2).
//! Default: 64 entries/node → mask = 0x3F.

use std::ops::Bound;

use futures::StreamExt;
use s5_core::{BlobId, BlobsRead, BlobsWrite, Hash};

use crate::context::{self, KDF_META};
use crate::layer::ReadableLayer;
use crate::node::{ContentRef, NODE_MAGIC, Node, NodeEntry, NodeHeader, NodeKind, Structural};
use crate::snapshot::Snapshot;

/// Default expected entries per leaf node.
///
/// Must be a power of 2 for the mask-based boundary check.
/// 64 entries/node gives ~4-8 KB nodes with typical directory entries.
pub const DEFAULT_ENTRIES_PER_NODE: u32 = 64;

/// Minimum entries per node (prevents degenerate single-entry nodes).
const MIN_ENTRIES_PER_NODE: usize = 4;

/// Statistics from a merge-and-persist operation.
#[derive(Clone, Debug, Default)]
pub struct MergeStats {
    /// Number of live entries in the final tree (excluding tombstones).
    pub entries: u64,
    /// Number of tombstones filtered during merge.
    pub tombstones_filtered: u64,
    /// Number of entries carried over unchanged from the old snapshot.
    pub entries_reused: u64,
    /// Number of entries added or updated from the change layer.
    pub entries_changed: u64,
    /// Number of leaf nodes in the final tree.
    pub leaf_nodes: u64,
    /// Number of internal nodes in the final tree.
    pub internal_nodes: u64,
    /// Number of nodes skipped (already in store).
    pub nodes_deduped: u64,
    /// Number of nodes uploaded (new).
    pub nodes_uploaded: u64,
    /// Total bytes uploaded to the blob store.
    pub bytes_uploaded: u64,
    /// Tree depth (0 = single leaf, 1+ = has internal levels).
    pub depth: u8,
}

impl Snapshot {
    /// Merges changes into this snapshot and persists the result as a new prolly tree.
    ///
    /// This is the primary write path. It:
    /// 1. Collects existing entries from `self` (skipped if empty)
    /// 2. Applies changes from `changes` (changes win on key collision)
    /// 3. Filters tombstones
    /// 4. Chunks entries into a prolly tree with content-defined boundaries
    /// 5. Uploads only new nodes (skips existing via `blob_contains`)
    ///
    /// Returns the new root `Hash` and `MergeStats`, or `None` if the
    /// resulting tree has no live entries.
    pub async fn merge_and_persist(
        &self,
        changes: &dyn ReadableLayer,
        store: &dyn BlobsWrite,
    ) -> anyhow::Result<Option<(Hash, [u8; 32], MergeStats)>> {
        let mut stats = MergeStats::default();

        // Phase 1: Collect merged entries (old + changes, changes win).
        let entries = self.collect_merged_entries(changes, &mut stats).await?;

        if entries.is_empty() {
            return Ok(None);
        }

        stats.entries = entries.len() as u64;

        // Phase 2: Determine chunk mask from root node's BuildContext or default.
        let mask = self.chunk_mask().await;

        // Phase 3: Chunk entries into leaf nodes.
        let leaf_nodes = chunk_entries(&entries, mask, &NodeKind::Namespace, 0);
        stats.leaf_nodes = leaf_nodes.len() as u64;

        // Phase 4: Build tree bottom-up with dedup.
        let (root_hash, root_plaintext_hash) = self
            .build_tree_dedup(leaf_nodes, store, &NodeKind::Namespace, &mut stats)
            .await?;

        Ok(Some((root_hash, root_plaintext_hash, stats)))
    }

    /// Collects entries from the old snapshot merged with the change layer.
    ///
    /// Changes take priority on key collision. Tombstones are filtered out
    /// of the final result.
    ///
    /// # HIGH PRIORITY TODO: Streaming
    ///
    /// This currently materializes the entire old snapshot into a `Vec` via
    /// `scan()`. For 1M+ files this is ~200 MB+ of heap allocation. The
    /// public API surface (`merge_and_persist` returning
    /// `Option<(Hash, [u8; 32], MergeStats)>`) is fine — the fix is internal:
    ///
    /// 1. Stream old entries via `scan()` + change entries via `scan()` in a
    ///    sorted merge iterator (both are already sorted).
    /// 2. Feed merged entries directly into `chunk_entries` as a streaming
    ///    chunker that emits `Node`s as they fill up.
    /// 3. Subtree-skip optimization: when an entire subtree is unchanged
    ///    (no keys in `changes` overlap its key range), reuse the existing
    ///    node hash without loading it at all.
    ///
    /// This is the single biggest scaling bottleneck and should be addressed
    /// before targeting 1M+ file trees.
    async fn collect_merged_entries(
        &self,
        changes: &dyn ReadableLayer,
        stats: &mut MergeStats,
    ) -> anyhow::Result<Vec<(String, NodeEntry)>> {
        // Collect all changes first (including tombstones for override logic).
        let mut change_map = std::collections::BTreeMap::new();
        let mut change_stream = changes.scan(Bound::Unbounded, Bound::Unbounded);
        while let Some(result) = change_stream.next().await {
            let (key, entry) = result?;
            change_map.insert(key, entry);
        }
        drop(change_stream);

        // If snapshot is empty, changes are the only source.
        if self.is_empty() {
            let mut entries = Vec::new();
            for (key, entry) in change_map {
                if entry.is_tombstone() {
                    stats.tombstones_filtered += 1;
                    continue;
                }
                stats.entries_changed += 1;
                entries.push((key, entry));
            }
            return Ok(entries);
        }

        // Merge: old entries + changes (changes win).
        let mut entries = Vec::new();
        let mut old_stream = self.scan(Bound::Unbounded, Bound::Unbounded);

        while let Some(result) = old_stream.next().await {
            let (key, entry) = result?;

            if let Some(changed) = change_map.remove(&key) {
                // Change overrides old entry.
                if changed.is_tombstone() {
                    stats.tombstones_filtered += 1;
                } else {
                    stats.entries_changed += 1;
                    entries.push((key, changed));
                }
            } else {
                // Old entry kept as-is.
                stats.entries_reused += 1;
                entries.push((key, entry));
            }
        }
        drop(old_stream);

        // Remaining changes are new keys not in the old snapshot.
        for (key, entry) in change_map {
            if entry.is_tombstone() {
                stats.tombstones_filtered += 1;
                continue;
            }
            stats.entries_changed += 1;
            entries.push((key, entry));
        }

        // Sort by key (BTreeMap iteration was sorted, but we appended new keys at the end).
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        Ok(entries)
    }

    /// Returns the chunk boundary mask from the root node's BuildContext,
    /// or the default if no BuildContext is set.
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
        DEFAULT_ENTRIES_PER_NODE - 1
    }

    /// Encodes a node through the node pipeline, uploads it only if
    /// it doesn't already exist in the store.
    ///
    /// Returns `(BlobId, plaintext_hash)`.
    async fn write_node_dedup(
        &self,
        node: &Node,
        store: &dyn BlobsWrite,
        read_store: &dyn BlobsRead,
        stats: &mut MergeStats,
    ) -> anyhow::Result<(BlobId, [u8; 32])> {
        let cbor = node
            .to_bytes()
            .map_err(|e| anyhow::anyhow!("encoding Node: {e}"))?;

        let plaintext_hash: [u8; 32] = *blake3::hash(&cbor).as_bytes();

        let result = context::pipeline_encode(
            &cbor,
            self.context().node.as_ref(),
            &plaintext_hash,
            KDF_META,
            self.context().keys.as_ref(),
        )?;

        // Compute what the CAS hash would be.
        let cas_hash = Hash::from(*blake3::hash(&result.bytes).as_bytes());

        // Check if it already exists.
        if read_store.blob_contains(cas_hash).await? {
            stats.nodes_deduped += 1;
            let blob_id = BlobId {
                hash: cas_hash,
                size: result.bytes.len() as u64,
            };
            return Ok((blob_id, plaintext_hash));
        }

        // Upload.
        let blob_id = store
            .blob_upload_bytes(result.bytes)
            .await
            .map_err(|e| anyhow::anyhow!("uploading Node: {e}"))?;

        stats.nodes_uploaded += 1;
        stats.bytes_uploaded += blob_id.size;

        Ok((blob_id, plaintext_hash))
    }

    /// Builds the tree bottom-up from nodes, using dedup writes.
    ///
    /// 1. Upload all nodes at the current level (skipping existing)
    /// 2. If only one node remains, it's the root — return its hash
    /// 3. Otherwise, create internal nodes pointing to them and recurse
    async fn build_tree_dedup(
        &self,
        nodes: Vec<Node>,
        store: &dyn BlobsWrite,
        kind: &NodeKind,
        stats: &mut MergeStats,
    ) -> anyhow::Result<(Hash, [u8; 32])> {
        let read_store: &dyn BlobsRead = self.store().as_ref();

        // Upload all nodes at this level, collecting (first_key, link_entry) tuples.
        let mut children: Vec<(String, NodeEntry)> = Vec::with_capacity(nodes.len());

        for node in &nodes {
            let (blob_id, plaintext_hash) = self
                .write_node_dedup(node, store, read_store, stats)
                .await?;

            // The first key in this node is the routing key for the parent.
            let first_key = node.entries.keys().next().cloned().unwrap_or_default();

            // Total plaintext size of all entries in this node.
            let total_size: u64 = node
                .entries
                .values()
                .filter_map(|e| e.content.as_ref())
                .map(|c| c.size)
                .sum();

            let link_entry = NodeEntry {
                content: Some(ContentRef {
                    structural: Structural::Link,
                    hash: *blob_id.hash.as_bytes(),
                    size: total_size,
                    plaintext_hash: Some(plaintext_hash),
                    stored_blocks: Some(blob_id.size),
                }),
                semantic: None,
                child_context: None,
                tombstone: None,
            };

            children.push((first_key, link_entry));
        }

        // Single node = root.
        if children.len() == 1 {
            let content = children[0]
                .1
                .content
                .as_ref()
                .expect("link entry always has content");
            let hash = content.hash();
            let plaintext_hash = content
                .plaintext_hash
                .expect("plaintext_hash always set by write_node_dedup");
            stats.depth = nodes[0].header.level;
            return Ok((hash, plaintext_hash));
        }

        // Build internal level.
        let level = nodes[0].header.level + 1;
        let mask = self.chunk_mask().await;
        let internal_nodes = chunk_entries(&children, mask, kind, level);
        stats.internal_nodes += internal_nodes.len() as u64;

        Box::pin(self.build_tree_dedup(internal_nodes, store, kind, stats)).await
    }
}

// ===========================================================================
// Chunking helpers (private)
// ===========================================================================

/// Chunks a sorted list of entries into nodes using content-defined boundaries.
///
/// Boundary condition: `blake3(key)[0..4] as u32 & mask == 0`.
/// Minimum node size is enforced to prevent degenerate single-entry nodes.
fn chunk_entries(
    entries: &[(String, NodeEntry)],
    mask: u32,
    kind: &NodeKind,
    level: u8,
) -> Vec<Node> {
    if entries.is_empty() {
        return Vec::new();
    }

    let mut nodes = Vec::new();
    let mut current = Vec::new();

    for (key, entry) in entries {
        current.push((key.clone(), entry.clone()));

        // Check boundary after adding (so the boundary entry is the LAST in the chunk).
        if current.len() >= MIN_ENTRIES_PER_NODE && is_boundary(key, mask) {
            nodes.push(entries_to_node(&current, kind, level));
            current.clear();
        }
    }

    // Flush remaining entries.
    if !current.is_empty() {
        nodes.push(entries_to_node(&current, kind, level));
    }

    nodes
}

/// Creates a `Node` from a list of entries.
fn entries_to_node(entries: &[(String, NodeEntry)], kind: &NodeKind, level: u8) -> Node {
    let mut node = Node {
        magic: NODE_MAGIC.to_string(),
        header: NodeHeader {
            level,
            kind: kind.clone(),
            build: None,
        },
        entries: Default::default(),
    };

    for (key, entry) in entries {
        node.entries.insert(key.clone(), entry.clone());
    }

    node
}

/// Returns true if `key` is a chunk boundary.
///
/// Uses the first 4 bytes of BLAKE3(key) masked with the target-size mask.
/// This is a pure function of the key — no dependency on neighboring entries —
/// which is critical for prolly tree stability (insert/delete only affects
/// the containing chunk, not cascading boundary shifts).
///
/// Efficient directory listing (FUSE readdir) is handled via skip-scan at
/// the query level, not via chunking alignment.
fn is_boundary(key: &str, mask: u32) -> bool {
    let hash = blake3::hash(key.as_bytes());
    let bytes = hash.as_bytes();
    let val = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    val & mask == 0
}
