//! Async read interface for FS5 tree layers.
//!
//! Both immutable snapshots and mutable overlays implement [`ReadableLayer`],
//! enabling uniform composition via [`MergedView`](crate::merge::MergedView).

use std::ops::Bound;

use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::node::NodeEntry;

/// Async read interface implemented by snapshots, overlays, and merged views.
///
/// All layers present the same ordered key-value interface. Tombstones
/// (deletion markers) are visible in raw lookups and scans so that merge
/// logic can apply LWW correctly.
///
/// # Tombstone Handling
///
/// - `get()` returns `None` for both missing keys and tombstones (convenience).
/// - `get_raw()` returns tombstone entries (needed for merge / conflict resolution).
/// - `scan()` yields tombstones in the stream; callers filter if needed.
#[async_trait]
pub trait ReadableLayer: Send + Sync {
    /// Exact key lookup. Returns `None` for missing keys AND tombstones.
    async fn get(&self, key: &str) -> anyhow::Result<Option<NodeEntry>>;

    /// Raw lookup that exposes tombstones (needed for merge).
    async fn get_raw(&self, key: &str) -> anyhow::Result<Option<NodeEntry>>;

    /// Ordered scan over a key range. Yields `(key, entry)` pairs in sorted order.
    /// Tombstones are visible in the stream.
    fn scan(
        &self,
        start: Bound<String>,
        end: Bound<String>,
    ) -> BoxStream<'_, anyhow::Result<(String, NodeEntry)>>;

    /// Scan all entries (full range).
    fn scan_all(&self) -> BoxStream<'_, anyhow::Result<(String, NodeEntry)>> {
        self.scan(Bound::Unbounded, Bound::Unbounded)
    }

    /// Chunking mask used when building child trees on top of this layer.
    /// `Snapshot` reads it from the root node's `BuildContext`; merge-style
    /// layers (`MergedView`, `WritableOverlay`) delegate to whichever
    /// underlying base owns the structural shape. The default is the
    /// project-wide constant for callers that don't have any opinion.
    ///
    /// `async` because `Snapshot` may need to fault in the root node from
    /// the blob store on first call.
    async fn chunk_mask(&self) -> u32 {
        // DEFAULT_ENTRIES_PER_NODE = 64; mask = entries - 1 = 0x3F.
        crate::persist::DEFAULT_ENTRIES_PER_NODE - 1
    }
}
