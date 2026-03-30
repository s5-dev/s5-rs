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

    // TODO: Add `readdir(prefix: &str)` — skip-scan for efficient directory listing.
    //
    // Lists direct children of `prefix` (e.g. "src/") by scanning (prefix, prefix0)
    // and skipping over subdirectory ranges: when encountering a key like "src/tests/",
    // jump to "src/tests0" instead of scanning all descendants.
    //
    // Cost: O(direct_children × log n) tree seeks, regardless of total descendant count.
    // For M2, a simple filter on `scan()` output is sufficient for reasonable directory
    // sizes. The skip-scan matters for FUSE on large trees.
}
