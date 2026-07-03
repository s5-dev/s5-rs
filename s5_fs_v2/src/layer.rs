//! Async read interface for FS5 tree layers.
//!
//! Both immutable snapshots and mutable overlays implement [`ReadableLayer`],
//! enabling uniform composition via [`MergedView`](crate::merge::MergedView).

use std::collections::BTreeMap;
use std::ops::Bound;

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};

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

/// A standalone, in-memory layer wrapping an explicit sorted map of
/// changes — i.e. the **sparse diff** itself, NOT a view layered over a
/// base.
///
/// This is the layer to hand `Snapshot::merge_and_persist` as its
/// `changes` argument. The merge treats `changes` as the set of
/// keys to add/update/tombstone *into* the base; its structural
/// incremental path assumes that set is SMALL. A [`WritableOverlay`]
/// is the wrong thing to pass there: `WritableOverlay::scan` returns
/// base ∪ overlay (the merged view), so the merge would collect the
/// entire tree as "changes", violate the small-changes premise, and
/// fall back to a full O(corpus) re-fold every cycle. Pass
/// `MapLayer::new(overlay.take())` instead — the diff only.
///
/// (See `WritableOverlay::take` / `::diff_layer` for getting the diff,
/// and CUTOVER 2026-06-17 session 5 for the incident this prevents.)
pub struct MapLayer {
    entries: BTreeMap<String, NodeEntry>,
}

impl MapLayer {
    /// Wrap an owned diff map. Keys are vault paths; values are the
    /// new [`NodeEntry`]s (live upserts) or tombstones (deletes).
    pub fn new(entries: BTreeMap<String, NodeEntry>) -> Self {
        Self { entries }
    }

    /// Number of entries (upserts + tombstones) in the diff.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the diff is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[async_trait]
impl ReadableLayer for MapLayer {
    async fn get(&self, key: &str) -> anyhow::Result<Option<NodeEntry>> {
        Ok(self.entries.get(key).filter(|e| !e.is_tombstone()).cloned())
    }

    async fn get_raw(&self, key: &str) -> anyhow::Result<Option<NodeEntry>> {
        Ok(self.entries.get(key).cloned())
    }

    fn scan(
        &self,
        start: Bound<String>,
        end: Bound<String>,
    ) -> BoxStream<'_, anyhow::Result<(String, NodeEntry)>> {
        // Yields ONLY this map's entries (the diff), in key order — the
        // critical difference from `WritableOverlay::scan`, which merges
        // in the base.
        let v: Vec<_> = self
            .entries
            .range((start, end))
            .map(|(k, e)| Ok((k.clone(), e.clone())))
            .collect();
        stream::iter(v).boxed()
    }
}

#[cfg(test)]
mod map_layer_tests {
    use super::*;
    use crate::node::NodeEntry;

    #[tokio::test]
    async fn scan_yields_only_its_own_entries_in_order() {
        // The contract the publish path relies on: a MapLayer scans as
        // exactly its diff, never base ∪ diff (the WritableOverlay bug).
        let now = 0u32;
        let mut m = BTreeMap::new();
        m.insert("b".to_string(), NodeEntry::tombstone(now));
        m.insert("a".to_string(), NodeEntry::tombstone(now));
        m.insert("c".to_string(), NodeEntry::tombstone(now));
        let layer = MapLayer::new(m);
        let got: Vec<String> = layer
            .scan_all()
            .map(|r| r.unwrap().0)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(got, vec!["a", "b", "c"], "diff-only, sorted");
        assert_eq!(layer.len(), 3);
    }
}
