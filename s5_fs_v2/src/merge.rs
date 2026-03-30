//! K-way priority merge over an ordered stack of [`ReadableLayer`]s.
//!
//! [`MergedView`] composes multiple read-only layers (snapshots, other views)
//! into a single unified view. Layer 0 has the highest priority — on key
//! collision, the entry from the lowest-index layer wins.
//!
//! Used for:
//! - **Layer composition**: union-filesystem semantics (e.g. Docker image layers)
//! - **Multi-snapshot merge**: combine several snapshots into one read view
//! - **Base for `WritableOverlay`**: stack read-only layers, then write on top

use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};

use crate::layer::ReadableLayer;
use crate::node::NodeEntry;

/// K-way priority merge over an ordered stack of layers.
///
/// Index 0 = highest priority. On key collision, the entry from the
/// lowest-index layer wins. Tombstones are preserved in the merged
/// stream — callers filter them if needed.
pub struct MergedView {
    layers: Vec<Arc<dyn ReadableLayer>>,
}

impl MergedView {
    /// Creates a new merged view from the given layers.
    /// Index 0 = highest priority.
    pub fn new(layers: Vec<Arc<dyn ReadableLayer>>) -> Self {
        Self { layers }
    }

    /// Returns the number of layers.
    pub fn layer_count(&self) -> usize {
        self.layers.len()
    }
}

#[async_trait]
impl ReadableLayer for MergedView {
    async fn get(&self, key: &str) -> anyhow::Result<Option<NodeEntry>> {
        // Walk layers top-down, first hit wins.
        for layer in &self.layers {
            if let Some(entry) = layer.get_raw(key).await? {
                if entry.is_tombstone() {
                    return Ok(None);
                }
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    async fn get_raw(&self, key: &str) -> anyhow::Result<Option<NodeEntry>> {
        // Walk layers top-down, first hit wins (tombstones visible).
        for layer in &self.layers {
            if let Some(entry) = layer.get_raw(key).await? {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    fn scan(
        &self,
        start: Bound<String>,
        end: Bound<String>,
    ) -> BoxStream<'_, anyhow::Result<(String, NodeEntry)>> {
        let streams: Vec<_> = self
            .layers
            .iter()
            .map(|layer| layer.scan(start.clone(), end.clone()))
            .collect();
        k_way_merge(streams).boxed()
    }
}

/// K-way merge of sorted streams with priority (first stream wins on ties).
///
/// Each stream must yield `(key, entry)` pairs in ascending key order.
/// On key collision, the entry from the earliest (lowest-index) stream wins.
fn k_way_merge<'a>(
    streams: Vec<BoxStream<'a, anyhow::Result<(String, NodeEntry)>>>,
) -> BoxStream<'a, anyhow::Result<(String, NodeEntry)>> {
    futures::stream::unfold(KMergeState::new(streams), |mut state| async move {
        match state.next().await {
            Ok(Some(item)) => Some((Ok(item), state)),
            Ok(None) => None,
            Err(e) => Some((Err(e), state)),
        }
    })
    .boxed()
}

/// Internal state for k-way merge.
struct KMergeState<'a> {
    /// (stream, buffered next item) per layer. Index = priority.
    heads: Vec<(
        BoxStream<'a, anyhow::Result<(String, NodeEntry)>>,
        Option<(String, NodeEntry)>,
    )>,
    initialized: bool,
}

impl<'a> KMergeState<'a> {
    fn new(streams: Vec<BoxStream<'a, anyhow::Result<(String, NodeEntry)>>>) -> Self {
        let heads = streams.into_iter().map(|s| (s, None)).collect();
        Self {
            heads,
            initialized: false,
        }
    }

    /// Advance: fill buffers, pick smallest key, skip duplicates.
    async fn next(&mut self) -> anyhow::Result<Option<(String, NodeEntry)>> {
        // Fill all empty buffers.
        for (stream, buf) in self.heads.iter_mut() {
            if !self.initialized || buf.is_none() {
                if let Some(result) = stream.next().await {
                    *buf = Some(result?);
                }
            }
        }
        self.initialized = true;

        // Find the smallest key (lowest index wins ties).
        let mut best_idx: Option<usize> = None;
        for (i, (_, buf)) in self.heads.iter().enumerate() {
            if let Some((key, _)) = buf {
                match best_idx {
                    None => best_idx = Some(i),
                    Some(bi) => {
                        let best_key = &self.heads[bi].1.as_ref().expect("best_idx points to populated buffer").0;
                        match key.cmp(best_key) {
                            Ordering::Less => best_idx = Some(i),
                            Ordering::Equal | Ordering::Greater => {}
                        }
                    }
                }
            }
        }

        let best_idx = match best_idx {
            Some(i) => i,
            None => return Ok(None), // All streams exhausted.
        };

        // Take the winner.
        let winner = self.heads[best_idx].1.take().expect("best_idx points to populated buffer");

        // Skip duplicate keys from lower-priority streams.
        for (i, (_, buf)) in self.heads.iter_mut().enumerate() {
            if i != best_idx {
                if let Some((key, _)) = buf {
                    if *key == winner.0 {
                        *buf = None; // Discard lower-priority duplicate.
                    }
                }
            }
        }

        Ok(Some(winner))
    }
}
