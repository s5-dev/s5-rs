//! Mutable overlay layer for FS5 trees.
//!
//! [`WritableOverlay`] is the mutable top layer that sits above a base
//! [`ReadableLayer`]. It owns the three pieces a writable layer needs:
//! the read view (`base`), the per-blob ops machinery (`pipeline`),
//! and the in-memory entry buffer (`entries`). Reads check the buffer
//! first and fall through to the base on miss. Writes (inserts,
//! tombstones) go into the buffer and are immediately visible.
//!
//! The base can be any `ReadableLayer`: a [`Snapshot`](crate::snapshot::Snapshot),
//! a [`MergedView`](crate::merge::MergedView), or even another `WritableOverlay`.
//!
//! # Why pipeline + base live together
//!
//! Every consumer that mutates an overlay needs both: the base for
//! reads, and the pipeline for materialising file bytes (encrypt on
//! commit, decrypt on read) and for the eventual flush. Holding them
//! as separate fields on each consumer (`WritableFs`,
//! `s5_fs_local::backup`, the publish convergence path, …) gave each
//! caller two `Arc`s that always travelled together and could
//! conceivably drift. Pinning them to the overlay collapses that and
//! gives the [`flush`](Self::flush) method a single self-contained
//! call site.
//!
//! # FUSE / live-mount pattern
//!
//! ```text
//! WritableOverlay (mutable; owns base + pipeline)
//!   ├── base: Snapshot / MergedView / another overlay
//!   └── pipeline: Pipeline (encrypt/decrypt machinery)
//!
//! Periodic flush:
//!   overlay.flush(store) → (root_hash, plaintext_hash, MergeStats)
//!   → caller wraps in a new Snapshot and clears the buffer
//! ```

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use s5_core::{BlobsWrite, Hash};

use crate::layer::ReadableLayer;
use crate::node::NodeEntry;
use crate::persist::MergeStats;
use crate::pipeline::Pipeline;

/// Mutable overlay backed by a `BTreeMap` behind a `RwLock`, with
/// fall-through reads to a base [`ReadableLayer`] and a [`Pipeline`]
/// for byte-level operations.
///
/// - Writes go into the local BTreeMap (inserts, tombstones)
/// - Reads check overlay first, fall through to base on miss
/// - `scan()` merges overlay entries with the base stream (overlay wins)
/// - [`flush`](Self::flush) folds the overlay into a fresh prolly tree
///
/// Write lock is held for microseconds (BTreeMap insert is O(log n)).
/// Concurrent readers are not blocked by each other.
pub struct WritableOverlay {
    entries: RwLock<BTreeMap<String, NodeEntry>>,
    base: Arc<dyn ReadableLayer>,
    pipeline: Arc<Pipeline>,
}

impl WritableOverlay {
    /// Construct an overlay over a base layer with an explicit pipeline.
    /// The pipeline owns the encryption keys + per-blob ops the overlay
    /// uses for [`flush`](Self::flush) and that consumers reach for via
    /// [`pipeline`](Self::pipeline) when materialising file bytes.
    pub fn new(base: Arc<dyn ReadableLayer>, pipeline: Arc<Pipeline>) -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
            base,
            pipeline,
        }
    }

    /// Inserts or updates an entry.
    pub fn put(&self, key: String, entry: NodeEntry) {
        self.entries
            .write()
            .expect("overlay lock poisoned")
            .insert(key, entry);
    }

    /// Marks a key as deleted by inserting a tombstone.
    pub fn delete(&self, key: String, tombstone: NodeEntry) {
        debug_assert!(
            tombstone.is_tombstone(),
            "delete() requires a tombstone entry"
        );
        self.entries
            .write()
            .expect("overlay lock poisoned")
            .insert(key, tombstone);
    }

    /// Returns the number of pending entries (including tombstones) in the overlay.
    pub fn pending_len(&self) -> usize {
        self.entries.read().expect("overlay lock poisoned").len()
    }

    /// Returns true if the overlay has no pending entries.
    pub fn pending_is_empty(&self) -> bool {
        self.entries
            .read()
            .expect("overlay lock poisoned")
            .is_empty()
    }

    /// Clears all pending entries from the overlay.
    pub fn clear(&self) {
        self.entries.write().expect("overlay lock poisoned").clear();
    }

    /// Takes a snapshot of the pending entries, consuming the overlay contents.
    /// The base layer is unaffected.
    pub fn take(&self) -> BTreeMap<String, NodeEntry> {
        std::mem::take(&mut *self.entries.write().expect("overlay lock poisoned"))
    }

    /// Returns the pending diff as a standalone [`MapLayer`] (a CLONE of
    /// the overlay's entries — the overlay is left intact). This is what
    /// to pass `merge_and_persist` as `changes`: it scans as the sparse
    /// diff ONLY, whereas passing the overlay (`&*self`) scans as base ∪
    /// diff and defeats the merge's structural incremental path. Use
    /// [`take`](Self::take) when the overlay is about to be discarded (no
    /// clone); use this when it must survive the merge.
    pub fn diff_layer(&self) -> crate::layer::MapLayer {
        crate::layer::MapLayer::new(self.entries.read().expect("overlay lock poisoned").clone())
    }

    /// Returns the base layer (an `Arc<dyn ReadableLayer>` clone). Held
    /// as `Arc` so callers that need to compose it (e.g. `WritableFs`'s
    /// `flush_overlay` passing it to `merge_and_persist`) don't have
    /// to up-cast or re-wrap.
    pub fn base(&self) -> &Arc<dyn ReadableLayer> {
        &self.base
    }

    /// Returns the pipeline owned by this overlay. Used by consumers
    /// (writable FUSE adapters, ingestors) that need to import bytes,
    /// export bytes, or otherwise touch blob-level operations.
    pub fn pipeline(&self) -> &Arc<Pipeline> {
        &self.pipeline
    }

    /// Folds the overlay's pending entries into a fresh prolly tree
    /// rooted on top of the base, producing a new persisted root.
    ///
    /// Returns `None` when the resulting tree has no live entries
    /// (everything was tombstones, or the overlay was empty over an
    /// empty base). Otherwise returns the new root hash, plaintext
    /// hash, and merge stats — caller wraps these in a `Snapshot`.
    ///
    /// The chunk mask is read from the base via the trait method —
    /// `Snapshot` returns its `BuildContext`-derived value, layered
    /// types delegate to whatever owns the structural shape, and
    /// callers without a strong opinion get the workspace default.
    pub async fn flush(
        &self,
        store: &dyn BlobsWrite,
    ) -> anyhow::Result<Option<(Hash, [u8; 32], MergeStats)>> {
        let chunk_mask = self.base.chunk_mask().await;
        // Pass the DIFF, not `self`. `WritableOverlay::scan` returns base ∪
        // diff, so passing `self` here would make `merge_and_persist`
        // collect the whole tree as "changes" and fall back to a full
        // O(corpus) re-fold (the structural incremental path bails on a
        // large change ratio). The diff-only layer keeps the merge
        // incremental. See CUTOVER 2026-06-17 session 5.
        let diff = self.diff_layer();
        self.pipeline
            .merge_and_persist(self.base.as_ref(), chunk_mask, &diff, store)
            .await
    }
}

#[async_trait]
impl ReadableLayer for WritableOverlay {
    async fn get(&self, key: &str) -> anyhow::Result<Option<NodeEntry>> {
        // Check overlay first.
        {
            let guard = self.entries.read().expect("overlay lock poisoned");
            if let Some(entry) = guard.get(key) {
                if entry.is_tombstone() {
                    // Overlay tombstone shadows the base — key is deleted.
                    return Ok(None);
                }
                return Ok(Some(entry.clone()));
            }
        }
        // Fall through to base.
        self.base.get(key).await
    }

    async fn get_raw(&self, key: &str) -> anyhow::Result<Option<NodeEntry>> {
        // Check overlay first (tombstones visible).
        {
            let guard = self.entries.read().expect("overlay lock poisoned");
            if let Some(entry) = guard.get(key) {
                return Ok(Some(entry.clone()));
            }
        }
        // Fall through to base.
        self.base.get_raw(key).await
    }

    fn scan(
        &self,
        start: Bound<String>,
        end: Bound<String>,
    ) -> BoxStream<'_, anyhow::Result<(String, NodeEntry)>> {
        // Snapshot overlay entries under the read lock.
        let overlay_entries: Vec<(String, NodeEntry)> = {
            let guard = self.entries.read().expect("overlay lock poisoned");
            guard
                .range((start.clone(), end.clone()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };

        let overlay_stream = stream::iter(overlay_entries.into_iter().map(Ok)).boxed();
        let base_stream = self.base.scan(start, end);

        // Two-way merge: overlay wins on key collision.
        merge_two(overlay_stream, base_stream).boxed()
    }

    /// Delegate to the base — the overlay buffer doesn't own structural
    /// shape, only entry mutations.
    async fn chunk_mask(&self) -> u32 {
        self.base.chunk_mask().await
    }
}

// ---------------------------------------------------------------------------
// Two-way sorted merge (overlay wins on collision)
// ---------------------------------------------------------------------------

/// Merges two sorted streams, yielding entries in key order.
/// On key collision, the entry from `primary` (overlay) wins and the
/// `secondary` (base) entry is discarded.
///
/// Each stream is polled at most until it returns `None` once;
/// subsequent rounds skip the exhausted stream. Without the
/// `done_p`/`done_s` flags we would re-poll a completed `unfold`-based
/// stream (e.g. `MergedView::scan`) and trip "Unfold must not be polled
/// after it returned `Poll::Ready(None)`".
fn merge_two<'a>(
    primary: BoxStream<'a, anyhow::Result<(String, NodeEntry)>>,
    secondary: BoxStream<'a, anyhow::Result<(String, NodeEntry)>>,
) -> BoxStream<'a, anyhow::Result<(String, NodeEntry)>> {
    futures::stream::unfold(
        MergeTwoState {
            primary,
            secondary,
            buf_p: None,
            buf_s: None,
            done_p: false,
            done_s: false,
        },
        |mut state| async move {
            if state.buf_p.is_none() && !state.done_p {
                match state.primary.next().await {
                    Some(Ok(item)) => state.buf_p = Some(item),
                    Some(Err(e)) => return Some((Err(e), state)),
                    None => state.done_p = true,
                }
            }
            if state.buf_s.is_none() && !state.done_s {
                match state.secondary.next().await {
                    Some(Ok(item)) => state.buf_s = Some(item),
                    Some(Err(e)) => return Some((Err(e), state)),
                    None => state.done_s = true,
                }
            }

            match (&state.buf_p, &state.buf_s) {
                (None, None) => None,
                (Some(_), None) => {
                    let item = state.buf_p.take().expect("checked Some above");
                    Some((Ok(item), state))
                }
                (None, Some(_)) => {
                    let item = state.buf_s.take().expect("checked Some above");
                    Some((Ok(item), state))
                }
                (Some((pk, _)), Some((sk, _))) => {
                    match pk.cmp(sk) {
                        std::cmp::Ordering::Less => {
                            let item = state.buf_p.take().expect("checked Some above");
                            Some((Ok(item), state))
                        }
                        std::cmp::Ordering::Greater => {
                            let item = state.buf_s.take().expect("checked Some above");
                            Some((Ok(item), state))
                        }
                        std::cmp::Ordering::Equal => {
                            // Primary wins, discard secondary.
                            let item = state.buf_p.take().expect("checked Some above");
                            state.buf_s = None;
                            Some((Ok(item), state))
                        }
                    }
                }
            }
        },
    )
    .boxed()
}

struct MergeTwoState<'a> {
    primary: BoxStream<'a, anyhow::Result<(String, NodeEntry)>>,
    secondary: BoxStream<'a, anyhow::Result<(String, NodeEntry)>>,
    buf_p: Option<(String, NodeEntry)>,
    buf_s: Option<(String, NodeEntry)>,
    done_p: bool,
    done_s: bool,
}
