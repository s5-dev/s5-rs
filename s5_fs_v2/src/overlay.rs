//! Mutable overlay layer for FS5 trees.
//!
//! [`WritableOverlay`] is the mutable top layer that sits above a base
//! [`ReadableLayer`]. Reads check the overlay first and fall through to
//! the base on miss. Writes (inserts, tombstones) go into an in-memory
//! `BTreeMap` and are immediately visible to subsequent reads.
//!
//! The base can be any `ReadableLayer`: a [`Snapshot`](crate::snapshot::Snapshot),
//! a [`MergedView`](crate::merge::MergedView), or even another `WritableOverlay`.
//!
//! # FUSE / live-mount pattern
//!
//! ```text
//! WritableOverlay (mutable, immediate reads)
//!   └── Snapshot (immutable prolly tree)
//!
//! Periodic flush:
//!   snapshot.merge_and_persist(&overlay, store)
//!   → swap base snapshot, clear overlay
//! ```

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::RwLock;

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};

use crate::layer::ReadableLayer;
use crate::node::NodeEntry;

/// Mutable overlay backed by a `BTreeMap` behind a `RwLock`, with
/// fall-through reads to a base [`ReadableLayer`].
///
/// - Writes go into the local BTreeMap (inserts, tombstones)
/// - Reads check overlay first, fall through to base on miss
/// - `scan()` merges overlay entries with the base stream (overlay wins)
///
/// Write lock is held for microseconds (BTreeMap insert is O(log n)).
/// Concurrent readers are not blocked by each other.
pub struct WritableOverlay {
    entries: RwLock<BTreeMap<String, NodeEntry>>,
    base: Box<dyn ReadableLayer>,
}

impl WritableOverlay {
    /// Creates an overlay on top of a base layer.
    pub fn new(base: Box<dyn ReadableLayer>) -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
            base,
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

    /// Returns a reference to the base layer.
    pub fn base(&self) -> &dyn ReadableLayer {
        &*self.base
    }

    /// Replaces the base layer, returning the old one.
    pub fn swap_base(&mut self, new_base: Box<dyn ReadableLayer>) -> Box<dyn ReadableLayer> {
        std::mem::replace(&mut self.base, new_base)
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
}

// ---------------------------------------------------------------------------
// Two-way sorted merge (overlay wins on collision)
// ---------------------------------------------------------------------------

/// Merges two sorted streams, yielding entries in key order.
/// On key collision, the entry from `primary` (overlay) wins and the
/// `secondary` (base) entry is discarded.
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
        },
        |mut state| async move {
            // Fill buffers.
            if state.buf_p.is_none()
                && let Some(result) = state.primary.next().await
            {
                match result {
                    Ok(item) => state.buf_p = Some(item),
                    Err(e) => return Some((Err(e), state)),
                }
            }
            if state.buf_s.is_none()
                && let Some(result) = state.secondary.next().await
            {
                match result {
                    Ok(item) => state.buf_s = Some(item),
                    Err(e) => return Some((Err(e), state)),
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
}
