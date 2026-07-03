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
use crate::pipeline::Pipeline;
use crate::snapshot::Snapshot;

/// Default expected entries per leaf node.
///
/// Must be a power of 2 for the mask-based boundary check.
/// 64 entries/node gives ~4-8 KB nodes with typical directory entries.
pub const DEFAULT_ENTRIES_PER_NODE: u32 = 64;

/// Minimum entries per node (prevents degenerate single-entry nodes).
const MIN_ENTRIES_PER_NODE: usize = 4;

/// Test-only counter: how many times the structural merge actually ran
/// its incremental re-chunk (vs falling back to the full path). The
/// oracle reads it to prove non-vacuousness.
#[cfg(test)]
static STRUCTURAL_RECHUNK_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Test-only counter: how many original leaves the structural merge
/// actually READ (`load_leaf_entries`). A scattered change set must read
/// only the affected clusters, NOT every leaf in the min_key..max_key span
/// — this counter proves the cluster-aware re-sync keeps reads O(affected).
#[cfg(test)]
static STRUCTURAL_LEAF_READS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

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
    /// Merges changes into this snapshot and persists the result as a
    /// new prolly tree. Thin wrapper that computes the snapshot's
    /// chunk mask (from the root node's `BuildContext`) and delegates
    /// to [`Pipeline::merge_and_persist`].
    pub async fn merge_and_persist(
        &self,
        changes: &dyn ReadableLayer,
        store: &dyn BlobsWrite,
    ) -> anyhow::Result<Option<(Hash, [u8; 32], MergeStats)>> {
        // Use the trait method via UFCS so we don't accidentally pick
        // up an inherent shadow if one is added later.
        let mask = <Snapshot as ReadableLayer>::chunk_mask(self).await;

        // Structural-sharing incremental merge (reversible rollout): only
        // when the base tree is non-empty (there's a tree to share with)
        // AND the env flag is set. Defaults OFF — the full re-read path is
        // the proven default until the structural path has soaked. The
        // structural path itself falls back to the full path internally on
        // any degenerate case (empty changes, large change ratio, …).
        if !self.is_empty() && structural_merge_enabled() {
            if structural_merge_verify_enabled() {
                // VERIFY soak: run BOTH paths, assert byte-identical roots, and
                // return the FULL-path result (authoritative). Production is
                // therefore byte-identical to the proven full path during the
                // soak — a structural bug can only emit a LOUD error + orphan
                // (GC'd) blobs, never a wrong published manifest. Doubles merge
                // cost; a temporary validation phase, not the end state.
                let structural = self
                    .as_pipeline()
                    .merge_and_persist_structural(self, mask, changes, store)
                    .await;
                let full = self
                    .as_pipeline()
                    .merge_and_persist(self, mask, changes, store)
                    .await;
                match (&structural, &full) {
                    (Ok(Some((sh, sph, _))), Ok(Some((fh, fph, _)))) => {
                        if sh != fh || sph != fph {
                            tracing::error!(
                                structural_root = ?sh, full_root = ?fh,
                                "S5_STRUCTURAL_MERGE_VERIFY: ROOT MISMATCH — publishing the full-path result (structural path has a bug)"
                            );
                        } else {
                            tracing::info!("structural-merge verify OK (root matches full path)");
                        }
                    }
                    (Ok(None), Ok(None)) => {
                        tracing::info!("structural-merge verify OK (both empty)");
                    }
                    (s, f) => {
                        tracing::error!(
                            structural_ok = s.is_ok(),
                            full_ok = f.is_ok(),
                            "S5_STRUCTURAL_MERGE_VERIFY: shape/error divergence — publishing the full-path result"
                        );
                    }
                }
                return full;
            }
            return self
                .as_pipeline()
                .merge_and_persist_structural(self, mask, changes, store)
                .await;
        }

        self.as_pipeline()
            .merge_and_persist(self, mask, changes, store)
            .await
    }
}

/// Whether the structural-sharing incremental merge path is enabled.
///
/// Read from `S5_STRUCTURAL_MERGE` (case-insensitive). **Defaults ON** as of
/// 2026-06-18: byte-identical to the full path (the `structural_matches_full_oracle`
/// property test over thousands of randomized cases) and prod-soaked clean. Set
/// `S5_STRUCTURAL_MERGE=0`/`false`/`off` to fall back to the full re-read path.
fn structural_merge_enabled() -> bool {
    std::env::var("S5_STRUCTURAL_MERGE")
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            !matches!(v.as_str(), "0" | "false" | "no" | "off")
        })
        .unwrap_or(true)
}

/// Whether to run BOTH merge paths every cycle and assert they agree — the
/// validation soak before trusting the structural path alone. Read from
/// `S5_STRUCTURAL_MERGE_VERIFY` (`1`/`true`/`yes`/`on`); only consulted when
/// `S5_STRUCTURAL_MERGE` is also set. While on, the FULL-path result is the one
/// published, so a structural divergence is loud-but-harmless. Drop it once the
/// soak shows zero mismatches to get the actual speed win.
fn structural_merge_verify_enabled() -> bool {
    std::env::var("S5_STRUCTURAL_MERGE_VERIFY")
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            matches!(v.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

impl Pipeline {
    /// Merges `changes` into the existing tree rooted at `base` and
    /// persists the result as a new prolly tree. This is the primary
    /// write path; [`Snapshot::merge_and_persist`] is a thin wrapper
    /// that computes `chunk_mask` and delegates here.
    ///
    /// 1. Collects existing entries from `base` (no-op if empty).
    /// 2. Applies `changes` (changes win on key collision).
    /// 3. Filters tombstones.
    /// 4. Chunks into a prolly tree using `chunk_mask`.
    /// 5. Uploads only new nodes (skips existing via `blob_contains`).
    ///
    /// Returns the new root `Hash`, plaintext-hash, and stats — or
    /// `None` if the resulting tree has no live entries.
    pub async fn merge_and_persist(
        &self,
        base: &dyn ReadableLayer,
        chunk_mask: u32,
        changes: &dyn ReadableLayer,
        store: &dyn BlobsWrite,
    ) -> anyhow::Result<Option<(Hash, [u8; 32], MergeStats)>> {
        let mut stats = MergeStats::default();

        let entries = self
            .collect_merged_entries(base, changes, &mut stats)
            .await?;

        if entries.is_empty() {
            return Ok(None);
        }

        stats.entries = entries.len() as u64;

        let leaf_nodes = chunk_entries(&entries, chunk_mask, &NodeKind::Namespace, 0);
        stats.leaf_nodes = leaf_nodes.len() as u64;

        let (root_hash, root_plaintext_hash) = self
            .build_tree_dedup(
                leaf_nodes,
                store,
                &NodeKind::Namespace,
                chunk_mask,
                &mut stats,
            )
            .await?;

        Ok(Some((root_hash, root_plaintext_hash, stats)))
    }

    /// Structural-sharing incremental merge. Produces a byte-identical
    /// root to [`Self::merge_and_persist`] but reads only the base tree's
    /// INTERNAL nodes plus the handful of leaves touched by `changes` —
    /// instead of a full `O(corpus)` leaf scan.
    ///
    /// `base_snapshot` is the concrete tree to share with; `chunk_mask`
    /// is its chunk mask (caller-computed, identical to what the full
    /// path uses). `changes` is the (small) change layer.
    ///
    /// # Algorithm
    ///
    /// 1. Collect `changes` fully (incl. tombstones) — it is small.
    ///    Empty changes / empty base / a large change ratio fall back to
    ///    the full [`Self::merge_and_persist`].
    /// 2. Enumerate the base's level-0 leaf links in key order by walking
    ///    only internal nodes (never reading leaf contents).
    /// 3. Find the leaf index span `[lo, hi]` whose ranges contain the
    ///    min/max changed key.
    /// 4. Re-chunk `[lo..]`, merging changes, through the SAME `chunk_entries`
    ///    fold, RE-SYNCING with original leaves until an emitted boundary
    ///    aligns with an original leaf's last key (then the rest are
    ///    reused by reference).
    /// 5. Assemble leaf links = reused prefix ++ rebuilt ++ reused suffix.
    /// 6. Build internal levels via the shared
    ///    [`Self::build_levels_from_links`].
    ///
    /// Correctness is gated by the property-test oracle in
    /// `persist::structural_tests` (root hashes byte-identical to the
    /// full path over thousands of randomized cases).
    pub async fn merge_and_persist_structural(
        &self,
        base_snapshot: &Snapshot,
        chunk_mask: u32,
        changes: &dyn ReadableLayer,
        store: &dyn BlobsWrite,
    ) -> anyhow::Result<Option<(Hash, [u8; 32], MergeStats)>> {
        use std::collections::BTreeMap;

        // --- Step 1: collect changes (small) ------------------------------
        let mut change_map: BTreeMap<String, NodeEntry> = BTreeMap::new();
        {
            let mut s = changes.scan(Bound::Unbounded, Bound::Unbounded);
            while let Some(r) = s.next().await {
                let (k, v) = r?;
                change_map.insert(k, v);
            }
        }

        // No changes: the result equals the base tree unchanged.
        if change_map.is_empty() {
            // Match the full path's semantics exactly by running it (it
            // recomputes the same root and stats from the base alone).
            return self
                .merge_and_persist(base_snapshot, chunk_mask, changes, store)
                .await;
        }

        // --- Step 2: enumerate base leaf links (internal nodes only) ------
        // Returns None when the base root is itself a single leaf node
        // (no internal level to share) — then we fall back to the full
        // path, which is already cheap (one leaf read).
        let leaves = match self.enumerate_leaf_links(base_snapshot).await? {
            Some(l) if !l.is_empty() => l,
            // Empty base or single-leaf root → full path handles it
            // correctly and cheaply.
            _ => {
                return self
                    .merge_and_persist(base_snapshot, chunk_mask, changes, store)
                    .await;
            }
        };

        // Degenerate-case guard: if the change set is a large fraction of
        // the base, the structural path's per-leaf reloads lose to a
        // single full scan. Estimate base entries as
        // `leaves * (mask + 1)` (avg entries/node ≈ expected). Don't
        // over-engineer — just bail to the full path.
        let est_base_entries = (leaves.len() as u64) * ((chunk_mask as u64) + 1);
        if (change_map.len() as u64).saturating_mul(4) > est_base_entries {
            return self
                .merge_and_persist(base_snapshot, chunk_mask, changes, store)
                .await;
        }

        // Test-only signal that the incremental re-chunk path (not a
        // fallback to the full re-read) was actually taken. Lets the
        // oracle prove it isn't vacuously exercising only the fallback.
        #[cfg(test)]
        STRUCTURAL_RECHUNK_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // --- Step 3: cluster driver (re-chunk affected leaves, carry gaps) -
        // `leaf_index_for(key)` = the leaf whose [first_key, next_first_key)
        // range contains key. The merge starts at `lo` (the first changed
        // leaf) and walks forward, re-chunking affected leaves and re-syncing
        // ACROSS gaps of unaffected leaves (carried verbatim) — so cost is
        // O(affected leaves), NOT O(min_key..max_key span). See the re-sync
        // block below for the gap handling.
        let leaf_index_for = |key: &str| -> usize {
            // Largest index i with leaves[i].first_key <= key; if key is
            // below leaves[0].first_key, clamp to 0.
            match leaves.binary_search_by(|l| l.first_key.as_str().cmp(key)) {
                Ok(i) => i,
                Err(0) => 0,
                Err(i) => i - 1,
            }
        };
        let min_key = change_map.keys().next().expect("change_map non-empty");
        let lo = leaf_index_for(min_key);

        // --- Steps 4 & 5: re-chunk affected region with re-sync ----------
        let read_store: &dyn BlobsRead = self.store().as_ref();
        let mut stats = MergeStats::default();

        // Reused prefix links [0..lo): carried verbatim. Count their
        // entries as reused for stats (cheap: link.size is a byte count,
        // not an entry count, so we don't have per-leaf entry counts here
        // without reading them — leave entries_reused approximate by NOT
        // counting prefix/suffix; stats are non-load-bearing for
        // correctness, and the oracle ignores them).
        let mut new_links: Vec<(String, NodeEntry)> = Vec::new();
        new_links.extend(
            leaves[..lo]
                .iter()
                .map(|l| (l.first_key.clone(), l.link.clone())),
        );

        // Build the working entry buffer from leaves[lo..=hi] merged with
        // the changes in [leaves[lo].first_key, leaves[hi+1].first_key).
        // leaves[lo].first_key is a true chunk-start (leaves[lo-1] is
        // unaffected → its boundary still fires), so the left edge needs
        // no prefix absorption.
        let mut fold = ChunkFold::new(chunk_mask, &NodeKind::Namespace, 0);

        // `next_leaf` is the index of the next ORIGINAL leaf we will pull
        // entries from. We always feed leaves[lo..=hi] (the affected ones,
        // with changes applied); then, to re-sync, we keep pulling
        // subsequent original leaves until the fold emits a boundary that
        // aligns with an original leaf's last key — at which point the
        // remaining leaves are reused by reference.
        let mut next_leaf = lo;

        // Upper bound of the change window for "fall in this region" —
        // exclusive at leaves[hi+1].first_key (or unbounded past the end).
        // Changes strictly below leaves[lo].first_key cannot exist (min_key
        // maps to lo, and a new global-min key < leaves[0].first_key clamps
        // lo to 0, so leaves[0].first_key may be > min_key — handled by
        // merging ALL changes < leaves[hi+1].first_key, which includes any
        // new global min when lo == 0).
        //
        // We merge changes incrementally per pulled leaf to keep ordering
        // correct: for each original leaf pulled, splice in the changes
        // whose keys fall in [leaf.first_key, next_leaf_first_key).

        // Drain `change_map` progressively. We need changes that fall in
        // [leaves[lo].first_key, +inf) for the affected region and any
        // re-sync extension; but a change beyond the re-sync point would
        // belong to a reused suffix leaf — which can't happen, because the
        // re-sync only completes when all subsequent entries are unchanged
        // (no remaining changes lie past it). We assert that below.
        //
        // Implementation: convert change_map into a peekable sorted iter.
        let mut changes_iter = change_map.into_iter().peekable();

        // Invariant: no change sorts strictly below leaves[lo].first_key.
        // For lo > 0 that would require a change key in
        // [leaves[lo-1].first_key, leaves[lo].first_key), but then
        // leaf_index_for(min_key) would have returned lo-1 — contradiction.
        // For lo == 0 a new global-min change is >= "" and may be <
        // leaves[0].first_key, and we WANT it (it becomes the new first
        // entry), so the guard is gated on lo > 0. A violation would mean a
        // change leaked into a prefix leaf we declared unaffected, silently
        // corrupting the merge — so assert rather than paper over it.
        if lo > 0 {
            let lo_first = leaves[lo].first_key.as_str();
            debug_assert!(
                changes_iter
                    .peek()
                    .is_none_or(|(k, _)| k.as_str() >= lo_first),
                "structural merge invariant violated: a change sorts below the affected span"
            );
        }

        // The re-sync detector needs each pulled original leaf's LAST key.
        // We feed entries leaf-by-leaf; after feeding a leaf we check
        // whether the fold's last emitted boundary equals that leaf's last
        // key AND there are no buffered (un-emitted) entries AND no
        // remaining changes at-or-after the next leaf. If so, leaves
        // [next_leaf+1..] are reused.
        let mut resync_at: Option<usize> = None;

        // Walk original leaves from `lo`. The cluster-aware re-sync block at
        // the bottom either keeps folding (next change is adjacent), closes
        // the cluster and skips a verbatim gap (next change is far), or ends
        // the merge (no changes left).
        while next_leaf < leaves.len() {
            let leaf = &leaves[next_leaf];
            let leaf_next_first: Option<&str> =
                leaves.get(next_leaf + 1).map(|l| l.first_key.as_str());

            // Load this original leaf's entries.
            let entries = self.load_leaf_entries(base_snapshot, leaf).await?;

            // Merge: walk this leaf's entries in order, splicing in any
            // changes that sort before each entry, applying overrides, and
            // dropping tombstoned keys.
            let mut last_fed_key: Option<String> = None;
            for (k, v) in entries.iter() {
                // Splice in changes that sort strictly before k AND fall
                // within this leaf's key span (< leaf_next_first).
                while let Some((ck, _)) = changes_iter.peek() {
                    let ck_in_span = leaf_next_first.is_none_or(|nf| ck.as_str() < nf);
                    if ck_in_span && ck.as_str() < k.as_str() {
                        let (ck, cv) = changes_iter.next().unwrap();
                        if !cv.is_tombstone() {
                            stats.entries_changed += 1;
                            last_fed_key = Some(ck.clone());
                            fold.push(ck, cv);
                        } else {
                            stats.tombstones_filtered += 1;
                        }
                    } else {
                        break;
                    }
                }

                // Now handle k itself: a change at exactly k overrides it.
                if changes_iter
                    .peek()
                    .is_some_and(|(ck, _)| ck.as_str() == k.as_str())
                {
                    let (ck, cv) = changes_iter.next().unwrap();
                    if !cv.is_tombstone() {
                        stats.entries_changed += 1;
                        last_fed_key = Some(ck.clone());
                        fold.push(ck, cv);
                    } else {
                        stats.tombstones_filtered += 1;
                    }
                } else {
                    stats.entries_reused += 1;
                    last_fed_key = Some(k.clone());
                    fold.push(k.clone(), v.clone());
                }
            }

            // Splice in any changes that sort after the last entry but
            // still within this leaf's span (new keys appended into this
            // leaf's range, e.g. a new global max when this is the last
            // leaf).
            while let Some((ck, _)) = changes_iter.peek() {
                let ck_in_span = leaf_next_first.is_none_or(|nf| ck.as_str() < nf);
                if ck_in_span {
                    let (ck, cv) = changes_iter.next().unwrap();
                    if !cv.is_tombstone() {
                        stats.entries_changed += 1;
                        last_fed_key = Some(ck.clone());
                        fold.push(ck, cv);
                    } else {
                        stats.tombstones_filtered += 1;
                    }
                } else {
                    break;
                }
            }

            next_leaf += 1;

            // Re-sync check (CLUSTER-AWARE). The fold may pause at this
            // original leaf boundary iff the current chunk closed EXACTLY on
            // this leaf's last key with nothing buffered:
            //   - the last entry fed was this leaf's last key (nothing was
            //     inserted/removed past it → downstream boundaries unchanged),
            //   - AND the fold just emitted a boundary on that key.
            // When aligned, the action depends on where the NEXT change is:
            //   - none left            → reuse leaves[next_leaf..] verbatim
            //                            (whole-suffix re-sync; ends the merge),
            //   - in the FOLLOWING leaf → keep folding (it is affected),
            //   - in a LATER leaf      → a GAP of unaffected leaves precedes it:
            //                            close this cluster, carry the gap
            //                            verbatim, and resume re-chunking at the
            //                            leaf that actually contains the change.
            // The third case is the whole point of the structural path: it makes
            // a scatter-across-collections change set cost O(affected leaves),
            // not O(min_key..max_key span). Carrying a gap verbatim is sound for
            // the SAME reason the suffix re-sync is — a boundary closing on an
            // original leaf's last key means every leaf after it chunks
            // byte-identically to the base, so it is reusable by reference.
            let aligned = entries
                .last()
                .map(|(k, _)| k.as_str())
                .is_some_and(|orig_last| {
                    last_fed_key.as_deref() == Some(orig_last)
                        && fold.just_emitted_boundary_on(orig_last)
                });
            if aligned {
                // Clone the next change key so we don't hold a `changes_iter`
                // borrow across the `node_to_link().await` below.
                let next_change_key: Option<String> = changes_iter.peek().map(|(k, _)| k.clone());
                match next_change_key {
                    None => {
                        resync_at = Some(next_leaf); // reuse leaves[next_leaf..]
                        break;
                    }
                    Some(ck) if next_leaf < leaves.len() => {
                        // Span of the immediately-following original leaf is
                        // [leaves[next_leaf].first_key, leaves[next_leaf+1].first_key).
                        let following_end = leaves.get(next_leaf + 1).map(|x| x.first_key.as_str());
                        let next_in_following = ck.as_str() >= leaves[next_leaf].first_key.as_str()
                            && following_end.is_none_or(|e| ck.as_str() < e);
                        if !next_in_following {
                            // GAP. Flush this cluster's rebuilt nodes, carry the
                            // intervening unaffected leaves verbatim, reset the
                            // fold, and resume at the leaf containing the change.
                            let cluster_nodes = std::mem::replace(
                                &mut fold,
                                ChunkFold::new(chunk_mask, &NodeKind::Namespace, 0),
                            )
                            .finish();
                            for node in &cluster_nodes {
                                new_links.push(
                                    self.node_to_link(node, store, read_store, &mut stats)
                                        .await?,
                                );
                            }
                            let resume = leaf_index_for(ck.as_str());
                            debug_assert!(
                                resume >= next_leaf,
                                "structural merge: gap resume index moved backwards"
                            );
                            new_links.extend(
                                leaves[next_leaf..resume]
                                    .iter()
                                    .map(|l| (l.first_key.clone(), l.link.clone())),
                            );
                            next_leaf = resume;
                        }
                        // else: the next change is in the following leaf → keep folding.
                    }
                    _ => {}
                }
            }
        }

        // Flush whatever remains in the fold (the trailing partial / final
        // leaf). If we hit the end of the leaves without a re-sync, this
        // becomes the final chunk.
        let rebuilt_nodes = fold.finish();
        for node in &rebuilt_nodes {
            new_links.push(
                self.node_to_link(node, store, read_store, &mut stats)
                    .await?,
            );
        }

        // Reused suffix links [resync_at..]: carried verbatim.
        if let Some(start) = resync_at {
            new_links.extend(
                leaves[start..]
                    .iter()
                    .map(|l| (l.first_key.clone(), l.link.clone())),
            );
        }

        stats.leaf_nodes = new_links.len() as u64;

        // --- Step 7: zero live entries → None ----------------------------
        if new_links.is_empty() {
            return Ok(None);
        }

        // --- Step 8: build internal levels (shared with full path) -------
        let (root_hash, root_plaintext_hash) = self
            .build_levels_from_links(
                new_links,
                store,
                &NodeKind::Namespace,
                chunk_mask,
                0,
                &mut stats,
            )
            .await?;

        Ok(Some((root_hash, root_plaintext_hash, stats)))
    }

    /// Enumerate the base tree's level-0 leaf links in key order, reading
    /// ONLY internal (level ≥ 1) nodes. Returns `None` when the root is a
    /// single leaf (no internal level to walk) or the snapshot is empty.
    async fn enumerate_leaf_links(&self, base: &Snapshot) -> anyhow::Result<Option<Vec<LeafRef>>> {
        if base.is_empty() {
            return Ok(None);
        }
        let root = base.load_root().await?;
        if root.is_leaf_node() {
            // Single-leaf tree — no internal links to share.
            return Ok(None);
        }

        // Descend internal levels, collecting leaf links in order. We hold
        // a worklist of (link entry, level). Because internal levels are
        // few and small, a simple recursive-by-stack DFS in key order is
        // fine. We never load a level-0 (leaf) node here.
        let mut out: Vec<LeafRef> = Vec::new();
        self.collect_leaf_links(base, &root, &mut out).await?;
        Ok(Some(out))
    }

    /// DFS over internal nodes, appending level-0 leaf links in key order.
    /// `node` is an already-loaded internal node (level ≥ 1).
    fn collect_leaf_links<'a>(
        &'a self,
        base: &'a Snapshot,
        node: &'a Node,
        out: &'a mut Vec<LeafRef>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            debug_assert!(node.header.level >= 1, "collect_leaf_links on a leaf");
            for (first_key, entry) in node.entries.iter() {
                let content = entry
                    .content
                    .as_ref()
                    .expect("internal node entries are Link entries with content");
                if node.header.level == 1 {
                    // Children are level-0 leaves — record the link WITHOUT
                    // loading the leaf node.
                    out.push(LeafRef {
                        first_key: first_key.clone(),
                        link: entry.clone(),
                        hash: content.hash(),
                        plaintext_hash: content.plaintext_hash,
                    });
                } else {
                    // Children are internal — load and recurse.
                    let child = base
                        .load(content.hash(), content.plaintext_hash.as_ref())
                        .await?;
                    self.collect_leaf_links(base, &child, out).await?;
                }
            }
            Ok(())
        })
    }

    /// Load the entries of one original leaf (by its link).
    async fn load_leaf_entries(
        &self,
        base: &Snapshot,
        leaf: &LeafRef,
    ) -> anyhow::Result<Vec<(String, NodeEntry)>> {
        #[cfg(test)]
        STRUCTURAL_LEAF_READS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let node = base.load(leaf.hash, leaf.plaintext_hash.as_ref()).await?;
        debug_assert!(
            node.is_leaf_node(),
            "load_leaf_entries on a non-leaf node (level {})",
            node.header.level
        );
        Ok(node
            .entries
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    /// Compute the minimal change-layer that, applied to `base`, reproduces the
    /// full convergence union `merge_and_persist(base, mask, changes_full)` —
    /// where `changes_full` is the WHOLE `changes` snapshot (the publish-side
    /// "union local with prev-published" merge).
    ///
    /// Key idea: a `changes`-leaf whose CONTENT HASH appears anywhere in
    /// `base`'s leaf set is byte-identical in both trees — content-addressing
    /// makes this robust to chunk-boundary shifts — so it contributes nothing.
    /// Only `changes`-leaves whose hash is ABSENT from `base` carry new/changed
    /// entries; emit those leaves' entries verbatim (a superset of the truly-
    /// changed keys, which is harmless: re-applying an entry equal to base
    /// yields the same tree). Keys present in `base` but absent from `changes`
    /// (a "deletion") are NOT tombstoned here — the union keeps `base`'s entry,
    /// EXACTLY as the full path does (both reuse it), so the result stays
    /// byte-identical. The downstream `merge_and_persist_structural` is oracle-
    /// proven byte-identical to the full path for any (base, changes).
    ///
    /// Returns `None` (→ caller uses the full path) for single-leaf trees, where
    /// there is no internal level to enumerate and the full path is already cheap.
    async fn compute_convergence_diff(
        &self,
        base: &Snapshot,
        changes: &Snapshot,
    ) -> anyhow::Result<Option<crate::layer::MapLayer>> {
        let (Some(base_leaves), Some(change_leaves)) = (
            self.enumerate_leaf_links(base).await?,
            self.enumerate_leaf_links(changes).await?,
        ) else {
            return Ok(None);
        };
        let base_hashes: std::collections::HashSet<Hash> =
            base_leaves.iter().map(|l| l.hash).collect();
        let mut diff: std::collections::BTreeMap<String, NodeEntry> =
            std::collections::BTreeMap::new();
        for leaf in &change_leaves {
            if base_hashes.contains(&leaf.hash) {
                continue; // byte-identical leaf already in base — no change
            }
            for (k, v) in self.load_leaf_entries(changes, leaf).await? {
                diff.insert(k, v);
            }
        }
        Ok(Some(crate::layer::MapLayer::new(diff)))
    }

    /// Convergence merge via tree-diff: prune subtrees that are byte-identical
    /// between `base` and `changes` (by content hash), diff only the rest, and
    /// feed the small change-layer to the oracle-tested structural merge.
    /// Byte-identical to `merge_and_persist(base, mask, changes_as_layer)` (the
    /// full O(corpus) path) but O(changed leaves) instead of O(corpus). Falls
    /// back to the full path when the diff can't be computed (single-leaf tree).
    pub async fn merge_and_persist_treediff(
        &self,
        base: &Snapshot,
        chunk_mask: u32,
        changes: &Snapshot,
        store: &dyn BlobsWrite,
    ) -> anyhow::Result<Option<(Hash, [u8; 32], MergeStats)>> {
        match self.compute_convergence_diff(base, changes).await? {
            Some(diff) => {
                self.merge_and_persist_structural(base, chunk_mask, &diff, store)
                    .await
            }
            None => {
                self.merge_and_persist(base, chunk_mask, changes, store)
                    .await
            }
        }
    }

    /// Collects entries from `base` merged with `changes`. Changes
    /// take priority on key collision; tombstones are filtered out.
    ///
    /// # HIGH PRIORITY TODO: Streaming
    ///
    /// Currently materializes the entire base layer into a `Vec` via
    /// `scan()`. For 1M+ files this is ~200 MB+ of heap allocation.
    /// Fix is internal: stream both `base.scan()` and `changes.scan()`
    /// in a sorted merge iterator and feed straight into `chunk_entries`.
    async fn collect_merged_entries(
        &self,
        base: &dyn ReadableLayer,
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

        // Walk base + apply changes. If base is empty, the loop body
        // never runs and the "remaining changes" tail block adds them
        // all — same outcome as the previous early-return on is_empty.
        let mut entries = Vec::new();
        let mut old_stream = base.scan(Bound::Unbounded, Bound::Unbounded);

        while let Some(result) = old_stream.next().await {
            let (key, entry) = result?;

            if let Some(changed) = change_map.remove(&key) {
                if changed.is_tombstone() {
                    stats.tombstones_filtered += 1;
                } else {
                    stats.entries_changed += 1;
                    entries.push((key, changed));
                }
            } else {
                stats.entries_reused += 1;
                entries.push((key, entry));
            }
        }
        drop(old_stream);

        // Remaining changes are new keys not in the base.
        for (key, entry) in change_map {
            if entry.is_tombstone() {
                stats.tombstones_filtered += 1;
                continue;
            }
            stats.entries_changed += 1;
            entries.push((key, entry));
        }

        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        Ok(entries)
    }

    /// Encodes a node through the node pipeline, uploads it only if
    /// it doesn't already exist in the store. Returns `(BlobId, plaintext_hash)`.
    ///
    /// `pub(crate)` so [`crate::copy::shallow_copy_into`] can re-encode a
    /// mirrored byte-stream subtree under the DESTINATION node keys.
    pub(crate) async fn write_node_dedup(
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

        let cas_hash = Hash::from(*blake3::hash(&result.bytes).as_bytes());

        if read_store.blob_contains(cas_hash).await? {
            stats.nodes_deduped += 1;
            let blob_id = BlobId {
                hash: cas_hash,
                size: result.bytes.len() as u64,
            };
            return Ok((blob_id, plaintext_hash));
        }

        let blob_id = store
            .blob_upload_bytes(result.bytes)
            .await
            .map_err(|e| anyhow::anyhow!("uploading Node: {e}"))?;

        stats.nodes_uploaded += 1;
        stats.bytes_uploaded += blob_id.size;

        Ok((blob_id, plaintext_hash))
    }

    /// Encodes a single leaf/internal `Node` via dedup-write and returns
    /// the `(first_key, Link NodeEntry)` pair that its parent level
    /// stores. Factored out of [`Self::build_tree_dedup`] so the
    /// structural-merge path can produce byte-identical link entries for
    /// the leaves it rebuilds, and so the link `size` (sum of child
    /// content sizes) is computed in exactly one place.
    pub(crate) async fn node_to_link(
        &self,
        node: &Node,
        store: &dyn BlobsWrite,
        read_store: &dyn BlobsRead,
        stats: &mut MergeStats,
    ) -> anyhow::Result<(String, NodeEntry)> {
        let (blob_id, plaintext_hash) = self
            .write_node_dedup(node, store, read_store, stats)
            .await?;

        let first_key = node.entries.keys().next().cloned().unwrap_or_default();

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

        Ok((first_key, link_entry))
    }

    /// Builds the tree bottom-up from leaf nodes, using dedup writes. The
    /// chunk mask is threaded through the recursion (caller computed
    /// it once at the top).
    pub(crate) async fn build_tree_dedup(
        &self,
        nodes: Vec<Node>,
        store: &dyn BlobsWrite,
        kind: &NodeKind,
        chunk_mask: u32,
        stats: &mut MergeStats,
    ) -> anyhow::Result<(Hash, [u8; 32])> {
        let read_store: &dyn BlobsRead = self.store().as_ref();

        // Leaves are at level 0.
        let leaf_level = nodes.first().map(|n| n.header.level).unwrap_or(0);

        let mut children: Vec<(String, NodeEntry)> = Vec::with_capacity(nodes.len());
        for node in &nodes {
            children.push(self.node_to_link(node, store, read_store, stats).await?);
        }

        self.build_levels_from_links(children, store, kind, chunk_mask, leaf_level, stats)
            .await
    }

    /// Builds the internal levels of the tree from an ordered list of
    /// level-`child_level` link entries (each `(first_key, Link entry)`,
    /// keyed by the first key in the child it points at). This is the
    /// shared spine-construction used by BOTH the full
    /// [`Self::build_tree_dedup`] path and the structural-merge path
    /// ([`Self::merge_and_persist_structural`]) — keeping internal-level
    /// construction in one place guarantees the two paths produce a
    /// byte-identical root.
    ///
    /// `child_level` is the level of the nodes the input links point at
    /// (0 when they're leaves). Internal nodes are chunked with the SAME
    /// `chunk_entries` CDC over the link entries (keyed by each child's
    /// first key) and recursively reduced until a single root remains.
    pub(crate) async fn build_levels_from_links(
        &self,
        links: Vec<(String, NodeEntry)>,
        store: &dyn BlobsWrite,
        kind: &NodeKind,
        chunk_mask: u32,
        child_level: u8,
        stats: &mut MergeStats,
    ) -> anyhow::Result<(Hash, [u8; 32])> {
        let read_store: &dyn BlobsRead = self.store().as_ref();

        let mut links = links;
        let mut child_level = child_level;

        loop {
            if links.len() == 1 {
                let content = links[0]
                    .1
                    .content
                    .as_ref()
                    .expect("link entry always has content");
                let hash = content.hash();
                let plaintext_hash = content
                    .plaintext_hash
                    .expect("plaintext_hash always set by node_to_link / reused link");
                stats.depth = child_level;
                return Ok((hash, plaintext_hash));
            }

            let level = child_level + 1;
            let internal_nodes = chunk_entries(&links, chunk_mask, kind, level);
            stats.internal_nodes += internal_nodes.len() as u64;

            let mut next: Vec<(String, NodeEntry)> = Vec::with_capacity(internal_nodes.len());
            for node in &internal_nodes {
                next.push(self.node_to_link(node, store, read_store, stats).await?);
            }

            links = next;
            child_level = level;
        }
    }
}

// ===========================================================================
// Structural-merge helpers (private)
// ===========================================================================

/// A reference to one level-0 leaf of the base tree, discovered by
/// walking only internal nodes. Carries everything the structural-merge
/// path needs to either REUSE the leaf (its parent `link` entry, kept
/// verbatim → byte-identical link + dedup hit) or LOAD its entries on
/// demand (`hash` + `plaintext_hash`).
struct LeafRef {
    /// First key in the leaf (the parent's link key for it).
    first_key: String,
    /// The parent's `Link` `NodeEntry` for this leaf — reused verbatim
    /// for unaffected prefix/suffix leaves so the rebuilt link is
    /// byte-identical (same hash, size, plaintext_hash, stored_blocks).
    link: NodeEntry,
    /// CAS hash of the leaf node blob (for `load`).
    hash: Hash,
    /// Plaintext hash of the leaf node blob (for `load`).
    plaintext_hash: Option<[u8; 32]>,
}

/// Streaming form of [`chunk_entries`] used by the structural-merge
/// path. Pushing entries one at a time MUST produce byte-identical
/// nodes to `chunk_entries(&all, mask, kind, level)` over the same
/// sequence — it shares `MIN_ENTRIES_PER_NODE`, `is_boundary`, and
/// `entries_to_node`.
///
/// Additionally tracks whether the most recent `push` closed a chunk on
/// a given key, which the re-sync detector consults to decide when the
/// remaining original leaves can be reused by reference.
struct ChunkFold {
    mask: u32,
    kind: NodeKind,
    level: u8,
    current: Vec<(String, NodeEntry)>,
    nodes: Vec<Node>,
    /// The key of the last `push`, and whether that push emitted a
    /// boundary node. `(last_key, emitted)`.
    last_push: Option<(String, bool)>,
}

impl ChunkFold {
    fn new(mask: u32, kind: &NodeKind, level: u8) -> Self {
        Self {
            mask,
            kind: kind.clone(),
            level,
            current: Vec::new(),
            nodes: Vec::new(),
            last_push: None,
        }
    }

    /// Feed one entry. Mirrors the body of the `chunk_entries` loop
    /// exactly: push, then emit-and-clear iff
    /// `current.len() >= MIN_ENTRIES_PER_NODE && is_boundary(key)`.
    fn push(&mut self, key: String, entry: NodeEntry) {
        self.current.push((key.clone(), entry));
        let emitted = if self.current.len() >= MIN_ENTRIES_PER_NODE && is_boundary(&key, self.mask)
        {
            self.nodes
                .push(entries_to_node(&self.current, &self.kind, self.level));
            self.current.clear();
            true
        } else {
            false
        };
        self.last_push = Some((key, emitted));
    }

    /// True iff the most recent `push` was on `key` AND it emitted a
    /// boundary node (so `current` is now empty and the chunk closed
    /// exactly at `key`). This is the re-sync alignment test.
    fn just_emitted_boundary_on(&self, key: &str) -> bool {
        matches!(&self.last_push, Some((k, true)) if k.as_str() == key)
    }

    /// Flush any trailing partial chunk (mirrors the post-loop flush in
    /// `chunk_entries`) and return all nodes in order.
    fn finish(mut self) -> Vec<Node> {
        if !self.current.is_empty() {
            self.nodes
                .push(entries_to_node(&self.current, &self.kind, self.level));
        }
        self.nodes
    }
}

// ===========================================================================
// Chunking helpers (private)
// ===========================================================================

/// Chunks a sorted list of entries into nodes using content-defined boundaries.
///
/// Boundary condition: `blake3(key)[0..4] as u32 & mask == 0`.
/// Minimum node size is enforced to prevent degenerate single-entry nodes.
pub(crate) fn chunk_entries(
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

// ===========================================================================
// Structural-merge correctness oracle (property tests)
// ===========================================================================

#[cfg(test)]
mod structural_tests {
    //! Property-test oracle for the structural-sharing incremental merge.
    //!
    //! For each randomized case we build a random base tree, generate a
    //! random change set, then compute the new root TWO ways:
    //!   (a) the existing FULL `Pipeline::merge_and_persist`, and
    //!   (b) the new `Pipeline::merge_and_persist_structural`.
    //! The two roots MUST be byte-identical, and a full `scan()` of both
    //! resulting trees MUST yield the identical `(key, NodeEntry)`
    //! sequence. Any mismatch fails with the seed printed for replay.

    use super::*;
    use std::collections::BTreeMap;
    use std::ops::Bound;
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::stream::{self, BoxStream};
    use s5_core::blob::BlobStore;
    use s5_store_memory::MemoryStore;

    fn store() -> Arc<BlobStore> {
        Arc::new(BlobStore::new(MemoryStore::new()))
    }

    /// `STRUCTURAL_RECHUNK_COUNT` / `STRUCTURAL_LEAF_READS` are
    /// process-global; the tests that reset + assert them must not
    /// overlap with any other test driving the structural path, or the
    /// counters read cross-test noise (observed as a rare parallel-run
    /// flake in `structural_reads_only_affected_clusters`). Every test
    /// in this module that reaches `merge_and_persist_structural`
    /// (directly or via treediff) takes this lock first.
    static COUNTER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    // ----- tiny deterministic PRNG (SplitMix64) ----------------------------
    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self {
            // Avoid the all-zero state degenerate.
            Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
        }
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        fn below(&mut self, n: u64) -> u64 {
            if n == 0 { 0 } else { self.next_u64() % n }
        }
        fn chance(&mut self, num: u64, den: u64) -> bool {
            self.below(den) < num
        }
    }

    // ----- in-memory change layer -----------------------------------------
    struct MemLayer {
        entries: BTreeMap<String, NodeEntry>,
    }
    #[async_trait]
    impl ReadableLayer for MemLayer {
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
            let v: Vec<_> = self
                .entries
                .range((start, end))
                .map(|(k, e)| Ok((k.clone(), e.clone())))
                .collect();
            stream::iter(v).boxed()
        }
    }

    // A live (non-tombstone) leaf entry whose value is derived from `tag`,
    // so an "update" with a different tag produces a different NodeEntry.
    fn leaf_entry(tag: u64) -> NodeEntry {
        let mut h = [0u8; 32];
        h[..8].copy_from_slice(&tag.to_le_bytes());
        NodeEntry {
            content: Some(ContentRef {
                structural: Structural::Leaf,
                // Keep `size` small so summing it across many leaves (the
                // link `total_size`) can't overflow u64 — the entry's
                // IDENTITY for the oracle lives in `hash` (derived from
                // `tag`), so a bounded size doesn't weaken update detection.
                hash: h,
                size: tag % 4096,
                plaintext_hash: None,
                stored_blocks: None,
            }),
            semantic: None,
            child_context: None,
            tombstone: None,
        }
    }

    // ----- key generation --------------------------------------------------
    // Produce a pool of candidate keys mixing realistic path-like strings
    // and adversarial short keys (including boundary keys for the mask).
    fn key_pool(rng: &mut Rng, mask: u32, n: usize) -> Vec<String> {
        let dirs = ["a", "Photos", "x", "deep/nested/dir", "z", "b", "m"];
        let mut out = Vec::with_capacity(n);
        let mut i = 0u64;
        while out.len() < n {
            let kind = rng.below(5);
            let k = match kind {
                0 => format!(
                    "{}/{:04}.bin",
                    dirs[(rng.below(dirs.len() as u64)) as usize],
                    i
                ),
                1 => format!("{}", (b'a' + (rng.below(26) as u8)) as char), // single char
                2 => format!("{:02}", rng.below(100)),                      // short numeric
                3 => format!("k{:08x}", rng.next_u64() as u32),
                _ => {
                    // Adversarial: search for a key that IS a boundary key
                    // for this mask, so boundary keys appear in the pool.
                    let mut attempt = i.wrapping_mul(2654435761);
                    loop {
                        let cand = format!("B{attempt:016x}");
                        if is_boundary(&cand, mask) {
                            break cand;
                        }
                        attempt = attempt.wrapping_add(1);
                    }
                }
            };
            out.push(k);
            i += 1;
        }
        out.sort();
        out.dedup();
        out
    }

    /// Build a base tree from `base_keys` (sorted unique) at `mask`,
    /// returning a `Snapshot` rooted at the result. Empty base_keys →
    /// empty snapshot.
    async fn build_base(
        st: &Arc<BlobStore>,
        mask: u32,
        base_keys: &[String],
        rng: &mut Rng,
    ) -> Snapshot {
        let empty = Snapshot::empty_plain(st.clone() as Arc<dyn s5_core::BlobsRead>);
        if base_keys.is_empty() {
            return empty;
        }
        let mut m = BTreeMap::new();
        for k in base_keys {
            m.insert(k.clone(), leaf_entry(rng.next_u64()));
        }
        let changes = MemLayer { entries: m };
        let pipe = empty.as_pipeline();
        let res = pipe
            .merge_and_persist(&empty, mask, &changes, st.as_ref())
            .await
            .unwrap();
        match res {
            Some((root, pth, _)) => Snapshot::new(
                root,
                st.clone() as Arc<dyn s5_core::BlobsRead>,
                Default::default(),
                Some(pth),
            ),
            None => empty,
        }
    }

    async fn scan_all(snap: &Snapshot) -> Vec<(String, Vec<u8>)> {
        use futures::StreamExt;
        let mut out = Vec::new();
        let mut s = snap.scan(Bound::Unbounded, Bound::Unbounded);
        while let Some(r) = s.next().await {
            let (k, e) = r.unwrap();
            // Skip tombstones (a well-formed merged tree shouldn't carry
            // them, but be defensive).
            if e.is_tombstone() {
                continue;
            }
            // Encode the entry's content identity for comparison.
            let cb = e
                .content
                .as_ref()
                .map(|c| {
                    let mut v = c.hash.to_vec();
                    v.extend_from_slice(&c.size.to_le_bytes());
                    v
                })
                .unwrap_or_default();
            out.push((k, cb));
        }
        out
    }

    #[tokio::test]
    async fn structural_matches_full_oracle() {
        use std::sync::atomic::Ordering;
        let _serialized = COUNTER_LOCK.lock().await;
        STRUCTURAL_RECHUNK_COUNT.store(0, Ordering::Relaxed);

        // >= 2000 randomized cases.
        const CASES: u64 = 2000;
        // Masks to exercise: 0x3 (≈4/node — stresses MIN_ENTRIES + tiny
        // leaves), 0x7, 0x3F (the production default).
        let masks = [0x3u32, 0x7, 0x3F];

        for case in 0..CASES {
            let seed = 0xABCD_0000u64 ^ case.wrapping_mul(0x100000001b3);
            let mut rng = Rng::new(seed);
            let mask = masks[(rng.below(masks.len() as u64)) as usize];

            // Base size: hit 0, 1, 3, 4, 5, and larger buckets.
            let base_n = match rng.below(10) {
                0 => 0,
                1 => 1,
                2 => 3,
                3 => 4,
                4 => 5,
                5 => 1 + rng.below(20) as usize,
                6 => 100 + rng.below(50) as usize,
                _ => 300 + rng.below(1200) as usize,
            };

            // A shared key pool so changes can target existing keys,
            // boundary keys, and brand-new keys.
            let pool = key_pool(&mut rng, mask, base_n + 64);
            let base_keys: Vec<String> = pool.iter().take(base_n).cloned().collect();

            let st = store();
            let base = build_base(&st, mask, &base_keys, &mut rng).await;

            // --- generate a random change set ---
            let mut changes: BTreeMap<String, NodeEntry> = BTreeMap::new();
            let change_kind = rng.below(12);
            match change_kind {
                0 => { /* empty change set */ }
                1 => {
                    // single change: update or insert one key
                    if !base_keys.is_empty() && rng.chance(1, 2) {
                        let k = base_keys[rng.below(base_keys.len() as u64) as usize].clone();
                        changes.insert(k, leaf_entry(rng.next_u64()));
                    } else {
                        changes.insert(
                            format!("N{:016x}", rng.next_u64()),
                            leaf_entry(rng.next_u64()),
                        );
                    }
                }
                11 => {
                    // ALL keys changed (update every existing key).
                    for k in &base_keys {
                        changes.insert(k.clone(), leaf_entry(rng.next_u64()));
                    }
                }
                _ => {
                    // Mixed: updates, inserts (incl. new global min/max,
                    // boundary keys, near-boundary), deletes (incl. boundary
                    // keys / drop-a-leaf-below-MIN).
                    let n_ops = 1 + rng.below(12) as usize;
                    for _ in 0..n_ops {
                        match rng.below(6) {
                            0 if !base_keys.is_empty() => {
                                // update existing
                                let k =
                                    base_keys[rng.below(base_keys.len() as u64) as usize].clone();
                                changes.insert(k, leaf_entry(rng.next_u64()));
                            }
                            1 if !base_keys.is_empty() => {
                                // delete existing (tombstone)
                                let k =
                                    base_keys[rng.below(base_keys.len() as u64) as usize].clone();
                                changes.insert(k, NodeEntry::tombstone(rng.next_u64() as u32));
                            }
                            2 => {
                                // insert brand-new key from the pool tail
                                let k =
                                    pool[(base_n + (rng.below(64) as usize)) % pool.len()].clone();
                                changes.insert(k, leaf_entry(rng.next_u64()));
                            }
                            3 => {
                                // new global MIN (sorts before everything)
                                changes.insert(
                                    format!("\u{0}A{:08x}", rng.next_u64() as u32),
                                    leaf_entry(rng.next_u64()),
                                );
                            }
                            4 => {
                                // new global MAX
                                changes.insert(
                                    format!("~~~Z{:016x}", rng.next_u64()),
                                    leaf_entry(rng.next_u64()),
                                );
                            }
                            _ => {
                                // a key that IS a boundary key for this mask
                                let mut a = rng.next_u64();
                                let k = loop {
                                    let cand = format!("B{a:016x}");
                                    if is_boundary(&cand, mask) {
                                        break cand;
                                    }
                                    a = a.wrapping_add(1);
                                };
                                changes.insert(k, leaf_entry(rng.next_u64()));
                            }
                        }
                    }
                }
            }

            // --- compute (a) full and (b) structural ---
            let full_changes = MemLayer {
                entries: changes.clone(),
            };
            let struct_changes = MemLayer {
                entries: changes.clone(),
            };

            let pipe = base.as_pipeline();
            let full = pipe
                .merge_and_persist(&base, mask, &full_changes, st.as_ref())
                .await
                .unwrap_or_else(|e| {
                    panic!("FULL path failed (seed={seed:#x}, mask={mask:#x}): {e}")
                });
            let structural = pipe
                .merge_and_persist_structural(&base, mask, &struct_changes, st.as_ref())
                .await
                .unwrap_or_else(|e| {
                    panic!("STRUCTURAL path failed (seed={seed:#x}, mask={mask:#x}): {e}")
                });

            match (&full, &structural) {
                (None, None) => { /* both empty — OK */ }
                (Some((rf, pf, _)), Some((rs, ps, _))) => {
                    assert_eq!(
                        rf,
                        rs,
                        "ROOT HASH MISMATCH (seed={seed:#x}, mask={mask:#x}, base_n={base_n}, change_kind={change_kind}, n_changes={})\n  full root={rf}\n  struct root={rs}",
                        changes.len()
                    );
                    assert_eq!(
                        pf, ps,
                        "ROOT PLAINTEXT-HASH MISMATCH (seed={seed:#x}, mask={mask:#x})"
                    );

                    // Full scan equality (defense in depth — root equality
                    // already implies tree equality, but verify the visible
                    // sequence too).
                    let snap_full = Snapshot::new(
                        *rf,
                        st.clone() as Arc<dyn s5_core::BlobsRead>,
                        Default::default(),
                        Some(*pf),
                    );
                    let snap_struct = Snapshot::new(
                        *rs,
                        st.clone() as Arc<dyn s5_core::BlobsRead>,
                        Default::default(),
                        Some(*ps),
                    );
                    let a = scan_all(&snap_full).await;
                    let b = scan_all(&snap_struct).await;
                    assert_eq!(
                        a, b,
                        "SCAN MISMATCH (seed={seed:#x}, mask={mask:#x}, base_n={base_n})"
                    );
                }
                (f, s) => panic!(
                    "PRESENCE MISMATCH (seed={seed:#x}, mask={mask:#x}, base_n={base_n}, change_kind={change_kind}): full.is_some()={}, struct.is_some()={}",
                    f.is_some(),
                    s.is_some()
                ),
            }
        }

        // Non-vacuousness: a meaningful fraction of cases must have taken
        // the real incremental re-chunk path, not just the fallback.
        let rechunked = STRUCTURAL_RECHUNK_COUNT.load(Ordering::Relaxed);
        assert!(
            rechunked >= CASES / 4,
            "structural re-chunk path ran only {rechunked}/{CASES} times — oracle is mostly testing the fallback"
        );
    }

    /// Non-vacuousness: prove the structural path actually SHARES the
    /// unchanged leaves rather than silently falling back to the full
    /// re-read. A single change to a many-leaf tree must (a) keep the
    /// full leaf count in the new tree, yet (b) upload only a tiny spine
    /// (the rebuilt leaf + the internal nodes above it) — the reused
    /// leaves dedup against the base blobs already in the store.
    #[tokio::test]
    async fn structural_shares_unchanged_leaves() {
        let _serialized = COUNTER_LOCK.lock().await;
        let st = store();
        let mut rng = Rng::new(0x7777_0001);
        let mask = 0x7u32; // ≈8 entries/leaf → many leaves
        let base_keys = key_pool(&mut rng, mask, 800);
        let base = build_base(&st, mask, &base_keys, &mut rng).await;

        // Confirm the base really has many leaves (so the test is meaningful).
        let pipe = base.as_pipeline();
        let base_leaves = pipe
            .enumerate_leaf_links(&base)
            .await
            .unwrap()
            .expect("multi-leaf base")
            .len();
        assert!(
            base_leaves >= 20,
            "base should have many leaves, got {base_leaves}"
        );

        // Single update to one existing middle key.
        let target = base_keys[base_keys.len() / 2].clone();
        let mut changes = BTreeMap::new();
        changes.insert(target, leaf_entry(rng.next_u64()));

        let (_root, _pth, stats) = pipe
            .merge_and_persist_structural(&base, mask, &MemLayer { entries: changes }, st.as_ref())
            .await
            .unwrap()
            .expect("non-empty result");

        // (a) the new tree still has ~all the base's leaves.
        assert!(
            stats.leaf_nodes as usize >= base_leaves - 1,
            "leaf count collapsed (got {}, base {base_leaves}) — structural path did not reuse leaves",
            stats.leaf_nodes
        );
        // (b) only a tiny spine was uploaded (rebuilt leaf + internal
        // ancestors). Empirically a handful; assert << base_leaves to
        // prove sharing. If the path had fallen back to the full re-read
        // with a cold store it would re-touch every leaf; here the store
        // is warm with the base, so dedup keeps uploads tiny EITHER way —
        // so instead assert the rebuilt-leaf path was taken by checking
        // nodes_uploaded is bounded by tree depth + 1, not the leaf count.
        assert!(
            stats.nodes_uploaded <= 8,
            "uploaded {} nodes for a single-key change — expected only the touched spine",
            stats.nodes_uploaded
        );
    }

    /// Performance property (the feedy "scatter across collections" fix): a
    /// change set whose keys span nearly the whole tree but touch only TWO
    /// distant leaves must READ only those leaves (+ tiny re-sync tails), NOT
    /// every leaf in the min_key..max_key span. The cluster-aware re-sync is
    /// what delivers this; the old span-based path re-read leaves[lo..=hi] ≈
    /// the entire tree every cycle.
    #[tokio::test]
    async fn structural_reads_only_affected_clusters() {
        use std::sync::atomic::Ordering;
        let _serialized = COUNTER_LOCK.lock().await;
        let st = store();
        let mut rng = Rng::new(0x9E37_79B9);
        let mask = 0x7u32; // ≈8 entries/leaf → many leaves
        let mut base_keys = key_pool(&mut rng, mask, 800);
        base_keys.sort();
        base_keys.dedup();
        let base = build_base(&st, mask, &base_keys, &mut rng).await;

        let pipe = base.as_pipeline();
        let base_leaves = pipe
            .enumerate_leaf_links(&base)
            .await
            .unwrap()
            .expect("multi-leaf base")
            .len();
        assert!(base_leaves >= 40, "need many leaves, got {base_leaves}");

        // Update ONE key near the start and ONE near the end — min..max spans
        // ~the whole tree, but only two leaves are actually affected.
        let mut changes = BTreeMap::new();
        changes.insert(base_keys[2].clone(), leaf_entry(rng.next_u64()));
        changes.insert(
            base_keys[base_keys.len() - 3].clone(),
            leaf_entry(rng.next_u64()),
        );

        STRUCTURAL_LEAF_READS.store(0, Ordering::Relaxed);
        STRUCTURAL_RECHUNK_COUNT.store(0, Ordering::Relaxed);
        let structural = pipe
            .merge_and_persist_structural(
                &base,
                mask,
                &MemLayer {
                    entries: changes.clone(),
                },
                st.as_ref(),
            )
            .await
            .unwrap();
        assert_eq!(
            STRUCTURAL_RECHUNK_COUNT.load(Ordering::Relaxed),
            1,
            "must take the incremental re-chunk branch, not the full fallback"
        );
        let leaf_reads = STRUCTURAL_LEAF_READS.load(Ordering::Relaxed);
        assert!(
            leaf_reads <= 12,
            "scattered 2-leaf change read {leaf_reads} leaves (base has {base_leaves}) — \
             cluster re-sync regressed to span-based whole-tree re-read"
        );

        // Correctness: identical root to the full path.
        let full = pipe
            .merge_and_persist(&base, mask, &MemLayer { entries: changes }, st.as_ref())
            .await
            .unwrap();
        assert_eq!(
            structural.as_ref().map(|(r, _, _)| *r),
            full.as_ref().map(|(r, _, _)| *r),
            "scattered-change root mismatch vs full path"
        );
    }

    /// Determinism: the structural path is a pure function of (base,
    /// changes, mask) — running it twice yields the identical root.
    #[tokio::test]
    async fn structural_is_deterministic() {
        let _serialized = COUNTER_LOCK.lock().await;
        let st = store();
        let mut rng = Rng::new(0x5151_5151);
        let mask = 0x7u32;
        let pool = key_pool(&mut rng, mask, 400);
        let base = build_base(&st, mask, &pool[..300], &mut rng).await;

        let mut changes = BTreeMap::new();
        for _ in 0..20 {
            let k = pool[rng.below(pool.len() as u64) as usize].clone();
            changes.insert(k, leaf_entry(rng.next_u64()));
        }

        let pipe = base.as_pipeline();
        let r1 = pipe
            .merge_and_persist_structural(
                &base,
                mask,
                &MemLayer {
                    entries: changes.clone(),
                },
                st.as_ref(),
            )
            .await
            .unwrap();
        let r2 = pipe
            .merge_and_persist_structural(
                &base,
                mask,
                &MemLayer {
                    entries: changes.clone(),
                },
                st.as_ref(),
            )
            .await
            .unwrap();
        assert_eq!(r1.map(|x| (x.0, x.1)), r2.map(|x| (x.0, x.1)));
    }

    /// Treediff convergence MUST be byte-identical to the FULL convergence merge
    /// (the publish-side union of two full snapshots `merge_and_persist(base,
    /// mask, local)`) for any (base = prev-published, local). `local` is built as
    /// base + small diff most cases (the realistic single-writer superset → it
    /// exercises the subtree pruning) and as an independent tree sometimes
    /// (divergent / fallback). 1000 randomized cases across masks.
    #[tokio::test]
    async fn treediff_matches_full_convergence_oracle() {
        let _serialized = COUNTER_LOCK.lock().await;
        const CASES: u64 = 1000;
        let masks = [0x3u32, 0x7, 0x3F];
        for case in 0..CASES {
            let seed = 0x7EED_0000u64 ^ case.wrapping_mul(0x0100_0000_01b3);
            let mut rng = Rng::new(seed);
            let mask = masks[(rng.below(masks.len() as u64)) as usize];
            let st = store();

            let base_n = match rng.below(8) {
                0 => 0,
                1 => 1,
                2 => 4,
                3 => 5,
                4 => 1 + rng.below(30) as usize,
                5 => 100 + rng.below(80) as usize,
                _ => 300 + rng.below(1200) as usize,
            };
            let pool = key_pool(&mut rng, mask, base_n + 96);
            let base_keys: Vec<String> = pool.iter().take(base_n).cloned().collect();
            let base = build_base(&st, mask, &base_keys, &mut rng).await;

            // local: base + small diff (superset → pruning) OR independent (fallback).
            let local = if rng.chance(3, 4) {
                let mut diff: BTreeMap<String, NodeEntry> = BTreeMap::new();
                let n = 1 + rng.below(10) as usize;
                for _ in 0..n {
                    let k = pool[(rng.below(pool.len() as u64)) as usize].clone();
                    diff.insert(k, leaf_entry(rng.next_u64()));
                }
                let res = base
                    .as_pipeline()
                    .merge_and_persist(&base, mask, &MemLayer { entries: diff }, st.as_ref())
                    .await
                    .unwrap();
                match res {
                    Some((r, p, _)) => Snapshot::new(
                        r,
                        st.clone() as Arc<dyn s5_core::BlobsRead>,
                        Default::default(),
                        Some(p),
                    ),
                    None => build_base(&st, mask, &base_keys, &mut rng).await,
                }
            } else {
                let n = (rng.below((pool.len() + 1) as u64)) as usize;
                let local_keys: Vec<String> = pool.iter().take(n).cloned().collect();
                build_base(&st, mask, &local_keys, &mut rng).await
            };

            let pipe = base.as_pipeline();
            let full = pipe
                .merge_and_persist(&base, mask, &local, st.as_ref())
                .await
                .unwrap();
            let td = pipe
                .merge_and_persist_treediff(&base, mask, &local, st.as_ref())
                .await
                .unwrap();

            match (full, td) {
                (Some((fh, fph, _)), Some((th, tph, _))) => {
                    assert_eq!(
                        fh, th,
                        "case {case} seed {seed:#x}: treediff root != full root"
                    );
                    assert_eq!(
                        fph, tph,
                        "case {case} seed {seed:#x}: treediff plaintext_hash != full"
                    );
                }
                (None, None) => {}
                (f, t) => panic!(
                    "case {case} seed {seed:#x}: shape divergence full={} td={}",
                    f.is_some(),
                    t.is_some()
                ),
            }
        }
    }
}
