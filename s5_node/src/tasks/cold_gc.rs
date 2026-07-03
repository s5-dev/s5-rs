//! Periodic cold-store garbage collection.
//!
//! The publisher's blob store grows without bound: every persist cycle
//! re-blobs the growing active ledger/rindex/interner chunks (orphaning the
//! prior blob) and appends a Transparent-Node history entry. Nothing ever
//! reclaims the orphans. This task is the reclaimer.
//!
//! ## Reachability (deletion correctness is the whole game)
//!
//! A blob is *live* iff it is reachable from the **current published
//! snapshot** or is one of the **retained published-TN blobs**:
//!
//! ```text
//! reachable = walk_hashes(snapshot(published_TN))      // content + chunks + tree nodes
//!           ∪ { published_TN blob hash }               // the current TN itself
//!           ∪ { history-entry hashes of published_TN }  // retained prior TNs (bounded by tn_history_keep)
//! ```
//!
//! `Snapshot::walk_hashes` is the *reader's own* traversal — it descends
//! `Structural::Link` file-content nodes into their ByteStream chunk trees,
//! so it reaches every 8/64-MiB chunk leaf, not just file roots. This is the
//! only correct reachability for a chunked vault: the older
//! `s5_fs::gc::collect_fs_reachable_hashes` is both chunk-blind and fail-open
//! on an absent plaintext root, and must NOT be used here.
//!
//! We read the *published* TN (plaintext under `plaintext_published_tn`) via
//! the registry, so no age identity is needed. Reads run against the tiered
//! store (hot+cold); deletion is restricted to the **cold** backend.
//!
//! ## Safety
//!
//! - Fail-closed: if nothing is published yet, or the vault_id can't be
//!   resolved, or the reachable set comes back empty, the pass deletes
//!   nothing.
//! - mtime grace gate: a candidate is deleted only once its blob mtime is
//!   older than `min_age`. A blob written by a revision published *during*
//!   the slow scan is far younger than the grace period, so the
//!   live-publisher race is structurally impossible. A blob whose mtime
//!   cannot be established (non-filesystem store) is treated as young and
//!   protected.
//! - Pins are always honored (delegated to `gc_store`).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures_util::StreamExt;
use s5_core::blob::{BlobStore, Blobs};
use s5_core::{BlobsRead, Hash, Pins, RegistryApi, StreamKey};
use tokio::sync::RwLock;

/// Summary of one cold-GC pass. Defined here (not pulled from `s5_fs`,
/// which is mid four-key migration and not part of downstream ingest
/// builds) so the s5_node↔consumer seam stays minimal. The consumer-side
/// [`GcReporter`] impl reads these fields to emit metrics.
#[derive(Debug, Default)]
pub struct GcReport {
    /// Total blobs examined in the cold store.
    pub total: usize,
    /// Kept because they have at least one pin.
    pub kept_by_pins: usize,
    /// Kept because they are reachable from the published snapshot / TN chain.
    pub kept_by_reachability: usize,
    /// Unreachable + unpinned + old enough — deleted (or, in dry-run, would be).
    pub candidates: Vec<Hash>,
    /// Of the candidates, those actually deleted (0 in dry-run).
    pub deleted: usize,
    /// Bytes reclaimed by deletions (0 in dry-run).
    pub bytes_reclaimed: u64,
    /// Bytes across every eligible+protected candidate (the reclaim estimate
    /// a dry run reports before flipping to live).
    pub bytes_candidate: u64,
    /// Unreachable + unpinned but younger than `min_age` — protected by the
    /// grace gate this pass.
    pub aged_out_protected: usize,
    /// Per-blob delete failures (non-fatal).
    pub delete_errors: Vec<(Hash, anyhow::Error)>,
}

use crate::membership::MembershipState;
use crate::tasks::publish::fetch_previous_published_node;
use crate::tasks::vault_persist::node_to_snapshot_parts;

/// Sink for cold-GC pass results. Implemented by the embedding ingest
/// deployment to emit metrics; the s5 layer stays free of any metrics
/// crate. The injection seam mirrors `gc_store`'s
/// `pins: &dyn Pins` — the detached task calls `reporter.report(..)` with
/// zero Prometheus knowledge.
pub trait GcReporter: Send + Sync {
    fn report(&self, report: &GcReport);
}

/// Everything one vault's cold-GC task needs. Built once at `run_node`
/// startup for each vault with `gc_enabled = true`.
pub struct ColdGcParams {
    /// Vault name (for `vault_id` resolution + logging).
    pub vault_name: String,
    /// The node's registry — where the current published TN lives.
    pub registry: Arc<dyn RegistryApi + Send + Sync>,
    /// Tiered store (hot+cold) used to read the published TN and walk
    /// content/tree/chunk nodes. Reads only — the capability view, not a
    /// path store (D15).
    pub tiered_store: Arc<dyn Blobs>,
    /// The cold-tier backend — the *only* place this task deletes. This is
    /// the explicit path-`BlobStore` view (D15): the mtime grace gate needs
    /// `modified`, a genuine path-store capability.
    pub cold_store: BlobStore,
    /// Pin registry; pinned blobs are never reclaimed.
    pub pins: Arc<dyn Pins>,
    /// Shared membership state; `vault_id_by_name` is populated after the
    /// first publish and gives us the vault_id for the stream key.
    pub membership: Arc<RwLock<MembershipState>>,
    /// This device's signing pubkey — the publisher half of the
    /// `StreamKey::Vault { pubkey, vault_id }` lookup.
    pub self_pubkey: [u8; 32],
    /// Age identity files for decrypting the published TN. Empty for
    /// `plaintext_published_tn` vaults; the fetch then parses the
    /// plaintext TN directly.
    pub identity_files: Vec<String>,
    /// Interval between passes.
    pub interval: Duration,
    /// Minimum blob mtime age before a candidate is eligible for deletion.
    pub min_age: Duration,
    /// When true, compute + report candidates but delete nothing.
    pub dry_run: bool,
    /// Optional metrics sink.
    pub reporter: Option<Arc<dyn GcReporter>>,
}

/// Spawn the detached periodic GC task. Runs a first pass shortly after
/// boot (early dry-run signal), then every `interval`.
pub fn spawn_cold_gc(params: ColdGcParams) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tracing::info!(
            vault = %params.vault_name,
            interval_secs = params.interval.as_secs(),
            min_age_secs = params.min_age.as_secs(),
            dry_run = params.dry_run,
            "cold-GC task started"
        );
        // Small initial delay so the first publish has a chance to land
        // (otherwise the first pass just no-ops on "nothing published").
        tokio::time::sleep(Duration::from_secs(120)).await;
        loop {
            match run_gc_pass(&params).await {
                Ok(Some(report)) => {
                    if let Some(reporter) = &params.reporter {
                        reporter.report(&report);
                    }
                }
                Ok(None) => {
                    tracing::info!(
                        vault = %params.vault_name,
                        "cold-GC pass skipped (nothing published / vault_id unresolved / empty reachable) — no deletions"
                    );
                }
                Err(e) => {
                    tracing::warn!(vault = %params.vault_name, error = %e, "cold-GC pass failed — no deletions this cycle");
                }
            }
            tokio::time::sleep(params.interval).await;
        }
    })
}

/// Resolve this vault's `vault_id` from the membership table (populated
/// after the first publish). `None` until then.
async fn resolve_vault_id(params: &ColdGcParams) -> Option<[u8; 16]> {
    let state = params.membership.read().await;
    state
        .vault_id_by_name
        .iter()
        .find(|(_, name)| name.as_str() == params.vault_name)
        .map(|(id, _)| *id)
}

/// Compute the live reachable set from the current published Transparent
/// Node. Returns `None` when nothing is published yet (caller skips the
/// pass — never delete when reachability can't be established).
async fn collect_published_reachable(
    params: &ColdGcParams,
    vault_id: [u8; 16],
) -> anyhow::Result<Option<HashSet<Hash>>> {
    let stream_key = StreamKey::Vault {
        pubkey: params.self_pubkey,
        vault_id,
    };

    let Some((node, tn_hash, _revision)) = fetch_previous_published_node(
        params.registry.as_ref(),
        params.tiered_store.as_ref(),
        &stream_key,
        &params.identity_files,
    )
    .await?
    else {
        return Ok(None);
    };

    let mut reachable: HashSet<Hash> = HashSet::new();

    // The current published TN blob itself, and every retained history
    // entry's prior-TN blob. (`tn_history_keep` bounds how many of these
    // exist; without it this set grows forever.)
    reachable.insert(tn_hash);
    for (key, entry) in &node.entries {
        if key.is_empty() {
            continue; // the "" current-snapshot entry → covered by walk below
        }
        if let Some(content) = &entry.content {
            reachable.insert(Hash::from(content.hash));
        }
    }

    // Every content/chunk/tree blob reachable from the published snapshot
    // root. `walk_hashes` is the chunk-aware reader traversal.
    let (root, root_plaintext_hash, ctx) = node_to_snapshot_parts(&node)?;
    let read_store: Arc<dyn BlobsRead> = params.tiered_store.clone();
    let snapshot = s5_fs_v2::snapshot::Snapshot::new(root, read_store, ctx, root_plaintext_hash);

    let mut hashes = std::pin::pin!(snapshot.walk_hashes());
    while let Some(h) = hashes.next().await {
        reachable.insert(h?);
    }

    Ok(Some(reachable))
}

/// One GC pass. `Ok(None)` => skipped (no deletions); `Ok(Some(report))`
/// => completed (report describes candidates / deletions).
async fn run_gc_pass(params: &ColdGcParams) -> anyhow::Result<Option<GcReport>> {
    let Some(vault_id) = resolve_vault_id(params).await else {
        return Ok(None);
    };
    let Some(reachable) = collect_published_reachable(params, vault_id).await? else {
        return Ok(None);
    };

    // Fail-closed: an empty reachable set means we failed to establish
    // liveness — never proceed (that path would mark the whole store
    // garbage). `walk_hashes` always yields ≥ the root, and we inserted the
    // TN hash, so empty here can only mean something went wrong.
    if reachable.is_empty() {
        tracing::warn!(vault = %params.vault_name, "cold-GC: reachable set empty — aborting pass (no deletions)");
        return Ok(None);
    }

    let report = sweep_cold(
        &params.cold_store,
        &reachable,
        params.pins.as_ref(),
        params.min_age,
        params.dry_run,
    )
    .await?;

    tracing::info!(
        vault = %params.vault_name,
        dry_run = params.dry_run,
        total = report.total,
        kept_pins = report.kept_by_pins,
        kept_reach = report.kept_by_reachability,
        reachable = reachable.len(),
        eligible_candidates = report.candidates.len(),
        aged_out_protected = report.aged_out_protected,
        deleted = report.deleted,
        bytes_reclaimed = report.bytes_reclaimed,
        bytes_candidate = report.bytes_candidate,
        errors = report.delete_errors.len(),
        "cold-GC pass complete"
    );

    Ok(Some(report))
}

/// The safety-critical sweep: classify every blob in `cold_store` against
/// the `reachable` set, the `pins` registry, and the mtime grace gate, and
/// delete only those that are unreachable **and** unpinned **and** older
/// than `min_age`. Pure of any reachability / registry / snapshot concerns
/// so it can be unit-tested directly.
async fn sweep_cold(
    cold_store: &BlobStore,
    reachable: &HashSet<Hash>,
    pins: &dyn Pins,
    min_age: Duration,
    dry_run: bool,
) -> anyhow::Result<GcReport> {
    let mut report = GcReport::default();
    let all_hashes = cold_store.list_hashes().await?;
    report.total = all_hashes.len();
    let now = SystemTime::now();

    for h in all_hashes {
        // Pinned blobs are never reclaimed.
        if !pins.get_pinners(h).await?.is_empty() {
            report.kept_by_pins += 1;
            continue;
        }
        // Reachable from the live snapshot / retained TN chain.
        if reachable.contains(&h) {
            report.kept_by_reachability += 1;
            continue;
        }

        // Unreachable + unpinned → garbage candidate, subject to the age gate.
        let size = cold_store.size(h).await.unwrap_or(0);
        report.bytes_candidate += size;

        if !old_enough(cold_store.modified(h).await.ok().flatten(), now, min_age) {
            report.aged_out_protected += 1;
            continue;
        }

        report.candidates.push(h);
        if dry_run {
            continue;
        }
        match cold_store.delete(h).await {
            Ok(()) => {
                report.deleted += 1;
                report.bytes_reclaimed += size;
            }
            Err(e) => report.delete_errors.push((h, e)),
        }
    }

    Ok(report)
}

/// mtime grace gate. A blob is old enough to reclaim only if its mtime is
/// known **and** at least `min_age` in the past. Unknown mtime (non-fs
/// store), a future mtime (clock skew), or no mtime at all → protected.
fn old_enough(mtime: Option<SystemTime>, now: SystemTime, min_age: Duration) -> bool {
    match mtime {
        Some(t) => now
            .duration_since(t)
            .map(|age| age >= min_age)
            .unwrap_or(false),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s5_core::blob::BlobStore;
    use s5_core::{PinContext, RegistryApi, RegistryPinner};
    use s5_registry::MemoryRegistry;
    use s5_store_local::{LocalStore, LocalStoreConfig};
    use std::time::Duration;

    /// Build a `BlobStore` over a fresh on-disk `LocalStore` (we need real
    /// mtimes for the grace gate; `MemoryStore` reports none).
    fn local_blobstore(dir: &std::path::Path) -> BlobStore {
        BlobStore::new(LocalStore::create(LocalStoreConfig {
            base_path: dir.to_string_lossy().into_owned(),
        }))
    }

    fn fresh_pinner() -> RegistryPinner<Arc<dyn RegistryApi + Send + Sync>> {
        let reg: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
        RegistryPinner::new(reg)
    }

    /// Age a blob file on disk by back-dating its mtime `secs` into the past.
    fn backdate(store_dir: &std::path::Path, store: &BlobStore, hash: Hash, secs: u64) {
        let path = store_dir.join(store.blob_path_for_hash(hash));
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let when = filetime::FileTime::from_unix_time(now - secs as i64, 0);
        filetime::set_file_mtime(&path, when).unwrap();
    }

    #[tokio::test]
    async fn sweep_keeps_reachable_pinned_and_young_deletes_only_old_garbage() {
        let tmp = tempfile::tempdir().unwrap();
        let store = local_blobstore(tmp.path());

        // Four blobs: reachable, pinned, unreachable-young, unreachable-old.
        let reachable_h = store
            .import_bytes(b"reachable".to_vec().into())
            .await
            .unwrap()
            .hash;
        let pinned_h = store
            .import_bytes(b"pinned".to_vec().into())
            .await
            .unwrap()
            .hash;
        let young_h = store
            .import_bytes(b"young-garbage".to_vec().into())
            .await
            .unwrap()
            .hash;
        let old_h = store
            .import_bytes(b"old-garbage".to_vec().into())
            .await
            .unwrap()
            .hash;

        // Age the "old" one well past the grace period; leave others fresh.
        backdate(tmp.path(), &store, old_h, 30 * 24 * 3600);

        let mut reachable = HashSet::new();
        reachable.insert(reachable_h);
        let pins = fresh_pinner();
        pins.pin_hash(pinned_h, PinContext::NodeId([0u8; 32]))
            .await
            .unwrap();
        let min_age = Duration::from_secs(7 * 24 * 3600);

        // Dry run: nothing deleted, but the old garbage is the sole candidate.
        let dry = sweep_cold(&store, &reachable, &pins, min_age, true)
            .await
            .unwrap();
        assert_eq!(dry.deleted, 0);
        assert_eq!(dry.candidates, vec![old_h]);
        assert_eq!(dry.kept_by_reachability, 1);
        assert_eq!(dry.kept_by_pins, 1);
        assert_eq!(dry.aged_out_protected, 1); // the young garbage
        assert!(
            store.contains(old_h).await.unwrap(),
            "dry run must not delete"
        );

        // Live run: only the old garbage is deleted; the other three survive.
        let live = sweep_cold(&store, &reachable, &pins, min_age, false)
            .await
            .unwrap();
        assert_eq!(live.deleted, 1);
        assert!(!store.contains(old_h).await.unwrap());
        assert!(store.contains(reachable_h).await.unwrap());
        assert!(store.contains(pinned_h).await.unwrap());
        assert!(store.contains(young_h).await.unwrap());
    }

    #[tokio::test]
    async fn empty_reachable_with_all_young_deletes_nothing() {
        // Mirrors the catastrophic case: even if reachability came back
        // empty, the grace gate protects every fresh blob. (run_gc_pass also
        // refuses to proceed on an empty reachable set; this is defense in
        // depth at the sweep layer.)
        let tmp = tempfile::tempdir().unwrap();
        let store = local_blobstore(tmp.path());
        for i in 0..5u8 {
            store.import_bytes(vec![i; 64].into()).await.unwrap();
        }
        let reachable = HashSet::new();
        let pins = fresh_pinner();
        let report = sweep_cold(
            &store,
            &reachable,
            &pins,
            Duration::from_secs(7 * 24 * 3600),
            false,
        )
        .await
        .unwrap();
        assert_eq!(report.deleted, 0);
        assert_eq!(report.aged_out_protected, 5);
    }

    #[test]
    fn old_enough_is_fail_safe() {
        let now = SystemTime::now();
        let min = Duration::from_secs(100);
        // Unknown mtime → protected.
        assert!(!old_enough(None, now, min));
        // Young → protected.
        assert!(!old_enough(Some(now - Duration::from_secs(10)), now, min));
        // Future mtime (clock skew) → protected.
        assert!(!old_enough(Some(now + Duration::from_secs(10)), now, min));
        // Old → eligible.
        assert!(old_enough(Some(now - Duration::from_secs(200)), now, min));
    }
}
