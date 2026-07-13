//! `PackingStore` — bundle many small content-addressed blobs into larger
//! packs before they hit a slow or expensive backend.
//!
//! ## Content-addressed packing
//!
//! `PackingStore` is a [`BlobsReadWrite`] + [`BlobsDelete`] provider keyed by
//! the blob's own `Hash` — not a path-based `Store`. There is no notion of
//! arbitrary string paths: a blob is addressed by its BLAKE3 hash, and the
//! in-pack index key is that hash's 12-byte prefix (`hash[..12]`). Uploads hash
//! their own input (`blob_upload_bytes` = `Hash::new`), so the store is
//! self-verifying by construction; staging files live under `hex(hash)`.
//!
//! The pack *bodies* themselves go to a content-addressed `B: BlobsReadWrite`
//! backend (e.g. a `BlobStore` over indexd/Sia) — that's where the pack hash is
//! derived and verified. The `B` backend stores whole 40–256 MiB packs; this
//! layer is what turns a stream of tiny blobs into those packs.
//!
//! ## Wire format
//!
//! See `manifest.rs`. Custom little-endian binary (not CBOR). Each pack body is
//! **self-describing**: `header ++ data`, where the **prepended** header is the
//! member table `(hash_prefix[12], absolute offset: u32)` (sorted ascending by
//! key) plus an `end_offset`, behind an `S5.pro` magic and zero-padded to a
//! 16 KiB data boundary. The header is at the front, so a reader resolves a
//! member with ranged GETs from offset 0 — no pack-size lookup. So a pack
//! rebuilds its own index from its head
//! ([`PackingStore::reconstruct_from_headers`]) with no durable manifest object.
//! The `index_cache` store is therefore a *local cache* of that index (per-pack
//! manifests under `manifests/<hex(pack_hash)>`, plus a consolidated bulk-index
//! snapshot) for fast warm restarts — not a durable dependency that could be lost
//! while the body survives.
//!
//! ## Deletion / GC
//!
//! Not implemented in this revision (spec §10.7 deferred). The
//! `blob_delete` impl returns `NotSupported`. The in-memory index
//! is build-then-grow; there is no reclamation path yet.

mod binpack;
mod manifest;

use manifest::{FIXED_HEADER_LEN, decode_header, encode_header, header_region_len, parse_count};
pub use manifest::{HASH_PREFIX_LEN, PackHeader, PackMember};

use bytes::Bytes;
use futures::stream::{self, Stream, StreamExt};
use s5_core::Hash;
use s5_core::blob::{
    BlobId, BlobResult, BlobsDelete, BlobsRead, BlobsReadWrite, BlobsWrite, StagingStats,
};
use s5_core::store::{Store, StoreResult};
use serde::{Deserialize, Serialize};

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::AsyncRead;
use tokio::sync::{Mutex, Notify, RwLock};

use crate::binpack::{PackGroup, PendingBlob, first_fit};

/// Configuration knobs.
#[derive(Clone, Debug)]
pub struct PackingConfig {
    /// Don't flush automatically until at least this many bytes are
    /// staged. Default 40 MiB.
    pub min_group_size: u64,
    /// Hard cap on a single pack's size. Default 256 MiB.
    pub max_group_size: u64,
    /// Slab unit for the waste-tolerance heuristic in `tryAdd`.
    /// Default 4 MiB (typical Sia slab size).
    pub slab_size: u64,
    /// Once a group's waste fraction drops below this, only entries
    /// that reduce waste or fit the tail are accepted. Default 0.10.
    pub waste_pct: f64,
    /// Background packing loop tick interval. Default 60 s.
    pub interval: Duration,
    /// `true` → `put_bytes` force-flushes inline (high latency,
    /// immediately durable). `false` → returns as soon as the bytes
    /// are durably staged.
    pub flush_on_put: bool,
    /// Backpressure bound on the staging spool, expressed as a multiple of
    /// `max_group_size` (i.e. "how many packs' worth may sit un-uploaded").
    /// `blob_upload_bytes` blocks once staging reaches `staging_max_packs *
    /// max_group_size`, so a fast ingest over a slow backend self-throttles to
    /// the upload rate instead of filling the disk (see `blob_upload_bytes`).
    /// Requires a running flush loop or `flush_on_put` to drain. Default 4
    /// (≈ 1 GiB at the 256 MiB default cap). `0` disables the bound.
    pub staging_max_packs: usize,
    // Index durability is no longer a backend concern: the durable index is the
    // self-describing prepended header in each pack body (see `manifest.rs` +
    // `Self::reconstruct_from_headers`). The `index_cache` store is just a *local
    // cache* of that index for fast warm restarts, so it carries no EC /
    // redundancy trade-off and is rebuildable from the bodies if lost.
    /// Key prefix for cached per-pack manifests in the index-cache store. Kept as
    /// `manifests/` for on-disk continuity; the sibling `manifests.todo/` holds
    /// pending-pack markers and `index-snapshot.v1` the serialized bulk index.
    pub index_cache_prefix: String,
    /// Flush a pending group once its oldest member has been staged this long,
    /// even below `min_group_size`. Without this, a sub-minimum tail (e.g. the
    /// handful of KB behind an identity publish) has NO background durability
    /// path at all — its only exit is piggybacking a later `blob_sync` barrier,
    /// which is exactly what bricked the 2026-07-02 recovery drill.
    /// Default 90 s; bounds the staged-only exposure of every writer to
    /// ~`max_pending_age + interval`.
    pub max_pending_age: Duration,
    /// Floor for the outer per-pack upload timeout: a flush waits at most
    /// `max(upload_timeout_floor, total_size / 1 MiB/s)` for the backend before
    /// giving up on the cycle. The staged entries survive (that is what the WAL
    /// is for) and the next tick retries. Without this a single wedged upload
    /// stalls the store forever, silently.
    ///
    /// Default 20 min. This must be generous: it is a whole-upload deadline, and
    /// the backend gives us no mid-upload progress signal, so too tight a bound
    /// kills a big backup that is still making slow progress on a poor link
    /// (the reviewer's "a slow connection can't finish a big backup" case). A
    /// healthy link finishes a 240 MiB pack in well under a minute, so a large
    /// floor never slows it down — the deadline only bites a genuinely wedged or
    /// crawling upload, which the WAL + next-tick retry then re-drives.
    pub upload_timeout_floor: Duration,
}

impl Default for PackingConfig {
    fn default() -> Self {
        Self {
            min_group_size: 40 * 1024 * 1024,
            max_group_size: 256 * 1024 * 1024,
            slab_size: 4 * 1024 * 1024,
            waste_pct: 0.10,
            interval: Duration::from_secs(60),
            flush_on_put: false,
            staging_max_packs: 4,
            index_cache_prefix: "manifests/".to_string(),
            max_pending_age: Duration::from_secs(90),
            upload_timeout_floor: Duration::from_secs(1200),
        }
    }
}

type Key = [u8; HASH_PREFIX_LEN];

/// Warn about a stalled upload pipeline once staging has sat at the
/// watermark with no successful flush for this long.
const STALL_WARN_AFTER: Duration = Duration::from_secs(5 * 60);

/// `gamma` for the BBHash build: higher = a bit larger but faster/cheaper to
/// build. 1.7 is the boomphf-recommended default (~3 bits/key).
const BULK_GAMMA: f64 = 1.7;

/// Fold `recent` into the bulk MPHF once it holds this many keys. Bounds the
/// `recent` HashMap (the un-MPHF'd tail) so its per-key RAM cost stays small.
const RECENT_REBUILD_KEYS: usize = 200_000;

/// The compact bulk index: a minimal perfect hash `key → slot` plus a slot-order
/// `pack index` array. ~3 bits/key for the MPHF + 4 B/key for the array — vs a
/// `HashMap`'s ~50 B/key. There is **no** separate verify-key array: the MPHF
/// yields only a *candidate* pack, and membership is confirmed by binary-searching
/// that pack's own sorted member table (which a read does anyway). A non-member
/// routes to an arbitrary pack whose search then misses, so the candidate is
/// self-verifying via `locate`. The MPHF is *static* (rebuilt to add keys), so
/// newly-flushed packs live in `Index::recent` until the next
/// [`PackingStore::rebuild_index`] folds them in.
struct Bulk {
    mphf: boomphf::Mphf<Key>,
    /// `slot → pack index` (into `Index::packs`).
    pack_idx: Vec<u32>,
}

impl std::fmt::Debug for Bulk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bulk")
            .field("keys", &self.pack_idx.len())
            .finish_non_exhaustive()
    }
}

#[derive(Default, Debug)]
struct Index {
    /// Parsed pack headers, in load/flush order; `bulk`/`recent` index into this.
    //
    // TODO(RAM/lazy-index): the member tables held here (~16 B/member) dominate
    // resident RAM at scale (~256 MB at 16M blobs vs ~70 MB for `bulk` alone).
    // Keep only `bulk` (MPHF + `pack_idx` → candidate pack) resident and read a
    // candidate pack's member table from the on-disk manifest cache on demand
    // (it is already persisted there), with an LRU over hot packs. ~5× less
    // resident RAM for one local read per cold lookup. Pairs with the tiered/
    // incremental MPHF in packing-store.md §8b/§9.
    packs: Vec<PackHeader>,
    /// Compact MPHF over `packs[..bulk_packs]`. `None` until first built.
    bulk: Option<Bulk>,
    /// Number of leading `packs` covered by `bulk`; the rest are in `recent`.
    bulk_packs: usize,
    /// Keys of `packs[bulk_packs..]` not yet folded into `bulk` → their pack
    /// index. Small (bounded by [`RECENT_REBUILD_KEYS`]).
    recent: HashMap<Key, u32>,
    /// Entries still in staging (not yet packed).
    pending: HashMap<Key, PendingBlob>,
    /// Pack hashes we know exist but whose headers haven't been read yet
    /// (the "todo" set). Populated by [`PackingStore::note_pack_hashes`] and
    /// drained by [`PackingStore::enrich`]. While this is non-empty the store
    /// is not yet *honest about negatives*: a lookup miss might be a blob living
    /// in one of these un-enriched packs, so a negative answer must enrich first.
    todo: HashSet<[u8; 32]>,
}

impl Index {
    /// Add a flushed/recovered pack: its members go into `recent` (the MPHF is
    /// static — they fold into `bulk` on the next rebuild).
    fn add_pack(&mut self, header: PackHeader) {
        let pack_idx = self.packs.len() as u32;
        for m in &header.members {
            self.recent.insert(m.hash_prefix, pack_idx);
        }
        self.packs.push(header);
    }

    fn contains(&self, key: &Key) -> bool {
        self.pending.contains_key(key) || self.locate(key).is_some()
    }

    /// Locate a packed entry by its 12-byte key, returning
    /// `(pack_hash, absolute_offset_in_pack, length)`. Tries the bulk MPHF's
    /// *candidate* pack first, confirming membership by binary-searching that
    /// pack's sorted member table (there is no separate verify array); then the
    /// `recent` tail (an exact map). A non-member's candidate search misses and
    /// falls through to `None`. The offset is absolute (stored directly — no
    /// cumulative sum); the length comes from the next member's offset (or
    /// `end_offset`).
    fn locate(&self, key: &Key) -> Option<(Hash, u32, u32)> {
        // Bulk candidate: the MPHF maps any key to *some* slot, so the pack's
        // own binary search is what actually verifies membership.
        if let Some(bulk) = &self.bulk
            && let Some(slot) = bulk.mphf.try_hash(key)
        {
            let header = &self.packs[bulk.pack_idx[slot as usize] as usize];
            if let Some((offset, length)) = header.locate(key) {
                return Some((header.pack_hash, offset, length));
            }
        }
        // Recent tail: an exact map, so a hit is a real member.
        let header = &self.packs[*self.recent.get(key)? as usize];
        let (offset, length) = header.locate(key)?;
        Some((header.pack_hash, offset, length))
    }
}

/// Build the bulk MPHF + slot arrays from `(key, pack_idx)` pairs (off the async
/// runtime — boomphf build is CPU-bound).
///
/// **Dedups its input.** `boomphf::Mphf::new` PANICS on duplicate keys (it
/// retries a bounded number of times, then aborts), and a key CAN legitimately
/// appear in two packs — two devices packing the same content-addressed blob
/// before they sync, or a pack re-enriched after a stale `todo` marker. A panic
/// here would render the whole store un-openable, so we fold to unique keys.
///
/// Dedup is by **sort-then-drop-adjacent**, not a `HashMap` over every key: at
/// 10M+ keys a hash table is a multi-hundred-MB transient allocation, whereas an
/// in-place sort reuses the pair buffer. Which pack index survives a shared key
/// doesn't matter — both packs hold identical bytes for it, so either serves the
/// read. This is the single choke point that keeps the invariant structurally
/// true regardless of how a duplicate arose.
fn build_bulk(keys: Vec<Key>, pack_of: Vec<u32>) -> Bulk {
    let mut pairs: Vec<(Key, u32)> = keys.into_iter().zip(pack_of).collect();
    pairs.sort_unstable_by_key(|(k, _)| *k);
    pairs.dedup_by(|a, b| a.0 == b.0); // keeps the first of each equal-key run
    let unique_keys: Vec<Key> = pairs.iter().map(|(k, _)| *k).collect();
    let mphf = boomphf::Mphf::new(BULK_GAMMA, &unique_keys);
    let mut pack_idx = vec![0u32; unique_keys.len()];
    for (k, pi) in &pairs {
        let slot = mphf.try_hash(k).expect("build key maps to a slot") as usize;
        pack_idx[slot] = *pi;
    }
    Bulk { mphf, pack_idx }
}

/// Reconstruct the bulk slot arrays from a persisted MPHF + the packs it covers,
/// WITHOUT a `Mphf::new` rebuild (the expensive part) — just one `try_hash` per
/// member. Returns `None` if any key fails to map into `[0, n)` (a corrupt /
/// mismatched snapshot), so the caller can fall back to a clean rebuild.
fn fill_bulk_from_mphf(mphf: boomphf::Mphf<Key>, packs: &[PackHeader]) -> Option<Bulk> {
    // Size `pack_idx` to the total member count. The MPHF spans only the UNIQUE
    // keys, so `try_hash` returns slots in `[0, unique)` ⊆ `[0, n)`; the few
    // trailing slots for a key that appears in two packs go unused (harmless).
    // Using the raw total avoids a transient `HashSet` over every key just to
    // learn the unique count — the `try_hash(..)?` (a miss ⇒ reject) and the
    // `slot >= n` guard already detect a snapshot whose MPHF doesn't match.
    let n: usize = packs.iter().map(|p| p.members.len()).sum();
    let mut pack_idx = vec![0u32; n];
    for (pi, pack) in packs.iter().enumerate() {
        for m in &pack.members {
            let slot = mphf.try_hash(&m.hash_prefix)? as usize;
            if slot >= n {
                return None;
            }
            pack_idx[slot] = pi as u32;
        }
    }
    Some(Bulk { mphf, pack_idx })
}

/// Bumped if the snapshot wire format changes; a mismatched blob is treated as
/// absent, so the index simply rebuilds from per-pack headers (the snapshot is a
/// pure cache, never a durability dependency).
const SNAPSHOT_VERSION: u8 = 1;

/// One pack in the persisted snapshot. `Hash` is not `serde`, so the hash rides
/// as raw bytes and members as `(prefix, offset)` pairs.
#[derive(Serialize, Deserialize)]
struct SnapshotPack {
    pack_hash: [u8; 32],
    members: Vec<(Key, u32)>,
    end_offset: u32,
}

impl SnapshotPack {
    fn into_header(self) -> PackHeader {
        PackHeader {
            pack_hash: Hash::from_bytes(self.pack_hash),
            members: self
                .members
                .into_iter()
                .map(|(hash_prefix, offset)| PackMember {
                    hash_prefix,
                    offset,
                })
                .collect(),
            end_offset: self.end_offset,
        }
    }
}

/// Borrowed view serialized to the cache store — avoids cloning the (large) MPHF
/// while writing. The slot arrays are NOT persisted; they're reconstructed via
/// [`fill_bulk_from_mphf`] on load (skips the costly `Mphf::new`).
#[derive(Serialize)]
struct SnapshotWrite<'a> {
    version: u8,
    packs: Vec<SnapshotPack>,
    mphf: &'a boomphf::Mphf<Key>,
}

/// Owned counterpart deserialized on load. Field order/types MUST match
/// [`SnapshotWrite`] (postcard is positional, not self-describing).
#[derive(Deserialize)]
struct SnapshotRead {
    version: u8,
    packs: Vec<SnapshotPack>,
    mphf: boomphf::Mphf<Key>,
}

/// Concurrent header reads per [`PackingStore::enrich`] pass.
const ENRICH_CONCURRENCY: usize = 16;

/// Outcome of probing one pending (todo) pack's front bytes.
enum EnrichOutcome {
    /// A valid self-describing pack — fold its header into the index.
    Enriched(PackHeader),
    /// Definitively NOT a pack (bad magic / version / decode) — drop the todo
    /// marker so `todo` can drain; never retried.
    NotAPack,
    /// The header couldn't be *read* (network / I/O) — keep the marker, retry on
    /// a later pass. Distinct from `NotAPack`: "couldn't read" ≠ "isn't a pack".
    Transient(anyhow::Error),
}

/// Three-store pack decorator. Only the pack-body backend `B` is generic (it's
/// content-addressed and on the hot read path); the local index cache and staging
/// are `Arc<dyn Store>` — always plain local stores, so the dyn dispatch is noise
/// next to their disk I/O and it keeps the type free of two extra params.
#[derive(Debug)]
pub struct PackingStore<B> {
    blobs: B,
    /// Local cache of the pack index (per-pack manifests + the bulk snapshot +
    /// `todo` markers). Rebuildable from the pack bodies; never load-bearing.
    index_cache: Arc<dyn Store>,
    /// Scratch store: blobs live here under `hex(hash)` until a pack fills.
    staging: Arc<dyn Store>,
    config: PackingConfig,
    state: RwLock<Index>,
    flush_notify: Notify,
    /// Serializes `pack_once`. Two concurrent flush cycles (e.g. the background
    /// tick and the publish `sync()` barrier) would otherwise snapshot the same
    /// `pending`, build overlapping packs, and race — the first deletes the
    /// staging files the second is still streaming ("staging read: No such
    /// file"). With streaming flush those reads happen *during* the long upload,
    /// so the window is wide; this lock closes it.
    flush_lock: Mutex<()>,
    /// Single-flights [`enrich`](Self::enrich): a storm of concurrent read-misses
    /// would otherwise each launch a full drain. The second caller waits, then
    /// finds `todo` already empty and returns cheaply.
    enrich_lock: Mutex<()>,
    /// Live byte count of blobs sitting in `staging` (not yet packed): the sum of
    /// `pending` lengths. Bumped in `blob_upload_bytes`, drained in `flush_group`
    /// as each pack uploads. Drives the staging backpressure watermark.
    staged_bytes: AtomicU64,
    /// Woken when `flush_group` frees staging space, so uploaders parked on the
    /// watermark can re-check and proceed.
    staging_drained: Notify,
    /// Diagnostic counters (cumulative over the store's lifetime): blobs skipped
    /// by the `blob_upload_bytes` dedup check vs. staged as new. Logged at each
    /// `blob_sync` so a snap's dedup behaviour is observable.
    dedup_hits: AtomicU64,
    dedup_misses: AtomicU64,
    /// When the last pack flush completed successfully (initialized to open
    /// time). Drives the stall warning in `run_upload_loop` and the
    /// [`flush_stats`](Self::flush_stats) gauge — during the 2026-07-02 drill a
    /// wedged upload froze the store for 15+ minutes with no signal anywhere.
    last_flush_ok: std::sync::Mutex<Instant>,
    /// `true` while a pack upload is in flight.
    flush_inflight: std::sync::atomic::AtomicBool,
}

/// Point-in-time flush gauges for status surfaces (`vup status`-style
/// honesty: staged-but-not-durable bytes are visible, not implied uploaded).
#[derive(Clone, Copy, Debug)]
pub struct FlushStats {
    /// Bytes sitting in the staging WAL, not yet inside a durable pack.
    pub staged_bytes: u64,
    /// How long ago the last pack flush completed successfully (or the store
    /// opened, if none has).
    pub since_last_flush_ok: Duration,
    /// A pack upload is currently in flight.
    pub inflight: bool,
}

impl<B> PackingStore<B>
where
    B: BlobsReadWrite + std::fmt::Debug + Send + Sync + 'static,
{
    pub async fn open(
        blobs: B,
        index_cache: Arc<dyn Store>,
        staging: Arc<dyn Store>,
        config: PackingConfig,
    ) -> StoreResult<Arc<Self>> {
        let store = Arc::new(Self {
            blobs,
            index_cache,
            staging,
            config,
            state: RwLock::new(Index::default()),
            flush_notify: Notify::new(),
            flush_lock: Mutex::new(()),
            enrich_lock: Mutex::new(()),
            staged_bytes: AtomicU64::new(0),
            staging_drained: Notify::new(),
            dedup_hits: AtomicU64::new(0),
            dedup_misses: AtomicU64::new(0),
            last_flush_ok: std::sync::Mutex::new(Instant::now()),
            flush_inflight: std::sync::atomic::AtomicBool::new(false),
        });
        // Fast path: install the persisted bulk index wholesale (no per-pack
        // header reads, no MPHF rebuild) plus only the tail of per-pack manifests
        // flushed since the snapshot. Falls back to a full per-pack load +
        // rebuild when there's no usable snapshot (cold device, or stale/corrupt
        // blob — the snapshot is a pure cache, never a durability dependency).
        if store.try_load_snapshot().await? {
            store.reconcile_staging().await?;
            // A large tail folds into the bulk MPHF (refreshing the snapshot); a
            // small tail stays in `recent`, re-derived cheaply on the next open.
            let recent_len = { store.state.read().await.recent.len() };
            if recent_len >= RECENT_REBUILD_KEYS {
                store.rebuild_index().await?;
            }
        } else {
            store.load_index().await?;
            store.reconcile_staging().await?;
            // Fold the loaded packs into the compact bulk MPHF (load_index leaves
            // them in `recent`) and persist the snapshot; a no-op for an empty store.
            store.rebuild_index().await?;
        }
        // Pick up any persisted `todo/` markers (packs discovered but not yet
        // enriched — e.g. an enrichment interrupted by a previous shutdown) so
        // they resume. Enrichment itself is driven by the caller / background /
        // the negative-answer gate; `open` never blocks on it.
        store.load_todos().await?;
        // Diagnostic: how much the dedup index knows at open. `keys` is the blob
        // count `contains_honest` can match against; a re-snap that re-uploads
        // duplicates while this is ~0 means the reconcile didn't populate the
        // index (`todo` still pending → enriched on first miss).
        {
            let st = store.state.read().await;
            let keys = st.recent.len() + st.bulk.as_ref().map_or(0, |b| b.pack_idx.len());
            tracing::info!(
                packs = st.packs.len(),
                keys,
                todo = st.todo.len(),
                "packing store opened: index populated"
            );
        }
        Ok(store)
    }

    fn manifest_path(&self, pack_hash: Hash) -> String {
        format!(
            "{}{}",
            self.config.index_cache_prefix,
            s5_core::blob::paths::path_for_hash(pack_hash, &self.index_cache.features())
        )
    }

    /// Reserved cache-store key holding the serialized bulk index. Lives under
    /// the index-cache prefix but is NOT a per-pack manifest — the per-pack scan
    /// and the tail recovery both skip it.
    fn snapshot_key(&self) -> String {
        format!("{}index-snapshot.v1", self.config.index_cache_prefix)
    }

    /// Recover a pack hash from a manifest cache path (`{prefix}{path_for_hash}`),
    /// reusing the blob-path decoder by swapping in the `blob3/` prefix it expects.
    fn pack_hash_from_manifest_path(&self, path: &str) -> Option<Hash> {
        let rest = path.strip_prefix(&self.config.index_cache_prefix)?;
        s5_core::blob::paths::hash_from_blob_path(
            &format!("blob3/{rest}"),
            &self.index_cache.features(),
        )
        .ok()
        .flatten()
    }

    /// Serialize the bulk-covered index to the cache store. Best-effort: a
    /// serialize/write failure is logged, never fatal — the index is always
    /// rebuildable from the per-pack headers. Called after a rebuild, when
    /// `bulk` covers `packs[..bulk_packs]`.
    //
    // TODO(RAM/chunked-snapshot): `to_allocvec` here (and `open_read_bytes(..,
    // None)` + `from_bytes` in `try_load_snapshot`) hold the WHOLE snapshot in
    // one buffer (~330 MB at 16M blobs). At extreme scale, serialize/read it in
    // bounded windows (or drop the member tables and re-derive them from per-pack
    // manifests on load — folds into the lazy-index TODO on `Index::packs`). It
    // is a pure cache and transient, so this is an at-scale nicety, not urgent.
    // See packing-store.md §8b/§9.
    async fn write_snapshot(&self) -> StoreResult<()> {
        let blob = {
            let state = self.state.read().await;
            let Some(bulk) = &state.bulk else {
                return Ok(());
            };
            let packs: Vec<SnapshotPack> = state.packs[..state.bulk_packs]
                .iter()
                .map(|h| SnapshotPack {
                    pack_hash: *h.pack_hash.as_bytes(),
                    members: h
                        .members
                        .iter()
                        .map(|m| (m.hash_prefix, m.offset))
                        .collect(),
                    end_offset: h.end_offset,
                })
                .collect();
            let snap = SnapshotWrite {
                version: SNAPSHOT_VERSION,
                packs,
                mphf: &bulk.mphf,
            };
            match postcard::to_allocvec(&snap) {
                Ok(v) => Bytes::from(v),
                Err(e) => {
                    tracing::warn!("packing: index snapshot serialize failed (skipping): {e}");
                    return Ok(());
                }
            }
        };
        let key = self.snapshot_key();
        if let Err(e) = self.index_cache.put_bytes(&key, blob).await {
            tracing::warn!("packing: index snapshot write failed (skipping): {e}");
        }
        Ok(())
    }

    /// Try to install the persisted bulk index. Returns `true` if a usable
    /// snapshot was loaded (the bulk + its covered packs are now in `state`, and
    /// any newer per-pack manifests have been folded into `recent`); `false` if
    /// there is no snapshot, or it's a stale/corrupt/mismatched blob (caller
    /// rebuilds from per-pack manifests instead).
    async fn try_load_snapshot(&self) -> StoreResult<bool> {
        let key = self.snapshot_key();
        if !self.index_cache.exists(&key).await? {
            return Ok(false);
        }
        let bytes = self.index_cache.open_read_bytes(&key, 0, None).await?;
        let snap: SnapshotRead = match postcard::from_bytes(&bytes) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("packing: ignoring unreadable index snapshot ({e}); rebuilding");
                return Ok(false);
            }
        };
        if snap.version != SNAPSHOT_VERSION {
            tracing::warn!(
                "packing: ignoring index snapshot v{} (expected v{SNAPSHOT_VERSION}); rebuilding",
                snap.version
            );
            return Ok(false);
        }

        let headers: Vec<PackHeader> = snap
            .packs
            .into_iter()
            .map(SnapshotPack::into_header)
            .collect();
        // Reconstruct the slot arrays from the persisted MPHF (skips Mphf::new);
        // a mismatch rejects the snapshot rather than installing a broken index.
        let Some(bulk) = fill_bulk_from_mphf(snap.mphf, &headers) else {
            tracing::warn!("packing: index snapshot MPHF did not match its packs; rebuilding");
            return Ok(false);
        };
        let covered: HashSet<[u8; 32]> = headers.iter().map(|h| *h.pack_hash.as_bytes()).collect();

        {
            let mut state = self.state.write().await;
            let bulk_packs = headers.len();
            state.packs = headers;
            state.bulk = Some(bulk);
            state.bulk_packs = bulk_packs;
            state.recent.clear();
        }

        // Tail: per-pack manifests flushed AFTER the snapshot (not in `covered`).
        // Load only those — the whole point of the snapshot is to skip re-reading
        // the packs it already covers.
        let snapshot_key = self.snapshot_key();
        let mut stream = self.index_cache.list().await?;
        let mut tail_paths = Vec::new();
        while let Some(path) = stream.next().await {
            let path = path?;
            if !path.starts_with(&self.config.index_cache_prefix) || path == snapshot_key {
                continue;
            }
            // Skip packs already covered when the hash is recoverable from the
            // path; otherwise fall through and let the value's hash decide.
            if let Some(h) = self.pack_hash_from_manifest_path(&path)
                && covered.contains(h.as_bytes())
            {
                continue;
            }
            tail_paths.push(path);
        }
        drop(stream);

        for path in tail_paths {
            let bytes = self.index_cache.open_read_bytes(&path, 0, None).await?;
            let header = PackHeader::from_cache_bytes(&bytes)
                .map_err(|e| anyhow::anyhow!("decoding manifest cache at {path}: {e}"))?;
            // Re-check by the value's true hash (covers the path-undecodable case).
            if covered.contains(header.pack_hash.as_bytes()) {
                continue;
            }
            self.state.write().await.add_pack(header);
        }
        Ok(true)
    }

    /// Key namespace for `todo` markers — a sibling of the index-cache prefix
    /// (`manifests/` → `manifests.todo/`) so the per-pack manifest scan never
    /// picks them up. Flat `hex(pack_hash)` names, reversible without a store
    /// feature lookup.
    fn todo_prefix(&self) -> String {
        let base = self
            .config
            .index_cache_prefix
            .strip_suffix('/')
            .unwrap_or(&self.config.index_cache_prefix);
        format!("{base}.todo/")
    }

    fn todo_path(&self, pack_hash: Hash) -> String {
        format!(
            "{}{}",
            self.todo_prefix(),
            hex::encode(pack_hash.as_bytes())
        )
    }

    fn hash_from_todo_path(&self, path: &str) -> Option<Hash> {
        let rest = path.strip_prefix(&self.todo_prefix())?;
        let bytes = hex::decode(rest).ok()?;
        let arr: [u8; 32] = bytes.try_into().ok()?;
        Some(Hash::from_bytes(arr))
    }

    /// **Discover** pack bodies cheaply: record each hash we don't already know
    /// (enriched or pending) as a persisted `todo/` marker + an entry in the
    /// in-memory `todo` set. No header reads happen here — that's [`enrich`]'s
    /// job. Typically fed the blob backend's enumeration (`blob3/<hash>`) once
    /// the backend itself is reconstructed.
    ///
    /// [`enrich`]: Self::enrich
    pub async fn note_pack_hashes(
        &self,
        pack_hashes: impl IntoIterator<Item = Hash>,
    ) -> StoreResult<()> {
        let fresh: Vec<Hash> = {
            let state = self.state.read().await;
            let enriched: HashSet<[u8; 32]> = state
                .packs
                .iter()
                .map(|p| *p.pack_hash.as_bytes())
                .collect();
            pack_hashes
                .into_iter()
                .filter(|h| {
                    let b = h.as_bytes();
                    !enriched.contains(b) && !state.todo.contains(b)
                })
                .collect()
        };
        if fresh.is_empty() {
            return Ok(());
        }
        for h in &fresh {
            self.index_cache
                .put_bytes(&self.todo_path(*h), Bytes::new())
                .await?;
        }
        let mut state = self.state.write().await;
        for h in &fresh {
            state.todo.insert(*h.as_bytes());
        }
        Ok(())
    }

    /// Load persisted `todo/` markers into the in-memory pending set (called on
    /// open, so an interrupted enrichment resumes).
    async fn load_todos(&self) -> StoreResult<()> {
        let mut stream = self.index_cache.list().await?;
        let mut hashes = Vec::new();
        while let Some(path) = stream.next().await {
            if let Some(h) = self.hash_from_todo_path(&path?) {
                hashes.push(*h.as_bytes());
            }
        }
        drop(stream);
        if hashes.is_empty() {
            return Ok(());
        }
        // Skip markers for packs already enriched (a manifest/snapshot covered
        // them, or a prior enrich's marker-delete failed): re-adding them to
        // `todo` would re-`add_pack` the same pack later and duplicate its keys.
        // The "already enriched" set is derived from `packs` (the in-memory view
        // of the cache-store manifests, populated by the load that runs before
        // this) — no standing side index. Delete the stale markers so they don't
        // accumulate across restarts.
        let stale: Vec<[u8; 32]> = {
            let mut state = self.state.write().await;
            let known: HashSet<[u8; 32]> = state
                .packs
                .iter()
                .map(|p| *p.pack_hash.as_bytes())
                .collect();
            let mut stale = Vec::new();
            for b in hashes {
                if known.contains(&b) {
                    stale.push(b);
                } else {
                    state.todo.insert(b);
                }
            }
            stale
        };
        for b in stale {
            let _ = self
                .index_cache
                .delete(&self.todo_path(Hash::from_bytes(b)))
                .await;
        }
        Ok(())
    }

    /// **Enrich** the pending set: read each todo pack's prepended header
    /// (concurrently, front bytes only), write its manifest 1:1, and fold it into
    /// the live index. Resolves every todo to a terminal state so `todo` drains:
    /// a definitive non-pack (bad magic / version / decode) drops its marker; a
    /// transient read failure keeps it for a later pass. Single-flighted, so a
    /// storm of read-misses triggers one drain. Returns the number newly enriched.
    pub async fn enrich(&self) -> StoreResult<u64> {
        let _guard = self.enrich_lock.lock().await;
        let todos: Vec<Hash> = {
            let state = self.state.read().await;
            state.todo.iter().map(|b| Hash::from_bytes(*b)).collect()
        };
        if todos.is_empty() {
            return Ok(0);
        }

        let outcomes: Vec<(Hash, EnrichOutcome)> = stream::iter(todos)
            .map(|h| async move { (h, self.enrich_one(h).await) })
            .buffer_unordered(ENRICH_CONCURRENCY)
            .collect()
            .await;

        let mut enriched = 0u64;
        for (h, outcome) in outcomes {
            match outcome {
                EnrichOutcome::Enriched(header) => {
                    // Persist the manifest, fold into the index, clear the marker.
                    self.index_cache
                        .put_bytes(&self.manifest_path(h), header.to_cache_bytes())
                        .await?;
                    {
                        let mut state = self.state.write().await;
                        state.add_pack(header);
                        state.todo.remove(h.as_bytes());
                    }
                    let _ = self.index_cache.delete(&self.todo_path(h)).await;
                    enriched += 1;
                }
                EnrichOutcome::NotAPack => {
                    // Terminal: drop the marker so `todo` can reach empty.
                    self.state.write().await.todo.remove(h.as_bytes());
                    let _ = self.index_cache.delete(&self.todo_path(h)).await;
                    tracing::warn!(
                        "enrich: {h} is not a self-describing pack — dropping from todo"
                    );
                }
                EnrichOutcome::Transient(e) => {
                    tracing::warn!("enrich: transient failure reading {h} (will retry): {e:?}");
                }
            }
        }
        if enriched > 0 {
            let recent_len = { self.state.read().await.recent.len() };
            if recent_len >= RECENT_REBUILD_KEYS {
                self.rebuild_index().await?;
            }
        }
        Ok(enriched)
    }

    /// Probe one pending pack's front bytes and classify it. A read error is
    /// `Transient` (retry); a magic/version/decode failure is `NotAPack` (drop) —
    /// this is what stops a foreign hash from pinning `todo` non-empty forever.
    async fn enrich_one(&self, pack_hash: Hash) -> EnrichOutcome {
        let prefix = match self
            .blobs
            .blob_download_slice(pack_hash, 0, Some(FIXED_HEADER_LEN as u64))
            .await
        {
            Ok(p) => p,
            Err(e) => return EnrichOutcome::Transient(e),
        };
        let count = match parse_count(&prefix) {
            Ok(c) => c,
            Err(_) => return EnrichOutcome::NotAPack,
        };
        let region_len = header_region_len(count as usize) as u64;
        let region = match self
            .blobs
            .blob_download_slice(pack_hash, 0, Some(region_len))
            .await
        {
            Ok(r) => r,
            Err(e) => return EnrichOutcome::Transient(e),
        };
        match decode_header(&region, pack_hash) {
            Ok(header) => EnrichOutcome::Enriched(header),
            Err(_) => EnrichOutcome::NotAPack,
        }
    }

    /// Membership check that is **honest about negatives**: a positive returns
    /// immediately, but a would-be negative first drains any pending packs (one
    /// of them could hold the key). Steady state (`todo` empty) is a single
    /// read-lock with no enrichment.
    ///
    /// If enrichment can't reach every known pack (a transient backend failure
    /// keeps its `todo` marker), a negative is **not** authoritative — the blob
    /// might live in a pack we couldn't read — so this returns a retryable error
    /// rather than a false "absent". See [`unreadable_packs_error`].
    async fn contains_honest(&self, key: &Key) -> StoreResult<bool> {
        {
            let state = self.state.read().await;
            if state.contains(key) {
                return Ok(true);
            }
            if state.todo.is_empty() {
                return Ok(false);
            }
        }
        self.enrich().await?;
        let state = self.state.read().await;
        if state.contains(key) {
            return Ok(true);
        }
        if state.todo.is_empty() {
            return Ok(false);
        }
        Err(unreadable_packs_error(state.todo.len()))
    }

    /// `locate` with the same negative-honesty as [`contains_honest`]: a hit
    /// returns at once; a miss with pending packs enriches, then re-locates. A
    /// miss that survives enrichment while packs remain unreadable is a retryable
    /// error, not `None` (see [`contains_honest`]).
    async fn locate_honest(&self, key: &Key) -> StoreResult<Option<(Hash, u32, u32)>> {
        {
            let state = self.state.read().await;
            if let Some(hit) = state.locate(key) {
                return Ok(Some(hit));
            }
            if state.todo.is_empty() {
                return Ok(None);
            }
        }
        self.enrich().await?;
        let state = self.state.read().await;
        if let Some(hit) = state.locate(key) {
            return Ok(Some(hit));
        }
        if state.todo.is_empty() {
            return Ok(None);
        }
        Err(unreadable_packs_error(state.todo.len()))
    }

    /// Load all manifests at startup and populate the in-memory index.
    async fn load_index(&self) -> StoreResult<()> {
        let snapshot_key = self.snapshot_key();
        let mut stream = self.index_cache.list().await?;
        let mut paths = Vec::new();
        while let Some(path) = stream.next().await {
            let path = path?;
            if path.starts_with(&self.config.index_cache_prefix) && path != snapshot_key {
                paths.push(path);
            }
        }
        drop(stream);

        let mut index = self.state.write().await;
        for path in paths {
            let bytes = self.index_cache.open_read_bytes(&path, 0, None).await?;
            let header = PackHeader::from_cache_bytes(&bytes)
                .map_err(|e| anyhow::anyhow!("decoding manifest cache at {path}: {e}"))?;
            index.add_pack(header);
        }
        Ok(())
    }

    /// One-shot recovery from pack **headers** — the manifest-free path, symmetric
    /// to `IndexdStore`'s `reconstruct_from_indexer`. The durable index is the
    /// prepended header in each pack body (`header ++ data`), so given the set of
    /// pack hashes — e.g. from
    /// [`BlobStore::list_hashes`](s5_core::blob::BlobStore::list_hashes) once the
    /// blob backend itself is reconstructed — this re-derives the full
    /// `key -> (pack, offset, length)` index with no durable manifest objects.
    ///
    /// Thin wrapper over [`note_pack_hashes`](Self::note_pack_hashes) +
    /// [`enrich`](Self::enrich): records the hashes as pending and drains them
    /// synchronously, returning the number newly enriched. Idempotent (known
    /// packs are skipped); a hash that isn't a readable self-describing pack is
    /// dropped, not fatal; a transient read failure is retained for a later
    /// `enrich`. Prefer `note_pack_hashes` + background `enrich` for a
    /// non-blocking open; use this when you want "rebuild now" semantics.
    pub async fn reconstruct_from_headers(
        &self,
        pack_hashes: impl IntoIterator<Item = Hash>,
    ) -> StoreResult<u64> {
        self.note_pack_hashes(pack_hashes).await?;
        self.enrich().await
    }

    /// Reconcile staging on startup: drop any staged entry whose key
    /// is already in the live index; re-enqueue the rest.
    async fn reconcile_staging(&self) -> StoreResult<()> {
        let paths = self.list_staging_paths().await?;
        let mut index = self.state.write().await;
        for path in paths {
            let Some(key) = key_from_staging_path(&path) else {
                tracing::warn!("reconcile: skipping staging entry with non-hash name: {path}");
                continue;
            };
            if index.locate(&key).is_some() {
                drop(index);
                self.staging.delete(&path).await?;
                index = self.state.write().await;
                continue;
            }
            let length = self.staging.size(&path).await? as u32;
            self.staged_bytes.fetch_add(length as u64, Ordering::AcqRel);
            index.pending.insert(
                key,
                PendingBlob {
                    key,
                    staging_path: path,
                    length,
                    // Fresh clock, not the file mtime: recovered WAL entries
                    // become max-age-stale within one `max_pending_age` and
                    // flush on an early tick, without an upload storm at open.
                    staged_at: Instant::now(),
                },
            );
        }
        if !index.pending.is_empty() {
            tracing::info!(
                blobs = index.pending.len(),
                bytes = self.staged_bytes.load(Ordering::Acquire),
                "packing: staging WAL recovered — un-uploaded blobs re-enqueued"
            );
        }
        Ok(())
    }

    /// Staging backpressure watermark in bytes: `staging_max_packs * max_group_size`
    /// (`0` ⇒ unbounded). Bounds how much un-uploaded data may spool to local disk.
    fn max_staged_bytes(&self) -> u64 {
        self.config.staging_max_packs as u64 * self.config.max_group_size
    }

    async fn list_staging_paths(&self) -> StoreResult<Vec<String>> {
        let mut stream = self.staging.list().await?;
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item?);
        }
        Ok(out)
    }

    /// Background loop. Run with `tokio::spawn(store.clone().run_upload_loop())`.
    //
    // TODO(scrub/bit-rot): add a sibling low-rate background scrub loop that
    // periodically re-fetches a SAMPLE of pack bodies from `blobs` and verifies
    // each against its content-addressed id (BLAKE3), catching Sia degradation
    // BEFORE a restore needs the data (restic `check`-style, proactive). Reads
    // already self-verify on access; this covers cold data that's rarely read.
    // Opt-in + rate-limited; surface an unrecoverable pack as an alert. Cheap:
    // content-addressing makes verification a hash comparison.
    pub async fn run_upload_loop(self: Arc<Self>) {
        let interval = self.config.interval;
        loop {
            let tick = tokio::time::sleep(interval);
            tokio::pin!(tick);
            tokio::select! {
                _ = &mut tick => {}
                _ = self.flush_notify.notified() => {}
            }
            if let Err(err) = self.pack_once(false).await {
                tracing::warn!("packing-store upload tick failed: {err:?}");
            }
            // Stall visibility: staging pinned at the watermark
            // with no successful flush for a while means writers are parked and
            // no durability progress is being made — say so, loudly, instead of
            // freezing silently like the 2026-07-02 drill did.
            let watermark = self.max_staged_bytes();
            let staged = self.staged_bytes.load(Ordering::Acquire);
            if watermark > 0 && staged >= watermark {
                let since_ok = self.last_flush_ok.lock().expect("not poisoned").elapsed();
                if since_ok >= STALL_WARN_AFTER {
                    tracing::warn!(
                        staged_bytes = staged,
                        watermark,
                        stalled_secs = since_ok.as_secs(),
                        inflight = self.flush_inflight.load(Ordering::Acquire),
                        "packing: uploads appear STALLED — staging is at the watermark and no \
                         pack has flushed; writers are blocked (check network / indexer health)"
                    );
                }
            }
        }
    }

    /// Point-in-time flush gauges — see [`FlushStats`].
    pub fn flush_stats(&self) -> FlushStats {
        FlushStats {
            staged_bytes: self.staged_bytes.load(Ordering::Acquire),
            since_last_flush_ok: self.last_flush_ok.lock().expect("not poisoned").elapsed(),
            inflight: self.flush_inflight.load(Ordering::Acquire),
        }
    }

    /// Drive one pack-and-flush cycle. `force_all = true` flushes
    /// every pending group regardless of size.
    pub async fn pack_once(&self, force_all: bool) -> StoreResult<()> {
        // One flush cycle at a time. Snapshotting `pending` AFTER taking the
        // lock means a waiter (e.g. the publish sync() barrier waiting on the
        // background tick) sees the already-flushed set and does no duplicate
        // work — so no two cycles race on the same staging files.
        let _flush = self.flush_lock.lock().await;
        let pending: Vec<PendingBlob> = {
            let state = self.state.read().await;
            let mut v: Vec<_> = state.pending.values().cloned().collect();
            v.sort_by(|a, b| a.staging_path.cmp(&b.staging_path));
            v
        };
        if pending.is_empty() {
            return Ok(());
        }

        let mut groups: Vec<PackGroup> = Vec::new();
        for blob in pending {
            first_fit(
                &mut groups,
                blob,
                self.config.slab_size,
                self.config.max_group_size,
                self.config.waste_pct,
            );
        }

        let mut first_err: Option<anyhow::Error> = None;
        for group in groups {
            // A group is ready when it is big enough — or old enough: a
            // sub-minimum tail (identity publishes are a few KB) must not sit
            // staged-only forever waiting for bulk data that may never come
            // (the recovery-drill brick).
            let stale = group
                .members
                .iter()
                .any(|m| m.staged_at.elapsed() >= self.config.max_pending_age);
            let ready = force_all || stale || group.total_size >= self.config.min_group_size;
            if !ready {
                continue;
            }
            // One failed flush must not starve the remaining groups: warn,
            // keep the staged entries (the WAL retries next tick), move on.
            // The first error still fails the cycle so a `blob_sync` barrier
            // keeps its durability contract.
            if let Err(err) = self.flush_group(group).await {
                tracing::warn!(
                    "packing: pack flush failed (staged data retained, retried next tick): {err:?}"
                );
                first_err.get_or_insert(err);
            }
        }
        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    async fn flush_group(&self, mut group: PackGroup) -> StoreResult<()> {
        tracing::info!(
            members = group.members.len(),
            bytes = group.total_size,
            "packing: flushing pack group"
        );
        // Members sorted ascending by key (a uniform BLAKE3 prefix) before the
        // header is built and the bytes concatenated — deterministic across
        // devices, and binary-searchable in the header.
        group.members.sort_by_key(|m| m.key);

        // Build the prepended header (`header ++ data`): the body's index lives
        // at the FRONT, so a reader resolves a member with ranged GETs from
        // offset 0 — no pack-size lookup. `pack_hash` covers the whole body and
        // is recovered as the object id; the header alone rebuilds the index on
        // recovery ([`Self::reconstruct_from_headers`]).
        let header_members: Vec<([u8; HASH_PREFIX_LEN], u32)> =
            group.members.iter().map(|b| (b.key, b.length)).collect();
        let (header_bytes, _end_offset) = encode_header(&header_members)?;
        // Bytes are ref-counted, so this clone is O(1); we parse it back into a
        // PackHeader (with absolute offsets) after upload for the index + cache.
        let header_for_index = header_bytes.clone();

        // Stream the body: the header first, then each staged blob in order — one
        // staged blob in flight at a time, so the WHOLE pack is never buffered in
        // RAM. Peak ingest memory is ~one staged blob regardless of pack size.
        let staging = self.staging.clone();
        let paths: Vec<(String, u32)> = group
            .members
            .iter()
            .map(|b| (b.staging_path.clone(), b.length))
            .collect();
        let body = stream::unfold(
            (staging, Some(header_bytes), paths.into_iter()),
            |(staging, mut header, mut paths)| async move {
                // Header first, exactly once.
                if let Some(h) = header.take() {
                    return Some((Ok(h), (staging, header, paths)));
                }
                let (path, expect_len) = paths.next()?;
                let item = match staging.open_read_bytes(&path, 0, None).await {
                    Ok(bytes) if bytes.len() as u32 == expect_len => Ok(bytes),
                    Ok(bytes) => Err(io::Error::other(format!(
                        "staging size mismatch for {path}: expected {expect_len}, got {}",
                        bytes.len()
                    ))),
                    Err(e) => Err(io::Error::other(format!("staging read {path}: {e}"))),
                };
                Some((item, (staging, header, paths)))
            },
        );
        // Outer timeout: the backend's own timeouts cover
        // individual sector writes, not the whole upload — a degraded host
        // pool can park an upload forever with no error anywhere. Size the
        // bound to the pack (≥ floor, ~1 MiB/s worst case); on expiry the
        // staged entries survive and the next tick retries.
        let upload_timeout = self
            .config
            .upload_timeout_floor
            .max(Duration::from_secs(group.total_size >> 20));
        self.flush_inflight
            .store(true, std::sync::atomic::Ordering::Release);
        let uploaded = tokio::time::timeout(
            upload_timeout,
            self.blobs.blob_upload_stream(Box::pin(body)),
        )
        .await;
        self.flush_inflight
            .store(false, std::sync::atomic::Ordering::Release);
        let blob_id = match uploaded {
            Ok(result) => result?,
            Err(_) => anyhow::bail!(
                "packing: pack upload timed out after {}s ({} bytes, {} members); \
                 staged data retained for retry",
                upload_timeout.as_secs(),
                group.total_size,
                group.members.len()
            ),
        };
        let pack_hash = blob_id.hash;
        tracing::info!(pack = %pack_hash, bytes = group.total_size, "packing: pack body uploaded");

        // The `manifests` store is a *local cache* (the durable index is the
        // header in the body). Caching the parsed header keeps warm restarts fast
        // — `load_index` reads it without touching the network.
        let header = decode_header(&header_for_index, pack_hash)?;
        let manifest_path = self.manifest_path(pack_hash);
        self.index_cache
            .put_bytes(&manifest_path, header.to_cache_bytes())
            .await?;

        // Update in-memory index + drop staging entries.
        let recent_len = {
            let mut state = self.state.write().await;
            for member in &header.members {
                state.pending.remove(&member.hash_prefix);
            }
            state.add_pack(header);
            state.recent.len()
        };
        let mut freed = 0u64;
        for blob in &group.members {
            self.staging.delete(&blob.staging_path).await?;
            freed += blob.length as u64;
        }
        // Staging shrank by a whole pack — release that much of the backpressure
        // budget and wake any uploader parked on the watermark.
        self.staged_bytes.fetch_sub(freed, Ordering::AcqRel);
        self.staging_drained.notify_waiters();
        *self.last_flush_ok.lock().expect("not poisoned") = Instant::now();
        // Fold the recent tail into the bulk MPHF once it's grown enough, so its
        // per-key RAM stays bounded. The rebuild builds off-lock.
        if recent_len >= RECENT_REBUILD_KEYS {
            self.rebuild_index().await?;
        }
        Ok(())
    }

    /// Fold every pack into the compact bulk MPHF — called after a cold
    /// load/reconstruct and when the `recent` tail grows past
    /// [`RECENT_REBUILD_KEYS`]. The MPHF build is CPU-bound, so it runs on a
    /// blocking thread off a *snapshot* of the keys; the write lock is held only
    /// to swap the result in (and re-derive `recent` for any packs added during
    /// the build).
    pub async fn rebuild_index(&self) -> StoreResult<()> {
        let (keys, pack_of, covered) = {
            let state = self.state.read().await;
            let mut keys = Vec::new();
            let mut pack_of = Vec::new();
            for (pi, pack) in state.packs.iter().enumerate() {
                for m in &pack.members {
                    keys.push(m.hash_prefix);
                    pack_of.push(pi as u32);
                }
            }
            (keys, pack_of, state.packs.len())
        };
        if keys.is_empty() {
            return Ok(());
        }
        // Build off a blocking thread on native (CPU-bound). wasm32 has no
        // thread pool / `spawn_blocking`, so build inline there — a browser
        // share-reader only ever indexes a handful of packs, so the build is
        // tiny; the owner-scale rebuild is a native concern.
        #[cfg(not(target_arch = "wasm32"))]
        let bulk = tokio::task::spawn_blocking(move || build_bulk(keys, pack_of))
            .await
            .map_err(|e| anyhow::anyhow!("packing index rebuild task: {e}"))?;
        #[cfg(target_arch = "wasm32")]
        let bulk = build_bulk(keys, pack_of);

        let mut state = self.state.write().await;
        state.bulk = Some(bulk);
        state.bulk_packs = covered;
        // Re-derive `recent` from packs flushed while the MPHF was building.
        state.recent.clear();
        let tail: Vec<(Key, u32)> = state.packs[covered..]
            .iter()
            .enumerate()
            .flat_map(|(off, pack)| {
                let pi = (covered + off) as u32;
                pack.members.iter().map(move |m| (m.hash_prefix, pi))
            })
            .collect();
        state.recent.extend(tail);
        drop(state); // release the write lock before re-acquiring read in write_snapshot

        // Persist the freshly-built bulk so the next open installs it wholesale
        // (no per-pack reads, no rebuild). Best-effort; never fatal.
        self.write_snapshot().await?;
        Ok(())
    }

    /// Read an entry's bytes from whatever pack contains it, or from
    /// staging if not yet packed.
    async fn read_entry(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        let key = hash_key(hash);

        let staging_path = {
            let state = self.state.read().await;
            state.pending.get(&key).map(|p| p.staging_path.clone())
        };
        if let Some(p) = staging_path {
            return self.staging.open_read_bytes(&p, offset, max_len).await;
        }

        // A miss here is only trustworthy once pending packs are enriched, so
        // `locate_honest` drains them before returning `None`.
        let (pack_hash, in_pack_offset, length) =
            self.locate_honest(&key).await?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("no such blob: {hash}"))
            })?;
        let pack_offset = in_pack_offset as u64 + offset;
        let effective_len = match max_len {
            Some(l) => l.min(length as u64 - offset),
            None => length as u64 - offset,
        };
        self.blobs
            .blob_download_slice(pack_hash, pack_offset, Some(effective_len))
            .await
    }
}

#[async_trait::async_trait]
impl<B> BlobsRead for PackingStore<B>
where
    B: BlobsReadWrite + std::fmt::Debug + Send + Sync + 'static,
{
    async fn blob_contains(&self, hash: Hash) -> BlobResult<bool> {
        // Honest about negatives: a "false" first drains any pending packs.
        self.contains_honest(&hash_key(hash)).await
    }

    async fn blob_get_size(&self, hash: Hash) -> BlobResult<u64> {
        let key = hash_key(hash);
        // Staged-but-not-packed blobs answer from `pending` without enriching.
        if let Some(length) = {
            let state = self.state.read().await;
            state.pending.get(&key).map(|p| p.length)
        } {
            return Ok(length as u64);
        }
        // A miss enriches before concluding "not found".
        if let Some((_, _, length)) = self.locate_honest(&key).await? {
            return Ok(length as u64);
        }
        Err(io::Error::new(io::ErrorKind::NotFound, format!("no such blob: {hash}")).into())
    }

    async fn blob_download(&self, hash: Hash) -> BlobResult<Bytes> {
        // Full reads re-verify the content address (BlobsRead contract):
        // the pack index maps truncated keys to pack slices, so a corrupted
        // or mis-mapped slice would otherwise return wrong bytes undetected.
        let bytes = self.read_entry(hash, 0, None).await?;
        s5_core::blob::verify_bytes(hash, bytes)
    }

    async fn blob_download_slice(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> BlobResult<Bytes> {
        if offset == 0 && max_len.is_none() {
            return self.blob_download(hash).await;
        }
        self.read_entry(hash, offset, max_len).await
    }

    async fn blob_read(&self, hash: Hash) -> BlobResult<Box<dyn AsyncRead + Send + Unpin>> {
        // A packed member is small by construction (the packer bundles SMALL
        // blobs), so one buffered, verified read is bounded; wrap it as a
        // cursor reader.
        let bytes = self.blob_download(hash).await?;
        Ok(Box::new(io::Cursor::new(bytes)))
    }
}

#[async_trait::async_trait]
impl<B> BlobsWrite for PackingStore<B>
where
    B: BlobsReadWrite + std::fmt::Debug + Send + Sync + 'static,
{
    async fn blob_upload_bytes(&self, bytes: Bytes) -> BlobResult<BlobId> {
        // The store is content-addressed: it hashes its own input, so a blob is
        // always stored under its true `Hash` and reads self-verify.
        let hash = Hash::new(&bytes);
        let length = bytes.len() as u32;
        let key = hash_key(hash);

        // Dedup: skip if already present. Honest about negatives — if the blob
        // *appears* new while packs are still pending, enrich first, so we don't
        // re-stage (and re-upload to Sia) a blob that already lives in an
        // un-enriched pack. A positive short-circuits without enriching; if some
        // packs are unreadable the dedup can't be proven, so `contains_honest`
        // errors (retryable) rather than re-staging into a duplicate pack.
        if self.contains_honest(&key).await? {
            self.dedup_hits.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(%hash, len = length, "packing: dedup HIT (already stored — skipping)");
            return Ok(BlobId::new(hash, length as u64));
        }
        self.dedup_misses.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(%hash, len = length, "packing: dedup MISS — staging NEW blob");

        // Staging backpressure: bound the un-uploaded spool so a fast ingest over
        // a slow backend self-throttles to the upload rate instead of filling the
        // disk. Block until staging is under the watermark; `flush_group`
        // decrements `staged_bytes` and wakes us as each pack uploads. Watermark 0
        // disables it. NB: needs a running flush loop (or `flush_on_put`) to
        // drain — the daemon spawns `run_upload_loop`; `flush_on_put` keeps the
        // spool near-empty so this never blocks.
        let watermark = self.max_staged_bytes();
        while watermark > 0 && self.staged_bytes.load(Ordering::Acquire) >= watermark {
            // Arm the waiter BEFORE the final re-check so a concurrent flush's
            // `notify_waiters` can't slip between the check and the await.
            let drained = self.staging_drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.staged_bytes.load(Ordering::Acquire) < watermark {
                break;
            }
            self.flush_notify.notify_one();
            drained.await;
        }

        let staging_path = staging_path_for(hash);
        self.staging.put_bytes(&staging_path, bytes).await?;
        self.staged_bytes.fetch_add(length as u64, Ordering::AcqRel);
        {
            let mut state = self.state.write().await;
            state.pending.insert(
                key,
                PendingBlob {
                    key,
                    staging_path,
                    length,
                    staged_at: Instant::now(),
                },
            );
        }

        if self.config.flush_on_put {
            self.pack_once(true).await?;
        }
        Ok(BlobId::new(hash, length as u64))
    }

    async fn blob_upload_reader<R, F>(
        &self,
        hash: Hash,
        _size: u64,
        mut reader: R,
        _on_progress: F,
    ) -> BlobResult<BlobId>
    where
        Self: Sized,
        R: AsyncRead + Send + Unpin + 'static,
        F: Fn(u64) -> io::Result<()> + Send + Sync + 'static,
    {
        // Packing only ever handles small blobs, so buffering to verify the
        // content hash is fine (and this path is never taken via `dyn Blobs`).
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        let id = self.blob_upload_bytes(Bytes::from(buf)).await?;
        if id.hash != hash {
            return Err(anyhow::anyhow!(
                "packing blob_upload_reader: content hashed to {} but caller declared {hash}",
                id.hash
            ));
        }
        Ok(id)
    }

    async fn blob_upload_stream<St>(&self, stream: St) -> BlobResult<BlobId>
    where
        Self: Sized,
        St: Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static,
    {
        // A stream carries no content hash up front, and packing handles only
        // small blobs, so collect then reuse the bytes path (which hashes it).
        let mut stream = stream;
        let mut buf = Vec::new();
        while let Some(chunk) = stream.next().await {
            buf.extend_from_slice(&chunk?);
        }
        self.blob_upload_bytes(Bytes::from(buf)).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn blob_upload_file(&self, path: std::path::PathBuf) -> BlobResult<BlobId> {
        let bytes = tokio::fs::read(&path).await?;
        self.blob_upload_bytes(Bytes::from(bytes)).await
    }

    async fn blob_sync(&self) -> BlobResult<()> {
        self.pack_once(true).await?;
        self.staging.sync().await?;
        self.blobs.blob_sync().await?;
        self.index_cache.sync().await?;
        // Diagnostic: cumulative dedup outcome since open. On a re-snap of
        // unchanged/duplicate content, `misses` should stay ~flat — a jump means
        // the dedup check isn't finding already-packed blobs.
        let packs = self.state.read().await.packs.len();
        tracing::info!(
            dedup_hits = self.dedup_hits.load(Ordering::Relaxed),
            dedup_misses = self.dedup_misses.load(Ordering::Relaxed),
            packs,
            "packing blob_sync: dedup counters (cumulative since open)"
        );
        Ok(())
    }

    /// Expose this store's live [`FlushStats`] as the trait-level
    /// [`StagingStats`] gauge (`vup status` / `vup doctor` durability honesty:
    /// staged-but-not-durable bytes are visible, not implied uploaded).
    fn staging_stats(&self) -> Option<StagingStats> {
        let s = self.flush_stats();
        Some(StagingStats {
            staged_bytes: s.staged_bytes,
            since_last_flush_secs: s.since_last_flush_ok.as_secs(),
            inflight: s.inflight,
        })
    }
}

#[async_trait::async_trait]
impl<B> BlobsDelete for PackingStore<B>
where
    B: BlobsReadWrite + std::fmt::Debug + Send + Sync + 'static,
{
    async fn blob_delete(&self, _hash: Hash) -> BlobResult<()> {
        // Deletion/GC deferred (spec §10.7): a packed blob can't be reclaimed
        // without rewriting its whole pack, which needs the GC design.
        //
        // TODO(gc/prune): implement the mark-sweep GC + threshold compaction from
        // packing-store.md §8 — restic-`prune` parity, reclaiming unreferenced
        // data. Lower urgency: keyed-convergent encryption makes orphaned blobs
        // unreadable without the vault secret, so this is a pure storage-COST
        // optimization, not a confidentiality/correctness need. Opt-in + lazy.
        Err(anyhow::anyhow!(
            "PackingStore: blob_delete is not implemented in this revision (deletion/GC deferred)"
        ))
    }
}

// ---- helpers ----

/// The error a negative lookup returns when it can't prove absence because some
/// known packs are currently unreadable (a transient backend failure kept their
/// `todo` markers). Retryable and semantically distinct from a definitive
/// `NotFound`: the caller should retry rather than conclude the blob is gone —
/// and, critically, the write path must NOT treat it as "new" and re-stage a
/// blob that may already be packed (that is what would mint a duplicate pack).
fn unreadable_packs_error(unreadable: usize) -> anyhow::Error {
    anyhow::anyhow!(
        "packing: cannot confirm blob absence — {unreadable} known pack(s) unreadable \
         (transient backend failure); retry"
    )
}

/// The 12-byte in-pack index key for a blob: the prefix of its BLAKE3 `Hash`.
/// 96 bits → negligible birthday collision at 10^9 blobs; reads verify the full
/// hash at the blob layer.
fn hash_key(hash: Hash) -> [u8; HASH_PREFIX_LEN] {
    let mut key = [0u8; HASH_PREFIX_LEN];
    key.copy_from_slice(&hash.as_bytes()[..HASH_PREFIX_LEN]);
    key
}

/// Staging path a blob is written under while it waits to be packed: the full
/// hex of its `Hash`. Self-describing, so [`reconcile_staging`] recovers the
/// index key from the filename alone on restart.
///
/// [`reconcile_staging`]: PackingStore::reconcile_staging
fn staging_path_for(hash: Hash) -> String {
    hex::encode(hash.as_bytes())
}

/// Recover the 12-byte index key from a staging filename (`hex(hash)`), or
/// `None` if it isn't a valid hash hex (a foreign file in the staging dir).
fn key_from_staging_path(path: &str) -> Option<[u8; HASH_PREFIX_LEN]> {
    let bytes = hex::decode(path).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut key = [0u8; HASH_PREFIX_LEN];
    key.copy_from_slice(&bytes[..HASH_PREFIX_LEN]);
    Some(key)
}

#[cfg(test)]
mod tests;
