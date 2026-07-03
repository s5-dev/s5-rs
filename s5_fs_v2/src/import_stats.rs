//! Process-wide import-path instrumentation (DIAGNOSTIC, 2026-06-17 s5).
//!
//! The publish snap re-reads ~1.9 GB/cycle for ~650 KB of new data at an
//! effective ~28 MB/s, and we could not tell from logs WHY: is the cost the
//! contended file READ, the per-chunk HASH, or the ENCODE (recompression
//! because positional dedup MISSES on the reloaded prev)? The per-chunk
//! `dedup_hits` counter in [`crate::snapshot::Snapshot::import_stream_with_prev`]
//! was computed and thrown away (`let _ = …`), so the snap path was blind.
//!
//! These are plain process-wide atomics (no signature ripple, trivial to
//! delete). The publish worker is at-most-one-in-flight (durability contract),
//! so a single backup owns the import path while it runs; the caller reads a
//! [`Counters`] snapshot before and after the walk and logs the DELTA — no
//! reset, so there is no race with a reset. Accumulation is `Relaxed`: these
//! are coarse aggregates, not a happens-before signal.
//!
//! Timing covers the three import phases per chunk: `read_ns` wraps the
//! chunker pull (I/O + contention), `hash_ns` the plaintext BLAKE3, `encode_ns`
//! the (concurrent) compress+upload of a non-deduped chunk. `read_ns`/`hash_ns`
//! are serial in the producer (wall-attributable); `encode_ns` is summed across
//! the in-flight window (CPU, may exceed wall). `dedup_hits`/`dedup_bytes` vs
//! `chunks`/`bytes_read` is the decisive signal: hits≈chunks ⇒ the 70 s is
//! contended READ (so #3's prefix-skip is the fix); hits≪chunks ⇒ the prev
//! dedup is broken and we are needlessly re-encoding (a different fix).

use std::sync::atomic::{AtomicU64, Ordering};

pub static READ_NS: AtomicU64 = AtomicU64::new(0);
pub static HASH_NS: AtomicU64 = AtomicU64::new(0);
pub static ENCODE_NS: AtomicU64 = AtomicU64::new(0);
pub static CHUNKS: AtomicU64 = AtomicU64::new(0);
pub static DEDUP_HITS: AtomicU64 = AtomicU64::new(0);
pub static DEDUP_BYTES: AtomicU64 = AtomicU64::new(0);
pub static BYTES_READ: AtomicU64 = AtomicU64::new(0);
/// Retries inside `retry_io` (the retry debug log is invisible at INFO) — the
/// decisive "is the 2.66 s/file sidecar cost a retry/backoff storm?" signal.
pub static RETRIES: AtomicU64 = AtomicU64::new(0);
/// Whole `Pipeline::import_bytes` (encode + blob_upload) — the SINGLE-CHUNK
/// path (every one-blob sidecar). read/hash/encode counters only cover the
/// MULTI-chunk loop, so single-chunk files were unaccounted; this closes the
/// "where do the ~123 ms/file go after the chunker fix?" gap (2026-06-18).
pub static IMPORT_BYTES_NS: AtomicU64 = AtomicU64::new(0);
/// `build_tree_dedup` (per-file prolly tree build + internal-node uploads) —
/// the other unaccounted slice (multi-chunk files).
pub static TREE_NS: AtomicU64 = AtomicU64::new(0);

#[inline]
pub(crate) fn add_import_bytes(ns: u64) {
    IMPORT_BYTES_NS.fetch_add(ns, Ordering::Relaxed);
}

#[inline]
pub(crate) fn add_tree(ns: u64) {
    TREE_NS.fetch_add(ns, Ordering::Relaxed);
}

/// Bump the retry counter (called from `s5_fs_local::backup::retry_io`).
#[inline]
pub fn add_retry() {
    RETRIES.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub(crate) fn add_read(ns: u64, bytes: u64) {
    READ_NS.fetch_add(ns, Ordering::Relaxed);
    BYTES_READ.fetch_add(bytes, Ordering::Relaxed);
    CHUNKS.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub(crate) fn add_hash(ns: u64) {
    HASH_NS.fetch_add(ns, Ordering::Relaxed);
}

#[inline]
pub(crate) fn add_encode(ns: u64) {
    ENCODE_NS.fetch_add(ns, Ordering::Relaxed);
}

#[inline]
pub(crate) fn add_dedup_hit(bytes: u64) {
    DEDUP_HITS.fetch_add(1, Ordering::Relaxed);
    DEDUP_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// A point-in-time read of the global import counters. Diff two reads
/// (before/after a snap) for per-snap attribution.
#[derive(Clone, Copy, Default, Debug)]
pub struct Counters {
    pub read_ns: u64,
    pub hash_ns: u64,
    pub encode_ns: u64,
    pub chunks: u64,
    pub dedup_hits: u64,
    pub dedup_bytes: u64,
    pub bytes_read: u64,
    pub retries: u64,
    pub import_bytes_ns: u64,
    pub tree_ns: u64,
}

/// Snapshot the current global counters.
pub fn read() -> Counters {
    Counters {
        read_ns: READ_NS.load(Ordering::Relaxed),
        hash_ns: HASH_NS.load(Ordering::Relaxed),
        encode_ns: ENCODE_NS.load(Ordering::Relaxed),
        chunks: CHUNKS.load(Ordering::Relaxed),
        dedup_hits: DEDUP_HITS.load(Ordering::Relaxed),
        dedup_bytes: DEDUP_BYTES.load(Ordering::Relaxed),
        bytes_read: BYTES_READ.load(Ordering::Relaxed),
        retries: RETRIES.load(Ordering::Relaxed),
        import_bytes_ns: IMPORT_BYTES_NS.load(Ordering::Relaxed),
        tree_ns: TREE_NS.load(Ordering::Relaxed),
    }
}

impl Counters {
    /// `self - earlier` — the per-snap delta (saturating).
    pub fn since(&self, earlier: &Counters) -> Counters {
        Counters {
            read_ns: self.read_ns.saturating_sub(earlier.read_ns),
            hash_ns: self.hash_ns.saturating_sub(earlier.hash_ns),
            encode_ns: self.encode_ns.saturating_sub(earlier.encode_ns),
            chunks: self.chunks.saturating_sub(earlier.chunks),
            dedup_hits: self.dedup_hits.saturating_sub(earlier.dedup_hits),
            dedup_bytes: self.dedup_bytes.saturating_sub(earlier.dedup_bytes),
            bytes_read: self.bytes_read.saturating_sub(earlier.bytes_read),
            retries: self.retries.saturating_sub(earlier.retries),
            import_bytes_ns: self.import_bytes_ns.saturating_sub(earlier.import_bytes_ns),
            tree_ns: self.tree_ns.saturating_sub(earlier.tree_ns),
        }
    }
}
