//! mtime-LRU size cap for a [`crate::LocalStore`] blob directory.
//!
//! A `LocalStore` written through as a cache (every miss re-`put_bytes`)
//! has no internal eviction — left unbounded it fills the disk. This is
//! the generic reclaim mechanism: walk `base_path`, sum sizes, and when
//! over `budget_bytes` delete oldest-by-**mtime** files down to
//! `low_watermark` of the budget.
//!
//! mtime, not atime: atime is commonly disabled for performance, so
//! mtime is the only age signal available. For a write-through cache the
//! file is (re)written on every fetch, so mtime ≈ "last populated" —
//! the right LRU-ish key here.
//!
//! Unlink-safe by construction: a concurrent reader's fd survives the
//! unlink (open inode / page cache), and any post-unlink miss falls
//! through to whatever backs the cache and re-populates. Eviction is
//! never a correctness risk — only a re-fetch cost. Files written within
//! the last [`PruneConfig::min_age`] are still protected: they were just
//! populated and a reader is likely about to touch them, so evicting
//! forces an immediate round-trip we just paid for. The `.tmp` staging
//! dir ([`crate::TMP_SUBDIR`]) is never touched — racing in-flight
//! uploads would break the atomic-write contract.
//!
//! This module is pure mechanism: it returns `(bytes, files)` evicted
//! and never reaches for observability. The host spawns the periodic
//! pass and is handed each pass's counters via the `on_complete`
//! callback to record however it likes.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Tunables for one prune pass plus the periodic cadence used by
/// [`spawn`]. `Copy` — cheap to hand to each blocking pass.
#[derive(Debug, Clone, Copy)]
pub struct PruneConfig {
    /// Hard byte budget. A pass evicts only when the directory total
    /// exceeds this.
    pub budget_bytes: u64,
    /// When over budget, evict down to this fraction of the budget.
    /// Headroom absorbs between-pass growth without thrashing the
    /// working set.
    pub low_watermark: f64,
    /// Never evict a file whose mtime is within this window — it was
    /// just written and a reader is likely about to consume it.
    pub min_age: Duration,
    /// Periodic pass cadence (used only by [`spawn`]).
    pub tick: Duration,
}

impl PruneConfig {
    /// Defaults: evict to 95 % of budget, protect files younger than
    /// 60 s, run every 5 min. (These were the fixed constants before
    /// this became host-configurable; `new` preserves that behavior.)
    pub fn new(budget_bytes: u64) -> Self {
        Self {
            budget_bytes,
            low_watermark: 0.95,
            min_age: Duration::from_secs(60),
            tick: Duration::from_secs(300),
        }
    }
}

/// Spawn the periodic prune on the current tokio runtime. `on_complete`
/// is invoked with `(bytes_evicted, files_evicted)` after every
/// successful pass — the host's seam for recording liveness/throughput
/// (a pass that stops firing means the cache is silently filling).
///
/// The caller owns the budget decision: a store whose
/// [`crate::LocalStoreConfig::prune_budget_bytes`] is `None` should not
/// call this at all (unbounded, by explicit configuration, never a
/// silent "safe" default).
pub fn spawn(
    base_path: PathBuf,
    cfg: PruneConfig,
    on_complete: impl Fn(u64, u64) + Send + 'static,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(cfg.tick);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let dir = base_path.clone();
            // FS walk + unlinks are blocking — keep them off the async
            // worker. A panic in the pass must not kill the loop.
            match tokio::task::spawn_blocking(move || prune_once(&dir, &cfg)).await {
                Ok((bytes, files)) => on_complete(bytes, files),
                Err(e) => tracing::warn!(error = %e, "prune: pass task failed (continuing)"),
            }
        }
    });
}

/// One prune pass. Returns `(bytes_evicted, files_evicted)` — pure
/// mechanism, no side effects beyond the unlinks themselves.
pub fn prune_once(base_path: &Path, cfg: &PruneConfig) -> (u64, u64) {
    let mut files: Vec<(PathBuf, SystemTime, u64)> = Vec::new();
    let mut total: u64 = 0;
    collect(base_path, base_path, &mut files, &mut total);

    let mut bytes_evicted: u64 = 0;
    let mut files_evicted: u64 = 0;
    if total > cfg.budget_bytes {
        let target = (cfg.budget_bytes as f64 * cfg.low_watermark) as u64;
        files.sort_unstable_by_key(|(_, mtime, _)| *mtime); // oldest first
        let now = SystemTime::now();
        for (path, mtime, size) in files {
            if total <= target {
                break;
            }
            // Protect just-written blobs (see module docs).
            if now.duration_since(mtime).unwrap_or(Duration::MAX) < cfg.min_age {
                continue;
            }
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    total = total.saturating_sub(size);
                    bytes_evicted += size;
                    files_evicted += 1;
                }
                Err(e) => {
                    // Race (already gone), cross-FS, perms: log and
                    // keep going. A single bad file must never stall
                    // the prune.
                    tracing::warn!(
                        path = %path.display(), error = %e,
                        "prune: unlink failed (skipping)"
                    );
                }
            }
        }
        tracing::info!(
            budget = cfg.budget_bytes,
            evicted_bytes = bytes_evicted,
            evicted_files = files_evicted,
            remaining_bytes = total,
            "prune: over-budget pass complete"
        );
    }

    (bytes_evicted, files_evicted)
}

/// Recursive (size, mtime) collector. Skips `{base}/.tmp`
/// ([`crate::TMP_SUBDIR`]) — the atomic-write staging dir; those are
/// in-flight uploads, not cached blobs, and racing them would break the
/// tmp+rename contract.
fn collect(root: &Path, dir: &Path, out: &mut Vec<(PathBuf, SystemTime, u64)>, total: &mut u64) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in rd.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        let path = entry.path();
        if ft.is_dir() {
            if dir == root && entry.file_name() == crate::TMP_SUBDIR {
                continue;
            }
            collect(root, &path, out, total);
        } else if ft.is_file()
            && let Ok(meta) = entry.metadata()
        {
            let size = meta.len();
            let mtime = meta.modified().unwrap_or(UNIX_EPOCH);
            *total += size;
            out.push((path, mtime, size));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write_aged(dir: &Path, name: &str, bytes: usize, age: Duration) {
        let p = dir.join(name);
        fs::write(&p, vec![0u8; bytes]).unwrap();
        let mtime = SystemTime::now() - age;
        filetime::set_file_mtime(&p, filetime::FileTime::from_system_time(mtime)).unwrap();
    }

    /// Characterization: `new` must reproduce the pre-config constants
    /// (0.95 watermark, 60 s min-age, 300 s tick) — the behavior this
    /// move must preserve.
    #[test]
    fn prune_config_new_preserves_prior_constants() {
        let c = PruneConfig::new(123);
        assert_eq!(c.budget_bytes, 123);
        assert_eq!(c.low_watermark, 0.95);
        assert_eq!(c.min_age, Duration::from_secs(60));
        assert_eq!(c.tick, Duration::from_secs(300));
    }

    #[test]
    fn evicts_oldest_first_to_watermark_and_protects_fresh() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // 4 × 1000 B = 4000 B. Two old, one mid, one fresh (<60 s).
        write_aged(root, "old_a", 1000, Duration::from_secs(3600));
        write_aged(root, "old_b", 1000, Duration::from_secs(1800));
        write_aged(root, "mid", 1000, Duration::from_secs(600));
        write_aged(root, "fresh", 1000, Duration::from_secs(5));
        // .tmp staging must be ignored entirely.
        fs::create_dir(root.join(".tmp")).unwrap();
        fs::write(root.join(".tmp").join("inflight"), vec![0u8; 9999]).unwrap();

        // Budget 2500 → target = 2500 * 0.95 = 2375. Must evict the two
        // oldest (old_a, old_b) → 2000 ≤ 2375; mid kept, fresh always
        // kept (age < 60 s), .tmp untouched.
        let (bytes, files) = prune_once(root, &PruneConfig::new(2500));
        assert_eq!(files, 2, "should evict exactly the 2 oldest");
        assert_eq!(bytes, 2000);
        assert!(!root.join("old_a").exists());
        assert!(!root.join("old_b").exists());
        assert!(root.join("mid").exists(), "mid under target → kept");
        assert!(root.join("fresh").exists(), "fresh (<60s) protected");
        assert!(root.join(".tmp").join("inflight").exists(), ".tmp skipped");
    }

    #[test]
    fn under_budget_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        write_aged(tmp.path(), "a", 100, Duration::from_secs(3600));
        let (bytes, files) = prune_once(tmp.path(), &PruneConfig::new(1_000_000));
        assert_eq!((bytes, files), (0, 0));
        assert!(tmp.path().join("a").exists());
    }
}
