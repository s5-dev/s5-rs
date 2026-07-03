//! Local filesystem backup: walk, diff, upload, persist.
//!
//! Supports two modes controlled by `BackupConfig.backup`:
//!
//! - **Sync mode** (`backup: false`, default): lightweight metadata — only
//!   mtime, file type. Change detection uses size + mtime.
//!
//! - **Backup mode** (`backup: true`): captures everything needed for a
//!   faithful restore — permissions, ownership (uid/gid + names), ctime,
//!   inode, device, nlink, extended attributes. Change detection additionally
//!   checks inode + ctime when the previous snapshot has them.

use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use ignore::WalkBuilder;
use ignore::overrides::Override;
use s5_core::{BlobsRead, BlobsWrite};
use s5_fs_v2::layer::ReadableLayer;
use s5_fs_v2::node::{ExtendedAttribute, FileType, NodeEntry, SemanticMeta, UnixMetadata};
use s5_fs_v2::overlay::WritableOverlay;
use s5_fs_v2::persist::MergeStats;
use s5_fs_v2::snapshot::Snapshot;
use tokio_util::sync::CancellationToken;

/// First-match-wins routing entry mapping a glob over the file's vault key
/// (relative path) to a per-file `TraversalContext` override applied during
/// import via [`Snapshot::import_stream_with_override`](s5_fs_v2::snapshot::Snapshot::import_stream_with_override).
///
/// The override is what gets stamped on `entry.child_context` — the vault's
/// default ctx supplies anything the override leaves as `None` (via
/// `merge_contexts` at write *and* read time, so the cascade reproduces the
/// same effective pipeline).
///
/// Build via the typical pattern:
///
/// ```ignore
/// let glob = globset::Glob::new("segments/**/*.seg")?.compile_matcher();
/// let route = PipelineRoute { glob, override_ctx };
/// ```
///
/// Pass through [`BackupConfig::routes`].
#[derive(Clone)]
pub struct PipelineRoute {
    pub glob: globset::GlobMatcher,
    pub override_ctx: s5_fs_v2::node::TraversalContext,
    /// APPEND-ONLY hint (#3): the caller guarantees files matching this route
    /// only ever GROW — their leading bytes never change (e.g. a
    /// log-structured publisher's interner packs, ledger appends, sealed
    /// segments). When set AND the route's chunking is `Fixed`, the
    /// incremental backup reuses the unchanged full-chunk prefix from the
    /// prev snapshot by reference and reads only the appended tail
    /// (`Snapshot::import_file_append`), instead of re-reading + re-hashing
    /// the whole file every publish. MUST be false for any route whose files
    /// can be rewritten in place — a violated hint would publish a stale
    /// prefix. Default false.
    pub append_only: bool,
}

impl std::fmt::Debug for PipelineRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineRoute")
            .field("glob", &self.glob.glob().glob())
            .finish_non_exhaustive()
    }
}

/// Configuration for a backup operation.
///
/// All boolean flags default to `false`.
pub struct BackupConfig {
    /// Maximum number of concurrent file uploads.
    pub max_concurrent_ops: usize,
    /// Skip incremental checks — always re-import every file.
    /// Useful for first-time backups or when the previous snapshot is untrusted.
    pub force_full: bool,
    /// Stay on the same filesystem — do not cross mount boundaries.
    pub one_file_system: bool,
    /// Backup mode: capture full metadata (permissions, ownership, xattrs,
    /// inode, ctime, nlink, device_id, user/group names).
    ///
    /// When false (sync mode), only file type, mtime, and content are stored.
    pub backup: bool,
    /// Per-key pipeline routing. First match wins, in declaration order.
    /// Files whose computed key matches a route are imported via
    /// `import_stream_with_override` with the route's `override_ctx`.
    /// Files matching no route fall through to the snapshot's default ctx
    /// (the existing `import_stream` path). Empty = no routing.
    pub routes: Vec<PipelineRoute>,
    /// Follow symlinks: stat and import the target file's content instead
    /// of storing the symlink as a metadata-only entry (target path string).
    /// When true, also calls `WalkBuilder::follow_links(true)` so symlinks
    /// to directories are descended into.
    ///
    /// Default: `false` — symlinks are stored as their target path string
    /// (cheap metadata, no content import). Set to `true` for use cases
    /// where the symlink targets are the canonical data (e.g. tiered
    /// storage where hot symlinks point to cold files).
    pub follow_symlinks: bool,
    /// Detect deletions: tombstone tree entries whose source file no longer
    /// exists on disk, so the snapshot mirrors the source rather than
    /// accumulating every file ever seen.
    ///
    /// Default `false` preserves the historical *additive* behaviour (a
    /// snapshot only ever grows) for append-only / archival callers. Turn it
    /// on for mirror semantics — required by any source that deletes files
    /// (e.g. one whose segment compaction removes superseded packs; left
    /// off, the published tree grows without bound and downstream GC can
    /// never reclaim the orphaned blobs because they stay reachable).
    ///
    /// Implemented by a per-entry on-disk existence check against the
    /// *previous* snapshot, so it is robust to partial/errored walks and to
    /// ignore-rule changes: a file that exists is never tombstoned. Skipped
    /// entirely when the backup is cancelled (partial state).
    pub detect_deletions: bool,
    /// Source exclude rules, as an `ignore` override matcher.
    ///
    /// The full-walk [`backup`] applies excludes through the caller-built
    /// [`WalkBuilder`] overrides, so excluded paths are never visited. The
    /// incremental [`backup_incremental`] drives off raw `notify` event paths
    /// — it has no walker — so it MUST consult this matcher or it republishes
    /// excluded paths (the watch loop sees every FS event under the source
    /// root, including the very subdirs the source config means to drop). The
    /// caller builds this from the same patterns it feeds the walker so both
    /// paths share one definition of "excluded". `None` = no excludes.
    pub exclude: Option<Override>,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            max_concurrent_ops: 8,
            force_full: false,
            one_file_system: false,
            backup: false,
            routes: Vec::new(),
            follow_symlinks: false,
            detect_deletions: false,
            exclude: None,
        }
    }
}

/// Statistics from a backup operation.
#[derive(Debug, Default)]
pub struct BackupStats {
    /// Files uploaded (new or changed).
    pub files_changed: AtomicU64,
    /// Files skipped (unchanged).
    pub files_skipped: AtomicU64,
    /// Files skipped due to errors (permission denied, IO errors).
    pub files_errored: AtomicU64,
    /// Directories processed.
    pub dirs_processed: AtomicU64,
    /// Symlinks processed.
    pub symlinks_processed: AtomicU64,
    /// Special files skipped (block/char device, fifo, socket).
    pub special_skipped: AtomicU64,
    /// Total bytes uploaded (plaintext).
    ///
    /// TODO: this counts plaintext bytes streamed through
    /// `import_stream` (i.e. read + CDC-hashed), which conflates "work
    /// done" with "blobs actually written" once per-chunk dedup is in
    /// play. Split into `bytes_read` (always counted) and
    /// `bytes_actually_uploaded` (post-dedup) so the metric reflects
    /// real storage cost. Plumb dedup hit counts up from
    /// `Snapshot::import_stream_with_prev`.
    pub bytes_uploaded: AtomicU64,
    /// Bytes actually READ from source files during import (`import_stream`).
    /// Equals `bytes_uploaded` (content_len) today; once append-aware import
    /// (#3) lands, an append-only file that only grew a tail reads ≪ its
    /// content_len, so `bytes_read ≪ bytes_uploaded` is the validation signal
    /// that #3 engaged. Distinct from `bytes_uploaded` (work credited) and
    /// from `MergeStats::bytes_uploaded` (new blob bytes after dedup).
    pub bytes_read: AtomicU64,
    /// Walk-phase decomposition (nanoseconds, summed across the changed-path
    /// loop) — attributes `walk_ms` to its three costs so we don't guess which
    /// dominates: `stat` (symlink_metadata), `is_changed` (prev-snapshot prolly
    /// lookups — cold-cache faulting), `import` (read + CDC + BLAKE3 + upload).
    pub stat_ns: AtomicU64,
    pub is_changed_ns: AtomicU64,
    pub import_ns: AtomicU64,
    /// Merge/persist statistics from the prolly tree.
    pub merge: Option<MergeStats>,
}

/// Back up a local directory into an S5 FS V2 snapshot.
///
/// Walks `source_dir` using the provided `walker`, diffs against
/// `prev_snapshot`, uploads changed file content to `blob_store` (typically
/// remote), and persists the prolly tree to `meta_store` (typically local).
///
/// The caller configures the [`WalkBuilder`] (ignore rules, cachedir, etc.)
/// before passing it in. This function adds a `one_file_system` filter when
/// `config.one_file_system` is set.
///
/// Returns the new [`Snapshot`] and [`BackupStats`], or `None` if the
/// directory is empty (no live entries).
///
/// # Store split
///
/// - `blob_store`: receives file content blobs (leaves). Typically a remote
///   store (S3, Sia).
/// - `meta_store`: receives prolly tree nodes (internal + leaf nodes). Typically
///   the vault's local store at `root_path`.
/// - `read_store`: used by the returned [`Snapshot`] for reads. Should be able
///   to read from both `meta_store` and `blob_store` — use
///   [`FallbackBlobsRead`](s5_fs_v2::fallback::FallbackBlobsRead) to combine
///   them, or pass a single store if both are the same.
///
/// For simple setups (tests, single store), pass the same store for all three.
/// Result of a backup operation: (snapshot, stats, was_cancelled)
pub struct BackupResult {
    pub snapshot: Option<(Snapshot, BackupStats)>,
    pub was_cancelled: bool,
}

#[allow(clippy::too_many_arguments)]
pub async fn backup(
    source_dir: &Path,
    prev_snapshot: &Snapshot,
    blob_store: &(dyn BlobsWrite + Sync),
    meta_store: &(dyn BlobsWrite + Sync),
    read_store: Arc<dyn BlobsRead>,
    config: &BackupConfig,
    mut walker: WalkBuilder,
    stats: Option<Arc<BackupStats>>,
    cancel: Option<CancellationToken>,
) -> anyhow::Result<BackupResult> {
    let t_start = std::time::Instant::now();

    let source_dir = source_dir
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", source_dir.display()))?;

    // Resolve the root device ID for one_file_system filtering.
    let root_dev = if config.one_file_system {
        Some(std::fs::metadata(&source_dir)?.dev())
    } else {
        None
    };

    let stats = stats.unwrap_or_else(|| Arc::new(BackupStats::default()));
    let pipeline = Arc::new(prev_snapshot.as_pipeline());
    let base: Arc<dyn s5_fs_v2::layer::ReadableLayer> = Arc::new(prev_snapshot.clone());
    let overlay = Arc::new(WritableOverlay::new(base, pipeline));

    // Add one_file_system filter if configured.
    if root_dev.is_some() {
        walker.filter_entry(move |entry| {
            if !entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                return true;
            }
            if let Some(root_dev) = root_dev
                && let Ok(m) = entry.metadata()
                && m.dev() != root_dev
            {
                return false;
            }
            true
        });
    }

    // Parallel walker: OS threads walk directories concurrently and send
    // entries through a channel. The async side processes them with
    // buffer_unordered for concurrent uploads.
    let (tx, rx) = tokio::sync::mpsc::channel::<ignore::DirEntry>(512);

    if config.follow_symlinks {
        walker.follow_links(true);
    }

    let walk_parallel = walker.build_parallel();
    let walk_handle = std::thread::spawn(move || {
        walk_parallel.visit(&mut ParallelSender(tx));
    });

    // Convert the mpsc receiver into a futures::Stream.
    let entry_stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|entry| (entry, rx))
    });

    // Process entries concurrently.
    // File content is uploaded to blob_store (remote), not meta_store.
    let stream = entry_stream
        .map(|entry| {
            let source_dir = source_dir.clone();
            let stats = stats.clone();
            let overlay = overlay.clone();
            async move {
                process_entry(
                    entry.path(),
                    &source_dir,
                    prev_snapshot,
                    blob_store,
                    &overlay,
                    &stats,
                    config,
                )
                .await
            }
        })
        .buffer_unordered(config.max_concurrent_ops);

    // Race the backup stream against cancellation.
    // Stream errors propagate via `?` in both branches; only cancellation
    // itself is a clean early exit that still persists partial state.
    let was_cancelled = if let Some(ref cancel) = cancel {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::info!("backup cancelled — saving partial state");
                true
            }
            r = stream.try_collect::<()>() => {
                r?;
                false
            }
        }
    } else {
        stream.try_collect::<()>().await?;
        false
    };

    let process_ms = t_start.elapsed().as_millis() as u64;

    // Wait for the walker threads to finish — join on a blocking thread so
    // we don't stall a tokio worker if the walker is still unwinding a slow
    // syscall (e.g. NFS, FUSE).
    let _ = tokio::task::spawn_blocking(move || walk_handle.join()).await;

    // ---- Deletion detection (mirror semantics; opt-in) ----
    // The walk above only adds/updates files it found; vanished files still
    // live in `prev_snapshot` and would ride forward forever (the source of
    // unbounded manifest growth under compaction). When enabled, tombstone
    // every prev-tree entry whose source file no longer exists on disk.
    //
    // Robust by construction: each entry's own tree key drives both the
    // existence check (`source_dir.join(key)`) and the tombstone, so there is
    // no key-format mismatch, and a file that still exists is never
    // tombstoned — regardless of ignore-rule changes or walk-traversal
    // errors. Only a definitive `NotFound` deletes; any other stat outcome
    // keeps the entry. Skipped on cancellation (the new tree is partial).
    let mut tombstoned = 0u64;
    if config.detect_deletions && !was_cancelled && !prev_snapshot.is_empty() {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as u32)
            .unwrap_or(0);
        let mut walk = std::pin::pin!(prev_snapshot.walk());
        while let Some(item) = walk.next().await {
            let (key, _entry) = item?;
            match std::fs::symlink_metadata(source_dir.join(&key)) {
                Ok(_) => {} // still on disk → keep
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    overlay.delete(key, NodeEntry::tombstone(now_secs));
                    tombstoned += 1;
                }
                Err(_) => {} // can't determine → keep (conservative)
            }
        }
        if tombstoned > 0 {
            tracing::info!(
                tombstoned,
                "backup deletion-detection: tombstoned entries whose source vanished"
            );
        }
    }

    let t_merge = std::time::Instant::now();

    // Persist the overlay into a new prolly tree.
    // Tree nodes go to meta_store (local vault).
    let result = prev_snapshot
        .merge_and_persist(&*overlay, meta_store)
        .await?;

    let merge_ms = t_merge.elapsed().as_millis() as u64;
    let total_ms = t_start.elapsed().as_millis() as u64;
    tracing::info!(
        files_changed = stats.files_changed.load(Ordering::Relaxed),
        files_skipped = stats.files_skipped.load(Ordering::Relaxed),
        files_errored = stats.files_errored.load(Ordering::Relaxed),
        bytes_uploaded = stats.bytes_uploaded.load(Ordering::Relaxed),
        dirs_processed = stats.dirs_processed.load(Ordering::Relaxed),
        symlinks_processed = stats.symlinks_processed.load(Ordering::Relaxed),
        process_ms = process_ms,
        merge_ms = merge_ms,
        total_ms = total_ms,
        was_cancelled = was_cancelled,
        "backup cycle complete"
    );

    let Some((root_hash, root_plaintext_hash, merge_stats)) = result else {
        // Empty overlay — return without snapshot but still report cancellation
        return Ok(BackupResult {
            snapshot: None,
            was_cancelled,
        });
    };

    let new_snapshot = Snapshot::new(
        root_hash,
        read_store,
        prev_snapshot.context().clone(),
        Some(root_plaintext_hash),
    );

    let mut stats = Arc::try_unwrap(stats).unwrap_or_else(|arc| BackupStats {
        files_changed: AtomicU64::new(arc.files_changed.load(Ordering::Relaxed)),
        files_skipped: AtomicU64::new(arc.files_skipped.load(Ordering::Relaxed)),
        files_errored: AtomicU64::new(arc.files_errored.load(Ordering::Relaxed)),
        dirs_processed: AtomicU64::new(arc.dirs_processed.load(Ordering::Relaxed)),
        symlinks_processed: AtomicU64::new(arc.symlinks_processed.load(Ordering::Relaxed)),
        special_skipped: AtomicU64::new(arc.special_skipped.load(Ordering::Relaxed)),
        bytes_uploaded: AtomicU64::new(arc.bytes_uploaded.load(Ordering::Relaxed)),
        bytes_read: AtomicU64::new(arc.bytes_read.load(Ordering::Relaxed)),
        stat_ns: AtomicU64::new(arc.stat_ns.load(Ordering::Relaxed)),
        is_changed_ns: AtomicU64::new(arc.is_changed_ns.load(Ordering::Relaxed)),
        import_ns: AtomicU64::new(arc.import_ns.load(Ordering::Relaxed)),
        merge: None,
    });
    stats.merge = Some(merge_stats);

    Ok(BackupResult {
        snapshot: Some((new_snapshot, stats)),
        was_cancelled,
    })
}

/// Incremental backup: apply ONLY `changed_paths` to `prev_snapshot`, instead
/// of walking the whole source tree and the whole prev-snapshot for deletion
/// detection. Cost is O(changed paths), not O(corpus) — this is the
/// event-driven counterpart to [`backup`], driven by a notify-watch loop that
/// collects changed/removed paths from filesystem events.
///
/// Per path: a definitive `NotFound` stat means the file was deleted/moved
/// away, so its key is tombstoned (same mirror-delete semantics as [`backup`]'s
/// deletion-detection pass); anything else is upserted via the shared
/// [`process_entry`] (which re-stats, dedups, and classifies dir/symlink/file).
///
/// **Not a correctness substitute for [`backup`].** inotify can miss in-place
/// `mmap` modifies (e.g. an actively-appended segment tail) and can drop events
/// on queue overflow, so callers MUST still run a periodic full [`backup`]
/// reconcile (and one on overflow) as the source of truth. This path only makes
/// the common case — a handful of changed files between reconciles — cheap.
#[allow(clippy::too_many_arguments)]
pub async fn backup_incremental(
    source_dir: &Path,
    changed_paths: &[PathBuf],
    prev_snapshot: &Snapshot,
    blob_store: &(dyn BlobsWrite + Sync),
    meta_store: &(dyn BlobsWrite + Sync),
    read_store: Arc<dyn BlobsRead>,
    config: &BackupConfig,
    stats: Option<Arc<BackupStats>>,
) -> anyhow::Result<BackupResult> {
    let source_dir = source_dir
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", source_dir.display()))?;

    let stats = stats.unwrap_or_else(|| Arc::new(BackupStats::default()));
    let pipeline = Arc::new(prev_snapshot.as_pipeline());
    let base: Arc<dyn s5_fs_v2::layer::ReadableLayer> = Arc::new(prev_snapshot.clone());
    let overlay = Arc::new(WritableOverlay::new(base, pipeline));

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as u32)
        .unwrap_or(0);

    let tombstoned = Arc::new(AtomicU64::new(0));
    let excluded = Arc::new(AtomicU64::new(0));
    // Snapshot the process-wide import counters so the snap log can attribute
    // import_ms to READ vs HASH vs ENCODE and report the per-chunk dedup
    // hit-rate (the decisive "is the 70 s contended-read or wasted-recompress?"
    // signal — DIAGNOSTIC, 2026-06-17 s5). Publish is at-most-one-in-flight, so
    // the delta over this snap is attributable to it.
    let import_before = s5_fs_v2::import_stats::read();
    let walk_start = std::time::Instant::now();

    // Dedup the changed-path batch: the watcher can deliver the same path
    // several times in one flush. `process_entry` reads CURRENT filesystem
    // state (not the event), so a repeat is idempotent — dedup only avoids
    // redundant work. Order is irrelevant (processed concurrently below).
    // Own the deduped paths (cheap — one alloc each, a few hundred per flush) so
    // the per-item future captures `PathBuf` by value: borrowing the iterated
    // `&PathBuf` into an `async move` trips a higher-ranked-lifetime bound.
    let mut seen: std::collections::HashSet<&std::path::Path> = std::collections::HashSet::new();
    let unique: Vec<PathBuf> = changed_paths
        .iter()
        .filter(|p| seen.insert(p.as_path()))
        .cloned()
        .collect();

    // Process concurrently — the SAME shared-overlay `process_entry`
    // concurrency the full `backup` runs in production (proven thread-safe).
    // `buffer_unordered(1)` degrades to serial, so this never *increases*
    // concurrency over the full walk. Correctness: `process_entry` reads the
    // file's current state, so the per-key result is independent of order; the
    // overlay's put/delete are concurrency-safe (full-backup invariant) and the
    // counters are atomic. A path appearing twice yields the same entry; a file
    // being written yields "some recent version" — exactly as in the serial
    // loop (the next snap converges either way). Errors propagate via `?` just
    // as the serial loop did.
    futures::stream::iter(unique)
        .map(|path| {
            let source_dir = source_dir.clone();
            let stats = stats.clone();
            let overlay = overlay.clone();
            let tombstoned = tombstoned.clone();
            let excluded = excluded.clone();
            async move {
                // notify can deliver paths outside the source (e.g. a watched
                // dir itself moved); ignore anything not under the canonical
                // source.
                let path: &std::path::Path = &path;
                if !path.starts_with(&source_dir) {
                    return Ok::<(), anyhow::Error>(());
                }
                let md = std::fs::symlink_metadata(path);
                // Honor the source excludes — symmetric with the full walk.
                if let Some(ov) = &config.exclude {
                    let is_dir = md.as_ref().map(|m| m.is_dir()).unwrap_or(false);
                    if ov.matched(path, is_dir).is_ignore() {
                        excluded.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                }
                match md {
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        // Vanished → tombstone.
                        if let Ok(key) = relative_key(path, &source_dir, false)
                            && !key.is_empty()
                        {
                            overlay.delete(key, NodeEntry::tombstone(now_secs));
                            tombstoned.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    _ => {
                        // Exists (or a transient stat error, which process_entry
                        // re-stats and counts as errored rather than losing).
                        process_entry(
                            path,
                            &source_dir,
                            prev_snapshot,
                            blob_store,
                            &overlay,
                            &stats,
                            config,
                        )
                        .await?;
                    }
                }
                Ok(())
            }
        })
        .buffer_unordered(config.max_concurrent_ops.max(1))
        .try_collect::<()>()
        .await?;

    let walk_ms = walk_start.elapsed().as_millis() as u64;
    let tombstoned = tombstoned.load(Ordering::Relaxed);
    let excluded = excluded.load(Ordering::Relaxed);

    // Pass the DIFF only, NOT the overlay. `WritableOverlay::scan` returns
    // base ∪ diff (overlay.rs `merge_two(overlay, base)`), so passing `&*overlay`
    // makes `merge_and_persist_structural` collect the WHOLE tree as `change_map`,
    // violating its "changes are small" premise → the degenerate-change-ratio
    // guard bails to the full O(corpus) re-fold EVERY cycle (proven live:
    // entries_reused=0 + merge_ms≈30s across every snap, CUTOVER 2026-06-17 s5).
    // `take()` (no clone — the overlay is discarded after) hands the sparse diff
    // straight to the merge so the structural cluster path actually engages.
    let diff = s5_fs_v2::layer::MapLayer::new(overlay.take());
    let merge_start = std::time::Instant::now();
    let result = prev_snapshot.merge_and_persist(&diff, meta_store).await?;
    let merge_ms = merge_start.elapsed().as_millis() as u64;

    let Some((root_hash, root_plaintext_hash, merge_stats)) = result else {
        tracing::info!(
            changed_paths = changed_paths.len() as u64,
            excluded,
            tombstoned,
            walk_ms,
            merge_ms,
            "backup_incremental: snap complete (empty merge / no-op)"
        );
        return Ok(BackupResult {
            snapshot: None,
            was_cancelled: false,
        });
    };

    let import_now = s5_fs_v2::import_stats::read().since(&import_before);

    // Phase + composition signal — closes the publish-path observability gap
    // (this path used to log NOTHING but a coarse end-of-task line). Read this
    // to attribute a slow snap and to confirm the merge stays incremental:
    //   walk_ms   — process the changed-path batch (stat + dedup + chunk reads),
    //   merge_ms  — the prolly-tree merge_and_persist (the structural re-chunk),
    //   changed_paths / excluded / tombstoned / files_changed / files_skipped —
    //               batch makeup; a healthy incremental has changed_paths small
    //               and files_skipped near 0 (a bloated batch = watcher noise),
    //   entries_changed vs entries_reused, nodes_uploaded vs nodes_deduped —
    //               proves the merge touched few leaves and reused the rest,
    //   bytes_processed (content_len of changed files, the old misleading
    //               `bytes_uploaded`) vs bytes_new (ACTUAL new blob bytes).
    tracing::info!(
        changed_paths = changed_paths.len() as u64,
        excluded,
        tombstoned,
        files_changed = stats.files_changed.load(Ordering::Relaxed),
        files_skipped = stats.files_skipped.load(Ordering::Relaxed),
        bytes_processed = stats.bytes_uploaded.load(Ordering::Relaxed),
        entries_changed = merge_stats.entries_changed,
        entries_reused = merge_stats.entries_reused,
        leaf_nodes = merge_stats.leaf_nodes,
        nodes_uploaded = merge_stats.nodes_uploaded,
        nodes_deduped = merge_stats.nodes_deduped,
        bytes_new = merge_stats.bytes_uploaded,
        bytes_read = stats.bytes_read.load(Ordering::Relaxed),
        walk_ms,
        merge_ms,
        // walk_ms decomposition (ms) — attributes the walk so we don't guess:
        //   stat_ms      = symlink_metadata over the changed-path batch
        //   is_changed_ms = prev-snapshot prolly lookups (cold-cache faulting)
        //   import_ms    = read + CDC + BLAKE3 + upload (what #3 targets)
        stat_ms = stats.stat_ns.load(Ordering::Relaxed) / 1_000_000,
        is_changed_ms = stats.is_changed_ns.load(Ordering::Relaxed) / 1_000_000,
        import_ms = stats.import_ns.load(Ordering::Relaxed) / 1_000_000,
        // import_ms decomposition (the ~28 MB/s mystery — DIAGNOSTIC):
        //   imp_read_ms   = time pulling chunks off disk (I/O + contention)
        //   imp_hash_ms   = plaintext BLAKE3 (serial in the producer)
        //   imp_encode_ms = compress+upload of NON-deduped chunks (summed over
        //                   the in-flight window — can exceed wall)
        //   imp_chunks    = chunks seen; imp_dedup_hits = positionally reused
        //   imp_dedup_pct = hit-rate: ≈100 ⇒ the read is the cost (#3 fixes it);
        //                   ≪100 ⇒ dedup broken on reloaded prev (re-encoding)
        //   imp_bytes_read / imp_dedup_bytes = bytes pulled vs bytes reused
        imp_read_ms = import_now.read_ns / 1_000_000,
        imp_hash_ms = import_now.hash_ns / 1_000_000,
        imp_encode_ms = import_now.encode_ns / 1_000_000,
        imp_chunks = import_now.chunks,
        imp_dedup_hits = import_now.dedup_hits,
        imp_dedup_pct = (import_now.dedup_hits * 100)
            .checked_div(import_now.chunks)
            .unwrap_or(0),
        imp_bytes_read = import_now.bytes_read,
        imp_dedup_bytes = import_now.dedup_bytes,
        // retry_io retries this snap — the retry debug log is invisible at INFO,
        // so this is the decisive "are the slow sidecars failing+retrying?"
        // signal. ~3×files ⇒ retry-backoff storm; ~0 ⇒ a slow successful op.
        imp_retries = import_now.retries,
        // The two slices that were unaccounted after the chunker fix (read/hash/
        // encode ≈ 0 but import_ms ~66 s over 536 serial files): single-chunk
        // import_bytes (encode+upload, every one-blob sidecar) + per-file
        // build_tree_dedup (tree build + node uploads). These + read/hash/encode
        // should now sum to ≈ import_ms — the rest is open()+prev_get+overhead.
        imp_import_bytes_ms = import_now.import_bytes_ns / 1_000_000,
        imp_tree_ms = import_now.tree_ns / 1_000_000,
        "backup_incremental: snap complete"
    );

    let new_snapshot = Snapshot::new(
        root_hash,
        read_store,
        prev_snapshot.context().clone(),
        Some(root_plaintext_hash),
    );

    let mut stats = Arc::try_unwrap(stats).unwrap_or_else(|arc| BackupStats {
        files_changed: AtomicU64::new(arc.files_changed.load(Ordering::Relaxed)),
        files_skipped: AtomicU64::new(arc.files_skipped.load(Ordering::Relaxed)),
        files_errored: AtomicU64::new(arc.files_errored.load(Ordering::Relaxed)),
        dirs_processed: AtomicU64::new(arc.dirs_processed.load(Ordering::Relaxed)),
        symlinks_processed: AtomicU64::new(arc.symlinks_processed.load(Ordering::Relaxed)),
        special_skipped: AtomicU64::new(arc.special_skipped.load(Ordering::Relaxed)),
        bytes_uploaded: AtomicU64::new(arc.bytes_uploaded.load(Ordering::Relaxed)),
        bytes_read: AtomicU64::new(arc.bytes_read.load(Ordering::Relaxed)),
        stat_ns: AtomicU64::new(arc.stat_ns.load(Ordering::Relaxed)),
        is_changed_ns: AtomicU64::new(arc.is_changed_ns.load(Ordering::Relaxed)),
        import_ns: AtomicU64::new(arc.import_ns.load(Ordering::Relaxed)),
        merge: None,
    });
    stats.merge = Some(merge_stats);

    Ok(BackupResult {
        snapshot: Some((new_snapshot, stats)),
        was_cancelled: false,
    })
}

// ===========================================================================
// Metadata collection
// ===========================================================================

/// Build `SemanticMeta` from filesystem metadata.
///
/// In sync mode (`backup: false`): file type + mtime only.
/// In backup mode (`backup: true`): everything — permissions, ownership,
/// ctime, inode, device, nlink, xattrs, user/group names.
fn build_semantic(
    path: &Path,
    meta: &std::fs::Metadata,
    file_type: FileType,
    backup: bool,
) -> SemanticMeta {
    let mtime_secs = meta.mtime();
    let timestamp: Option<u32> = mtime_secs.try_into().ok();

    let unix = if backup {
        Some(build_unix_full(path, meta, file_type))
    } else {
        // Sync mode: only file type, no permissions/ownership.
        Some(UnixMetadata {
            file_type: Some(file_type),
            permissions: None,
            uid: None,
            gid: None,
            ctime: None,
            user: None,
            group: None,
            inode: None,
            device_id: None,
            nlink: None,
            extended_attributes: None,
        })
    };

    SemanticMeta {
        timestamp,
        timestamp_subsec_nanos: Some(meta.mtime_nsec() as u32),
        media_type: None,
        unix,
        warc: None,
    }
}

/// Build full Unix metadata for backup mode.
fn build_unix_full(path: &Path, meta: &std::fs::Metadata, file_type: FileType) -> UnixMetadata {
    let uid = meta.uid();
    let gid = meta.gid();

    UnixMetadata {
        file_type: Some(file_type),
        permissions: Some(meta.mode()),
        uid: Some(uid),
        gid: Some(gid),
        ctime: {
            let ct = meta.ctime();
            if ct >= 0 { Some(ct as u64) } else { None }
        },
        user: lookup_username(uid),
        group: lookup_groupname(gid),
        inode: Some(meta.ino()),
        device_id: Some(meta.dev()),
        nlink: Some(meta.nlink()),
        extended_attributes: read_xattrs(path),
    }
}

/// Read extended attributes from a path. Returns `None` on error or if empty.
fn read_xattrs(path: &Path) -> Option<Vec<ExtendedAttribute>> {
    // Use the non-deref variant to read xattrs on the symlink itself,
    // not the target. For regular files/dirs, this is the same.
    let names: Vec<_> = match xattr::list(path) {
        Ok(iter) => iter.collect(),
        Err(e) => {
            tracing::debug!(path = %path.display(), "failed to list xattrs: {e}");
            return None;
        }
    };

    if names.is_empty() {
        return None;
    }

    let mut attrs = Vec::with_capacity(names.len());
    for name in names {
        let name_str = name.to_string_lossy().into_owned();
        let value = match xattr::get(path, &name) {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!(
                    path = %path.display(),
                    xattr = %name_str,
                    "failed to read xattr: {e}"
                );
                None
            }
        };
        attrs.push(ExtendedAttribute {
            name: name_str,
            value,
        });
    }

    Some(attrs)
}

/// Resolve a UID to a username via `getpwuid_r`.
fn lookup_username(uid: u32) -> Option<String> {
    // Buffer for getpwuid_r. 1024 is generous for most systems.
    let mut buf = vec![0u8; 1024];
    let mut pwd: libc::passwd = unsafe { std::mem::zeroed() };
    let mut result: *mut libc::passwd = std::ptr::null_mut();

    let ret = unsafe {
        libc::getpwuid_r(
            uid,
            &mut pwd,
            buf.as_mut_ptr() as *mut libc::c_char,
            buf.len(),
            &mut result,
        )
    };

    if ret != 0 || result.is_null() {
        return None;
    }

    let name = unsafe { std::ffi::CStr::from_ptr(pwd.pw_name) };
    name.to_str().ok().map(|s| s.to_owned())
}

/// Resolve a GID to a group name via `getgrgid_r`.
fn lookup_groupname(gid: u32) -> Option<String> {
    let mut buf = vec![0u8; 1024];
    let mut grp: libc::group = unsafe { std::mem::zeroed() };
    let mut result: *mut libc::group = std::ptr::null_mut();

    let ret = unsafe {
        libc::getgrgid_r(
            gid,
            &mut grp,
            buf.as_mut_ptr() as *mut libc::c_char,
            buf.len(),
            &mut result,
        )
    };

    if ret != 0 || result.is_null() {
        return None;
    }

    let name = unsafe { std::ffi::CStr::from_ptr(grp.gr_name) };
    name.to_str().ok().map(|s| s.to_owned())
}

// ===========================================================================
// Parallel walker visitor
// ===========================================================================

/// Adapter that bridges `ignore`'s parallel walker into a `tokio::sync::mpsc`
/// channel. Each walker thread gets its own `Sender` clone; when all threads
/// finish, the channel closes naturally.
struct ParallelSender(tokio::sync::mpsc::Sender<ignore::DirEntry>);

impl ignore::ParallelVisitorBuilder<'_> for ParallelSender {
    fn build(&mut self) -> Box<dyn ignore::ParallelVisitor> {
        Box::new(ParallelSenderVisitor(self.0.clone()))
    }
}

struct ParallelSenderVisitor(tokio::sync::mpsc::Sender<ignore::DirEntry>);

impl ignore::ParallelVisitor for ParallelSenderVisitor {
    fn visit(&mut self, entry: Result<ignore::DirEntry, ignore::Error>) -> ignore::WalkState {
        match entry {
            Ok(dent) => {
                if self.0.blocking_send(dent).is_err() {
                    // Receiver dropped (e.g. backup cancelled) — stop walking.
                    ignore::WalkState::Quit
                } else {
                    ignore::WalkState::Continue
                }
            }
            Err(err) => {
                tracing::warn!("walk error: {err}");
                ignore::WalkState::Continue
            }
        }
    }
}

// ===========================================================================
// Change detection
// ===========================================================================

/// Compute the relative key for a path entry.
fn relative_key(path: &Path, base: &Path, is_dir: bool) -> anyhow::Result<String> {
    let rel = path
        .strip_prefix(base)
        .with_context(|| format!("{} is not under {}", path.display(), base.display()))?;

    let key = rel
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("path is not valid UTF-8: {}", rel.display()))?
        .to_string();

    if is_dir && !key.is_empty() {
        Ok(format!("{key}/"))
    } else {
        Ok(key)
    }
}

/// Check if an entry has changed compared to the previous snapshot.
///
/// Always checks size + mtime. Additionally checks inode + ctime when the
/// previous snapshot has them (i.e. it was created in backup mode).
async fn is_changed(
    key: &str,
    meta: &std::fs::Metadata,
    prev_snapshot: &Snapshot,
) -> anyhow::Result<bool> {
    let Some(prev_entry) = prev_snapshot.get(key).await? else {
        tracing::info!(
            key = key,
            reason = "new",
            size = meta.len(),
            "backup change detected"
        );
        return Ok(true); // New entry.
    };

    let prev_semantic = match &prev_entry.semantic {
        Some(s) => s,
        None => {
            tracing::info!(
                key = key,
                reason = "no_prev_semantic",
                size = meta.len(),
                "backup change detected"
            );
            return Ok(true); // No metadata to compare.
        }
    };

    // Size check (only meaningful for regular files).
    let prev_size = prev_entry.content.as_ref().map(|c| c.size).unwrap_or(0);
    if meta.is_file() && meta.len() != prev_size {
        tracing::info!(
            key = key,
            reason = "size",
            size_old = prev_size,
            size_new = meta.len(),
            "backup change detected"
        );
        return Ok(true);
    }

    // Mtime check.
    let mtime_secs = meta.mtime();
    let prev_ts = prev_semantic.timestamp.map(|t| t as i64);
    let prev_ns = prev_semantic.timestamp_subsec_nanos;

    if prev_ts != Some(mtime_secs) || prev_ns != Some(meta.mtime_nsec() as u32) {
        tracing::info!(
            key = key,
            reason = "mtime",
            size = meta.len(),
            mtime_old = ?prev_ts,
            mtime_new = mtime_secs,
            "backup change detected"
        );
        return Ok(true);
    }

    // Extended checks when previous snapshot has backup-mode metadata.
    if let Some(unix) = &prev_semantic.unix {
        // Inode check: detect file replacement via rename-into-place.
        if let Some(prev_inode) = unix.inode
            && meta.ino() != prev_inode
        {
            tracing::info!(
                key = key,
                reason = "inode",
                size = meta.len(),
                inode_old = prev_inode,
                inode_new = meta.ino(),
                "backup change detected"
            );
            return Ok(true);
        }

        // Ctime check: detect metadata-only changes (permissions, ownership).
        if let Some(prev_ctime) = unix.ctime {
            let ctime = meta.ctime();
            if ctime >= 0 && ctime as u64 != prev_ctime {
                tracing::info!(
                    key = key,
                    reason = "ctime",
                    size = meta.len(),
                    ctime_old = prev_ctime,
                    ctime_new = ctime as u64,
                    "backup change detected"
                );
                return Ok(true);
            }
        }
    }

    Ok(false)
}

// ===========================================================================
// Entry processing
// ===========================================================================

/// Retry a filesystem operation.
///
/// Not-found and permission-denied errors fail immediately. Other I/O errors
/// are retried with exponential backoff up to 3 times (max ~3 seconds).
///
/// Not-found is intrinsic, not transient: producers that unlink files between
/// `readdir` and `stat` (e.g. a segment compactor) race the walk by
/// design, and the file will be correctly absent from the next cycle's walk.
/// Retrying used to burn 100+200+400 ms of backoff per vanished file — at
/// sustained compaction churn (~700 vanished files/cycle, 2026-06-11) that was
/// ~8 min of a ~12 min publish cycle, dominating cadence.
async fn retry_io<F, Fut, T>(path: &Path, op_name: &str, mut f: F) -> anyhow::Result<Option<T>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::io::Result<T>>,
{
    let mut delay = Duration::from_millis(100);
    let mut attempts = 0;
    let max_attempts = 4; // Initial + 3 retries

    loop {
        attempts += 1;
        match f().await {
            Ok(v) => return Ok(Some(v)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Unlinked between readdir and here (compactor race): skip
                // without retry — see the doc comment.
                tracing::debug!(path = %path.display(), error = %e, "vanished before {}, skipping", op_name);
                return Ok(None);
            }
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                // Permission denied: don't retry, just warn and skip.
                tracing::warn!(path = %path.display(), error = %e, "permission denied, skipping {}", op_name);
                return Ok(None);
            }
            Err(e) if attempts >= max_attempts => {
                // Out of retries: warn and skip.
                tracing::warn!(path = %path.display(), error = %e, "failed to {} after {} attempts, skipping", op_name, attempts);
                return Ok(None);
            }
            Err(e) => {
                s5_fs_v2::import_stats::add_retry();
                tracing::debug!(path = %path.display(), error = %e, attempt = attempts, "retrying {}", op_name);
                tokio::time::sleep(delay).await;
                delay *= 2; // Exponential backoff
            }
        }
    }
}

/// Process a single directory entry from the walker.
async fn process_entry(
    path: &Path,
    source_dir: &Path,
    prev_snapshot: &Snapshot,
    blob_store: &(dyn BlobsWrite + Sync),
    overlay: &WritableOverlay,
    stats: &BackupStats,
    config: &BackupConfig,
) -> anyhow::Result<()> {
    // 1. Stat the entry
    // follow_symlinks: use metadata() which follows symlinks, so the
    // target's file type + content are imported (and the symlink path
    // itself appears as the vault key). Default: symlink_metadata()
    // which does not follow, so symlinks are stored as their target
    // path string via the ft.is_symlink() branch below.
    let stat_fn = if config.follow_symlinks {
        std::fs::metadata
    } else {
        std::fs::symlink_metadata
    };
    let t_stat = std::time::Instant::now();
    let meta_opt = retry_io(path, "stat", || async { stat_fn(path) }).await?;
    stats
        .stat_ns
        .fetch_add(t_stat.elapsed().as_nanos() as u64, Ordering::Relaxed);
    let Some(meta) = meta_opt else {
        stats.files_errored.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    };

    let ft = meta.file_type();

    if ft.is_dir() {
        let key = relative_key(path, source_dir, true)?;
        if key.is_empty() {
            return Ok(()); // Skip the root directory itself.
        }

        if !config.force_full && !is_changed(&key, &meta, prev_snapshot).await? {
            stats.dirs_processed.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // Directory: metadata-only entry, no content blob.
        let semantic = build_semantic(path, &meta, FileType::Directory, config.backup);
        let node_entry = NodeEntry {
            content: None,
            semantic: Some(semantic),
            child_context: None,
            tombstone: None,
        };

        overlay.put(key, node_entry);
        stats.dirs_processed.fetch_add(1, Ordering::Relaxed);
    } else if ft.is_symlink() {
        let key = relative_key(path, source_dir, false)?;

        if !config.force_full && !is_changed(&key, &meta, prev_snapshot).await? {
            stats.symlinks_processed.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // Symlink: store raw target bytes as blob content.
        let target_opt = retry_io(path, "read_link", || async { std::fs::read_link(path) }).await?;
        let Some(target) = target_opt else {
            stats.files_errored.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        };
        let target_bytes = target.as_os_str().as_bytes();

        let semantic = build_semantic(path, &meta, FileType::Symlink, config.backup);
        let node_entry = prev_snapshot
            .import_bytes(target_bytes, blob_store, Some(semantic))
            .await?;

        overlay.put(key, node_entry);
        stats.symlinks_processed.fetch_add(1, Ordering::Relaxed);
        stats
            .bytes_uploaded
            .fetch_add(target_bytes.len() as u64, Ordering::Relaxed);
    } else if ft.is_file() {
        let key = relative_key(path, source_dir, false)?;

        let t_chg = std::time::Instant::now();
        let changed = config.force_full || is_changed(&key, &meta, prev_snapshot).await?;
        stats
            .is_changed_ns
            .fetch_add(t_chg.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if !changed {
            stats.files_skipped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // Everything below (prev-chunk walk + open + read + CDC + BLAKE3 +
        // upload) is the "import" phase — the cost #3 (append-tail read)
        // targets. Time the whole span so `import_ms` vs `is_changed_ms` in
        // the snap log says whether walk_ms is read-bound or lookup-bound.
        let t_import = std::time::Instant::now();

        // Regular file: stream content, chunk, upload blobs.
        let semantic = build_semantic(path, &meta, FileType::Regular, config.backup);

        // First-match route lookup. None = use snapshot's default ctx.
        let route = config.routes.iter().find(|r| r.glob.is_match(key.as_str()));

        // Per-chunk dedup: walk the prev entry's ByteStream tree once
        // up front, then pass the chunk list to `import_stream*` so each
        // new chunk can short-circuit to the prev entry when plaintext
        // BLAKE3 matches. Skips compression + blob upload on hits.
        // Empty Vec when there is no prev entry, prev is a single Leaf,
        // or the walk fails — all of which fall back to fresh import.
        //
        // For an APPEND-ONLY route (#3, `r.append_only`) with Fixed chunking,
        // `prev_chunks` also seeds `import_file_append`, which reuses the
        // unchanged full-chunk prefix by reference and reads ONLY the appended
        // tail — instead of re-reading + re-hashing the whole file (the 1.4 GB
        // dids_plc.bin was the bulk of the snap's import time, measured
        // 2026-06-17 s5). Plain dedup only skips the upload, not the read.
        // Split the import into prev-tree-lookup vs the actual import call, so a
        // single 52-byte `.eseg.didx` slow line says WHERE its ~2.66 s goes
        // (prolly-tree walk vs blob upload/encode) — DIAGNOSTIC 2026-06-18 s5.
        let t_prev = std::time::Instant::now();
        let prev_chunks: Vec<s5_fs_v2::node::NodeEntry> = match prev_snapshot.get(&key).await {
            Ok(Some(prev_entry)) => prev_snapshot
                .collect_byte_stream_chunks(&prev_entry)
                .await
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        let prev_get_ms = t_prev.elapsed().as_millis() as u64;

        // (entry, content_len, bytes_actually_read). The append fast path reads
        // only the tail; every other path reads the whole file (bytes_read ==
        // content_len there).
        let t_imp_call = std::time::Instant::now();
        let import_opt = retry_io(path, "import", || async {
            let (entry, bytes_read) = match route {
                // #3 append-aware: only when the route is hinted append-only
                // AND we have a prev to reuse a prefix from. import_file_append
                // self-guards (non-Fixed / no full prefix / shrank → full read)
                // and reports actual bytes read.
                Some(r) if r.append_only && !prev_chunks.is_empty() => prev_snapshot
                    .import_file_append_with_override(
                        path,
                        blob_store,
                        Some(semantic.clone()),
                        &r.override_ctx,
                        &prev_chunks,
                    )
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?,
                Some(r) => {
                    let file = tokio::fs::File::open(path).await?;
                    let entry = prev_snapshot
                        .import_stream_with_override_and_prev(
                            file,
                            blob_store,
                            Some(semantic.clone()),
                            &r.override_ctx,
                            &prev_chunks,
                        )
                        .await
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                    let n = entry.content.as_ref().map(|c| c.size).unwrap_or(0);
                    (entry, n)
                }
                None => {
                    let file = tokio::fs::File::open(path).await?;
                    let entry = prev_snapshot
                        .import_stream_with_prev(
                            file,
                            blob_store,
                            Some(semantic.clone()),
                            &prev_chunks,
                        )
                        .await
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                    let n = entry.content.as_ref().map(|c| c.size).unwrap_or(0);
                    (entry, n)
                }
            };

            // Get the actual size imported (could differ slightly from meta if file changed)
            let size = entry.content.as_ref().map(|c| c.size).unwrap_or(0);
            Ok((entry, size, bytes_read))
        })
        .await?;

        let Some((node_entry, content_len, bytes_read)) = import_opt else {
            stats.files_errored.fetch_add(1, Ordering::Relaxed);
            stats
                .import_ns
                .fetch_add(t_import.elapsed().as_nanos() as u64, Ordering::Relaxed);
            return Ok(());
        };

        overlay.put(key, node_entry);
        stats.files_changed.fetch_add(1, Ordering::Relaxed);
        stats
            .bytes_uploaded
            .fetch_add(content_len, Ordering::Relaxed);
        // Bytes actually read from disk this import — the appended tail on the
        // #3 append-aware path, the whole file otherwise. `bytes_read ≪
        // bytes_processed` (sum of content_len) is the signal #3 engaged on the
        // big append-only files (interner/ledger).
        stats.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
        let import_elapsed = t_import.elapsed();
        stats
            .import_ns
            .fetch_add(import_elapsed.as_nanos() as u64, Ordering::Relaxed);
        // Per-file attribution for the dominant snap cost: which single file
        // owns the wall-clock? (imports run concurrently via buffer_unordered,
        // so the slowest single file is the critical path.) Gated at 2 s so only
        // the heavy files (dids_plc.bin, active segments) log — no flood.
        // DIAGNOSTIC (2026-06-17 s5); drop with the import_stats wiring.
        let import_ms = import_elapsed.as_millis() as u64;
        if import_ms >= 2000 {
            tracing::info!(
                path = %path.display(),
                import_ms,
                // Split: prev_get_ms = prolly-tree lookup of the prev entry +
                // its chunk walk; import_call_ms = the retry_io(open+read+
                // encode+upload) span. For a 52-byte sidecar these localize the
                // ~2.66 s to the tree walk vs the blob store.
                prev_get_ms,
                import_call_ms = t_imp_call.elapsed().as_millis() as u64,
                content_len,
                bytes_read,
                "snap: slow file import"
            );
        }
    } else {
        // Block device, char device, fifo, socket — skip.
        tracing::debug!(path = %path.display(), "skipping special file");
        stats.special_skipped.fetch_add(1, Ordering::Relaxed);
    }

    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use s5_core::blob::BlobStore;
    use s5_fs_v2::node::{
        BlobPipeline, CompressionStrategy, FileChunkingStrategy, TraversalContext,
    };
    use s5_store_memory::MemoryStore;

    fn glob(pat: &str) -> globset::GlobMatcher {
        globset::Glob::new(pat).unwrap().compile_matcher()
    }

    fn route(pat: &str, ctx: TraversalContext) -> PipelineRoute {
        PipelineRoute {
            glob: glob(pat),
            override_ctx: ctx,
            append_only: false,
        }
    }

    fn store() -> Arc<BlobStore> {
        Arc::new(BlobStore::new(MemoryStore::new()))
    }

    /// Build a small directory tree with two shape classes. Files under
    /// `segments/` should hit the zstd route; files under `ledger/`
    /// should hit the uncompressed route. Verify child_context on each.
    #[tokio::test]
    async fn routes_stamp_per_subtree_overrides() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path();
        std::fs::create_dir_all(src.join("segments")).unwrap();
        std::fs::create_dir_all(src.join("ledger")).unwrap();
        // Use compressible data so zstd actually runs (would skip otherwise
        // under skip_when_unhelpful which we leave off here).
        std::fs::write(src.join("segments").join("a.seg"), vec![0xAA; 4096]).unwrap();
        std::fs::write(src.join("ledger").join("b.ril"), vec![0xBB; 4096]).unwrap();

        let s = store();
        let prev = s5_fs_v2::snapshot::Snapshot::empty(
            s.clone() as Arc<dyn s5_core::BlobsRead>,
            TraversalContext::default(),
        );

        let routes = vec![
            // Match files under segments/ → force zstd + 8 KiB chunking.
            route(
                "segments/**",
                TraversalContext {
                    keys: None,
                    leaf: Some(BlobPipeline {
                        compression: Some(CompressionStrategy::Zstd),
                        padding: None,
                        encryption: None,
                        skip_when_unhelpful: None,
                    }),
                    node: None,
                    chunking: Some(FileChunkingStrategy::Fixed { chunk_size: 8192 }),
                },
            ),
            // Match files under ledger/ → force Uncompressed + None chunking.
            route(
                "ledger/**",
                TraversalContext {
                    keys: None,
                    leaf: Some(BlobPipeline {
                        compression: Some(CompressionStrategy::Uncompressed),
                        padding: None,
                        encryption: None,
                        skip_when_unhelpful: None,
                    }),
                    node: None,
                    chunking: Some(FileChunkingStrategy::None),
                },
            ),
        ];

        let cfg = BackupConfig {
            routes: routes.clone(),
            ..Default::default()
        };
        let walker = WalkBuilder::new(src);
        let result = backup(
            src,
            &prev,
            &*s,
            &*s,
            s.clone() as Arc<dyn s5_core::BlobsRead>,
            &cfg,
            walker,
            None,
            None,
        )
        .await
        .unwrap();
        let (snap, _stats) = result.snapshot.expect("snapshot produced");

        // Walk and collect entries we care about.
        let mut by_key: std::collections::HashMap<String, NodeEntry> =
            std::collections::HashMap::new();
        let mut walked = std::pin::pin!(snap.walk());
        while let Some(item) = walked.next().await {
            let (key, entry) = item.unwrap();
            by_key.insert(key, entry);
        }

        // segments/a.seg → zstd override stamped on child_context.
        let seg = by_key
            .get("segments/a.seg")
            .expect("segments/a.seg not found");
        let cc = seg
            .child_context
            .as_ref()
            .expect("segments entry should have child_context");
        assert_eq!(
            cc.leaf.as_ref().unwrap().compression,
            Some(CompressionStrategy::Zstd),
            "segments route should stamp Zstd"
        );
        assert!(matches!(
            cc.chunking,
            Some(FileChunkingStrategy::Fixed { chunk_size: 8192 })
        ));

        // ledger/b.ril → uncompressed override stamped.
        let led = by_key.get("ledger/b.ril").expect("ledger/b.ril not found");
        let cc = led
            .child_context
            .as_ref()
            .expect("ledger entry should have child_context");
        assert_eq!(
            cc.leaf.as_ref().unwrap().compression,
            Some(CompressionStrategy::Uncompressed),
            "ledger route should stamp Uncompressed"
        );
        assert!(matches!(cc.chunking, Some(FileChunkingStrategy::None)));

        // Round-trip both files via the parent snapshot — exercises the
        // read-side cascade.
        let seg_bytes = snap.export_bytes(seg).await.unwrap();
        assert_eq!(seg_bytes.len(), 4096);
        assert!(seg_bytes.iter().all(|&b| b == 0xAA));

        let led_bytes = snap.export_bytes(led).await.unwrap();
        assert_eq!(led_bytes.len(), 4096);
        assert!(led_bytes.iter().all(|&b| b == 0xBB));
    }

    /// `backup_incremental` applies ONLY the given paths: changed files are
    /// re-imported (upsert), vanished files tombstoned, and files NOT in the
    /// path set are carried forward untouched from the baseline snapshot.
    #[tokio::test]
    async fn incremental_upserts_changed_tombstones_vanished_keeps_rest() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path();
        std::fs::create_dir_all(src.join("segments")).unwrap();
        std::fs::write(src.join("segments").join("a.seg"), vec![0xAA; 2048]).unwrap();
        std::fs::write(src.join("segments").join("b.seg"), vec![0xBB; 2048]).unwrap();
        std::fs::write(src.join("segments").join("keep.seg"), vec![0xEE; 1024]).unwrap();

        let s = store();
        let read = s.clone() as Arc<dyn s5_core::BlobsRead>;
        let prev = s5_fs_v2::snapshot::Snapshot::empty(read.clone(), TraversalContext::default());
        let cfg = BackupConfig::default();

        // Baseline full backup → a.seg + b.seg + keep.seg.
        let base = backup(
            src,
            &prev,
            &*s,
            &*s,
            read.clone(),
            &cfg,
            WalkBuilder::new(src),
            None,
            None,
        )
        .await
        .unwrap()
        .snapshot
        .expect("baseline snapshot produced")
        .0;

        // Mutate: change a.seg, delete b.seg, add c.seg; leave keep.seg alone
        // AND out of the changed-path set.
        std::fs::write(src.join("segments").join("a.seg"), vec![0xCC; 4096]).unwrap();
        std::fs::remove_file(src.join("segments").join("b.seg")).unwrap();
        std::fs::write(src.join("segments").join("c.seg"), vec![0xDD; 2048]).unwrap();

        let changed = vec![
            src.join("segments").join("a.seg"),
            src.join("segments").join("b.seg"),
            src.join("segments").join("c.seg"),
        ];

        let snap = backup_incremental(src, &changed, &base, &*s, &*s, read.clone(), &cfg, None)
            .await
            .unwrap()
            .snapshot
            .expect("incremental snapshot produced")
            .0;

        let mut by_key: std::collections::HashMap<String, NodeEntry> =
            std::collections::HashMap::new();
        let mut walked = std::pin::pin!(snap.walk());
        while let Some(item) = walked.next().await {
            let (key, entry) = item.unwrap();
            by_key.insert(key, entry);
        }

        // a.seg upserted → re-imported with the new content.
        let a = by_key.get("segments/a.seg").expect("a.seg should remain");
        assert!(a.tombstone.is_none(), "a.seg should be live");
        let a_bytes = snap.export_bytes(a).await.unwrap();
        assert_eq!(a_bytes.len(), 4096);
        assert!(
            a_bytes.iter().all(|&b| b == 0xCC),
            "a.seg holds new content"
        );

        // c.seg added.
        let c = by_key.get("segments/c.seg").expect("c.seg should be added");
        assert!(c.tombstone.is_none(), "c.seg should be live");
        assert_eq!(snap.export_bytes(c).await.unwrap().len(), 2048);

        // b.seg vanished → gone (absent) or tombstoned.
        let b = by_key.get("segments/b.seg");
        assert!(
            b.is_none_or(|e| e.tombstone.is_some()),
            "b.seg should be gone, got {b:?}"
        );

        // keep.seg was NOT in changed_paths → carried forward untouched.
        let keep = by_key
            .get("segments/keep.seg")
            .expect("keep.seg should survive incremental");
        assert!(keep.tombstone.is_none(), "keep.seg should be live");
        let keep_bytes = snap.export_bytes(keep).await.unwrap();
        assert_eq!(keep_bytes.len(), 1024);
        assert!(keep_bytes.iter().all(|&b| b == 0xEE));
    }

    /// Regression guard for the 2026-06-17 session-5 incident: the merge
    /// MUST receive the sparse DIFF, not the merged-view overlay. We pass
    /// `MapLayer::new(overlay.take())`; if a refactor reverts to passing the
    /// `WritableOverlay` (whose `scan` = base ∪ diff), the merge collects the
    /// whole tree as "changes" → `entries_reused` collapses to 0 and
    /// `entries_changed` ≈ the whole tree (a full O(corpus) re-fold every
    /// cycle). Asserting `entries_reused > entries_changed` for a 1-of-many
    /// change is the cheap, env-independent tripwire (holds on the FULL merge
    /// path too, so it needs no `S5_STRUCTURAL_MERGE`).
    #[tokio::test]
    async fn incremental_passes_diff_not_merged_view() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path();
        std::fs::create_dir_all(src.join("segments")).unwrap();
        // A base with MANY files so "reused" vs "changed" is unambiguous.
        for i in 0..50u32 {
            std::fs::write(
                src.join("segments").join(format!("f{i:03}.seg")),
                vec![i as u8; 256],
            )
            .unwrap();
        }

        let s = store();
        let read = s.clone() as Arc<dyn s5_core::BlobsRead>;
        let prev = s5_fs_v2::snapshot::Snapshot::empty(read.clone(), TraversalContext::default());
        let cfg = BackupConfig::default();

        let base = backup(
            src,
            &prev,
            &*s,
            &*s,
            read.clone(),
            &cfg,
            WalkBuilder::new(src),
            None,
            None,
        )
        .await
        .unwrap()
        .snapshot
        .expect("baseline")
        .0;

        // Change exactly ONE file.
        std::fs::write(src.join("segments").join("f007.seg"), vec![0xCC; 512]).unwrap();
        let changed = vec![src.join("segments").join("f007.seg")];

        let (_, stats) = backup_incremental(src, &changed, &base, &*s, &*s, read, &cfg, None)
            .await
            .unwrap()
            .snapshot
            .expect("incremental");
        let merge = stats.merge.expect("merge stats");

        // The diff carried ONE upsert; the rest of the base is carried forward.
        // The anti-regression signal is `entries_changed` staying tiny — the
        // overlay-as-changes bug re-fed the WHOLE tree as changes
        // (entries_reused=0, entries_changed≈51, a full O(corpus) refold).
        //
        // NB on the counts (see `s5_fs_v2::persist` § entries_reused): reuse is
        // counted at ENTRY level only inside the leaf that was actually
        // rebuilt (the changed key's siblings); subtrees reused wholesale by
        // link are NOT entry-enumerated (doing so would defeat the O(changed)
        // merge). So for a 1-of-50 change the counts are ~changed=1,
        // reused=<siblings-in-the-rebuilt-leaf> — not reused≈49. The invariant
        // is therefore "changes are tiny and reuse dominates", not a fixed
        // reuse floor.
        assert!(
            merge.entries_changed <= 2,
            "incremental refold regression: expected ~1 changed entry, got \
             changed={} reused={} (the whole tree was re-fed as changes?)",
            merge.entries_changed,
            merge.entries_reused
        );
        assert!(
            merge.entries_reused > merge.entries_changed,
            "expected entry-level reuse to dominate the single change, got \
             reused={} changed={}",
            merge.entries_reused,
            merge.entries_changed
        );
    }

    /// #3 wiring guard: an `append_only` + Fixed-chunked route must read only
    /// the appended TAIL on an incremental, not the whole file. Builds a
    /// multi-chunk append-only file, grows it by a small tail, and asserts
    /// `stats.bytes_read ≪ file size` (the full prefix chunks were reused by
    /// reference) — and that the grown file still round-trips. Guards against a
    /// refactor silently dropping `route.append_only` or the
    /// `import_file_append` dispatch.
    #[tokio::test]
    async fn incremental_append_only_reads_only_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path();
        std::fs::create_dir_all(src.join("interner_packs")).unwrap();
        let file = src.join("interner_packs").join("dids.bin");
        let chunk_size = 64 * 1024u32;
        let base_len = 5 * chunk_size as usize + 100; // 5 full chunks + partial
        std::fs::write(&file, vec![0xAB; base_len]).unwrap();

        let s = store();
        let read = s.clone() as Arc<dyn s5_core::BlobsRead>;
        let prev = s5_fs_v2::snapshot::Snapshot::empty(read.clone(), TraversalContext::default());

        // append_only route over interner_packs with Fixed chunking.
        let octx = TraversalContext {
            chunking: Some(s5_fs_v2::node::FileChunkingStrategy::Fixed { chunk_size }),
            ..Default::default()
        };
        let cfg = BackupConfig {
            routes: vec![PipelineRoute {
                glob: glob("interner_packs/**"),
                override_ctx: octx,
                append_only: true,
            }],
            ..BackupConfig::default()
        };

        let base = backup(
            src,
            &prev,
            &*s,
            &*s,
            read.clone(),
            &cfg,
            WalkBuilder::new(src),
            None,
            None,
        )
        .await
        .unwrap()
        .snapshot
        .expect("base")
        .0;

        // Append-only growth: a small tail.
        {
            use std::io::Write as _;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&file)
                .unwrap();
            f.write_all(&[0xCD; 1234]).unwrap();
        }

        let changed = vec![file.clone()];
        let (snap, stats) = backup_incremental(src, &changed, &base, &*s, &*s, read, &cfg, None)
            .await
            .unwrap()
            .snapshot
            .expect("incr");

        let total = (base_len + 1234) as u64;
        let read_bytes = stats.bytes_read.load(Ordering::Relaxed);
        assert!(
            read_bytes < total / 2,
            "append-only import must read ≪ the whole file (read={read_bytes} total={total})"
        );

        // Correctness: the grown file round-trips to the exact bytes.
        let mut by_key: std::collections::HashMap<String, NodeEntry> =
            std::collections::HashMap::new();
        let mut walked = std::pin::pin!(snap.walk());
        while let Some(item) = walked.next().await {
            let (k, e) = item.unwrap();
            by_key.insert(k, e);
        }
        let e = by_key
            .get("interner_packs/dids.bin")
            .expect("file present in snapshot");
        assert_eq!(
            snap.export_bytes(e).await.unwrap().len(),
            total as usize,
            "grown file must round-trip"
        );
    }

    /// The incremental path MUST honor the source exclude matcher, symmetric
    /// with the full walk. Regression guard for the watch-mode leak: excluded
    /// subtrees (crawl/, *.mphf sidecars) were republished into the vault on
    /// every FS event because `backup_incremental` only filtered on
    /// `starts_with(source_dir)` and ignored `config.exclude`.
    #[tokio::test]
    async fn incremental_honors_source_excludes() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path();
        std::fs::create_dir_all(src.join("segments")).unwrap();
        std::fs::create_dir_all(src.join("crawl")).unwrap();
        std::fs::create_dir_all(src.join("rindex").join("app.bsky.feed.post")).unwrap();
        std::fs::write(src.join("segments").join("a.seg"), vec![0xAA; 2048]).unwrap();

        let s = store();
        let read = s.clone() as Arc<dyn s5_core::BlobsRead>;
        let base = s5_fs_v2::snapshot::Snapshot::empty(read.clone(), TraversalContext::default());

        // Same matcher the node task builds: one `!pattern` ignore rule per
        // source exclude. Mirrors an ingest deployment's `crawl/**` +
        // `rindex/**/*.mphf` excludes.
        let mut ob = ignore::overrides::OverrideBuilder::new(src);
        ob.add("!crawl/**").unwrap();
        ob.add("!rindex/**/*.mphf").unwrap();
        let cfg = BackupConfig {
            exclude: Some(ob.build().unwrap()),
            ..Default::default()
        };

        // Write an included file + excluded files, and hand ALL of them to the
        // incremental path as "changed" — exactly what the watch loop forwards.
        std::fs::write(src.join("crawl").join("tracker.snap"), vec![0xBB; 4096]).unwrap();
        std::fs::write(
            src.join("rindex")
                .join("app.bsky.feed.post")
                .join("0001.ridx.mphf"),
            vec![0xCC; 4096],
        )
        .unwrap();
        std::fs::write(
            src.join("rindex")
                .join("app.bsky.feed.post")
                .join("0001.ridx"),
            vec![0xDD; 4096],
        )
        .unwrap();
        let changed = vec![
            src.join("segments").join("a.seg"),
            src.join("crawl").join("tracker.snap"),
            src.join("rindex")
                .join("app.bsky.feed.post")
                .join("0001.ridx.mphf"),
            src.join("rindex")
                .join("app.bsky.feed.post")
                .join("0001.ridx"),
        ];

        let snap = backup_incremental(src, &changed, &base, &*s, &*s, read.clone(), &cfg, None)
            .await
            .unwrap()
            .snapshot
            .expect("incremental snapshot produced")
            .0;

        let mut live = std::collections::HashSet::new();
        let mut walked = std::pin::pin!(snap.walk());
        while let Some(item) = walked.next().await {
            let (key, entry) = item.unwrap();
            if entry.tombstone.is_none() {
                live.insert(key);
            }
        }

        // Included paths published.
        assert!(
            live.contains("segments/a.seg"),
            "included .seg published; got {live:?}"
        );
        assert!(
            live.contains("rindex/app.bsky.feed.post/0001.ridx"),
            "included .ridx published; got {live:?}"
        );
        // Excluded paths must NOT leak into the vault.
        assert!(
            !live.contains("crawl/tracker.snap"),
            "crawl/ must be excluded on the incremental path; got {live:?}"
        );
        assert!(
            !live.contains("rindex/app.bsky.feed.post/0001.ridx.mphf"),
            "*.mphf sidecar must be excluded on the incremental path; got {live:?}"
        );
    }

    /// First-match-wins: when two routes overlap, the earlier one
    /// applies. This is the documented `.gitignore`-style behaviour.
    #[tokio::test]
    async fn first_match_wins() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path();
        std::fs::create_dir_all(src.join("segments")).unwrap();
        std::fs::write(src.join("segments").join("x.seg"), vec![0u8; 1024]).unwrap();

        let s = store();
        let prev = s5_fs_v2::snapshot::Snapshot::empty(
            s.clone() as Arc<dyn s5_core::BlobsRead>,
            TraversalContext::default(),
        );

        let zstd_ctx = TraversalContext {
            keys: None,
            leaf: Some(BlobPipeline {
                compression: Some(CompressionStrategy::Zstd),
                padding: None,
                encryption: None,
                skip_when_unhelpful: None,
            }),
            node: None,
            chunking: None,
        };
        let uncompressed_ctx = TraversalContext {
            keys: None,
            leaf: Some(BlobPipeline {
                compression: Some(CompressionStrategy::Uncompressed),
                padding: None,
                encryption: None,
                skip_when_unhelpful: None,
            }),
            node: None,
            chunking: None,
        };

        // First route ("segments/**") matches before the second
        // ("**") — the first wins.
        let cfg = BackupConfig {
            routes: vec![
                route("segments/**", zstd_ctx),
                route("**", uncompressed_ctx),
            ],
            ..Default::default()
        };
        let walker = WalkBuilder::new(src);
        let result = backup(
            src,
            &prev,
            &*s,
            &*s,
            s.clone() as Arc<dyn s5_core::BlobsRead>,
            &cfg,
            walker,
            None,
            None,
        )
        .await
        .unwrap();
        let (snap, _stats) = result.snapshot.unwrap();

        let mut walked = std::pin::pin!(snap.walk());
        while let Some(item) = walked.next().await {
            let (key, entry) = item.unwrap();
            if key == "segments/x.seg" {
                let cc = entry.child_context.as_ref().unwrap();
                assert_eq!(
                    cc.leaf.as_ref().unwrap().compression,
                    Some(CompressionStrategy::Zstd),
                    "first-match-wins: segments/** should win over **"
                );
                return;
            }
        }
        panic!("segments/x.seg not walked");
    }

    /// Files matching no route fall through to the snapshot's default
    /// ctx — no child_context stamped.
    #[tokio::test]
    async fn no_match_no_override_stamped() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path();
        std::fs::write(src.join("loose.txt"), b"hello").unwrap();

        let s = store();
        let prev = s5_fs_v2::snapshot::Snapshot::empty(
            s.clone() as Arc<dyn s5_core::BlobsRead>,
            TraversalContext::default(),
        );

        // Only routes matching segments/ — nothing for loose.txt.
        let cfg = BackupConfig {
            routes: vec![route(
                "segments/**",
                TraversalContext {
                    keys: None,
                    leaf: Some(BlobPipeline {
                        compression: Some(CompressionStrategy::Zstd),
                        padding: None,
                        encryption: None,
                        skip_when_unhelpful: None,
                    }),
                    node: None,
                    chunking: None,
                },
            )],
            ..Default::default()
        };
        let walker = WalkBuilder::new(src);
        let result = backup(
            src,
            &prev,
            &*s,
            &*s,
            s.clone() as Arc<dyn s5_core::BlobsRead>,
            &cfg,
            walker,
            None,
            None,
        )
        .await
        .unwrap();
        let (snap, _stats) = result.snapshot.unwrap();

        let mut walked = std::pin::pin!(snap.walk());
        while let Some(item) = walked.next().await {
            let (key, entry) = item.unwrap();
            if key == "loose.txt" {
                assert!(
                    entry.child_context.is_none(),
                    "no-match files should not get a child_context override stamped"
                );
                let bytes = snap.export_bytes(&entry).await.unwrap();
                assert_eq!(&bytes[..], b"hello");
                return;
            }
        }
        panic!("loose.txt not walked");
    }

    /// Collect the set of file/dir keys a snapshot's `walk()` yields.
    async fn keys_of(snap: &Snapshot) -> std::collections::HashSet<String> {
        let mut set = std::collections::HashSet::new();
        let mut w = std::pin::pin!(snap.walk());
        while let Some(item) = w.next().await {
            set.insert(item.unwrap().0);
        }
        set
    }

    /// Two backups: between them a file is deleted on disk and another added.
    /// With `detect_deletions`, the vanished file is tombstoned (gone from the
    /// new snapshot) while the survivor and the newcomer are present.
    #[tokio::test]
    async fn detect_deletions_tombstones_vanished_files() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path();
        std::fs::create_dir_all(src.join("segments")).unwrap();
        std::fs::write(src.join("segments/a.seg"), vec![0xAA; 1024]).unwrap();
        std::fs::write(src.join("segments/b.seg"), vec![0xBB; 1024]).unwrap();

        let s = store();
        let empty = Snapshot::empty(
            s.clone() as Arc<dyn s5_core::BlobsRead>,
            TraversalContext::default(),
        );
        let cfg = BackupConfig {
            detect_deletions: true,
            ..Default::default()
        };

        let r1 = backup(
            src,
            &empty,
            &*s,
            &*s,
            s.clone() as Arc<dyn s5_core::BlobsRead>,
            &cfg,
            WalkBuilder::new(src),
            None,
            None,
        )
        .await
        .unwrap();
        let (snap1, _) = r1.snapshot.expect("snap1");
        assert!(keys_of(&snap1).await.contains("segments/a.seg"));

        // a.seg vanishes (compaction); c.seg appears.
        std::fs::remove_file(src.join("segments/a.seg")).unwrap();
        std::fs::write(src.join("segments/c.seg"), vec![0xCC; 1024]).unwrap();

        let r2 = backup(
            src,
            &snap1,
            &*s,
            &*s,
            s.clone() as Arc<dyn s5_core::BlobsRead>,
            &cfg,
            WalkBuilder::new(src),
            None,
            None,
        )
        .await
        .unwrap();
        let (snap2, _) = r2.snapshot.expect("snap2");
        let k2 = keys_of(&snap2).await;
        assert!(
            !k2.contains("segments/a.seg"),
            "deleted file must be tombstoned out of the new snapshot"
        );
        assert!(k2.contains("segments/b.seg"), "survivor must be kept");
        assert!(k2.contains("segments/c.seg"), "new file must be added");
    }

    /// Default (additive) mode: a vanished file rides forward — the historical
    /// behaviour, and the regression guard for append-only s5 callers.
    #[tokio::test]
    async fn additive_mode_keeps_vanished_files() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path();
        std::fs::create_dir_all(src.join("segments")).unwrap();
        std::fs::write(src.join("segments/a.seg"), vec![0xAA; 1024]).unwrap();

        let s = store();
        let empty = Snapshot::empty(
            s.clone() as Arc<dyn s5_core::BlobsRead>,
            TraversalContext::default(),
        );
        let cfg = BackupConfig::default(); // detect_deletions = false

        let r1 = backup(
            src,
            &empty,
            &*s,
            &*s,
            s.clone() as Arc<dyn s5_core::BlobsRead>,
            &cfg,
            WalkBuilder::new(src),
            None,
            None,
        )
        .await
        .unwrap();
        let (snap1, _) = r1.snapshot.expect("snap1");

        std::fs::remove_file(src.join("segments/a.seg")).unwrap();

        let r2 = backup(
            src,
            &snap1,
            &*s,
            &*s,
            s.clone() as Arc<dyn s5_core::BlobsRead>,
            &cfg,
            WalkBuilder::new(src),
            None,
            None,
        )
        .await
        .unwrap();
        // Empty overlay (nothing walked, no deletions) → no new snapshot;
        // the previous one stands and still carries a.seg.
        let snap2 = r2.snapshot.map(|(s, _)| s).unwrap_or(snap1);
        assert!(
            keys_of(&snap2).await.contains("segments/a.seg"),
            "additive mode must retain the vanished file"
        );
    }
}
