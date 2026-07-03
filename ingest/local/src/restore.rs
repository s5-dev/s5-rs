//! Restore a snapshot to a local filesystem directory.
//!
//! Supports two modes controlled by `RestoreConfig.backup`:
//!
//! - **Sync mode** (`backup: false`, default): writes files and directories,
//!   restores mtime only.
//!
//! - **Backup mode** (`backup: true`): restores everything available in the
//!   snapshot — permissions, ownership (uid/gid via `lchown`), extended
//!   attributes, and timestamps.

use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use s5_fs_v2::node::{FileType, NodeEntry};
use s5_fs_v2::snapshot::Snapshot;

/// Files (and symlinks) restored concurrently. Restore is download-bound, so
/// overlapping per-file fetches recovers the latency a serial walk wasted — the
/// big win for many-small-files snapshots (each small file is a single
/// download). Composes with the per-file chunk concurrency in `export_bytes`;
/// the working set stays small (transient download buffers, no staging).
const RESTORE_FILE_CONCURRENCY: usize = 8;

/// Configuration for a restore operation.
#[derive(Default)]
pub struct RestoreConfig {
    /// Backup mode: restore full metadata (permissions, ownership, xattrs).
    ///
    /// When false (sync mode), only file content and mtime are restored.
    /// When true, also restores permissions, uid/gid (requires CAP_CHOWN),
    /// and extended attributes.
    pub backup: bool,

    /// Restore only this subtree of the snapshot (D20 `vault:path`).
    ///
    /// A `/`-separated path prefix within the snapshot tree (no leading or
    /// trailing slash required — both are trimmed). The subtree's contents are
    /// **re-rooted** so they land directly under `target_dir` — i.e.
    /// `subtree = "sub"` writes `sub/beta.txt` to `target_dir/beta.txt`, the
    /// inverse of `backup SRC vault:sub`. When the prefix names a single file,
    /// that file is restored under its basename. `None` restores the whole
    /// tree.
    pub subtree: Option<String>,
}

/// Statistics from a restore operation.
#[derive(Debug, Default)]
pub struct RestoreStats {
    /// Regular files restored.
    pub files_restored: AtomicU64,
    /// Directories created.
    pub dirs_created: AtomicU64,
    /// Symlinks created.
    pub symlinks_created: AtomicU64,
    /// Special files skipped.
    pub special_skipped: AtomicU64,
    /// Total bytes written (plaintext).
    pub bytes_written: AtomicU64,
}

/// Restore a snapshot to a local directory.
///
/// Walks the snapshot tree and writes every entry to `target_dir`,
/// restoring metadata according to `config`.
///
/// Directories are created first (via the walk's sorted order — `"src/"`
/// comes before `"src/main.rs"`), so parent directories always exist
/// before their children.
pub async fn restore(
    snapshot: &Snapshot,
    target_dir: &Path,
    config: &RestoreConfig,
) -> anyhow::Result<RestoreStats> {
    let stats = RestoreStats::default();

    tokio::fs::create_dir_all(target_dir)
        .await
        .with_context(|| format!("creating target dir {}", target_dir.display()))?;

    let mut walk = std::pin::pin!(snapshot.walk());

    // Collect directory entries so we can restore their metadata after
    // all children have been written (writing children updates dir mtime).
    let mut dir_entries: Vec<(String, NodeEntry)> = Vec::new();

    // Files and symlinks are download-bound, so restore them concurrently — the
    // big win for many small files. Directories are created inline in walk order,
    // so a parent always exists before its children are dispatched.
    let mut inflight = FuturesUnordered::new();

    // Normalised subtree prefix (leading/trailing slashes trimmed), if any.
    let subtree = config
        .subtree
        .as_deref()
        .map(|s| s.trim_matches('/'))
        .filter(|s| !s.is_empty());

    while let Some(result) = walk.next().await {
        let (raw_key, entry) = result?;

        let file_type = entry
            .semantic
            .as_ref()
            .and_then(|s| s.unix.as_ref())
            .and_then(|u| u.file_type.as_ref());

        // Subtree filter + re-root: keep only entries under the prefix and
        // strip it so the subtree's contents land at `target_dir`.
        let key = match subtree {
            None => raw_key,
            Some(prefix) => {
                if raw_key == prefix {
                    // The prefix entry itself: a directory contributes nothing
                    // (its children re-root to the target root, created on
                    // demand); a file restores under its basename.
                    if matches!(file_type, Some(FileType::Directory)) {
                        continue;
                    }
                    prefix.rsplit('/').next().unwrap_or(prefix).to_string()
                } else if let Some(rest) = raw_key
                    .strip_prefix(prefix)
                    .and_then(|r| r.strip_prefix('/'))
                {
                    rest.to_string()
                } else {
                    continue;
                }
            }
        };

        match file_type {
            Some(FileType::Directory) => {
                let target_path = target_dir.join(&key);
                tokio::fs::create_dir_all(&target_path)
                    .await
                    .with_context(|| format!("creating dir {}", target_path.display()))?;
                stats.dirs_created.fetch_add(1, Ordering::Relaxed);
                // Defer metadata restore until after children are written.
                dir_entries.push((key, entry));
            }
            Some(FileType::BlockDevice)
            | Some(FileType::CharDevice)
            | Some(FileType::Fifo)
            | Some(FileType::Socket) => {
                tracing::debug!(
                    path = %target_dir.join(&key).display(),
                    file_type = ?file_type,
                    "skipping special file"
                );
                stats.special_skipped.fetch_add(1, Ordering::Relaxed);
            }
            // Symlink, Regular, or default (None): fetch + write concurrently.
            _ => {
                while inflight.len() >= RESTORE_FILE_CONCURRENCY {
                    if let Some(r) = inflight.next().await {
                        r?;
                    }
                }
                inflight.push(restore_file_or_symlink(
                    snapshot, target_dir, key, entry, config, &stats,
                ));
            }
        }
    }

    // Drain the remaining in-flight file/symlink restores.
    while let Some(r) = inflight.next().await {
        r?;
    }
    // Release the futures' borrow of `stats` before we move it out below.
    drop(inflight);

    // Restore directory metadata in reverse order (deepest first) so that
    // setting mtime on parent dirs isn't clobbered by child writes.
    for (key, entry) in dir_entries.iter().rev() {
        let target_path = target_dir.join(key);
        restore_metadata(&target_path, entry, config);
    }

    Ok(stats)
}

/// Restore one regular file or symlink — the download-bound entries, run
/// concurrently by [`restore`]. Directories and special files are handled
/// inline there (cheap, and ordering-sensitive).
async fn restore_file_or_symlink(
    snapshot: &Snapshot,
    target_dir: &Path,
    key: String,
    entry: NodeEntry,
    config: &RestoreConfig,
    stats: &RestoreStats,
) -> anyhow::Result<()> {
    let target_path = target_dir.join(&key);
    let is_symlink = matches!(
        entry
            .semantic
            .as_ref()
            .and_then(|s| s.unix.as_ref())
            .and_then(|u| u.file_type.as_ref()),
        Some(FileType::Symlink)
    );

    if is_symlink {
        let target_bytes = snapshot.export_bytes(&entry).await?;
        let target = std::ffi::OsStr::from_bytes(&target_bytes);

        // Remove existing file/symlink if present.
        let _ = tokio::fs::remove_file(&target_path).await;
        tokio::fs::symlink(target, &target_path)
            .await
            .with_context(|| format!("creating symlink {}", target_path.display()))?;

        // In backup mode, restore symlink ownership via lchown.
        if config.backup {
            restore_ownership(&target_path, &entry);
        }
        stats.symlinks_created.fetch_add(1, Ordering::Relaxed);
    } else {
        // Regular file (explicit, or the default when no file_type is set).
        let content = snapshot.export_bytes(&entry).await?;
        let content_len = content.len() as u64;

        if let Some(parent) = target_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating parent dir {}", parent.display()))?;
        }

        tokio::fs::write(&target_path, &content)
            .await
            .with_context(|| format!("writing {}", target_path.display()))?;

        restore_metadata(&target_path, &entry, config);

        stats.files_restored.fetch_add(1, Ordering::Relaxed);
        stats
            .bytes_written
            .fetch_add(content_len, Ordering::Relaxed);
    }
    Ok(())
}

// ===========================================================================
// Metadata restore
// ===========================================================================

/// Restore metadata on a regular file or directory.
///
/// Always restores mtime. In backup mode, also restores permissions,
/// ownership, and extended attributes. Best-effort: failures are logged
/// at debug level, not propagated.
fn restore_metadata(path: &Path, entry: &NodeEntry, config: &RestoreConfig) {
    let Some(semantic) = &entry.semantic else {
        return;
    };

    // Always restore mtime (both sync and backup mode).
    if let Some(secs) = semantic.timestamp {
        let nanos = semantic.timestamp_subsec_nanos.unwrap_or(0);
        let mtime =
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::new(secs as u64, nanos);
        let times = std::fs::FileTimes::new().set_modified(mtime);

        if let Ok(file) = std::fs::File::open(path)
            && let Err(e) = file.set_times(times)
        {
            tracing::debug!(path = %path.display(), "failed to set mtime: {e}");
        }
    }

    // Backup-mode only: permissions, ownership, xattrs.
    if !config.backup {
        return;
    }

    let Some(unix) = &semantic.unix else {
        return;
    };

    // Permissions.
    if let Some(mode) = unix.permissions {
        let perms = std::fs::Permissions::from_mode(mode);
        if let Err(e) = std::fs::set_permissions(path, perms) {
            tracing::debug!(path = %path.display(), "failed to set permissions: {e}");
        }
    }

    // Ownership (uid/gid).
    restore_ownership(path, entry);

    // Extended attributes.
    if let Some(attrs) = &unix.extended_attributes {
        for attr in attrs {
            if let Some(value) = &attr.value
                && let Err(e) = xattr::set(path, &attr.name, value)
            {
                tracing::debug!(
                    path = %path.display(),
                    xattr = %attr.name,
                    "failed to set xattr: {e}"
                );
            }
        }
    }
}

/// Restore ownership (uid/gid) via `lchown`.
///
/// Uses `lchown` (not `chown`) so it works on symlinks too.
/// Requires CAP_CHOWN (typically root). Failures are logged, not propagated.
fn restore_ownership(path: &Path, entry: &NodeEntry) {
    let unix = match entry.semantic.as_ref().and_then(|s| s.unix.as_ref()) {
        Some(u) => u,
        None => return,
    };

    let uid = unix.uid.map(|u| u as libc::uid_t).unwrap_or(u32::MAX);
    let gid = unix.gid.map(|g| g as libc::gid_t).unwrap_or(u32::MAX);

    // u32::MAX (-1) means "don't change" for lchown.
    if uid == u32::MAX && gid == u32::MAX {
        return;
    }

    let c_path = match std::ffi::CString::new(path.as_os_str().as_encoded_bytes()) {
        Ok(p) => p,
        Err(_) => return,
    };

    let ret = unsafe { libc::lchown(c_path.as_ptr(), uid, gid) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        tracing::debug!(
            path = %path.display(),
            uid = ?unix.uid,
            gid = ?unix.gid,
            "failed to lchown: {err}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ignore::WalkBuilder;
    use s5_core::blob::BlobStore;
    use s5_store_memory::MemoryStore;
    use std::sync::Arc;

    /// Backup a tree of many small files (> RESTORE_FILE_CONCURRENCY, so the
    /// bounded window cycles) plus a larger file across nested dirs, then restore
    /// it through the concurrent path and verify every byte + the directory
    /// structure round-trips.
    #[tokio::test]
    async fn backup_restore_round_trip() {
        let store = Arc::new(BlobStore::new(MemoryStore::new()));

        let src_tmp = tempfile::tempdir().unwrap();
        let src = src_tmp.path();
        std::fs::create_dir_all(src.join("a/b")).unwrap();
        std::fs::create_dir_all(src.join("c/d")).unwrap();
        let mut expected: Vec<(String, Vec<u8>)> = Vec::new();
        for i in 0..25 {
            let rel = format!("a/b/file{i:02}.txt");
            let data = format!("content of file {i}\n").repeat(i + 1).into_bytes();
            std::fs::write(src.join(&rel), &data).unwrap();
            expected.push((rel, data));
        }
        let big = vec![0xCDu8; 300 * 1024];
        std::fs::write(src.join("c/d/big.bin"), &big).unwrap();
        expected.push(("c/d/big.bin".to_string(), big));

        let prev = s5_fs_v2::snapshot::Snapshot::empty(
            store.clone() as Arc<dyn s5_core::BlobsRead>,
            s5_fs_v2::node::TraversalContext::default(),
        );
        let cfg = crate::backup::BackupConfig::default();
        let result = crate::backup::backup(
            src,
            &prev,
            &*store,
            &*store,
            store.clone() as Arc<dyn s5_core::BlobsRead>,
            &cfg,
            WalkBuilder::new(src),
            None,
            None,
        )
        .await
        .unwrap();
        let (snap, _) = result.snapshot.expect("snapshot produced");

        let dst_tmp = tempfile::tempdir().unwrap();
        let dst = dst_tmp.path();
        let stats = restore(&snap, dst, &RestoreConfig::default())
            .await
            .unwrap();
        assert_eq!(
            stats.files_restored.load(Ordering::Relaxed),
            expected.len() as u64,
            "all regular files restored"
        );

        for (rel, data) in &expected {
            let got = std::fs::read(dst.join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"));
            assert_eq!(&got, data, "content mismatch for {rel}");
        }
        assert!(dst.join("a/b").is_dir());
        assert!(dst.join("c/d").is_dir());
    }
}
