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
use s5_fs_v2::node::{FileType, NodeEntry};
use s5_fs_v2::snapshot::Snapshot;

/// Configuration for a restore operation.
pub struct RestoreConfig {
    /// Backup mode: restore full metadata (permissions, ownership, xattrs).
    ///
    /// When false (sync mode), only file content and mtime are restored.
    /// When true, also restores permissions, uid/gid (requires CAP_CHOWN),
    /// and extended attributes.
    pub backup: bool,
}

impl Default for RestoreConfig {
    fn default() -> Self {
        Self { backup: false }
    }
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

    while let Some(result) = walk.next().await {
        let (key, entry) = result?;
        let target_path = target_dir.join(&key);

        let file_type = entry
            .semantic
            .as_ref()
            .and_then(|s| s.unix.as_ref())
            .and_then(|u| u.file_type.as_ref());

        match file_type {
            Some(FileType::Directory) => {
                tokio::fs::create_dir_all(&target_path)
                    .await
                    .with_context(|| format!("creating dir {}", target_path.display()))?;
                stats.dirs_created.fetch_add(1, Ordering::Relaxed);
                // Defer metadata restore until after children are written.
                dir_entries.push((key, entry));
            }
            Some(FileType::Symlink) => {
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
            }
            Some(FileType::BlockDevice)
            | Some(FileType::CharDevice)
            | Some(FileType::Fifo)
            | Some(FileType::Socket) => {
                tracing::debug!(
                    path = %target_path.display(),
                    file_type = ?file_type,
                    "skipping special file"
                );
                stats.special_skipped.fetch_add(1, Ordering::Relaxed);
            }
            Some(FileType::Regular) | None => {
                // Regular file (explicit or default when no file_type set).
                let content = snapshot.export_bytes(&entry).await?;
                let content_len = content.len() as u64;

                if let Some(parent) = target_path.parent() {
                    tokio::fs::create_dir_all(parent)
                        .await
                        .with_context(|| {
                            format!("creating parent dir {}", parent.display())
                        })?;
                }

                tokio::fs::write(&target_path, &content)
                    .await
                    .with_context(|| format!("writing {}", target_path.display()))?;

                restore_metadata(&target_path, &entry, config);

                stats.files_restored.fetch_add(1, Ordering::Relaxed);
                stats.bytes_written.fetch_add(content_len, Ordering::Relaxed);
            }
        }
    }

    // Restore directory metadata in reverse order (deepest first) so that
    // setting mtime on parent dirs isn't clobbered by child writes.
    for (key, entry) in dir_entries.iter().rev() {
        let target_path = target_dir.join(key);
        restore_metadata(&target_path, entry, config);
    }

    Ok(stats)
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
        let mtime = std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::new(secs as u64, nanos);
        let times = std::fs::FileTimes::new().set_modified(mtime);

        if let Ok(file) = std::fs::File::open(path) {
            if let Err(e) = file.set_times(times) {
                tracing::debug!(path = %path.display(), "failed to set mtime: {e}");
            }
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
            if let Some(value) = &attr.value {
                if let Err(e) = xattr::set(path, &attr.name, value) {
                    tracing::debug!(
                        path = %path.display(),
                        xattr = %attr.name,
                        "failed to set xattr: {e}"
                    );
                }
            }
        }
    }
}

/// Restore ownership (uid/gid) via `lchown`.
///
/// Uses `lchown` (not `chown`) so it works on symlinks too.
/// Requires CAP_CHOWN (typically root). Failures are logged, not propagated.
fn restore_ownership(path: &Path, entry: &NodeEntry) {
    let unix = match entry
        .semantic
        .as_ref()
        .and_then(|s| s.unix.as_ref())
    {
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
