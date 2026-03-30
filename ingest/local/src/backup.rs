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
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use ignore::WalkBuilder;
use s5_core::{BlobsRead, BlobsWrite};
use s5_fs_v2::layer::ReadableLayer;
use s5_fs_v2::node::{ExtendedAttribute, FileType, NodeEntry, SemanticMeta, UnixMetadata};
use s5_fs_v2::overlay::WritableOverlay;
use s5_fs_v2::persist::MergeStats;
use s5_fs_v2::snapshot::Snapshot;

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
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            max_concurrent_ops: 8,
            force_full: false,
            one_file_system: false,
            backup: false,
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
    /// Directories processed.
    pub dirs_processed: AtomicU64,
    /// Symlinks processed.
    pub symlinks_processed: AtomicU64,
    /// Special files skipped (block/char device, fifo, socket).
    pub special_skipped: AtomicU64,
    /// Total bytes uploaded (plaintext).
    pub bytes_uploaded: AtomicU64,
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
pub async fn backup(
    source_dir: &Path,
    prev_snapshot: &Snapshot,
    blob_store: &(dyn BlobsWrite + Sync),
    meta_store: &(dyn BlobsWrite + Sync),
    read_store: Arc<dyn BlobsRead>,
    config: &BackupConfig,
    mut walker: WalkBuilder,
) -> anyhow::Result<Option<(Snapshot, BackupStats)>> {
    let source_dir = source_dir
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", source_dir.display()))?;

    // Resolve the root device ID for one_file_system filtering.
    let root_dev = if config.one_file_system {
        Some(std::fs::metadata(&source_dir)?.dev())
    } else {
        None
    };

    let stats = Arc::new(BackupStats::default());
    let overlay = Arc::new(WritableOverlay::new(Box::new(prev_snapshot.clone())));

    // Add one_file_system filter if configured.
    if root_dev.is_some() {
        walker.filter_entry(move |entry| {
            if !entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                return true;
            }
            if let Some(root_dev) = root_dev {
                if let Ok(m) = entry.metadata() {
                    if m.dev() != root_dev {
                        return false;
                    }
                }
            }
            true
        });
    }

    let walk = walker.build();

    // Process entries concurrently.
    // File content is uploaded to blob_store (remote), not meta_store.
    futures::stream::iter(walk.filter_map(Result::ok))
        .map(|entry| {
            let source_dir = source_dir.clone();
            let stats = stats.clone();
            let overlay = overlay.clone();
            async move {
                process_entry(
                    entry,
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
        .buffer_unordered(config.max_concurrent_ops)
        .try_collect::<()>()
        .await?;

    // Persist the overlay into a new prolly tree.
    // Tree nodes go to meta_store (local vault).
    let result = prev_snapshot
        .merge_and_persist(&*overlay, meta_store)
        .await?;

    let Some((root_hash, root_plaintext_hash, merge_stats)) = result else {
        return Ok(None);
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
        dirs_processed: AtomicU64::new(arc.dirs_processed.load(Ordering::Relaxed)),
        symlinks_processed: AtomicU64::new(arc.symlinks_processed.load(Ordering::Relaxed)),
        special_skipped: AtomicU64::new(arc.special_skipped.load(Ordering::Relaxed)),
        bytes_uploaded: AtomicU64::new(arc.bytes_uploaded.load(Ordering::Relaxed)),
        merge: None,
    });
    stats.merge = Some(merge_stats);

    Ok(Some((new_snapshot, stats)))
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
fn build_unix_full(
    path: &Path,
    meta: &std::fs::Metadata,
    file_type: FileType,
) -> UnixMetadata {
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
        return Ok(true); // New entry.
    };

    let prev_semantic = match &prev_entry.semantic {
        Some(s) => s,
        None => return Ok(true), // No metadata to compare.
    };

    // Size check (only meaningful for regular files).
    let prev_size = prev_entry.content.as_ref().map(|c| c.size).unwrap_or(0);
    if meta.is_file() && meta.len() != prev_size {
        return Ok(true);
    }

    // Mtime check.
    let mtime_secs = meta.mtime();
    let prev_ts = prev_semantic.timestamp.map(|t| t as i64);
    let prev_ns = prev_semantic.timestamp_subsec_nanos;

    if prev_ts != Some(mtime_secs) || prev_ns != Some(meta.mtime_nsec() as u32) {
        return Ok(true);
    }

    // Extended checks when previous snapshot has backup-mode metadata.
    if let Some(unix) = &prev_semantic.unix {
        // Inode check: detect file replacement via rename-into-place.
        if let Some(prev_inode) = unix.inode {
            if meta.ino() != prev_inode {
                return Ok(true);
            }
        }

        // Ctime check: detect metadata-only changes (permissions, ownership).
        if let Some(prev_ctime) = unix.ctime {
            let ctime = meta.ctime();
            if ctime >= 0 && ctime as u64 != prev_ctime {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

// ===========================================================================
// Entry processing
// ===========================================================================

/// Process a single directory entry from the walker.
async fn process_entry(
    entry: ignore::DirEntry,
    source_dir: &Path,
    prev_snapshot: &Snapshot,
    blob_store: &(dyn BlobsWrite + Sync),
    overlay: &WritableOverlay,
    stats: &BackupStats,
    config: &BackupConfig,
) -> anyhow::Result<()> {
    let path = entry.path();
    let meta = entry
        .metadata()
        .with_context(|| format!("metadata for {}", path.display()))?;

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
        let target = std::fs::read_link(path)
            .with_context(|| format!("reading symlink {}", path.display()))?;
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

        if !config.force_full && !is_changed(&key, &meta, prev_snapshot).await? {
            stats.files_skipped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // Regular file: read content, upload blob.
        let content = tokio::fs::read(path)
            .await
            .with_context(|| format!("reading {}", path.display()))?;
        let content_len = content.len() as u64;

        let semantic = build_semantic(path, &meta, FileType::Regular, config.backup);
        let node_entry = prev_snapshot
            .import_bytes(&content, blob_store, Some(semantic))
            .await?;

        overlay.put(key, node_entry);
        stats.files_changed.fetch_add(1, Ordering::Relaxed);
        stats.bytes_uploaded.fetch_add(content_len, Ordering::Relaxed);
    } else {
        // Block device, char device, fifo, socket — skip.
        tracing::debug!(path = %path.display(), "skipping special file");
        stats.special_skipped.fetch_add(1, Ordering::Relaxed);
    }

    Ok(())
}
