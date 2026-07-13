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

use std::ffi::{CString, OsStr};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context;
use cap_std::ambient_authority;
use cap_std::fs::Dir;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use s5_fs_v2::node::{FileType, NodeEntry};
use s5_fs_v2::snapshot::Snapshot;

/// The setuid + setgid mode bits (`S_ISUID | S_ISGID`).
const SETID_BITS: u32 = 0o6000;

/// Reject a snapshot entry key that could escape the restore target when
/// joined onto it.
///
/// Snapshot keys are arbitrary strings. A normal local backup only ever
/// produces clean relative paths (they come from `strip_prefix`), but a
/// **crafted or shared snapshot** — the "before sharing is used for real"
/// threat — can carry an absolute path (`/etc/cron.d/x`, which `Path::join`
/// would follow by discarding the target root) or `..` segments that climb out
/// of `target_dir`. Neither can ever be a legitimate entry, so we reject rather
/// than silently rewrite: a hostile name fails the restore loudly instead of
/// landing outside the target.
///
/// This is the first line of defence; symlinked-ancestor traversal (a symlink
/// entry `foo -> /etc` plus a file `foo/passwd`) is closed separately by
/// materialising every entry through a [`cap_std::fs::Dir`] sandbox rooted at
/// the target, which refuses to resolve a path out of the tree.
fn validate_relative_key(key: &str) -> anyhow::Result<()> {
    if key.is_empty() {
        anyhow::bail!("snapshot entry has an empty name");
    }
    if key.as_bytes().contains(&0) {
        anyhow::bail!("snapshot entry name contains a NUL byte: {key:?}");
    }
    let mut saw_normal = false;
    for comp in Path::new(key).components() {
        match comp {
            Component::Normal(_) => saw_normal = true,
            // RootDir / Prefix = absolute; ParentDir = `..`; CurDir = `.`.
            other => anyhow::bail!(
                "snapshot entry name {key:?} contains an unsafe path component ({other:?}); \
                 absolute paths and `..`/`.` segments are rejected on restore"
            ),
        }
    }
    if !saw_normal {
        anyhow::bail!("snapshot entry name {key:?} resolves to an empty relative path");
    }
    Ok(())
}

/// Create a symlink at `key` (relative to the sandbox `root`) pointing at
/// `target`, storing the target **verbatim**.
///
/// `cap_std::fs::Dir::symlink` refuses an absolute or escaping target — correct
/// for a capability sandbox, but wrong for a backup tool, which must faithfully
/// restore a symlink that legitimately points at `/usr/bin/…` or `../sibling`.
/// So we resolve the link's *parent* through the sandbox (`open_dir`, which
/// won't traverse out of the tree) and then `symlinkat` the verbatim target
/// against that parent's fd. Placement stays contained; the stored target is
/// unrestricted; and later writes that would *traverse* this symlink are still
/// refused by `cap-std`.
fn symlink_verbatim(root: &Dir, key: &str, target: &OsStr) -> anyhow::Result<()> {
    let path = Path::new(key);
    let (parent_dir, base) = match path.parent().filter(|p| !p.as_os_str().is_empty()) {
        Some(parent) => {
            root.create_dir_all(parent)
                .with_context(|| format!("creating parent dir {}", parent.display()))?;
            let dir = root
                .open_dir(parent)
                .with_context(|| format!("opening parent dir {}", parent.display()))?;
            let base = path
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("symlink key {key:?} has no final component"))?;
            (Some(dir), base)
        }
        None => (None, path.as_os_str()),
    };
    let dir_fd = parent_dir
        .as_ref()
        .map(|d| d.as_raw_fd())
        .unwrap_or_else(|| root.as_raw_fd());

    // Best-effort remove of an existing entry at this exact path (final
    // component is never followed by unlinkat without AT_SYMLINK flags).
    let base_c = CString::new(base.as_bytes())
        .with_context(|| format!("symlink name {key:?} contains a NUL byte"))?;
    unsafe { libc::unlinkat(dir_fd, base_c.as_ptr(), 0) };

    let target_c = CString::new(target.as_bytes())
        .with_context(|| format!("symlink target for {key:?} contains a NUL byte"))?;
    let ret = unsafe { libc::symlinkat(target_c.as_ptr(), dir_fd, base_c.as_ptr()) };
    if ret != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("creating symlink {key}"));
    }
    Ok(())
}

/// Strip the setuid and setgid bits from a restored file/directory mode.
///
/// A snapshot may come from another user when a vault is shared, so restore
/// must never be able to drop a setuid-root (or setgid) binary onto the host —
/// the reviewer flagged this explicitly. The sticky bit (`0o1000`) and the
/// standard rwx bits are preserved.
fn sanitize_mode(mode: u32) -> u32 {
    mode & !SETID_BITS
}

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

    // Every write goes through this sandbox handle rather than a
    // `target_dir.join(key)` path. `cap_std::fs::Dir` resolves paths relative to
    // the target's directory fd and refuses to escape it — so an absolute key,
    // a `..` key, or a symlinked ancestor (`foo -> /etc` with a later
    // `foo/passwd`) can never write outside the target. This is the containment
    // boundary; `validate_relative_key` below is defence-in-depth plus legible
    // errors. Metadata (chmod/chown/xattr/mtime) is applied afterwards over the
    // resolved path — safe because the entry was already placed inside the
    // sandbox, so no ancestor it traverses leaves the tree.
    let root = Dir::open_ambient_dir(target_dir, ambient_authority())
        .with_context(|| format!("opening restore root {}", target_dir.display()))?;

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

        // The tree root (and a subtree's own root entry) re-roots to an empty
        // key — nothing to materialise, the target dir already exists. Skip it
        // before validation, which treats a truly empty name as malformed.
        if key.is_empty() {
            continue;
        }
        // Reject absolute / `..` / NUL keys before they reach the sandbox —
        // clear error, and defence-in-depth behind `root`.
        validate_relative_key(&key)?;

        match file_type {
            Some(FileType::Directory) => {
                root.create_dir_all(&key)
                    .with_context(|| format!("creating dir {key}"))?;
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
                    snapshot, target_dir, &root, key, entry, config, &stats,
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
    root: &Dir,
    key: String,
    entry: NodeEntry,
    config: &RestoreConfig,
    stats: &RestoreStats,
) -> anyhow::Result<()> {
    // Resolved path for metadata ops only (chmod/chown/xattr/mtime). The entry
    // itself is created through `root`, so it is guaranteed inside the target;
    // applying metadata over the resolved path can't escape.
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
        let target = OsStr::from_bytes(&target_bytes);

        // Create the link inside the sandbox with its target stored verbatim — a
        // symlink may legitimately point anywhere (absolute, `..`); only
        // *traversing* it to write another entry is sandboxed (that write would
        // leave the tree and `cap-std` refuses it).
        symlink_verbatim(root, &key, target)?;

        // In backup mode, restore symlink ownership via lchown.
        if config.backup {
            restore_ownership(&target_path, &entry);
        }
        stats.symlinks_created.fetch_add(1, Ordering::Relaxed);
    } else {
        // Regular file (explicit, or the default when no file_type is set).
        let content = snapshot.export_bytes(&entry).await?;
        let content_len = content.len() as u64;

        if let Some(parent) = Path::new(&key)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
        {
            root.create_dir_all(parent)
                .with_context(|| format!("creating parent dir {}", parent.display()))?;
        }

        // Remove-then-write so a stale symlink left at this path by a previous
        // restore is replaced, never followed.
        let _ = root.remove_file(&key);
        root.write(&key, content.as_ref())
            .with_context(|| format!("writing {key}"))?;

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

    // Permissions — with setuid/setgid stripped (see `sanitize_mode`).
    if let Some(mode) = unix.permissions {
        let perms = std::fs::Permissions::from_mode(sanitize_mode(mode));
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

    #[test]
    fn validate_relative_key_accepts_clean_paths() {
        validate_relative_key("file.txt").unwrap();
        validate_relative_key("a/b/c.txt").unwrap();
        validate_relative_key("weird name with spaces.txt").unwrap();
    }

    #[test]
    fn validate_relative_key_rejects_traversal_and_absolute() {
        for bad in [
            "",            // empty
            "/etc/passwd", // absolute
            "../escape",   // parent traversal
            "a/../../etc", // interior traversal
            "a/../b",      // interior traversal
            "./x",         // leading current-dir
            "a\0b",        // embedded NUL
        ] {
            assert!(
                validate_relative_key(bad).is_err(),
                "validate_relative_key should reject {bad:?}"
            );
        }
    }

    #[test]
    fn sanitize_mode_strips_setid_keeps_the_rest() {
        // setuid + setgid + sticky + rwxr-xr-x → setuid/setgid gone, rest kept.
        assert_eq!(sanitize_mode(0o7755), 0o1755);
        assert_eq!(sanitize_mode(0o4755) & SETID_BITS, 0, "setuid removed");
        assert_eq!(sanitize_mode(0o2755) & SETID_BITS, 0, "setgid removed");
        assert_eq!(sanitize_mode(0o644), 0o644, "ordinary mode untouched");
        assert_eq!(sanitize_mode(0o1777), 0o1777, "sticky bit preserved");
    }

    /// The containment boundary: a snapshot's symlink entry pointing outside the
    /// target must not let a later entry write through it. `cap-std` refuses to
    /// resolve `esc/secret` because it escapes the sandbox root.
    #[test]
    fn cap_std_root_refuses_symlinked_ancestor_escape() {
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret"), b"top secret").unwrap();

        let root_tmp = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root_tmp.path().join("esc")).unwrap();

        let root = Dir::open_ambient_dir(root_tmp.path(), ambient_authority()).unwrap();
        assert!(
            root.write("esc/secret", b"pwned").is_err(),
            "cap-std must refuse to write through an escaping symlink"
        );
        assert_eq!(
            std::fs::read(outside.path().join("secret")).unwrap(),
            b"top secret",
            "the file outside the target must be untouched"
        );
    }

    /// Regression guard: restoring a symlink whose target is an absolute path is
    /// legitimate and must still work (cap-std's own `symlink` refuses it, which
    /// is why `symlink_verbatim` exists). Nested placement is sandboxed but the
    /// stored target is faithful.
    #[test]
    fn symlink_verbatim_stores_absolute_target() {
        let root_tmp = tempfile::tempdir().unwrap();
        let root = Dir::open_ambient_dir(root_tmp.path(), ambient_authority()).unwrap();
        symlink_verbatim(&root, "sub/link", OsStr::new("/etc/hostname")).unwrap();

        let link = root_tmp.path().join("sub/link");
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            std::fs::read_link(&link).unwrap(),
            Path::new("/etc/hostname")
        );
    }

    /// And a symlink `symlink_verbatim` places pointing outside the target still
    /// can't be *traversed* by a subsequent sandboxed write.
    #[test]
    fn symlink_verbatim_target_is_not_traversable() {
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret"), b"top secret").unwrap();

        let root_tmp = tempfile::tempdir().unwrap();
        let root = Dir::open_ambient_dir(root_tmp.path(), ambient_authority()).unwrap();
        symlink_verbatim(&root, "esc", OsStr::new(outside.path().to_str().unwrap())).unwrap();

        assert!(
            root.write("esc/secret", b"pwned").is_err(),
            "writing through a stored escaping symlink must be refused"
        );
        assert_eq!(
            std::fs::read(outside.path().join("secret")).unwrap(),
            b"top secret"
        );
    }
}
