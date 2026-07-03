//! `FileAttr` builders for files and directories.
//!
//! Times are read from the entry's semantic metadata when present:
//! `mtime` from `semantic.timestamp` (+ `timestamp_subsec_nanos`),
//! `ctime` from `semantic.unix.ctime` (falling back to `mtime`).
//! `atime` aliases to `mtime` since the v2 schema has no atime field.
//! Implicit directories — the ones inferred from descendant paths,
//! with no backing `NodeEntry` — fall back to the Unix epoch; the
//! schema doesn't carry directory metadata yet.
//!
//! Permissions are read-only / read-only-execute. The writable
//! adapter surfaces its own attribute paths but reuses these helpers
//! for committed entries.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuse3::path::prelude::*;
use s5_fs_v2::node::NodeEntry;

/// Block size we report to the kernel. 4 KiB matches typical page size
/// and keeps `stat`'s `st_blocks` math sensible.
pub(crate) const BLOCK_SIZE: u32 = 4096;

/// How long the kernel may cache the entry/attr replies before
/// re-asking. One second strikes a balance between staleness on writes
/// and avoiding lookup amplification on long directory walks.
//
// TODO(perf): 1s is a conservative placeholder. The daemon owns *every*
// mutation — kernel writes funnel through `WritableFs`, and remote HEAD swaps
// go through the mount manager — so staleness is fully observable on our side.
// Once we surface the FUSE notifier and push `notify_inval_{inode,entry}` on
// change (see `mount.rs`), raise this TTL by orders of magnitude: minutes for
// directories, and effectively unbounded for immutable content-addressed
// leaves (a given hash never changes its bytes). Long TTL + active
// invalidation is exactly what removes the per-stat kernel<->daemon round trip
// on a hot working set — the reason "FUSE is slow on metadata" does not apply
// to a RAM-backed, mutation-aware daemon like this one.
pub(crate) const ENTRY_TTL: Duration = Duration::from_secs(1);

pub(crate) fn file_attr(entry: &NodeEntry) -> FileAttr {
    let size = entry.content.as_ref().map(|c| c.size).unwrap_or(0);
    let mtime = entry_mtime(entry);
    let ctime = entry_ctime(entry).unwrap_or(mtime);
    FileAttr {
        size,
        blocks: size.div_ceil(BLOCK_SIZE as u64),
        atime: mtime,
        mtime,
        ctime,
        kind: FileType::RegularFile,
        perm: 0o444,
        nlink: 1,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: BLOCK_SIZE,
    }
}

pub(crate) fn dir_attr() -> FileAttr {
    // Implicit directories have no backing NodeEntry → no timestamp
    // source. Fall back to the epoch until the schema grows
    // first-class directory metadata.
    FileAttr {
        size: 0,
        blocks: 0,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        kind: FileType::Directory,
        perm: 0o555,
        nlink: 2,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: BLOCK_SIZE,
    }
}

/// Pull the modification time from `semantic.timestamp` (Unix seconds)
/// and `timestamp_subsec_nanos`. Returns the epoch when no timestamp is
/// recorded — typical for entries imported by an older snap that
/// didn't carry mtime yet.
fn entry_mtime(entry: &NodeEntry) -> SystemTime {
    let Some(sem) = entry.semantic.as_ref() else {
        return UNIX_EPOCH;
    };
    let Some(secs) = sem.timestamp else {
        return UNIX_EPOCH;
    };
    let nanos = sem.timestamp_subsec_nanos.unwrap_or(0);
    UNIX_EPOCH + Duration::new(secs as u64, nanos)
}

/// Pull the status-change time from `semantic.unix.ctime` (Unix
/// seconds). Returns `None` when not recorded — caller falls back to
/// the file's mtime.
fn entry_ctime(entry: &NodeEntry) -> Option<SystemTime> {
    let secs = entry
        .semantic
        .as_ref()
        .and_then(|s| s.unix.as_ref())
        .and_then(|u| u.ctime)?;
    Some(UNIX_EPOCH + Duration::from_secs(secs))
}
