//! Local filesystem backup and restore for S5 FS V2.
//!
//! This crate provides bidirectional operations between a local filesystem
//! and an S5 FS V2 prolly tree:
//!
//! - **Backup** ([`backup`]): Walk a local directory, diff against a previous
//!   snapshot, upload changed files, and persist a new snapshot.
//! - **Restore** ([`restore`]): Walk a snapshot tree and write entries back
//!   to a local directory with metadata (permissions, timestamps, ownership).
//!
//! # Design
//!
//! This crate owns the full loop for both directions. It knows about:
//! - Unix file types (regular, directory, symlink, special files)
//! - Unix metadata (permissions, uid/gid, timestamps, xattrs)
//! - Incremental diffing (size + mtime comparison)
//! - `.gitignore` / `.ignore` / `CACHEDIR.TAG` filtering
//!
//! # Key Convention
//!
//! - Files: relative path, no trailing slash — `"src/main.rs"`
//! - Directories: relative path with trailing slash — `"src/"`
//! - Symlinks: relative path, no trailing slash — `"lib/libfoo.so"`
//!
//! Symlink targets are stored as raw bytes in the blob store (not UTF-8
//! assumed — symlink targets are arbitrary `[u8]` on Unix).

mod backup;
mod restore;

pub use backup::{BackupConfig, BackupResult, BackupStats, backup};
pub use ignore::WalkBuilder;
pub use restore::{RestoreConfig, RestoreStats, restore};
