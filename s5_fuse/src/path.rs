//! Path/key conversion + snapshot lookups.
//!
//! `s5_fs_v2`'s key space is flat — files are keyed by their path with
//! no leading slash, and directories aren't materialised (they exist
//! only by virtue of having descendants). The helpers in this module
//! convert FUSE-style `/dir/file` paths into the snapshot's key space
//! and answer "is this path a file, an implicit directory, or neither?"
//! against any [`ReadableLayer`] — so both the read-only and writable
//! filesystem impls share the same lookup logic.
//!
//! Functions are intentionally free-standing (no shared state) so they
//! work over `Snapshot`, `WritableOverlay`, or `MergedView` without
//! duplicating the resolution logic per impl.

use std::ffi::{OsStr, OsString};
use std::ops::Bound;

use fuse3::path::prelude::*;
use fuse3::{Errno, Result as FuseResult};
use futures_util::StreamExt;
use s5_fs_v2::layer::ReadableLayer;
use s5_fs_v2::node::{NodeEntry, Structural};
use tracing::warn;

/// Strip the leading `/` (and any trailing `/`) so a FUSE-style
/// `/dir/inner.bin` becomes the snapshot key `dir/inner.bin`. The
/// snapshot root is the empty string.
pub(crate) fn snapshot_key(path: &OsStr) -> String {
    let s = path.to_string_lossy();
    s.trim_start_matches('/').trim_end_matches('/').to_string()
}

/// Join an absolute parent path (`/`, `/dir`, …) with a leaf `name`.
/// `lookup` and `create` callbacks need the full path before calling
/// [`snapshot_key`].
pub(crate) fn join(parent: &OsStr, name: &OsStr) -> OsString {
    let mut joined = OsString::with_capacity(parent.len() + 1 + name.len());
    let p = parent.to_string_lossy();
    if p == "/" {
        joined.push("/");
        joined.push(name);
    } else {
        joined.push(parent);
        joined.push("/");
        joined.push(name);
    }
    joined
}

/// What [`resolve`] found at a given key.
pub(crate) enum ResolvedEntry {
    /// `NodeEntry` is boxed because the variant otherwise dwarfs the
    /// 0-sized `Directory`/`Tombstone` cases (clippy::large_enum_variant).
    File(Box<NodeEntry>),
    /// No entry at the key, but at least one descendant exists — treat
    /// as an implicit directory.
    Directory,
    /// Key has a tombstone in the layer; from the FUSE point of view,
    /// the path doesn't exist.
    Tombstone,
}

/// Resolve a snapshot key against a layer:
/// - `File(entry)` if the key names a leaf or chunked file
/// - `Directory` if any descendant exists under `key/`
/// - `Tombstone` if the layer carries an explicit deletion marker
/// - `Err(ENOENT)` otherwise
pub(crate) async fn resolve(layer: &dyn ReadableLayer, key: &str) -> FuseResult<ResolvedEntry> {
    if key.is_empty() {
        // The empty key is the FUSE root, which is always a directory.
        return Ok(ResolvedEntry::Directory);
    }
    match layer.get_raw(key).await {
        Ok(Some(entry)) => {
            if entry.is_tombstone() {
                return Ok(ResolvedEntry::Tombstone);
            }
            match entry.content.as_ref().map(|c| &c.structural) {
                Some(Structural::Leaf) | Some(Structural::Link) => {
                    Ok(ResolvedEntry::File(Box::new(entry)))
                }
                _ => {
                    // Other structural variants (none in v2 today) — treat
                    // as nonexistent rather than guess.
                    Err(Errno::from(libc::ENOENT))
                }
            }
        }
        Ok(None) => {
            if has_descendant(layer, key).await? {
                Ok(ResolvedEntry::Directory)
            } else {
                Err(Errno::from(libc::ENOENT))
            }
        }
        Err(err) => {
            warn!(key, error = %err, "layer.get_raw failed");
            Err(Errno::from(libc::EIO))
        }
    }
}

/// True if any entry exists with `prefix/` as its key prefix. Used to
/// detect implicit directories. Only inspects the first matching entry,
/// so the cost is one tree seek regardless of subtree size.
async fn has_descendant(layer: &dyn ReadableLayer, prefix: &str) -> FuseResult<bool> {
    let lo = format!("{prefix}/");
    let hi = format!("{prefix}/\u{10FFFF}");
    let mut stream = layer.scan(Bound::Included(lo), Bound::Excluded(hi));
    match stream.next().await {
        Some(Ok(_)) => Ok(true),
        Some(Err(err)) => {
            warn!(prefix, error = %err, "scan failed");
            Err(Errno::from(libc::EIO))
        }
        None => Ok(false),
    }
}

/// Like [`list_children`] but carries the `NodeEntry` for direct file
/// children alongside their kind, so callers (notably `readdirplus`)
/// can stamp `FileAttr` from `entry.semantic` without paying an extra
/// `get` per child. Directory children get `None` (directories are
/// implicit in prolly-tree key structure — no per-dir entry exists).
pub(crate) async fn list_children_with_entries(
    layer: &dyn ReadableLayer,
    key: &str,
) -> FuseResult<Vec<(String, FileType, Option<NodeEntry>)>> {
    let prefix = if key.is_empty() {
        String::new()
    } else {
        format!("{key}/")
    };
    let lo = prefix.clone();
    let hi = if prefix.is_empty() {
        "\u{10FFFF}".to_string()
    } else {
        format!("{prefix}\u{10FFFF}")
    };
    let mut stream = layer.scan(Bound::Included(lo), Bound::Excluded(hi));

    let mut out: Vec<(String, FileType, Option<NodeEntry>)> = Vec::new();
    let mut last_dir: Option<String> = None;
    while let Some(item) = stream.next().await {
        let (full_key, entry) = item.map_err(|err| {
            warn!(key, error = %err, "scan child failed");
            Errno::from(libc::EIO)
        })?;
        let suffix = full_key.strip_prefix(&prefix).unwrap_or(&full_key);
        match suffix.split_once('/') {
            Some((dir_name, _rest)) => {
                if last_dir.as_deref() != Some(dir_name) {
                    out.push((dir_name.to_string(), FileType::Directory, None));
                    last_dir = Some(dir_name.to_string());
                }
            }
            None => {
                let kind = match entry.content.as_ref().map(|c| &c.structural) {
                    Some(Structural::Leaf) | Some(Structural::Link) => FileType::RegularFile,
                    _ => continue, // tombstone or unsupported
                };
                out.push((suffix.to_string(), kind, Some(entry)));
            }
        }
    }
    Ok(out)
}

/// List the immediate children of a directory key by scanning all
/// entries under `key/` and deduping the next path segment. Subtree
/// entries contribute their first segment as `Directory`; direct
/// entries contribute their concrete kind.
pub(crate) async fn list_children(
    layer: &dyn ReadableLayer,
    key: &str,
) -> FuseResult<Vec<(String, FileType)>> {
    let prefix = if key.is_empty() {
        String::new()
    } else {
        format!("{key}/")
    };
    let lo = prefix.clone();
    let hi = if prefix.is_empty() {
        "\u{10FFFF}".to_string()
    } else {
        format!("{prefix}\u{10FFFF}")
    };
    let mut stream = layer.scan(Bound::Included(lo), Bound::Excluded(hi));

    let mut out: Vec<(String, FileType)> = Vec::new();
    let mut last_dir: Option<String> = None;
    while let Some(item) = stream.next().await {
        let (full_key, entry) = item.map_err(|err| {
            warn!(key, error = %err, "scan child failed");
            Errno::from(libc::EIO)
        })?;
        let suffix = full_key.strip_prefix(&prefix).unwrap_or(&full_key);
        match suffix.split_once('/') {
            Some((dir_name, _rest)) => {
                if last_dir.as_deref() != Some(dir_name) {
                    out.push((dir_name.to_string(), FileType::Directory));
                    last_dir = Some(dir_name.to_string());
                }
            }
            None => {
                let kind = match entry.content.as_ref().map(|c| &c.structural) {
                    Some(Structural::Leaf) | Some(Structural::Link) => FileType::RegularFile,
                    _ => continue, // tombstone or unsupported
                };
                out.push((suffix.to_string(), kind));
            }
        }
    }
    Ok(out)
}
