//! Tree listing: walk a vault's snapshot into a depth-bounded, subtree-scoped
//! flat listing (`vup list vault:[path][#snap]`).
//!
//! The heavy lifting is [`super::restore::open_vault_snapshot`] — the same
//! meta-then-durable read chain and `#snap` resolution a restore uses. On top
//! of that, [`list_tree`] streams [`Snapshot::walk`] and projects each entry to
//! a `(path, is_dir, size)` triple, applying the subtree filter and depth
//! bound. Kept as a plain `pub` function (not an RPC method) so the E2E harness
//! can drive it against the `DurableBackend` seam without a live daemon.

use futures_util::StreamExt;
use s5_fs_v2::node::FileType;
use s5_node_api::TreeEntry;

use super::TaskExecutorContext;

/// Walk `vault`'s snapshot into a flat `Vec<TreeEntry>` in prolly-tree key
/// order (parents precede their children).
///
/// - `snapshot`: optional `#snap` selector; `None` = the current snapshot.
/// - `subtree`: optional path prefix; only entries under it are returned,
///   re-rooted so the prefix is stripped from displayed paths. A subtree that
///   matches nothing is an error (it was not found), mirroring subtree restore.
/// - `max_depth`: optional depth bound relative to the listing root (1 = the
///   immediate children only); `None` = unbounded.
///
/// Directories (from `ingest/local`) are metadata-only entries carrying a
/// trailing-slash key and `FileType::Directory`; either signal marks a dir.
/// Paths in the result never carry a trailing slash — `is_dir` distinguishes.
pub async fn list_tree(
    ctx: &TaskExecutorContext,
    vault: &str,
    snapshot: Option<&str>,
    subtree: Option<&str>,
    max_depth: Option<u32>,
) -> anyhow::Result<Vec<TreeEntry>> {
    let snap = super::restore::open_vault_snapshot(ctx, vault, snapshot).await?;

    // Normalise the subtree prefix: drop surrounding slashes; empty == whole
    // vault.
    let prefix: Option<String> = subtree
        .map(|s| s.trim_matches('/').to_string())
        .filter(|s| !s.is_empty());

    let mut entries: Vec<TreeEntry> = Vec::new();
    let mut walk = std::pin::pin!(snap.walk());
    while let Some(item) = walk.next().await {
        let (raw_path, entry) = item?;
        if entry.is_tombstone() {
            continue;
        }

        // `ingest/local` flags directories both structurally (trailing-slash
        // key) and semantically (`FileType::Directory`); accept either.
        let type_is_dir = entry
            .semantic
            .as_ref()
            .and_then(|s| s.unix.as_ref())
            .and_then(|u| u.file_type.as_ref())
            .is_some_and(|ft| matches!(ft, FileType::Directory));
        let is_dir = type_is_dir || raw_path.ends_with('/');

        // Logical path with no trailing slash — the tree render adds one for
        // dirs, and depth/subtree logic wants clean components.
        let norm = raw_path.strip_suffix('/').unwrap_or(&raw_path);

        // Subtree filter + re-root.
        let rel: &str = match &prefix {
            Some(pre) => {
                if norm == pre {
                    // The subtree root entry itself — we list its contents.
                    continue;
                }
                match norm.strip_prefix(pre).and_then(|r| r.strip_prefix('/')) {
                    Some(r) => r,
                    None => continue, // outside the subtree
                }
            }
            None => norm,
        };
        if rel.is_empty() {
            continue;
        }

        // Depth bound (1-based component count, relative to the listing root).
        if let Some(max) = max_depth
            && rel.split('/').count() as u32 > max
        {
            continue;
        }

        let size = entry.content.as_ref().map(|c| c.size).unwrap_or(0);
        entries.push(TreeEntry {
            path: rel.to_string(),
            is_dir,
            size,
        });
    }

    // A subtree that matched nothing was not found — surface it loudly rather
    // than printing an empty listing (mirrors subtree restore).
    if let Some(pre) = &prefix
        && entries.is_empty()
    {
        anyhow::bail!("subtree '{pre}' not found in vault '{vault}'");
    }

    Ok(entries)
}
