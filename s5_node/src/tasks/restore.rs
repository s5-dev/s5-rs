//! Restore task: load a vault snapshot and write it to a local directory.
//!
//! `run_restore` reads the vault root from a local age-encrypted file, builds a
//! combined meta+blob read store, and restores. Disaster recovery (no local
//! state) is a separate flow: `vup recover` resolves every vault's HEAD via the
//! config vault + discovery key ([`crate::bootstrap`]).
//!
//! Two optional selectors (D20 `vault:path#snap`) narrow what is restored:
//!   - `snapshot`: a past snapshot resolved from the vault's published registry
//!     history (revision number / timestamp / hash prefix). `None` restores the
//!     current local root.
//!   - `subtree`: a path prefix, re-rooted so its contents land at the target.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use ed25519_dalek::VerifyingKey;
use s5_core::blob::Blobs;
use s5_core::{BlobsRead, FallbackBlobsRead, Hash, RegistryApi, StreamKey};
use s5_fs_local::{RestoreConfig, restore};
use s5_fs_v2::node::TraversalContext;
use s5_fs_v2::snapshot::Snapshot;
use s5_node_api::TaskProgressMap;
use tokio_util::sync::CancellationToken;

use super::TaskReporter;

use super::publish::{
    device_signing_key, download_published_node, fetch_previous_published_node,
    vault_id_from_context,
};
use super::vault_persist::{load_vault_root, node_to_snapshot_parts, vault_root_path};
use super::{
    TaskExecutorContext, resolve_store, resolve_vault, resolve_vault_key_info,
    vault_meta_store_open, vault_meta_store_path,
};

/// Run a restore task.
///
/// 1. Resolve vault from config.
/// 2. Load the current snapshot from the vault root Transparent Node.
///    When `snapshot` is set, resolve that selector against the published
///    registry history to a past snapshot instead.
/// 3. Open the vault's local meta store.
/// 4. Build a combined read store (meta local + blob stores from vault config).
/// 5. Call `s5_fs_local::restore()` (scoped to `subtree` when set).
#[allow(clippy::too_many_arguments)]
pub async fn run_restore(
    ctx: &TaskExecutorContext,
    vault_name: &str,
    target_path: &str,
    blob_store_override: Option<&str>,
    snapshot_selector: Option<&str>,
    subtree: Option<&str>,
    reporter: TaskReporter,
    _cancel: CancellationToken,
) -> anyhow::Result<()> {
    let (vault, identity_files, read_store_names, meta_store_name) = {
        let config = ctx.config.read().await;
        let vault = resolve_vault(&config, vault_name)?.clone();
        let (_, id_files) = resolve_vault_key_info(&config, vault_name)?;
        let read_store_names: Vec<String> = config
            .vault_read_stores(vault_name, &vault)?
            .into_iter()
            .map(str::to_string)
            .collect();
        let meta_store_name = config.vault_meta_store(vault_name, &vault)?.to_string();
        (vault, id_files, read_store_names, meta_store_name)
    };

    // -- Load vault root from Transparent Node --
    // Always loaded: it is the current snapshot AND the source of the vault's
    // `vault_id` (its KEY_SLOT_RECOVERY slot), which keys the published stream
    // a `#snap` selector resolves against.
    let current_path = vault_root_path(&vault.root_path);
    let (mut root, mut root_plaintext_hash, mut context) =
        load_vault_root(&current_path, &identity_files)
            .context("reading vault root")?
            .ok_or_else(|| {
                anyhow!(
                    "vault '{}' has no snapshot to restore (root file not found)",
                    vault_name
                )
            })?;

    // -- Resolve a past snapshot (D20 `#snap`) against published history --
    if let Some(selector) = snapshot_selector {
        let registry = ctx.registry.as_ref().ok_or_else(|| {
            anyhow!("no registry configured — cannot resolve snapshot selector '#{selector}'")
        })?;
        // Published TNs live on the vault's meta primary (D1).
        let meta_store = resolve_store(&ctx.stores, &meta_store_name)?.clone();
        let vault_id = vault_id_from_context(&context)
            .with_context(|| format!("deriving vault_id for vault '{vault_name}'"))?;
        let signing_key = device_signing_key(&ctx.node_secret);
        let stream_key = StreamKey::Vault {
            pubkey: VerifyingKey::from(&signing_key).to_bytes(),
            vault_id,
        };

        let resolved = resolve_snapshot_selector(
            registry.as_ref(),
            meta_store.as_ref(),
            &stream_key,
            &identity_files,
            selector,
        )
        .await
        .with_context(|| format!("resolving snapshot '#{selector}' for vault '{vault_name}'"))?;

        tracing::info!(
            vault = vault_name,
            selector,
            revision = resolved.revision,
            timestamp = %resolved.timestamp,
            root = %resolved.root.fmt_short(),
            "resolved snapshot selector to a past snapshot"
        );
        root = resolved.root;
        root_plaintext_hash = resolved.plaintext_hash;
        context = resolved.context;
    }

    tracing::info!(
        vault = vault_name,
        root = %root.fmt_short(),
        has_keys = context.keys.is_some(),
        subtree = subtree.unwrap_or(""),
        "loaded snapshot for restore"
    );

    // -- Open the vault's local meta store, if it exists --
    // The local meta store holds the FS5 tree nodes for a warm vault and is a
    // read fast-path only: the same nodes also live in the durable stores (a
    // published snapshot is fully self-contained there — that's what lets a
    // peer, or a cold-recovered device, read it). So a MISSING meta dir is NOT
    // fatal — it's exactly the disaster-recovery case: fall back to reading
    // tree nodes straight from the durable stores.
    let meta_path = vault_meta_store_path(&vault);
    let meta_store: Option<Arc<_>> = if meta_path.exists() {
        Some(Arc::new(vault_meta_store_open(&vault)?))
    } else {
        tracing::info!(
            vault = vault_name,
            "no local meta store — reading tree nodes from the durable stores (recovery path)"
        );
        None
    };

    // -- Build read store for file content --
    let blob_read: Arc<dyn BlobsRead> = if let Some(store_name) = blob_store_override {
        let store = resolve_store(&ctx.stores, store_name)?;
        store.clone()
    } else {
        // The vault's read chain (data store, then meta store when
        // distinct — decision D1), folded into nested fallbacks.
        let mut stores: Vec<Arc<dyn BlobsRead>> = Vec::new();
        for name in &read_store_names {
            let store = resolve_store(&ctx.stores, name)?;
            stores.push(store.clone());
        }
        // Fold into nested FallbackBlobsRead: first -> fallback(second -> ...)
        let mut combined: Arc<dyn BlobsRead> = stores.pop().unwrap();
        while let Some(primary) = stores.pop() {
            combined = Arc::new(FallbackBlobsRead::new(primary, combined));
        }
        combined
    };

    // Combined reader: local meta store first (tree-node fast path) when
    // present, then the durable blob stores (tree nodes + file content).
    let read_store: Arc<dyn BlobsRead> = match meta_store {
        Some(meta) => Arc::new(FallbackBlobsRead::new(meta, blob_read)),
        None => blob_read,
    };

    // -- Build snapshot with full context and restore --
    let snapshot = Snapshot::new(root, read_store, context, root_plaintext_hash);
    let target = PathBuf::from(target_path);
    let config = RestoreConfig {
        backup: true,
        subtree: subtree.map(String::from),
    };

    // Initialize progress
    {
        let mut states = TaskProgressMap::new();
        states
            .count("files_restored", 0, None)
            .set_display_label("files restored");
        states.bytes("bytes", 0, None).set_display_label("written");
        reporter.init_progress(states);
    }

    let stats = restore(&snapshot, &target, &config)
        .await
        .with_context(|| format!("restoring vault '{}' to {}", vault_name, target_path))?;

    // Final progress update
    let files_restored = stats
        .files_restored
        .load(std::sync::atomic::Ordering::Relaxed)
        + stats
            .dirs_created
            .load(std::sync::atomic::Ordering::Relaxed)
        + stats
            .symlinks_created
            .load(std::sync::atomic::Ordering::Relaxed);
    let bytes_written = stats
        .bytes_written
        .load(std::sync::atomic::Ordering::Relaxed);

    // A subtree selector that matched nothing writes an empty target — surface
    // that loudly rather than silently "succeeding" with zero files.
    if let Some(path) = subtree
        && files_restored == 0
    {
        return Err(anyhow!(
            "subtree '{path}' not found in vault '{vault_name}''s snapshot — nothing restored"
        ));
    }

    reporter.update_progress(|states| {
        if let Some(s) = states.get_mut("files_restored") {
            s.progress = files_restored;
        }
        if let Some(s) = states.get_mut("bytes") {
            s.progress = bytes_written;
        }
    });

    tracing::info!(
        vault = vault_name,
        target = target_path,
        files_restored,
        bytes_written,
        "restore completed"
    );

    Ok(())
}

/// Open a vault's snapshot read-only for traversal (the `list` tree view).
///
/// Mirrors [`run_restore`]'s snapshot-opening steps — load the current vault
/// root, optionally resolve a `#snap` selector against the published registry
/// history, and build the same meta-then-durable read chain — but stops at a
/// ready-to-[`Snapshot::walk`] handle: no restore, no progress reporting, no
/// blob-store override (a listing always reads through the vault's own chain).
pub(crate) async fn open_vault_snapshot(
    ctx: &TaskExecutorContext,
    vault_name: &str,
    snapshot_selector: Option<&str>,
) -> anyhow::Result<Snapshot> {
    let (vault, identity_files, read_store_names, meta_store_name) = {
        let config = ctx.config.read().await;
        let vault = resolve_vault(&config, vault_name)?.clone();
        let (_, id_files) = resolve_vault_key_info(&config, vault_name)?;
        let read_store_names: Vec<String> = config
            .vault_read_stores(vault_name, &vault)?
            .into_iter()
            .map(str::to_string)
            .collect();
        let meta_store_name = config.vault_meta_store(vault_name, &vault)?.to_string();
        (vault, id_files, read_store_names, meta_store_name)
    };

    // Current vault root: the live snapshot AND the source of `vault_id`.
    let current_path = vault_root_path(&vault.root_path);
    let (mut root, mut root_plaintext_hash, mut context) =
        load_vault_root(&current_path, &identity_files)
            .context("reading vault root")?
            .ok_or_else(|| {
                anyhow!(
                    "vault '{}' has no snapshot to list (root file not found)",
                    vault_name
                )
            })?;

    // Resolve a past snapshot (D20 `#snap`) against published history.
    if let Some(selector) = snapshot_selector {
        let registry = ctx.registry.as_ref().ok_or_else(|| {
            anyhow!("no registry configured — cannot resolve snapshot selector '#{selector}'")
        })?;
        let meta_store = resolve_store(&ctx.stores, &meta_store_name)?.clone();
        let vault_id = vault_id_from_context(&context)
            .with_context(|| format!("deriving vault_id for vault '{vault_name}'"))?;
        let signing_key = device_signing_key(&ctx.node_secret);
        let stream_key = StreamKey::Vault {
            pubkey: VerifyingKey::from(&signing_key).to_bytes(),
            vault_id,
        };
        let resolved = resolve_snapshot_selector(
            registry.as_ref(),
            meta_store.as_ref(),
            &stream_key,
            &identity_files,
            selector,
        )
        .await
        .with_context(|| format!("resolving snapshot '#{selector}' for vault '{vault_name}'"))?;
        root = resolved.root;
        root_plaintext_hash = resolved.plaintext_hash;
        context = resolved.context;
    }

    // Local meta store (tree-node fast path) when present; fall back to the
    // durable stores otherwise (the disaster-recovery / cold-device path).
    let meta_path = vault_meta_store_path(&vault);
    let meta_store: Option<Arc<_>> = if meta_path.exists() {
        Some(Arc::new(vault_meta_store_open(&vault)?))
    } else {
        None
    };

    // Fold the vault's read chain into nested fallbacks: first -> (second -> …).
    let mut stores: Vec<Arc<dyn BlobsRead>> = Vec::new();
    for name in &read_store_names {
        stores.push(resolve_store(&ctx.stores, name)?.clone());
    }
    let mut blob_read: Arc<dyn BlobsRead> = stores
        .pop()
        .ok_or_else(|| anyhow!("vault '{vault_name}' has no read stores configured"))?;
    while let Some(primary) = stores.pop() {
        blob_read = Arc::new(FallbackBlobsRead::new(primary, blob_read));
    }
    let read_store: Arc<dyn BlobsRead> = match meta_store {
        Some(meta) => Arc::new(FallbackBlobsRead::new(meta, blob_read)),
        None => blob_read,
    };

    Ok(Snapshot::new(
        root,
        read_store,
        context,
        root_plaintext_hash,
    ))
}

// ---------------------------------------------------------------------------
// Snapshot-selector resolution (D20 `vault:#snap`)
// ---------------------------------------------------------------------------

/// One published snapshot in a vault's history, before its root is resolved.
///
/// A published Transparent Node is a chain: its `""` entry is the current
/// snapshot, and each timestamped history entry points at a *previous*
/// encrypted-TN blob (whose own `""` entry is that older snapshot). We build
/// the cheap metadata first (revision / timestamp / display hash), match the
/// selector against it, and only then download+decrypt the single winner.
#[derive(Debug)]
struct SnapshotCandidate {
    /// 1-based chronological index among distinct published snapshots
    /// (oldest = 1). The current snapshot is the highest revision.
    revision: usize,
    /// The history-record timestamp (RFC 3339), or `"current"`.
    timestamp: String,
    /// Hex hash exactly as `list_snapshots` prints it: the encrypted-TN blob
    /// hash for a history entry, the tree root for the current entry.
    display_hash: String,
    kind: CandidateKind,
}

#[derive(Debug)]
enum CandidateKind {
    /// The current snapshot — resolve from the fetched TN's `""` entry.
    Current,
    /// A history entry — download+decrypt this encrypted-TN blob, then read
    /// its `""` entry.
    History(Hash),
}

/// A snapshot resolved from published history, ready to open.
struct ResolvedSnapshot {
    root: Hash,
    plaintext_hash: Option<[u8; 32]>,
    context: TraversalContext,
    revision: usize,
    timestamp: String,
}

/// Resolve a `#snap` selector against a vault's published registry history.
///
/// `selector` is one of, in precedence order:
///   1. a bare integer — a 1-based revision (oldest published snapshot = 1);
///   2. an exact timestamp string as shown by `list_snapshots` (or `current`);
///   3. a hash prefix, matched against the same hashes `list_snapshots` prints
///      (must be unambiguous).
async fn resolve_snapshot_selector(
    registry: &dyn RegistryApi,
    meta_store: &dyn Blobs,
    stream_key: &StreamKey,
    identity_files: &[String],
    selector: &str,
) -> anyhow::Result<ResolvedSnapshot> {
    // Fetch the current published TN (the head of the history chain).
    let (current_node, _hash, _rev) =
        fetch_previous_published_node(registry, meta_store, stream_key, identity_files)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "vault has no published snapshots — run `backup` at least once \
                     before restoring at a snapshot"
                )
            })?;

    // History entries (non-empty keys) in ascending timestamp order == oldest
    // → newest == revisions 1..H.
    let mut history: Vec<(&String, Hash)> = current_node
        .entries
        .iter()
        .filter(|(k, _)| !k.is_empty())
        .filter_map(|(k, e)| e.content.as_ref().map(|c| (k, Hash::from(c.hash))))
        .collect();
    history.sort_by(|a, b| a.0.cmp(b.0));

    let mut candidates: Vec<SnapshotCandidate> = history
        .into_iter()
        .enumerate()
        .map(|(i, (ts, blob))| SnapshotCandidate {
            revision: i + 1,
            timestamp: ts.clone(),
            display_hash: blob.to_hex(),
            kind: CandidateKind::History(blob),
        })
        .collect();

    // The current snapshot is the newest revision.
    let current_content = current_node
        .transparent_entry()
        .and_then(|e| e.content.as_ref())
        .ok_or_else(|| anyhow!("published vault root is not a Transparent Node"))?;
    candidates.push(SnapshotCandidate {
        revision: candidates.len() + 1,
        timestamp: String::from("current"),
        display_hash: Hash::from(current_content.hash).to_hex(),
        kind: CandidateKind::Current,
    });

    let matched = select_candidate(&candidates, selector)?;
    let revision = matched.revision;
    let timestamp = matched.timestamp.clone();

    let (root, plaintext_hash, context) = match &matched.kind {
        CandidateKind::Current => {
            node_to_snapshot_parts(&current_node).context("reading current published snapshot")?
        }
        CandidateKind::History(blob) => {
            let node = download_published_node(meta_store, *blob, identity_files)
                .await
                .with_context(|| format!("fetching historical snapshot {}", blob.fmt_short()))?;
            node_to_snapshot_parts(&node).context("reading historical snapshot")?
        }
    };

    Ok(ResolvedSnapshot {
        root,
        plaintext_hash,
        context,
        revision,
        timestamp,
    })
}

/// Pick the candidate a selector names (revision number, timestamp, or hash
/// prefix), or fail with the available list.
fn select_candidate<'a>(
    candidates: &'a [SnapshotCandidate],
    selector: &str,
) -> anyhow::Result<&'a SnapshotCandidate> {
    let sel = selector.trim();
    let total = candidates.len();

    // 1. In-range integer → 1-based revision. An *out-of-range* integer falls
    //    through (it may be an all-digit hash prefix); it only errors below if
    //    nothing else matches either.
    if let Ok(rev) = sel.parse::<usize>()
        && (1..=total).contains(&rev)
    {
        return Ok(candidates
            .iter()
            .find(|c| c.revision == rev)
            .expect("revision in range"));
    }

    // 2. Exact timestamp / "current".
    if let Some(c) = candidates.iter().find(|c| c.timestamp == sel) {
        return Ok(c);
    }

    // 3. Hash prefix (case-insensitive, unambiguous).
    let sel_lc = sel.to_ascii_lowercase();
    let matches: Vec<&SnapshotCandidate> = candidates
        .iter()
        .filter(|c| c.display_hash.starts_with(&sel_lc))
        .collect();
    match matches.as_slice() {
        [one] => Ok(one),
        // An integer that reached here was out of the revision range — give the
        // revision-flavoured hint rather than a hash-prefix one.
        [] if sel.parse::<usize>().is_ok() => Err(anyhow!(
            "no snapshot #{sel} — the vault has {total} published snapshot(s) (1..={total})"
        )),
        [] => Err(anyhow!(
            "no snapshot matches selector '{selector}'. Available: {}",
            summarize_candidates(candidates)
        )),
        many => Err(anyhow!(
            "snapshot selector '{selector}' is ambiguous — it matches {} snapshots; \
             use more hash characters or a revision number (1..={total})",
            many.len()
        )),
    }
}

/// A short "#rev <hash12> <timestamp>" listing for error messages.
fn summarize_candidates(candidates: &[SnapshotCandidate]) -> String {
    candidates
        .iter()
        .map(|c| {
            let short = &c.display_hash[..c.display_hash.len().min(12)];
            format!("#{} {} ({})", c.revision, short, c.timestamp)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(revision: usize, timestamp: &str, hash_hex: &str) -> SnapshotCandidate {
        SnapshotCandidate {
            revision,
            timestamp: timestamp.to_string(),
            display_hash: hash_hex.to_string(),
            kind: CandidateKind::Current,
        }
    }

    fn sample() -> Vec<SnapshotCandidate> {
        vec![
            candidate(1, "2025-01-01T00:00:00Z", "aabbccdd11223344"),
            candidate(2, "2025-06-01T00:00:00Z", "ffee00112233aabb"),
            candidate(3, "current", "0011223344556677"),
        ]
    }

    #[test]
    fn selects_by_revision_number() {
        let c = sample();
        assert_eq!(select_candidate(&c, "1").unwrap().revision, 1);
        assert_eq!(select_candidate(&c, "3").unwrap().revision, 3);
    }

    #[test]
    fn revision_out_of_range_errors() {
        let c = sample();
        let err = select_candidate(&c, "9").unwrap_err().to_string();
        assert!(err.contains("no snapshot #9"), "{err}");
    }

    #[test]
    fn selects_by_timestamp_and_current() {
        let c = sample();
        assert_eq!(
            select_candidate(&c, "2025-06-01T00:00:00Z")
                .unwrap()
                .revision,
            2
        );
        assert_eq!(select_candidate(&c, "current").unwrap().revision, 3);
    }

    #[test]
    fn selects_by_unambiguous_hash_prefix() {
        let c = sample();
        assert_eq!(select_candidate(&c, "ffee").unwrap().revision, 2);
        // Case-insensitive.
        assert_eq!(select_candidate(&c, "AABB").unwrap().revision, 1);
    }

    #[test]
    fn ambiguous_hash_prefix_errors() {
        // Two hashes share the leading "00".
        let c = vec![
            candidate(1, "t1", "00aa"),
            candidate(2, "t2", "00bb"),
            candidate(3, "current", "11cc"),
        ];
        let err = select_candidate(&c, "00").unwrap_err().to_string();
        assert!(err.contains("ambiguous"), "{err}");
    }

    #[test]
    fn unknown_selector_lists_available() {
        let c = sample();
        let err = select_candidate(&c, "zzzz").unwrap_err().to_string();
        assert!(err.contains("no snapshot matches"), "{err}");
        assert!(err.contains("#1"), "{err}");
    }
}
