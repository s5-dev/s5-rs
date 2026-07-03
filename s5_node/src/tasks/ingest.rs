//! Ingest task: walk source dirs, import blobs, persist snapshot.
//!
//! Resolves vault/source/store from config, builds a `WalkBuilder`,
//! calls `s5_fs_local::backup()`, and merges the result into the vault.
//! On completion, saves the new snapshot as an age-encrypted Transparent Node.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use rand::Rng;
use s5_core::blob::tee::TeeBlobsWrite;
use s5_core::{BlobsRead, CachedBlobsRead, FallbackBlobsRead};
use s5_fs_local::{
    BackupConfig, BackupResult, BackupStats, PipelineRoute, WalkBuilder, backup, backup_incremental,
};
use s5_fs_v2::node::{BlobPipeline, CompressionStrategy, FileChunkingStrategy, TraversalContext};
use s5_fs_v2::snapshot::Snapshot;
use s5_node_api::TaskProgressMap;
use s5_node_api::config::{
    BlobPipelineConfig, CompressionConfig, FileChunkingConfig, NodeConfigVault, PipelineRouteConfig,
};
use tokio_util::sync::CancellationToken;

use super::TaskReporter;

use super::vault_persist::{
    inprogress_root_path, load_vault_root, remove_inprogress, save_vault_root, vault_root_path,
};
use super::{
    TaskExecutorContext, resolve_source, resolve_store, resolve_vault, resolve_vault_key_info,
    vault_meta_store_open,
};

/// Run an ingest task.
///
/// 1. Resolve vault, source, and blob_store from config.
/// 2. Open (or create) the vault's local meta store.
/// 3. Load previous snapshot from vault root (or inprogress for resume).
/// 4. Build a WalkBuilder from source config.
/// 5. Call `s5_fs_local::backup()` for each source path.
/// 6. Save new snapshot as age-encrypted Transparent Node.
///
/// Returns `Ok(was_cancelled)` where `was_cancelled` is true if the task
/// was cancelled mid-backup (partial snapshot was saved).
#[allow(clippy::too_many_arguments)]
pub async fn run_ingest(
    ctx: &TaskExecutorContext,
    vault_name: &str,
    source_name: &str,
    blob_store_name: &str,
    _target_path: Option<&str>,
    reporter: TaskReporter,
    cancel: CancellationToken,
    // When `Some`, run the incremental path: apply only these changed paths
    // (filtered per source) via `backup_incremental` instead of a full walk.
    changed_paths: Option<&[PathBuf]>,
) -> anyhow::Result<bool> {
    // -- Resolve config references --
    // Clone the config data we need and drop the lock immediately so that
    // config patches (e.g. `vup automate add`) are not blocked during a long backup.
    let (vault, source, recipients, identity_files) = {
        let config = ctx.config.read().await;
        let vault = resolve_vault(&config, vault_name)?.clone();
        let source = resolve_source(&config, source_name)?.clone();
        let (recipients, identity_files) = resolve_vault_key_info(&config, vault_name)?;
        (vault, source, recipients, identity_files)
    };
    let blob_store = resolve_store(&ctx.stores, blob_store_name)?;

    if source.paths.is_empty() {
        return Err(anyhow!("source '{}' has no paths configured", source_name));
    }

    // -- Open the vault's local meta store --
    // Auto-detects fjall (default for new vaults) vs LocalStore (existing
    // pre-fjall vaults with `meta/blob3/`); see `vault_meta_store_open`.
    let meta_store = vault_meta_store_open(&vault)?;

    // -- Build a combined read store (meta + blob) --
    // Wrap the meta store in a read cache: prolly tree nodes are small
    // (4-16 KiB) and read repeatedly during change detection — caching
    // them avoids redundant disk I/O + decryption for each file checked.
    let cached_meta: Arc<dyn BlobsRead> =
        Arc::new(CachedBlobsRead::new(Arc::new(meta_store.clone())));
    let read_store: Arc<dyn BlobsRead> =
        Arc::new(FallbackBlobsRead::new(cached_meta, blob_store.clone()));

    // -- Load previous snapshot --
    // Try inprogress first (resume), then current, then start empty.
    let prev_snapshot = {
        let inprogress_path = inprogress_root_path(&vault.root_path);
        let current_path = vault_root_path(&vault.root_path);

        let has_ip = inprogress_path.exists();
        let has_cur = current_path.exists();
        tracing::debug!(
            inprogress_exists = has_ip,
            current_exists = has_cur,
            "resume check"
        );

        // Load the previous vault root. We MUST propagate decrypt/parse errors
        // here: silently falling through to "no previous snapshot" would
        // generate a fresh master key and overwrite the real vault root,
        // destroying access to every existing blob. Only "file does not exist"
        // (i.e. `Ok(None)`) is a legitimate reason to start fresh.
        let vault_root = if has_ip {
            load_vault_root(&inprogress_path, &identity_files).with_context(|| {
                format!(
                    "loading inprogress vault root at {}",
                    inprogress_path.display()
                )
            })?
        } else if has_cur {
            tracing::debug!("no inprogress snapshot, trying current path");
            load_vault_root(&current_path, &identity_files).with_context(|| {
                format!("loading current vault root at {}", current_path.display())
            })?
        } else {
            None
        };

        match vault_root {
            Some((root, root_plaintext_hash, context)) => {
                tracing::info!(
                    vault = vault_name,
                    root = %root.fmt_short(),
                    has_keys = context.keys.is_some(),
                    "loaded previous snapshot"
                );
                Snapshot::new(root, read_store.clone(), context, root_plaintext_hash)
            }
            None => initial_snapshot_for_vault(&vault, vault_name, read_store.clone()),
        }
    };

    // -- Initialize progress (shared across all sources) --
    {
        let mut states = TaskProgressMap::new();
        // Honesty: this counts bytes handed to the store, which
        // on the packing/Sia backend means STAGED in the local spool — the
        // durable upload happens in pack flushes behind it. "1 GiB uploaded"
        // while zero bytes had left the machine is how the drill's stall went
        // unnoticed; say "staged" instead.
        states.bytes("bytes", 0, None).set_display_label("staged");
        states
            .count("files_added", 0, None)
            .set_display_label("files added");
        states
            .count("files_skipped", 0, None)
            .set_display_label("unchanged");
        states
            .count("files_errored", 0, None)
            .set_display_label("errors");
        reporter.init_progress(states);
    }

    // Shared stats accumulator across all sources
    let stats = Arc::new(BackupStats::default());

    // -- Run backup for each source path --
    let mut current_snapshot = prev_snapshot;
    let mut was_cancelled = false;

    for source_path_str in &source.paths {
        if cancel.is_cancelled() {
            return Ok(true); // Return that we were cancelled
        }

        let source_path = PathBuf::from(source_path_str);
        if !source_path.exists() {
            tracing::warn!(path = %source_path.display(), "source path does not exist, skipping");
            continue;
        }

        // Build WalkBuilder with source config
        let mut walker = WalkBuilder::new(&source_path);
        walker.hidden(source.skip_hidden);
        walker.git_ignore(source.respect_ignore_files);
        walker.ignore(source.respect_ignore_files);

        // Skip directories containing a valid CACHEDIR.TAG unless include_caches is set.
        if !source.include_caches {
            walker.filter_entry(|entry| {
                if !entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                    return true;
                }
                let tag_path = entry.path().join("CACHEDIR.TAG");
                if let Ok(mut file) = std::fs::File::open(&tag_path) {
                    let mut buf = [0u8; 43];
                    if std::io::Read::read_exact(&mut file, &mut buf).is_ok()
                        && &buf == b"Signature: 8a477f597d28d172789f06886806bc55"
                    {
                        return false;
                    }
                }
                true
            });
        }

        // Build the source exclude matcher ONCE and share it between both
        // backup paths: the full walk applies it via the WalkBuilder overrides
        // (excluded paths are never visited); the incremental path has no
        // walker and consults `backup_config.exclude` directly. Building it in
        // only one place is load-bearing — when the incremental path lacked it,
        // the watch loop republished excluded subtrees (crawl/, other_records/,
        // *.mphf) into the vault on every FS event.
        let exclude = if source.exclude.is_empty() {
            None
        } else {
            let mut overrides = ignore::overrides::OverrideBuilder::new(&source_path);
            for pattern in &source.exclude {
                overrides
                    .add(&format!("!{pattern}"))
                    .with_context(|| format!("invalid exclude pattern: {pattern}"))?;
            }
            Some(overrides.build().context("building exclude overrides")?)
        };
        if let Some(ov) = &exclude {
            walker.overrides(ov.clone());
        }

        let mut backup_config = BackupConfig {
            backup: true,
            one_file_system: source.one_file_system,
            follow_symlinks: source.follow_symlinks,
            detect_deletions: source.detect_deletions,
            routes: compile_pipeline_routes(&vault.pipelines)
                .with_context(|| format!("compiling vault.{vault_name}.pipelines"))?,
            exclude,
            ..Default::default()
        };
        if let Some(n) = source.max_concurrent_ops {
            backup_config.max_concurrent_ops = n;
        }

        tracing::info!(
            source = source_path_str,
            vault = vault_name,
            blob_store = blob_store_name,
            "starting ingest"
        );

        // Tree nodes go to both local meta and remote blob store,
        // so disaster recovery is possible from the remote alone.
        let tee_meta = TeeBlobsWrite::new(&meta_store, blob_store.as_ref());

        // Spawn a background task that reports live progress 5 times per second.
        let stats_for_reporter = stats.clone();
        let reporter_for_bg = reporter.clone();
        let reporter_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                let changed = stats_for_reporter
                    .files_changed
                    .load(std::sync::atomic::Ordering::Relaxed);
                let skipped = stats_for_reporter
                    .files_skipped
                    .load(std::sync::atomic::Ordering::Relaxed);
                let errored = stats_for_reporter
                    .files_errored
                    .load(std::sync::atomic::Ordering::Relaxed);
                let uploaded = stats_for_reporter
                    .bytes_uploaded
                    .load(std::sync::atomic::Ordering::Relaxed);
                reporter_for_bg.update_progress(|states| {
                    if let Some(s) = states.get_mut("bytes") {
                        s.progress = uploaded;
                    }
                    if let Some(s) = states.get_mut("files_added") {
                        s.progress = changed;
                    }
                    if let Some(s) = states.get_mut("files_skipped") {
                        s.progress = skipped;
                    }
                    if let Some(s) = states.get_mut("files_errored") {
                        s.progress = errored;
                    }
                });
            }
        });

        let result = match changed_paths {
            Some(all_changed) => {
                // Incremental: apply only the changed paths under this source.
                // The walker built above is unused on this path —
                // backup_incremental drives off the event paths instead.
                let under: Vec<PathBuf> = all_changed
                    .iter()
                    .filter(|p| p.starts_with(&source_path))
                    .cloned()
                    .collect();
                let _ = walker;
                backup_incremental(
                    &source_path,
                    &under,
                    &current_snapshot,
                    blob_store.as_ref(),
                    &tee_meta,
                    read_store.clone(),
                    &backup_config,
                    Some(stats.clone()),
                )
                .await
            }
            None => {
                backup(
                    &source_path,
                    &current_snapshot,
                    blob_store.as_ref(),
                    &tee_meta,
                    read_store.clone(),
                    &backup_config,
                    walker,
                    Some(stats.clone()),
                    Some(cancel.clone()),
                )
                .await
            }
        };

        // Stop the live reporter BEFORE propagating any error — otherwise
        // `?` on a backup failure skips the abort and leaks a 5 Hz task that
        // keeps overwriting the terminal state via the shared reporter.
        reporter_handle.abort();

        let result = result
            .with_context(|| format!("backup failed for source path {}", source_path.display()))?;

        let changed = stats
            .files_changed
            .load(std::sync::atomic::Ordering::Relaxed);
        let skipped = stats
            .files_skipped
            .load(std::sync::atomic::Ordering::Relaxed);
        let errored = stats
            .files_errored
            .load(std::sync::atomic::Ordering::Relaxed);
        let uploaded = stats
            .bytes_uploaded
            .load(std::sync::atomic::Ordering::Relaxed);

        tracing::info!(
            source = source_path_str,
            files_changed = changed,
            files_skipped = skipped,
            files_errored = errored,
            bytes_uploaded = uploaded,
            "ingest completed for source path"
        );

        // Update progress with accumulated final stats
        reporter.update_progress(|states| {
            if let Some(s) = states.get_mut("bytes") {
                s.progress = uploaded;
            }
            if let Some(s) = states.get_mut("files_added") {
                s.progress = changed;
            }
            if let Some(s) = states.get_mut("files_skipped") {
                s.progress = skipped;
            }
            if let Some(s) = states.get_mut("files_errored") {
                s.progress = errored;
            }
        });

        let BackupResult {
            snapshot,
            was_cancelled: source_cancelled,
        } = result;
        if source_cancelled {
            was_cancelled = true;
            tracing::info!("source {} was cancelled, stopping", source_path_str);
        }
        if let Some((new_snapshot, _stats)) = snapshot {
            current_snapshot = new_snapshot;

            // Save in-progress checkpoint on cancellation OR when running multiple source paths.
            // This ensures partial state is preserved for resume.
            if !current_snapshot.is_empty() && (source_cancelled || source.paths.len() > 1) {
                let ip_path = inprogress_root_path(&vault.root_path);
                std::fs::create_dir_all(&vault.root_path).ok();
                if let Err(e) = save_vault_root(&ip_path, &current_snapshot, &recipients) {
                    tracing::warn!(error = %e, "failed to save inprogress checkpoint");
                }
            }
        } else {
            tracing::info!(source = source_path_str, "no changes detected");
        }

        // If this source was cancelled, stop processing remaining sources
        if was_cancelled {
            break;
        }
    }

    // -- Save snapshot root --
    // Skip if cancelled — we already saved to inprogress, don't publish a partial snapshot
    if !current_snapshot.is_empty() && !was_cancelled {
        let current_path = vault_root_path(&vault.root_path);
        // Ensure vault root_path exists
        std::fs::create_dir_all(&vault.root_path)
            .with_context(|| format!("creating vault root at {}", vault.root_path))?;

        save_vault_root(&current_path, &current_snapshot, &recipients)
            .context("saving vault root")?;

        // Clean up any in-progress file
        remove_inprogress(&vault.root_path).ok();
    }

    tracing::info!(vault = vault_name, "ingest task completed");
    Ok(was_cancelled)
}

/// Build the empty starting snapshot for a brand-new vault.
///
/// Honors `vault.plaintext_tree`:
/// - `true`  → `Snapshot::empty_plain` — no encryption keys, no
///   compression. Tree nodes go to disk as plaintext CBOR; pair with a
///   per-ingest `pipeline` override (when that lands) to apply
///   compression on a per-subtree basis. Leaves the Transparent Node
///   wrapper (`vault_persist::save_vault_root`) still age-encrypting to
///   `recipients` — that's a separate concern for local-disk
///   defense-in-depth and applies independently of this flag.
/// - `false` → `Snapshot::empty_encrypted_split` with three freshly-
///   generated 32-byte secrets (leaf, node, recovery). Existing default.
pub(crate) fn initial_snapshot_for_vault(
    vault: &NodeConfigVault,
    vault_name: &str,
    read_store: Arc<dyn BlobsRead>,
) -> Snapshot {
    if vault.plaintext_tree {
        if vault.plaintext_published_tn {
            // Plaintext tree that WILL be published: the tree stays plaintext
            // (anonymous consumers read it), but the root needs a
            // KEY_SLOT_RECOVERY slot — the publish derives `vault_id` from it and
            // fails without it. Generate the recovery seed now (fixed for the
            // vault's life, committed to `vault_id`). Without this a freshly
            // created plaintext-published vault has no recovery slot and every
            // publish fails ("vault root has no KEY_SLOT_RECOVERY slot");
            // existing vaults got theirs from the 2026-05 four-key migration.
            tracing::info!(
                vault = vault_name,
                "no previous snapshot — initialising plaintext tree with recovery slot \
                 (plaintext_published_tn)"
            );
            let mut recovery_secret = [0u8; 32];
            rand::rng().fill_bytes(&mut recovery_secret);
            Snapshot::empty_plain_with_recovery(read_store, recovery_secret)
        } else {
            tracing::info!(
                vault = vault_name,
                "no previous snapshot — initialising plaintext tree (vault.plaintext_tree = true)"
            );
            Snapshot::empty_plain(read_store)
        }
    } else {
        tracing::info!(
            vault = vault_name,
            "no previous snapshot — generating encryption keys"
        );
        let mut leaf_key = [0u8; 32];
        let mut node_key = [0u8; 32];
        let mut recovery_secret = [0u8; 32];
        rand::rng().fill_bytes(&mut leaf_key);
        rand::rng().fill_bytes(&mut node_key);
        rand::rng().fill_bytes(&mut recovery_secret);
        Snapshot::empty_encrypted_split(read_store, leaf_key, node_key, recovery_secret)
    }
}

/// Compile a vault's `pipelines: Vec<PipelineRouteConfig>` into the
/// runtime-shaped `Vec<PipelineRoute>` that backup() consumes.
///
/// First-match-wins ordering is preserved (same Vec order). Each route's
/// glob is compiled here once at task start; matching at file-import time
/// is then a cheap walk over a pre-compiled GlobMatcher per route.
///
/// Returns an error if any glob is malformed — surface to the caller so
/// a typo doesn't silently route nothing.
fn compile_pipeline_routes(routes: &[PipelineRouteConfig]) -> anyhow::Result<Vec<PipelineRoute>> {
    routes
        .iter()
        .map(|r| {
            let glob = globset::Glob::new(&r.glob)
                .with_context(|| format!("invalid glob pattern '{}'", r.glob))?
                .compile_matcher();
            let override_ctx = TraversalContext {
                keys: None,
                leaf: r.pipeline.as_ref().map(blob_pipeline_from_config),
                node: None,
                chunking: r.chunking.as_ref().map(file_chunking_from_config),
            };
            Ok(PipelineRoute {
                glob,
                override_ctx,
                append_only: r.append_only,
            })
        })
        .collect()
}

fn blob_pipeline_from_config(c: &BlobPipelineConfig) -> BlobPipeline {
    BlobPipeline {
        compression: c.compression.as_ref().map(|cc| match cc {
            CompressionConfig::Uncompressed => CompressionStrategy::Uncompressed,
            // No level → unit `Zstd` variant (default level). With level
            // → the new `ZstdLevel` variant added at minicbor tag 0x03
            // for back-compat.
            CompressionConfig::Zstd { level: None } => CompressionStrategy::Zstd,
            CompressionConfig::Zstd { level: Some(l) } => {
                CompressionStrategy::ZstdLevel { level: *l }
            }
        }),
        // Padding/encryption knobs not yet exposed via TOML — see
        // `BlobPipelineConfig` doc comment. They stay at None (= inherit
        // from parent / vault default).
        padding: None,
        encryption: None,
        skip_when_unhelpful: c.skip_when_unhelpful,
    }
}

fn file_chunking_from_config(c: &FileChunkingConfig) -> FileChunkingStrategy {
    match c {
        FileChunkingConfig::None => FileChunkingStrategy::None,
        FileChunkingConfig::Fixed { chunk_size } => FileChunkingStrategy::Fixed {
            chunk_size: *chunk_size,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s5_core::blob::BlobStore;
    use s5_store_memory::MemoryStore;

    fn vault_for_test(plaintext_tree: bool) -> NodeConfigVault {
        NodeConfigVault {
            root_path: "/tmp/test-vault".into(),
            key: "test-key".into(),
            data_store: None,
            preset: None,
            recipients: vec![],
            sources: vec![],
            meta_store: None,
            plaintext_tree,
            plaintext_published_tn: false,
            watch: false,
            members: vec![],
            pipelines: vec![],
            vault_id: None,
            ..Default::default()
        }
    }

    fn read_store() -> Arc<dyn BlobsRead> {
        Arc::new(BlobStore::new(MemoryStore::new()))
    }

    #[test]
    fn plaintext_tree_skips_key_generation() {
        let vault = vault_for_test(true);
        let snap = initial_snapshot_for_vault(&vault, "test", read_store());

        // Plaintext tree → TraversalContext::default() → no keys, no leaf
        // pipeline, no node pipeline.
        let ctx = snap.context();
        assert!(ctx.keys.is_none(), "plaintext_tree must not generate keys");
        assert!(
            ctx.leaf.is_none(),
            "plaintext_tree must not pre-set a leaf pipeline (per-ingest override is the way to opt in)"
        );
        assert!(
            ctx.node.is_none(),
            "plaintext_tree must not pre-set a node pipeline"
        );
    }

    #[test]
    fn encrypted_default_generates_full_split_keys() {
        let vault = vault_for_test(false);
        let snap = initial_snapshot_for_vault(&vault, "test", read_store());

        // Default → empty_encrypted_split → all three slots populated.
        let ctx = snap.context();
        let keys = ctx
            .keys
            .as_ref()
            .expect("encrypted vault must have key slots");
        // KEY_SLOT_LEAF = 0x10, KEY_SLOT_NODE = 0x11, KEY_SLOT_RECOVERY = 0x12.
        // Use the public constants from s5_fs_v2 so we don't pin magic numbers.
        use s5_fs_v2::snapshot::{KEY_SLOT_LEAF, KEY_SLOT_NODE, KEY_SLOT_RECOVERY};
        assert!(keys.get(&KEY_SLOT_LEAF).is_some(), "leaf key missing");
        assert!(keys.get(&KEY_SLOT_NODE).is_some(), "node key missing");
        assert!(
            keys.get(&KEY_SLOT_RECOVERY).is_some(),
            "recovery secret missing"
        );
    }
}
