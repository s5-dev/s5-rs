//! Ingest task: walk source dirs, import blobs, persist snapshot.
//!
//! Resolves vault/source/store from config, builds a `WalkBuilder`,
//! calls `s5_fs_local::backup()`, and merges the result into the vault.
//! On completion, saves the new snapshot as an age-encrypted Transparent Node.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use rand::RngCore;
use s5_core::blob::BlobStore;
use s5_core::blob::tee::TeeBlobsWrite;
use s5_core::{BlobsRead, FallbackBlobsRead};
use s5_fs_local::{BackupConfig, WalkBuilder, backup};
use s5_fs_v2::snapshot::Snapshot;
use s5_node_api::TaskProgress;
use s5_store_local::LocalStore;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use super::vault_persist::{
    inprogress_root_path, load_vault_root, remove_inprogress, save_vault_root, vault_root_path,
};
use super::{
    TaskExecutorContext, resolve_source, resolve_store, resolve_vault, vault_meta_store_path,
};

/// Run an ingest task.
///
/// 1. Resolve vault, source, and blob_store from config.
/// 2. Open (or create) the vault's local meta store.
/// 3. Load previous snapshot from vault root (or inprogress for resume).
/// 4. Build a WalkBuilder from source config.
/// 5. Call `s5_fs_local::backup()` for each source path.
/// 6. Save new snapshot as age-encrypted Transparent Node.
pub async fn run_ingest(
    ctx: &TaskExecutorContext,
    vault_name: &str,
    source_name: &str,
    blob_store_name: &str,
    _target_path: Option<&str>,
    progress: Arc<RwLock<Option<TaskProgress>>>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // -- Resolve config references --
    // Clone the config data we need and drop the lock immediately so that
    // config patches (e.g. `vup add`) are not blocked during a long backup.
    let (vault, source) = {
        let config = ctx.config.read().await;
        let vault = resolve_vault(&config, vault_name)?.clone();
        let source = resolve_source(&config, source_name)?.clone();
        (vault, source)
    };
    let blob_store = resolve_store(&ctx.stores, blob_store_name)?;

    if source.paths.is_empty() {
        return Err(anyhow!("source '{}' has no paths configured", source_name));
    }

    // -- Open the vault's local meta store --
    let meta_path = vault_meta_store_path(&vault);
    std::fs::create_dir_all(&meta_path)
        .with_context(|| format!("creating meta store at {}", meta_path.display()))?;
    let meta_store = BlobStore::new(LocalStore::create(s5_store_local::LocalStoreConfig {
        base_path: meta_path.to_string_lossy().into_owned(),
    }));

    // -- Build a combined read store (meta + blob) --
    let read_store: Arc<dyn BlobsRead> = Arc::new(FallbackBlobsRead::new(
        Arc::new(meta_store.clone()),
        Arc::new(blob_store.clone()),
    ));

    // -- Load previous snapshot --
    // Try inprogress first (resume), then current, then start empty.
    let prev_snapshot = {
        let inprogress_path = inprogress_root_path(&vault.root_path);
        let current_path = vault_root_path(&vault.root_path);

        let vault_root = load_vault_root(&inprogress_path, &ctx.node_secret, vault_name)
            .ok()
            .flatten()
            .or_else(|| {
                load_vault_root(&current_path, &ctx.node_secret, vault_name)
                    .ok()
                    .flatten()
            });

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
            None => {
                tracing::info!(
                    vault = vault_name,
                    "no previous snapshot — generating encryption keys"
                );
                let mut leaf_key = [0u8; 32];
                let mut node_key = [0u8; 32];
                rand::rngs::OsRng.fill_bytes(&mut leaf_key);
                rand::rngs::OsRng.fill_bytes(&mut node_key);
                Snapshot::empty_encrypted_split(read_store.clone(), leaf_key, node_key)
            }
        }
    };

    // -- Initialize progress --
    {
        let mut p = progress.write().await;
        *p = Some(TaskProgress::Ingest {
            files_scanned: 0,
            files_changed: 0,
            files_skipped: 0,
            files_errored: 0,
            bytes_uploaded: 0,
        });
    }

    // -- Run backup for each source path --
    let mut current_snapshot = prev_snapshot;

    for source_path_str in &source.paths {
        if cancel.is_cancelled() {
            return Err(anyhow!("task cancelled"));
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

        // Add exclude patterns
        if !source.exclude.is_empty() {
            let mut overrides = ignore::overrides::OverrideBuilder::new(&source_path);
            for pattern in &source.exclude {
                overrides
                    .add(&format!("!{pattern}"))
                    .with_context(|| format!("invalid exclude pattern: {pattern}"))?;
            }
            let built = overrides.build().context("building exclude overrides")?;
            walker.overrides(built);
        }

        let backup_config = BackupConfig {
            backup: true,
            one_file_system: source.one_file_system,
            ..Default::default()
        };

        tracing::info!(
            source = source_path_str,
            vault = vault_name,
            blob_store = blob_store_name,
            "starting ingest"
        );

        // Tree nodes go to both local meta and remote blob store,
        // so disaster recovery is possible from the remote alone.
        let tee_meta = TeeBlobsWrite::new(&meta_store, blob_store);

        let result = backup(
            &source_path,
            &current_snapshot,
            blob_store,
            &tee_meta,
            read_store.clone(),
            &backup_config,
            walker,
        )
        .await
        .with_context(|| format!("backup failed for source path {}", source_path.display()))?;

        if let Some((new_snapshot, stats)) = result {
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

            // Update progress
            {
                let mut p = progress.write().await;
                *p = Some(TaskProgress::Ingest {
                    files_scanned: changed + skipped + errored,
                    files_changed: changed,
                    files_skipped: skipped,
                    files_errored: errored,
                    bytes_uploaded: uploaded,
                });
            }

            current_snapshot = new_snapshot;

            // Save in-progress checkpoint (for resume if we crash between source paths)
            if !current_snapshot.is_empty() && source.paths.len() > 1 {
                let ip_path = inprogress_root_path(&vault.root_path);
                std::fs::create_dir_all(&vault.root_path).ok();
                if let Err(e) =
                    save_vault_root(&ip_path, &current_snapshot, &ctx.node_secret, vault_name)
                {
                    tracing::warn!(error = %e, "failed to save inprogress checkpoint");
                }
            }
        } else {
            tracing::info!(source = source_path_str, "no changes detected");
        }
    }

    // -- Save snapshot root --
    if !current_snapshot.is_empty() {
        let current_path = vault_root_path(&vault.root_path);
        // Ensure vault root_path exists
        std::fs::create_dir_all(&vault.root_path)
            .with_context(|| format!("creating vault root at {}", vault.root_path))?;

        save_vault_root(
            &current_path,
            &current_snapshot,
            &ctx.node_secret,
            vault_name,
        )
        .context("saving vault root")?;

        // Clean up any in-progress file
        remove_inprogress(&vault.root_path).ok();
    }

    tracing::info!(vault = vault_name, "ingest task completed");
    Ok(())
}
