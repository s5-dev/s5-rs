//! Restore task: load a vault snapshot and write it to a local directory.
//!
//! Two restore paths:
//!
//! 1. **Local restore** (`run_restore`): reads the vault root from a local
//!    age-encrypted file, builds a combined meta+blob read store, and restores.
//!
//! 2. **Remote restore** (`run_remote_restore`): disaster recovery using only
//!    the paper age key + vault name. Derives the recovery registry key,
//!    looks up the vault's signing key, fetches the latest encrypted TN
//!    from the blob store, age-decrypts with the paper key, and restores
//!    from the remote store.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use s5_core::{BlobsRead, FallbackBlobsRead, RegistryApi, StreamKey};
use s5_fs_local::{RestoreConfig, restore};
use s5_fs_v2::node::Node;
use s5_fs_v2::snapshot::Snapshot;
use s5_node_api::TaskProgressMap;
use s5_store_local::LocalStore;
use tokio_util::sync::CancellationToken;

use super::TaskReporter;

use super::publish::age_decrypt_with_secret_key;
use super::vault_persist::{load_vault_root, node_to_snapshot_parts, vault_root_path};
use super::{
    TaskExecutorContext, resolve_store, resolve_vault, resolve_vault_key_info,
    vault_meta_store_path,
};

/// Run a restore task.
///
/// 1. Resolve vault from config.
/// 2. Load the current snapshot from the vault root Transparent Node.
/// 3. Open the vault's local meta store.
/// 4. Build a combined read store (meta local + blob stores from vault config).
/// 5. Call `s5_fs_local::restore()`.
pub async fn run_restore(
    ctx: &TaskExecutorContext,
    vault_name: &str,
    target_path: &str,
    blob_store_override: Option<&str>,
    reporter: TaskReporter,
    _cancel: CancellationToken,
) -> anyhow::Result<()> {
    let (vault, identity_files) = {
        let config = ctx.config.read().await;
        let vault = resolve_vault(&config, vault_name)?.clone();
        let (_, id_files) = resolve_vault_key_info(&config, vault_name)?;
        (vault, id_files)
    };

    // -- Load vault root from Transparent Node --
    let current_path = vault_root_path(&vault.root_path);
    let (root, root_plaintext_hash, context) = load_vault_root(&current_path, &identity_files)
        .context("reading vault root")?
        .ok_or_else(|| {
            anyhow!(
                "vault '{}' has no snapshot to restore (root file not found)",
                vault_name
            )
        })?;

    tracing::info!(
        vault = vault_name,
        root = %root.fmt_short(),
        has_keys = context.keys.is_some(),
        "loaded snapshot for restore"
    );

    // -- Open the vault's local meta store --
    let meta_path = vault_meta_store_path(&vault);
    if !meta_path.exists() {
        return Err(anyhow!(
            "vault '{}' meta store not found at {}",
            vault_name,
            meta_path.display()
        ));
    }
    let meta_store = Arc::new(s5_core::blob::BlobStore::new(LocalStore::create(
        s5_store_local::LocalStoreConfig {
            base_path: meta_path.to_string_lossy().into_owned(),
        },
    )));

    // -- Build read store for file content --
    let blob_read: Arc<dyn BlobsRead> = if let Some(store_name) = blob_store_override {
        let store = resolve_store(&ctx.stores, store_name)?;
        Arc::new(store.clone())
    } else if vault.blob_stores.is_empty() {
        return Err(anyhow!(
            "vault '{}' has no blob_stores configured and no blob_store override given",
            vault_name
        ));
    } else {
        // Chain vault's blob stores as fallbacks (first = primary, rest = fallbacks).
        let mut stores: Vec<Arc<dyn BlobsRead>> = Vec::new();
        for name in &vault.blob_stores {
            let store = resolve_store(&ctx.stores, name)?;
            stores.push(Arc::new(store.clone()));
        }
        // Fold into nested FallbackBlobsRead: first -> fallback(second -> ...)
        let mut combined: Arc<dyn BlobsRead> = stores.pop().unwrap();
        while let Some(primary) = stores.pop() {
            combined = Arc::new(FallbackBlobsRead::new(primary, combined));
        }
        combined
    };

    // Combined reader: meta store (for tree nodes) + blob stores (for file content)
    let read_store: Arc<dyn BlobsRead> =
        Arc::new(FallbackBlobsRead::new(meta_store.clone(), blob_read));

    // -- Build snapshot with full context and restore --
    let snapshot = Snapshot::new(root, read_store, context, root_plaintext_hash);
    let target = PathBuf::from(target_path);
    let config = RestoreConfig { backup: true };

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

// ---------------------------------------------------------------------------
// Remote restore (disaster recovery)
// ---------------------------------------------------------------------------

/// Disaster recovery restore using only a paper age key + vault name.
///
/// Flow:
/// 1. Derive recovery Ed25519 key from age secret + vault name
/// 2. Look up recovery registry entry → get vault's verifying key bytes
/// 3. Look up vault's registry entry → get encrypted TN hash + revision
/// 4. Download encrypted TN blob from store
/// 5. Age-decrypt with the paper key
/// 6. Parse Node → extract snapshot parts
/// 7. Build Snapshot using the remote store as read backend
/// 8. Restore files to target path
// Variables resolved at the top of the function are used only by the
// unreachable code below (kept as a starting point for the v3 rewrite),
// so silence the unused warnings at function scope.
#[allow(unused_variables)]
pub async fn run_remote_restore(
    ctx: &TaskExecutorContext,
    age_secret_key: &str,
    vault_name: &str,
    blob_store_name: &str,
    target_path: &str,
    reporter: TaskReporter,
    _cancel: CancellationToken,
) -> anyhow::Result<()> {
    let registry = ctx
        .registry
        .as_ref()
        .ok_or_else(|| anyhow!("no registry configured — cannot perform remote restore"))?;

    let blob_store = resolve_store(&ctx.stores, blob_store_name)?;

    // -- Step 1: paper-only recovery is not yet wired for the v3 schema --
    //
    // The v3 recovery flow:
    //   1. Derive recovery_age_secret = argon2id(paper_passphrase, …)
    //   2. Fetch the vault root blob from a configured store
    //      (e.g. the relay S3 bucket holding meta blobs)
    //   3. age-decrypt the vault root with recovery_age_secret
    //   4. Read KEY_SLOT_RECOVERY from its TraversalContext.keys
    //   5. Derive vault_id + recovery_signing_key from that secret
    //      (see s5_node::tasks::publish::{derive_vault_id, recovery_signing_key})
    //   6. registry.get(StreamKey::Vault { pubkey: recovery_pubkey, vault_id })
    //      → its payload holds a device's signing pubkey
    //   7. registry.get(StreamKey::Vault { pubkey: device_pubkey, vault_id })
    //      → current snapshot HEAD hash
    //
    // The legacy `(age_secret, vault_name)` lookup that this function
    // used in the pre-v3 schema produces wrong stream keys for v3
    // vaults, so rather than silently returning empty, we surface a
    // clear error until the new flow is implemented.
    let _ = age_secret_key;
    return Err(anyhow!(
        "remote restore for vault '{vault_name}' is not yet supported on \
         the v3 schema — the recovery flow needs to fetch and \
         age-decrypt the vault root first to derive vault_id from \
         KEY_SLOT_RECOVERY (see snapshot-publication.md § Vault ID \
         derivation)"
    ));

    // The unreachable code below preserves the shape of the legacy
    // restore so the v3 reimplementation has a starting point.
    #[allow(unreachable_code)]
    let recovery_stream_key: StreamKey = unreachable!();
    #[allow(unreachable_code)]
    let recovery_entry = registry
        .get(&recovery_stream_key)
        .await
        .context("fetching recovery registry entry")?
        .ok_or_else(|| {
            anyhow!(
                "no recovery entry found for vault '{}' — \
                 was this vault ever published with a recovery key?",
                vault_name
            )
        })?;

    // The recovery entry's hash field stores the device's signing pubkey.
    let device_pubkey_bytes: [u8; 32] = *recovery_entry.hash.as_bytes();
    let vault_stream_key = StreamKey::Vault {
        pubkey: device_pubkey_bytes,
        vault_id: [0u8; 16],
    };

    tracing::info!(
        vault = vault_name,
        vault_pubkey = hex::encode(device_pubkey_bytes),
        "found vault pubkey via recovery entry"
    );

    // -- Step 3: Vault entry → encrypted TN hash --
    let vault_entry = registry
        .get(&vault_stream_key)
        .await
        .context("fetching vault registry entry")?
        .ok_or_else(|| {
            anyhow!(
                "no published snapshot found for vault '{}' — \
                 vault pubkey {} has no registry entry",
                vault_name,
                hex::encode(device_pubkey_bytes)
            )
        })?;

    let encrypted_tn_hash = vault_entry.hash;

    tracing::info!(
        vault = vault_name,
        revision = vault_entry.revision,
        encrypted_blob = %encrypted_tn_hash.fmt_short(),
        "found latest published snapshot"
    );

    // -- Step 4: Download encrypted TN --
    let encrypted_bytes = blob_store
        .blob_download(encrypted_tn_hash)
        .await
        .map_err(|e| anyhow!("downloading encrypted Transparent Node: {e}"))?;

    tracing::info!(
        vault = vault_name,
        size = encrypted_bytes.len(),
        "downloaded encrypted Transparent Node"
    );

    // -- Step 5: Age-decrypt with paper key --
    let cbor = age_decrypt_with_secret_key(&encrypted_bytes, age_secret_key)
        .context("decrypting Transparent Node with paper key")?;

    // -- Step 6: Parse Node → snapshot parts --
    // The published TN may have history entries (timestamps → previous hashes).
    // We only care about the current entry at "".
    let node = Node::from_bytes(&cbor).map_err(|e| anyhow!("CBOR decode Transparent Node: {e}"))?;

    let (root, root_plaintext_hash, context) = node_to_snapshot_parts(&node)
        .context("extracting snapshot from published Transparent Node")?;

    let history_count = node.entries.len() - 1; // exclude ""

    tracing::info!(
        vault = vault_name,
        root = %root.fmt_short(),
        has_keys = context.keys.is_some(),
        history_entries = history_count,
        "decrypted snapshot metadata"
    );

    // -- Step 7: Build Snapshot using remote store --
    let read_store: Arc<dyn BlobsRead> = Arc::new(blob_store.clone());
    let snapshot = Snapshot::new(root, read_store, context, root_plaintext_hash);

    // -- Step 8: Restore --
    let target = PathBuf::from(target_path);
    let config = RestoreConfig { backup: true };

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
        .with_context(|| {
            format!(
                "restoring vault '{}' from remote to {}",
                vault_name, target_path
            )
        })?;

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
        "remote restore completed"
    );

    Ok(())
}
