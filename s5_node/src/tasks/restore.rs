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
pub async fn run_remote_restore(
    ctx: &TaskExecutorContext,
    age_secret_key: &str,
    vault_name: &str,
    blob_store_name: &str,
    target_path: &str,
    reporter: TaskReporter,
    _cancel: CancellationToken,
) -> anyhow::Result<()> {
    use super::publish::{derive_vault_id, recovery_signing_key};
    use ed25519_dalek::VerifyingKey;
    use s5_fs_v2::snapshot::KEY_SLOT_RECOVERY;

    let registry = ctx
        .registry
        .as_ref()
        .ok_or_else(|| anyhow!("no registry configured, cannot perform remote restore"))?;

    let blob_store = resolve_store(&ctx.stores, blob_store_name)?;

    // Step 1: discover the vault root by enumerating blobs in the
    // store and trying age-decrypt with the paper key. The first blob
    // that decrypts and parses as a Node carrying KEY_SLOT_RECOVERY is
    // a vault root. Note: this finds *some* vault root for the paper
    // recipient (which gives us recovery_secret); the actual current
    // HEAD comes from the registry lookup in step 4 below, so even an
    // older snap's vault root is fine here.
    tracing::info!(
        vault = vault_name,
        "scanning relay store for a vault root decryptable with the paper key"
    );
    let recovery_secret = discover_recovery_secret(blob_store, age_secret_key).await?;
    tracing::info!(vault = vault_name, "found a vault root, deriving vault_id");

    // Step 2: derive vault_id + recovery_signing_key from the recovered secret.
    let vault_id = derive_vault_id(&recovery_secret);
    let recovery_key = recovery_signing_key(&recovery_secret);
    let recovery_verifying: VerifyingKey = (&recovery_key).into();
    let recovery_stream_key = StreamKey::Vault {
        pubkey: recovery_verifying.to_bytes(),
        vault_id,
    };

    // Step 3: recovery entry → device's signing pubkey.
    let recovery_entry = registry
        .get(&recovery_stream_key)
        .await
        .context("fetching recovery registry entry")?
        .ok_or_else(|| {
            anyhow!(
                "no recovery entry found for vault '{vault_name}' (vault_id={}). \
                 Was this vault ever published?",
                hex::encode(vault_id)
            )
        })?;
    let device_pubkey_bytes: [u8; 32] = *recovery_entry.hash.as_bytes();
    tracing::info!(
        vault = vault_name,
        device_pubkey = hex::encode(device_pubkey_bytes),
        "resolved device signing pubkey via recovery entry"
    );

    // Step 4: vault entry under (device_pubkey, vault_id) → latest TN hash.
    let vault_stream_key = StreamKey::Vault {
        pubkey: device_pubkey_bytes,
        vault_id,
    };
    let vault_entry = registry
        .get(&vault_stream_key)
        .await
        .context("fetching vault registry entry")?
        .ok_or_else(|| {
            anyhow!(
                "no published snapshot found for vault '{vault_name}' under \
                 device pubkey {} + vault_id {}",
                hex::encode(device_pubkey_bytes),
                hex::encode(vault_id),
            )
        })?;
    let encrypted_tn_hash = vault_entry.hash;
    tracing::info!(
        vault = vault_name,
        revision = vault_entry.revision,
        encrypted_blob = %encrypted_tn_hash.fmt_short(),
        "found latest published snapshot"
    );

    // Step 5: fetch + age-decrypt the latest TN.
    let encrypted_bytes = blob_store
        .blob_download(encrypted_tn_hash)
        .await
        .map_err(|e| anyhow!("downloading encrypted Transparent Node: {e}"))?;
    let cbor = age_decrypt_with_secret_key(&encrypted_bytes, age_secret_key)
        .context("decrypting latest Transparent Node with paper key")?;

    // Step 6: Parse Node → snapshot parts. The published TN may have
    // history entries (timestamps → previous hashes); only the "" entry
    // (current snapshot) matters here.
    let node = Node::from_bytes(&cbor).map_err(|e| anyhow!("CBOR decode Transparent Node: {e}"))?;
    let (root, root_plaintext_hash, context) = node_to_snapshot_parts(&node)
        .context("extracting snapshot from published Transparent Node")?;

    // Defence: the freshly-fetched TN must carry the same recovery_secret
    // we derived vault_id from. A mismatch means the registry entry
    // points at a substituted blob (recovery_secret would be different)
    // and we should not splice that into the restore.
    let fetched_recovery_secret = context
        .keys
        .as_ref()
        .and_then(|m| m.get(&KEY_SLOT_RECOVERY).copied())
        .ok_or_else(|| anyhow!("latest TN has no KEY_SLOT_RECOVERY slot"))?;
    if fetched_recovery_secret != recovery_secret {
        return Err(anyhow!(
            "vault root substitution detected: latest TN's KEY_SLOT_RECOVERY \
             does not match the discovered vault root's. Refusing to restore."
        ));
    }

    let history_count = node.entries.len() - 1;
    tracing::info!(
        vault = vault_name,
        root = %root.fmt_short(),
        has_keys = context.keys.is_some(),
        history_entries = history_count,
        "decrypted snapshot metadata"
    );

    // Step 7: build Snapshot using the remote store as read backend.
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

/// Bootstrap step for paper-only recovery: enumerate every blob in the
/// store, try to age-decrypt with the paper key, and return the
/// `recovery_secret` from the first blob that decrypts and parses as a
/// vault root (a `Node` whose `TraversalContext.keys` carries
/// `KEY_SLOT_RECOVERY`).
///
/// Once we have `recovery_secret` we can derive `vault_id` and look up
/// the registry entry for the actual current HEAD — see
/// [`run_remote_restore`]. The blob found here may be an older snap;
/// that's fine, we only need it for `recovery_secret`.
///
/// O(N) over the relay's blob count. For the M3 demo (single vault,
/// single device) the relay holds dozens of blobs and the first hit
/// is typically the encrypted TN. Enumeration order is whatever the
/// underlying `Store::list` returns; we stop at the first match.
async fn discover_recovery_secret(
    blob_store: &s5_core::blob::BlobStore,
    age_secret_key: &str,
) -> anyhow::Result<[u8; 32]> {
    use s5_fs_v2::snapshot::KEY_SLOT_RECOVERY;

    let hashes = blob_store
        .list_hashes()
        .await
        .map_err(|e| anyhow!("listing blobs in relay store: {e}"))?;

    let total = hashes.len();
    for hash in hashes {
        let bytes = match blob_store.blob_download(hash).await {
            Ok(b) => b,
            Err(_) => continue,
        };
        // Most blobs are vault-encrypted leaves, not age-encrypted.
        // age_decrypt_with_secret_key fails fast on those; we just skip.
        let cbor = match age_decrypt_with_secret_key(&bytes, age_secret_key) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let node = match Node::from_bytes(&cbor) {
            Ok(n) => n,
            Err(_) => continue,
        };
        let entry = match node.transparent_entry() {
            Some(e) => e,
            None => continue,
        };
        let recovery_secret = entry
            .child_context
            .as_ref()
            .and_then(|ctx| ctx.keys.as_ref())
            .and_then(|keys| keys.get(&KEY_SLOT_RECOVERY).copied());
        if let Some(secret) = recovery_secret {
            return Ok(secret);
        }
    }

    Err(anyhow!(
        "scanned {total} blob(s) in the relay store, found no vault root \
         decryptable with the supplied paper key. Either the wrong store \
         is configured, or no snapshot of any vault has been published \
         to it yet for this paper recipient."
    ))
}
