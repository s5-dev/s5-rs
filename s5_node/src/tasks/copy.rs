//! Copy task: the D21 sharing primitive.
//!
//! `run_copy` re-homes a source vault (or subtree) into a destination vault.
//!
//! * **Shallow** (default): reuse the source leaf ciphertext byte-for-byte and
//!   inline each leaf's *per-blob* key into the destination namespace nodes,
//!   which are re-encoded under the destination's own node keys. The source
//!   master data key is NEVER written into any destination structure — only
//!   `derive_blob_key(KDF_LEAF, src_master, plaintext_hash)` leaves the source
//!   vault, and only into nodes the destination reader can already decrypt.
//!   See [`s5_fs_v2::copy`].
//! * **Deep** (`--deep`): re-import the source plaintext under the
//!   destination's own derivation — brand-new ciphertext, no inlined keys,
//!   true future-revocability.
//!
//! Two guards protect the source-master-never-shared invariant:
//!   1. **Plaintext-dest guard** — a shallow copy into a destination whose node
//!      pipeline is not encrypting would leave the inlined keys in plaintext
//!      nodes; refused.
//!   2. **Honesty gate** — a shallow copy that widens the reader set (the
//!      destination has readers the source lacks) discloses the source data to
//!      those extra readers; refused unless `confirm_widen` (CLI `--yes`) or
//!      `--deep`.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, anyhow, bail};
use async_trait::async_trait;
use s5_core::blob::Blobs;
use s5_core::blob::tee::TeeBlobsWrite;
use s5_core::{BlobsRead, FallbackBlobsRead, Hash};
use s5_fs_v2::copy::{BlobReplicator, deep_copy_into, shallow_copy_into};
use s5_fs_v2::layer::MapLayer;
use s5_fs_v2::node::EncryptionStrategy;
use s5_fs_v2::snapshot::Snapshot;
use s5_node_api::TaskProgressMap;
use tokio_util::sync::CancellationToken;

use super::restore::open_vault_snapshot;
use super::vault_persist::{load_vault_root, save_vault_root, vault_root_path};
use super::{
    TaskExecutorContext, TaskReporter, resolve_store, resolve_vault, resolve_vault_key_info,
    vault_meta_store_open,
};

/// Content-addressed blob replicator: download a source ciphertext blob and,
/// unless it already exists in the destination store, re-upload it verbatim.
/// A shared store makes every call a no-op (the `blob_contains` gate), which is
/// what gives copy its zero-byte-movement property.
struct StoreReplicator {
    src_read: Arc<dyn BlobsRead>,
    dst: Arc<dyn Blobs>,
    uploads: AtomicU64,
}

#[async_trait]
impl BlobReplicator for StoreReplicator {
    async fn replicate(&self, hash: Hash) -> anyhow::Result<()> {
        if self.dst.blob_contains(hash).await? {
            return Ok(());
        }
        let bytes = self
            .src_read
            .blob_download(hash)
            .await
            .with_context(|| format!("replicating blob {hash}"))?;
        let id = self.dst.blob_upload_bytes(bytes).await?;
        // Ciphertext is reused verbatim, so the content address MUST be
        // preserved — a mismatch means something re-encrypted en route.
        anyhow::ensure!(
            id.hash == hash,
            "replicated blob changed hash ({hash} -> {}) — ciphertext must be byte-identical",
            id.hash
        );
        self.uploads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Run a copy task. See the module docs for the shallow/deep split and guards.
#[allow(clippy::too_many_arguments)]
pub async fn run_copy(
    ctx: &TaskExecutorContext,
    src_vault_name: &str,
    src_path: Option<&str>,
    src_snap: Option<&str>,
    dst_vault_name: &str,
    dst_path: Option<&str>,
    blob_store_name: &str,
    keys: &[String],
    deep: bool,
    confirm_widen: bool,
    reporter: TaskReporter,
    _cancel: CancellationToken,
) -> anyhow::Result<()> {
    // -- Phase 1: resolve config (drop the lock before any long work) --
    let (dst_vault, dst_identity_files, dst_save_recipients, mut src_readers, mut dst_readers) = {
        let config = ctx.config.read().await;
        let dst_vault = resolve_vault(&config, dst_vault_name)?.clone();
        let (dst_save_recipients, dst_identity_files) =
            resolve_vault_key_info(&config, dst_vault_name)?;
        let src_vault = resolve_vault(&config, src_vault_name)?;
        let src_readers = resolve_recipient_set(&config, &src_vault.recipients);
        let dst_readers = resolve_recipient_set(&config, keys);
        (
            dst_vault,
            dst_identity_files,
            dst_save_recipients,
            src_readers,
            dst_readers,
        )
    };

    // Union in any member-derived recipients (production sharing path).
    if let Some(m) = ctx.membership.as_ref() {
        let state = m.read().await;
        if let Some(vm) = state.vaults.get(src_vault_name) {
            src_readers.extend(vm.age_recipients.iter().cloned());
        }
        if let Some(vm) = state.vaults.get(dst_vault_name) {
            dst_readers.extend(vm.age_recipients.iter().cloned());
        }
    }

    let blob_store = resolve_store(&ctx.stores, blob_store_name)?.clone();

    // -- Open the source snapshot (handles `#snap` via Stage-2 history) --
    let src_snap_handle = open_vault_snapshot(ctx, src_vault_name, src_snap)
        .await
        .with_context(|| format!("opening source vault '{src_vault_name}'"))?;

    // The source leaf master = the key the source leaf pipeline references.
    // NEVER written into any destination structure — only used to derive the
    // per-blob keys the shallow path inlines.
    let src_leaf_master = leaf_master_of(&src_snap_handle);

    // -- Open (or create) the destination snapshot --
    let dst_meta = vault_meta_store_open(&dst_vault)?;
    let dst_meta_read: Arc<dyn BlobsRead> = Arc::new(dst_meta.clone());
    let blob_read: Arc<dyn BlobsRead> = blob_store.clone();
    let dst_read: Arc<dyn BlobsRead> = Arc::new(FallbackBlobsRead::new(dst_meta_read, blob_read));

    let dst_current = vault_root_path(&dst_vault.root_path);
    let dst_snap = match load_vault_root(&dst_current, &dst_identity_files)
        .context("reading destination vault root")?
    {
        Some((root, ph, ctx_)) => Snapshot::new(root, dst_read.clone(), ctx_, ph),
        None => {
            super::ingest::initial_snapshot_for_vault(&dst_vault, dst_vault_name, dst_read.clone())
        }
    };
    let dst_ctx = dst_snap.context().clone();

    // -- Guards (source-master-never-shared) --
    if !deep {
        // Break point #1: a plaintext destination would store the inlined keys
        // in plaintext nodes.
        let dst_node_encrypting = dst_ctx
            .node
            .as_ref()
            .and_then(|p| p.encryption.as_ref())
            .map(|(s, _)| *s != EncryptionStrategy::Plaintext)
            .unwrap_or(false);
        if !dst_node_encrypting {
            bail!(
                "refusing a shallow copy into '{dst_vault_name}:': its metadata tree is not \
                 encrypted, so the inlined per-blob keys would be stored in the clear. Use \
                 `--deep` to re-encrypt under the destination's keys, or target an encrypted vault."
            );
        }

        // Honesty gate (F-C/F-D): a shallow copy inlines source per-blob keys
        // into destination nodes readable by the DESTINATION reader set. Any
        // reader the destination has that the source lacks gains the ability to
        // decrypt source data they couldn't before.
        let widened: Vec<String> = dst_readers.difference(&src_readers).cloned().collect();
        if !widened.is_empty() && !confirm_widen {
            bail!(
                "refusing to widen the reader set: '{dst_vault_name}:' has {} reader(s) that \
                 '{src_vault_name}:' does not, so a shallow copy would disclose the source data to \
                 them. Re-run with confirmation (CLI `--yes`) to proceed, or use `--deep` to \
                 re-encrypt under the destination's keys (true revocability).",
                widened.len()
            );
        }
    }

    // -- Progress --
    {
        let mut states = TaskProgressMap::new();
        states
            .count("files_copied", 0, None)
            .set_display_label("files copied");
        reporter.init_progress(states);
    }

    // Tree nodes go to both the local meta store and the durable blob store so
    // the copied snapshot is self-contained (recoverable from the durable store
    // alone), mirroring ingest.
    let tee = TeeBlobsWrite::new(&dst_meta, blob_store.as_ref());

    // -- Build the destination namespace entries --
    let entries = if deep {
        // Deep re-imports data leaves under the DEST pipeline: they go straight
        // to the durable data store (like ingest); the namespace nodes land via
        // the tee in `merge_and_persist` below.
        deep_copy_into(
            &src_snap_handle,
            src_path,
            &dst_snap,
            blob_store.as_ref(),
            dst_path,
        )
        .await
        .context("deep copy")?
    } else {
        let repl = StoreReplicator {
            src_read: src_snap_handle.store().clone(),
            dst: blob_store.clone(),
            uploads: AtomicU64::new(0),
        };
        let entries = shallow_copy_into(
            &src_snap_handle,
            src_path,
            src_leaf_master.as_ref(),
            &dst_ctx,
            &tee,
            dst_read.clone(),
            dst_path,
            &repl,
        )
        .await
        .context("shallow copy")?;
        tracing::info!(
            src = src_vault_name,
            dst = dst_vault_name,
            data_blobs_uploaded = repl.uploads.load(Ordering::Relaxed),
            "shallow copy replicated data blobs (0 = shared store / already present)"
        );
        entries
    };

    let file_count = entries.len() as u64;
    reporter.update_progress(|states| {
        if let Some(s) = states.get_mut("files_copied") {
            s.progress = file_count;
        }
    });

    if entries.is_empty() {
        bail!(
            "copy matched nothing — source '{src_vault_name}:{}' is empty or the subtree does not \
             exist",
            src_path.unwrap_or("")
        );
    }

    // -- Merge into the destination tree + persist the new root --
    let layer = MapLayer::new(entries);
    let (new_root, new_ph, _stats) = dst_snap
        .merge_and_persist(&layer, &tee)
        .await
        .context("merging copied entries into the destination tree")?
        .ok_or_else(|| anyhow!("copy produced an empty destination tree"))?;

    let new_snap = Snapshot::new(new_root, dst_read.clone(), dst_ctx, Some(new_ph));
    std::fs::create_dir_all(&dst_vault.root_path)
        .with_context(|| format!("creating destination vault root at {}", dst_vault.root_path))?;
    save_vault_root(&dst_current, &new_snap, &dst_save_recipients)
        .context("saving destination vault root")?;

    // -- Publish the destination snapshot (so peers / cold devices can read) --
    super::publish::run_publish(ctx, dst_vault_name, keys)
        .await
        .with_context(|| format!("publishing destination vault '{dst_vault_name}'"))?;

    tracing::info!(
        src = src_vault_name,
        dst = dst_vault_name,
        deep,
        files = file_count,
        root = %new_root.fmt_short(),
        "copy completed"
    );
    Ok(())
}

/// The effective leaf master of an encrypted snapshot: the key its leaf
/// pipeline references (`KEY_SLOT_MASTER` for a single-master vault,
/// `KEY_SLOT_LEAF` for the split shape). `None` for a plaintext source or a
/// non-`DeterministicChaCha20` leaf pipeline.
fn leaf_master_of(snap: &Snapshot) -> Option<[u8; 32]> {
    let (strat, slot) = snap.context().leaf.as_ref()?.encryption.as_ref()?;
    if *strat != EncryptionStrategy::DeterministicChaCha20 {
        return None;
    }
    snap.context().keys.as_ref()?.get(slot).copied()
}

/// Resolve a list of `[key.*]` names to their age recipient public keys.
fn resolve_recipient_set(
    config: &crate::config::S5NodeConfig,
    key_names: &[String],
) -> BTreeSet<String> {
    key_names
        .iter()
        .filter_map(|n| config.key.get(n).map(|k| k.public_key.clone()))
        .collect()
}
