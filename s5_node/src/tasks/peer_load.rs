//! Load a peer's published snapshot for live multi-peer mounts.
//!
//! Each device that participates in a vault publishes its current
//! Transparent Node tip into the registry under
//! `StreamKey::Vault { pubkey: device_signing_pubkey, vault_id }` and
//! mirrors the encrypted blob to the vault's meta store. This module
//! is the read-side mirror: given a peer's `device_signing_pubkey` and
//! the shared `vault_id`, it walks the registry → store → age-decrypt
//! pipeline and returns a [`Snapshot`] over that peer's tip — ready to
//! drop into a [`MergedView`](s5_fs_v2::merge::MergedView) alongside our
//! own snapshot.
//!
//! This is the primitive behind `vup mount <vault>:` showing peers' files.
//! Today there is no `[vault.<name>.peers]` config field that records the
//! enumerated peer pubkeys (a future Pair flow concern); for now the
//! caller specifies peers explicitly. Keeping the helper this narrow
//! means the same primitive serves the eventual config-driven enumeration
//! and any explicit `--peer <hex>` CLI flag without rewriting.
//!
//! The shared `vault_id` is derived from the vault's `recovery_secret`
//! (carried in `KEY_SLOT_RECOVERY` of the vault root's
//! `TraversalContext`). It is identical across all peers of a given
//! vault — that's what makes `(peer_pubkey, vault_id)` a usable lookup
//! key on the receive side.

use std::sync::Arc;

use anyhow::Context;
use s5_core::blob::Blobs;
use s5_core::{BlobsRead, RegistryApi, StreamKey};
use s5_fs_v2::snapshot::Snapshot;

use crate::tasks::publish::fetch_previous_published_node;
use crate::tasks::vault_persist::node_to_snapshot_parts;

/// Load a single peer's currently published snapshot.
///
/// Returns `Ok(None)` when the peer has nothing published yet (no
/// registry entry under `(peer_pubkey, vault_id)`). Returns an error
/// when the entry exists but the blob is missing, the age decryption
/// fails (we are not in the recipient set), or the CBOR is corrupted.
///
/// `read_store` is what the returned `Snapshot` will use for subsequent
/// node + leaf fetches — typically the same fall-through chain the
/// caller wires for its own snapshot, since peer leaves live in the
/// same content-addressed store(s).
pub async fn load_peer_snapshot(
    peer_pubkey: [u8; 32],
    vault_id: [u8; 16],
    registry: &dyn RegistryApi,
    blob_store: &dyn Blobs,
    identity_files: &[String],
    read_store: Arc<dyn BlobsRead>,
) -> anyhow::Result<Option<Snapshot>> {
    let stream_key = StreamKey::Vault {
        pubkey: peer_pubkey,
        vault_id,
    };

    let Some((node, _entry_hash, _revision)) =
        fetch_previous_published_node(registry, blob_store, &stream_key, identity_files)
            .await
            .with_context(|| {
                format!(
                    "fetch peer TN (pubkey={}, vault_id={})",
                    hex::encode(peer_pubkey),
                    hex::encode(vault_id),
                )
            })?
    else {
        return Ok(None);
    };

    let (root, root_plaintext_hash, ctx) = node_to_snapshot_parts(&node)
        .context("extracting snapshot parts from peer Transparent Node")?;

    Ok(Some(Snapshot::new(
        root,
        read_store,
        ctx,
        root_plaintext_hash,
    )))
}
