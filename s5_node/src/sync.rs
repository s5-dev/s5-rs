use std::{path::Path, sync::Arc};

use anyhow::Result;
use ed25519_dalek::SigningKey as DalekSigningKey;
use s5_blobs::Client as BlobsClient;
use s5_core::{RegistryApi, StreamKey};
use s5_fs::{DirActorContext, FS5, SigningKey as FsSigningKey};

use crate::{RemoteRegistry, S5Node};

// Re-export from s5_fs for convenience
pub use s5_fs::derive_sync_keys as derive_sync_keys_raw;

/// Derived cryptographic material for FS sync between trusted peers.
#[derive(Clone)]
pub struct SyncKeys {
    pub encryption_key: [u8; 32],
    pub signing_key: DalekSigningKey,
    pub public_key: [u8; 32],
}

impl SyncKeys {
    /// Returns the registry stream key associated with these derived keys.
    pub fn stream_key(&self) -> StreamKey {
        StreamKey::PublicKeyEd25519(self.public_key)
    }

    /// Returns the signing key in the FS5 context wrapper.
    pub fn fs_signing_key(&self) -> FsSigningKey {
        FsSigningKey::new(self.signing_key.to_bytes())
    }
}

/// Derives the XChaCha20 encryption key and Ed25519 signing key from a shared secret.
///
/// Uses `s5_fs::derive_sync_keys` internally to ensure consistent
/// key derivation across all clients.
pub fn derive_sync_keys(shared_secret: impl AsRef<[u8]>) -> SyncKeys {
    let (encryption_key, signing_key_bytes, public_key) =
        s5_fs::derive_sync_keys(shared_secret.as_ref());

    SyncKeys {
        encryption_key,
        signing_key: DalekSigningKey::from_bytes(&signing_key_bytes),
        public_key,
    }
}

/// Opens a plaintext FS5 instance rooted at the given path.
pub fn open_plaintext_fs(path: &Path) -> Result<FS5> {
    let context = DirActorContext::open_local_root(path)?;
    Ok(FS5::open(context))
}

/// Opens an encrypted FS5 instance backed by remote blob + registry services.
pub fn open_encrypted_fs(
    stream_key: StreamKey,
    derived: &SyncKeys,
    blob_client: BlobsClient,
    registry: RemoteRegistry,
) -> FS5 {
    // Client implements BlobsReadWrite directly via the server feature
    let blob_store: Arc<dyn s5_core::BlobsReadWrite> = Arc::new(blob_client);
    let registry_arc: Arc<dyn RegistryApi + Send + Sync> = Arc::new(registry);

    let context = DirActorContext::new_encrypted_registry(
        stream_key,
        derived.fs_signing_key(),
        derived.encryption_key,
        blob_store,
        registry_arc,
    );
    FS5::open(context)
}

/// Pushes the current plaintext snapshot into the encrypted FS and publishes it remotely.
///
/// TODO(perf): pipeline blob upload and hashing/outboard generation with
/// network sends in the blob layer so large snapshot pushes do not leave
/// either CPU or network idle for extended periods.
///
/// TODO: Consider an optimization or mode where we "only sync inline blobs for now"
/// to improve ergonomics for small-file workloads, as suggested in README.md.
pub async fn push_snapshot(plaintext: &FS5, encrypted: &FS5) -> Result<()> {
    let snapshot = plaintext.export_snapshot().await?;
    encrypted.merge_from_snapshot(snapshot).await?;
    encrypted.sync().await?;
    Ok(())
}

/// Pulls the latest encrypted snapshot into the plaintext FS.
///
/// TODO(perf): cache decrypted/parsed directory snapshots on the
/// plaintext side between sync ticks if repeated pulls dominate
/// latency for read-heavy remote nodes.
pub async fn pull_snapshot(encrypted: &FS5, plaintext: &FS5) -> Result<()> {
    let snapshot = encrypted.export_snapshot().await?;
    plaintext.merge_from_snapshot(snapshot).await?;
    plaintext.sync().await?;
    Ok(())
}

/// Runs all configured file syncs for a node.
///
/// NOTE: The old `[sync.*]` config section has been removed. Sync is now
/// handled through the vault/task model. This function is a no-op stub
/// until the new task executor integrates sync logic.
pub async fn run_file_sync(_node: &S5Node) -> Result<()> {
    // The old config.sync field no longer exists. Sync operations will be
    // driven by the task executor via RunTask RPC in the future.
    tracing::debug!("run_file_sync: no-op (sync config removed, use tasks instead)");
    Ok(())
}
