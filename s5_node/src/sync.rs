use std::{path::Path, sync::Arc};

use anyhow::Result;
use blake3::derive_key;
use ed25519_dalek::SigningKey as DalekSigningKey;
use s5_blobs::Client as BlobsClient;
use s5_core::{BlobStore, StreamKey, api::streams::RegistryApi};
use s5_fs::{
    dir::ENCRYPTION_TYPE_XCHACHA20_POLY1305,
    DirContext,
    DirContextParentLink,
    FS5,
    SigningKey as FsSigningKey,
};


use crate::{RemoteBlobStore, RemoteRegistry};

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
pub fn derive_sync_keys(shared_secret: impl AsRef<[u8]>) -> SyncKeys {
    let material = shared_secret.as_ref();
    let encryption_key = derive_key("s5/fs/sync/xchacha20", material);
    let signing_seed = derive_key("s5/fs/sync/ed25519", material);
    let signing_key = DalekSigningKey::from_bytes(&signing_seed);
    let public_key = *signing_key.verifying_key().as_bytes();

    SyncKeys {
        encryption_key,
        signing_key,
        public_key,
    }
}

/// Opens a plaintext FS5 instance rooted at the given path.
pub fn open_plaintext_fs(path: &Path) -> Result<FS5> {
    let context = DirContext::open_local_root(path)?;
    Ok(FS5::open(context))
}

/// Opens an encrypted FS5 instance backed by remote blob + registry services.
pub fn open_encrypted_fs(
    stream_key: StreamKey,
    derived: &SyncKeys,
    blob_client: BlobsClient,
    registry: RemoteRegistry,
) -> FS5 {
    let remote_store = RemoteBlobStore::new(blob_client);
    let blob_store = BlobStore::new(remote_store);
    let signing_key = derived.fs_signing_key();
    let link = DirContextParentLink::RegistryKey {
        public_key: stream_key,
        signing_key: Some(signing_key.clone()),
    };
    let registry_arc: Arc<dyn RegistryApi + Send + Sync> = Arc::new(registry);
    let mut context = DirContext::new(link, blob_store, registry_arc);
    context.encryption_type = Some(ENCRYPTION_TYPE_XCHACHA20_POLY1305);
    context.keys.insert(0x0e, derived.encryption_key);
    context.signing_key = Some(signing_key);
    FS5::open(context)
}

/// Pushes the current plaintext snapshot into the encrypted FS and publishes it remotely.
pub async fn push_snapshot(plaintext: &FS5, encrypted: &FS5) -> Result<()> {
    let snapshot = plaintext.export_snapshot().await?;
    encrypted.merge_from_snapshot(snapshot).await?;
    encrypted.save().await?;
    Ok(())
}

/// Pulls the latest encrypted snapshot into the plaintext FS.
pub async fn pull_snapshot(encrypted: &FS5, plaintext: &FS5) -> Result<()> {
    let snapshot = encrypted.export_snapshot().await?;
    plaintext.merge_from_snapshot(snapshot).await?;
    plaintext.save().await?;
    Ok(())
}
