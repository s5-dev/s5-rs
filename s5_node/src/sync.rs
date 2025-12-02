use std::{path::Path, sync::Arc};

use anyhow::Result;
use ed25519_dalek::SigningKey as DalekSigningKey;
use s5_blobs::{Client as BlobsClient, RemoteBlobStore};
use s5_core::{BlobStore, RegistryApi, StreamKey};
use s5_fs::{DirContext, FS5, SigningKey as FsSigningKey};

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
    let registry_arc: Arc<dyn RegistryApi + Send + Sync> = Arc::new(registry);

    let context = DirContext::new_encrypted_registry(
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
    encrypted.save().await?;
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
    plaintext.save().await?;
    Ok(())
}

/// Runs all configured file syncs for a node.
///
/// This is the orchestration previously implemented as
/// `S5Node::run_file_sync`; the method now forwards here.
pub async fn run_file_sync(node: &S5Node) -> Result<()> {
    use s5_blobs::Client as BlobsClient;
    use std::{path::Path, str::FromStr};

    for (name, sync_cfg) in &node.config.sync {
        tracing::info!("sync.{name} -> {}", sync_cfg.local_path);
        // Determine untrusted hop (first entry)
        let Some(first) = sync_cfg.via_untrusted.first() else {
            continue;
        };
        let Some(peer) = node.config.peer.get(first) else {
            tracing::warn!("sync.{name}: via_untrusted peer '{}' not found", first);
            continue;
        };

        // Use the peer id string (EndpointId Debug/Display) for dialing
        let dial_str = peer.id.clone();

        // Derive keys
        let keys = derive_sync_keys(&sync_cfg.shared_secret);
        let stream_key = keys.stream_key();

        // Open plaintext FS once
        let plaintext = open_plaintext_fs(Path::new(&sync_cfg.local_path))?;

        // Prepare owned captures for optional spawn
        let endpoint = node.endpoint.clone();
        let sync_name = name.clone();
        if let Some(secs) = sync_cfg.interval_secs {
            tracing::info!("sync.{name}: starting continuous sync every {secs}s");
            let plaintext_fs = plaintext.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(secs));
                loop {
                    interval.tick().await;
                    match iroh::EndpointId::from_str(&dial_str) {
                        Ok(pid) => {
                            let peer_addr: iroh::EndpointAddr = pid.into();
                            let blobs_client =
                                BlobsClient::connect(endpoint.clone(), peer_addr.clone());
                            let registry_client =
                                RemoteRegistry::connect(endpoint.clone(), peer_addr.clone());
                            let encrypted =
                                open_encrypted_fs(stream_key, &keys, blobs_client, registry_client);
                            if let Err(err) = push_snapshot(&plaintext_fs, &encrypted).await {
                                tracing::warn!("sync.{sync_name}: push failed: {err}");
                            }
                            if let Err(err) = pull_snapshot(&encrypted, &plaintext_fs).await {
                                tracing::warn!("sync.{sync_name}: pull failed: {err}");
                            }
                        }
                        Err(_) => tracing::warn!(
                            "sync.{sync_name}: invalid endpoint id string '{}'; set peer.endpoint_id",
                            dial_str
                        ),
                    }
                }
            });
        } else {
            match iroh::EndpointId::from_str(&dial_str) {
                Ok(pid) => {
                    let peer_addr: iroh::EndpointAddr = pid.into();
                    let blobs_client = BlobsClient::connect(endpoint.clone(), peer_addr.clone());
                    let registry_client = RemoteRegistry::connect(endpoint.clone(), peer_addr);
                    let encrypted =
                        open_encrypted_fs(stream_key, &keys, blobs_client, registry_client);
                    if let Err(err) = push_snapshot(&plaintext, &encrypted).await {
                        tracing::warn!("sync.{name}: push failed: {err}");
                    }
                    if let Err(err) = pull_snapshot(&encrypted, &plaintext).await {
                        tracing::warn!("sync.{name}: pull failed: {err}");
                    }
                }
                Err(_) => tracing::warn!(
                    "sync.{name}: invalid endpoint id string '{}'; set peer.endpoint_id",
                    dial_str
                ),
            }
        }
    }
    Ok(())
}
