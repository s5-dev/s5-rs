use crate::config::{NodeConfigPeer, NodeConfigStore, S5NodeConfig};
use base64::Engine;
use iroh::{Endpoint, SecretKey, protocol::Router};
use s5_blobs::{ALPN as BLOBS_ALPN, BlobsServer};
use s5_core::{BlobStore, RedbRegistry, store::StoreResult};
use s5_store_local::LocalStore;
use s5_store_memory::MemoryStore;
use s5_store_s3::S3Store;
use s5_store_sia::SiaStore;
use std::{collections::HashMap, path::PathBuf, str::FromStr};

pub mod sync;

pub use s5_registry::{
    ALPN as REGISTRY_ALPN, Client as RegistryClient, RegistryServer, RemoteRegistry,
};
pub use sync::{SyncKeys, derive_sync_keys};

pub mod config;

pub struct S5Node {
    pub config: S5NodeConfig,
    pub stores: HashMap<String, BlobStore>,
    pub registry: Option<RedbRegistry>,
    pub endpoint: Endpoint,
    pub router: Router,
}

fn load_secret_key(identity: &crate::config::NodeConfigIdentity) -> Option<SecretKey> {
    // Prefer inline key over file
    if let Some(s) = &identity.secret_key {
        if let Some(sk) = parse_secret_key_string(s) {
            return Some(sk);
        }
    }
    if let Some(path) = &identity.secret_key_file {
        if let Ok(bytes) = std::fs::read(path) {
            if let Ok(s) = std::str::from_utf8(&bytes) {
                if let Some(sk) = parse_secret_key_string(s.trim()) {
                    return Some(sk);
                }
            }
            if let Some(sk) = parse_secret_key_bytes(&bytes) {
                return Some(sk);
            }
        }
    }
    None
}

fn parse_secret_key_string(s: &str) -> Option<SecretKey> {
    let s = s.trim();
    if let Ok(bytes) = hex::decode(s) {
        if let Some(sk) = parse_secret_key_bytes(&bytes) {
            return Some(sk);
        }
    }
    if let Ok(bytes) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s) {
        if let Some(sk) = parse_secret_key_bytes(&bytes) {
            return Some(sk);
        }
    }
    None
}

fn parse_secret_key_bytes(bytes: &[u8]) -> Option<SecretKey> {
    if bytes.len() == 32 {
        let mut arr = [0u8; 32];
        arr.copy_from_slice(bytes);
        return Some(SecretKey::from_bytes(&arr));
    }
    None
}

impl S5Node {
    pub async fn new(config: S5NodeConfig, registry: Option<RedbRegistry>) -> anyhow::Result<Self> {
        // Build stores from config
        let mut stores: HashMap<String, BlobStore> = HashMap::new();
        for (name, store_config) in &config.store {
            let store: BlobStore = create_store(store_config.clone()).await?;
            stores.insert(name.clone(), store);
        }

        // Map peer id -> blobs ACL config
        let mut peer_cfg: HashMap<String, s5_blobs::PeerConfigBlobs> = HashMap::new();
        for (_name, NodeConfigPeer { id, blobs, .. }) in &config.peer {
            if !id.is_empty() {
                peer_cfg.insert(id.clone(), blobs.clone());
            }
        }

        // Create iroh endpoint with optional stable secret key
        let mut builder = Endpoint::builder();
        if let Some(sec) = load_secret_key(&config.identity) {
            builder = builder.secret_key(sec);
        }
        let endpoint = builder.bind().await?;

        // Create and register protocol servers
        let blobs_server = BlobsServer::new(stores.clone(), peer_cfg);
        let mut router_builder = Router::builder(endpoint.clone()).accept(BLOBS_ALPN, blobs_server);
        if let Some(registry_ref) = registry.as_ref() {
            router_builder =
                router_builder.accept(REGISTRY_ALPN, RegistryServer::new(registry_ref.clone()));
        }
        let router = router_builder.spawn();

        Ok(Self {
            config,
            stores,
            registry,
            endpoint,
            router,
        })
    }

    pub async fn run_file_sync(&self) -> anyhow::Result<()> {
        use crate::sync::{
            derive_sync_keys, open_encrypted_fs, open_plaintext_fs, pull_snapshot, push_snapshot,
        };
        use s5_blobs::Client as BlobsClient;
        use std::path::Path;

        for (name, sync_cfg) in &self.config.sync {
            tracing::info!("sync.{name} -> {}", sync_cfg.local_path);
            // Determine untrusted hop (first entry)
            let Some(first) = sync_cfg.via_untrusted.first() else {
                continue;
            };
            let Some(peer) = self.config.peer.get(first) else {
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
            let endpoint = self.endpoint.clone();
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
                                let encrypted = open_encrypted_fs(
                                    stream_key,
                                    &keys,
                                    blobs_client,
                                    registry_client,
                                );
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
                        let blobs_client =
                            BlobsClient::connect(endpoint.clone(), peer_addr.clone());
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

    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.router.shutdown().await?;
        Ok(())
    }
}

pub async fn create_store(config: NodeConfigStore) -> StoreResult<BlobStore> {
    let store: Box<dyn s5_core::store::Store + 'static> = match config {
        NodeConfigStore::SiaRenterd(config) => Box::new(SiaStore::create(config).await?),
        NodeConfigStore::Local(config) => Box::new(LocalStore::create(config)),
        NodeConfigStore::S3(config) => Box::new(S3Store::create(config)),
        NodeConfigStore::Memory => Box::new(MemoryStore::new()),
    };
    Ok(BlobStore::new_boxed(store))
}

pub async fn run_node(
    config_file_path: std::path::PathBuf,
    config: S5NodeConfig,
) -> anyhow::Result<()> {
    // Determine registry path
    let registry_path: PathBuf = if let Some(p) = &config.registry_path {
        p.into()
    } else {
        let base = config_file_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        base.join("registry")
    };
    std::fs::create_dir_all(&registry_path)?;
    let registry = RedbRegistry::open(&registry_path)?;

    let node = S5Node::new(config, Some(registry)).await?;

    // Ensure we are online to populate addr() properly
    node.endpoint.online().await;

    tracing::info!("s5_node online");
    tracing::info!("endpoint id: {}", node.endpoint.id());
    tracing::info!("endpoint id (acl): {:?}", node.endpoint.id());
    tracing::info!("endpoint addr: {:?}", node.endpoint.addr());

    // Fire-and-forget one-shot sync; keep services alive
    if let Err(err) = node.run_file_sync().await {
        tracing::warn!("file sync failed: {err}");
    }

    tokio::signal::ctrl_c().await?;
    node.router.shutdown().await?;
    Ok(())
}
