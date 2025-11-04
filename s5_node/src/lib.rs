use crate::config::{NodeConfigPeer, NodeConfigStore, S5NodeConfig};
use iroh::{protocol::Router, Endpoint};
use s5_blobs::{BlobsServer, ALPN as BLOBS_ALPN};
use s5_core::{store::StoreResult, BlobStore, RedbRegistry};
use s5_store_local::LocalStore;
use s5_store_memory::MemoryStore;
use s5_store_s3::S3Store;
use s5_store_sia::SiaStore;
use std::collections::HashMap;

mod registry_protocol;
mod store_remote;
pub mod sync;

pub use registry_protocol::{
    Client as RegistryClient,
    RemoteRegistry,
    RegistryServer,
    ALPN as REGISTRY_ALPN,
};
pub use store_remote::RemoteBlobStore;
pub use sync::{derive_sync_keys, SyncKeys};

pub mod config;

pub struct S5Node {
    pub config: S5NodeConfig,
    pub stores: HashMap<String, BlobStore>,
    pub registry: Option<RedbRegistry>,
    pub endpoint: Endpoint,
    pub router: Router,
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
        for (_name, NodeConfigPeer { id, blobs }) in &config.peer {
            if !id.is_empty() {
                peer_cfg.insert(id.clone(), blobs.clone());
            }
        }

        // Create iroh endpoint (use defaults; discovery_n0 + bind)
        let endpoint = Endpoint::builder().bind().await?;

        // Create and register protocol servers
        let blobs_server = BlobsServer::new(stores.clone(), peer_cfg);
        let mut builder = Router::builder(endpoint.clone()).accept(BLOBS_ALPN, blobs_server);
        if let Some(registry_ref) = registry.as_ref() {
            builder = builder.accept(REGISTRY_ALPN, RegistryServer::new(registry_ref.clone()));
        }
        let router = builder.spawn();

        Ok(Self {
            config,
            stores,
            registry,
            endpoint,
            router,
        })
    }

    pub async fn run_file_sync(&self) -> anyhow::Result<()> {
        for (name, sync) in &self.config.sync {
            tracing::info!("sync.{name} -> {}", sync.local_path);
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

pub async fn run_node(_config_file_path: std::path::PathBuf, config: S5NodeConfig) -> anyhow::Result<()> {
    let node = S5Node::new(config, None).await?;
    tracing::info!("s5_node online");
    tokio::signal::ctrl_c().await?;
    node.router.shutdown().await?;
    Ok(())
}
