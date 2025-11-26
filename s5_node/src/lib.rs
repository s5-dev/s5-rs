//! The main S5 node implementation.
//!
//! This crate orchestrates the various S5 components into a running node:
//!
//! - **Storage management**: Initializes configured blob stores (local, S3, Sia, memory).
//! - **Networking**: Sets up the Iroh endpoint and router, registering protocol handlers
//!   for [`s5_blobs`] and [`s5_registry`].
//! - **Sync**: Runs file synchronization jobs via [`sync::run_file_sync`].
//! - **FUSE**: Spawns FUSE mounts for configured filesystems via [`fuse::spawn_fuse_mounts`].
//!
//! # Usage
//!
//! This crate is primarily used by the `s5_cli` binary, but can be embedded in other
//! applications. See [`S5Node`] for the main entry point.

use crate::config::{NodeConfigPeer, NodeConfigStore, S5NodeConfig};
use iroh::{Endpoint, protocol::Router};
use s5_blobs::{ALPN as BLOBS_ALPN, BlobsServer};
use s5_core::{BlobStore, RedbRegistry, store::StoreResult};
use s5_store_local::LocalStore;
use s5_store_memory::MemoryStore;
use s5_store_s3::S3Store;
use s5_store_sia::SiaStore;
use std::{collections::HashMap, sync::Arc};

pub mod config;
pub mod fuse;
pub mod identity;
pub mod sync;

pub use s5_registry::{
    ALPN as REGISTRY_ALPN, Client as RegistryClient, RegistryServer, RemoteRegistry,
};
pub use sync::{SyncKeys, derive_sync_keys};

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
        for NodeConfigPeer { id, blobs, .. } in config.peer.values() {
            if !id.is_empty() {
                peer_cfg.insert(id.clone(), blobs.clone());
            }
        }

        // Create iroh endpoint with optional stable secret key
        let mut builder = Endpoint::builder();
        if let Some(sec) = identity::load_secret_key(&config.identity) {
            builder = builder.secret_key(sec);
        }
        let endpoint = builder.bind().await?;

        // Create and register protocol servers.
        // When a registry is available we also create a `RegistryPinner`
        // and pass it into the blobs server so that per-node blob pins
        // (PinContext::NodeId) can be maintained and enforced at the
        // transport layer.
        let pinner: Option<Arc<dyn s5_core::Pins>> = registry
            .as_ref()
            .map(|r| Arc::new(s5_core::RegistryPinner::new(r.clone())) as Arc<dyn s5_core::Pins>);

        let blobs_server = BlobsServer::new(stores.clone(), peer_cfg, pinner);
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
        crate::sync::run_file_sync(self).await
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.router.shutdown().await?;
        Ok(())
    }
}

pub async fn create_store(config: NodeConfigStore) -> StoreResult<BlobStore> {
    // TODO: Consider using iroh's memstore for syncing blobs as an alternative backend?
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
    let registry_path = config::registry_path(&config_file_path, &config);
    std::fs::create_dir_all(&registry_path)?;
    let registry = RedbRegistry::open(&registry_path)?;

    let node = S5Node::new(config, Some(registry)).await?;

    // Ensure we are online to populate addr() properly
    node.endpoint.online().await;

    tracing::info!("s5_node online");
    // Single canonical endpoint id string used for both configs and ACLs.
    tracing::info!("endpoint id: {}", node.endpoint.id());
    tracing::info!("endpoint addr: {:?}", node.endpoint.addr());

    // Spawn configured FUSE mounts (best-effort)
    if let Err(err) = crate::fuse::spawn_fuse_mounts(&node).await {
        tracing::warn!("failed to spawn FUSE mounts: {err}");
    }

    // Fire-and-forget one-shot sync; keep services alive
    if let Err(err) = crate::sync::run_file_sync(&node).await {
        tracing::warn!("file sync failed: {err}");
    }

    tokio::signal::ctrl_c().await?;
    node.router.shutdown().await?;
    Ok(())
}
