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

use crate::config::{NodeConfigPeer, NodeConfigRegistry, NodeConfigStore, S5NodeConfig};
use anyhow::anyhow;
use iroh::{Endpoint, EndpointId, protocol::Router};
use s5_blobs::{ALPN as BLOBS_ALPN, BlobsServer};
use s5_core::{BlobStore, RegistryApi, store::StoreResult};
use s5_registry::MemoryRegistry;
use s5_registry_redb::RedbRegistry;
use s5_registry_store::StoreRegistry;
use s5_store_local::LocalStore;
use s5_store_memory::MemoryStore;
use s5_store_s3::S3Store;
use s5_store_sia::SiaStore;
use std::{collections::BTreeMap, collections::HashMap, path::Path, str::FromStr, sync::Arc};

pub mod config;
pub mod fuse;
pub mod identity;
pub mod sync;

pub use s5_registry::{
    ALPN as REGISTRY_ALPN, Client as RegistryClient, MultiRegistry, RegistryServer, RemoteRegistry,
    TeeRegistry, WritePolicy,
};
pub use sync::{SyncKeys, derive_sync_keys};

pub struct S5Node {
    pub config: S5NodeConfig,
    pub stores: HashMap<String, BlobStore>,
    pub registry: Option<Arc<dyn RegistryApi + Send + Sync>>,
    pub endpoint: Endpoint,
    pub router: Router,
}

impl S5Node {
    /// Creates a new S5Node, creating its own iroh endpoint.
    ///
    /// For cases where you need to create the endpoint first (e.g., for remote
    /// registries), use [`new_with_endpoint`] instead.
    ///
    /// Note: If your config uses a relative `secret_key_file` path, prefer
    /// [`run_node`] which resolves paths relative to the config file.
    pub async fn new(
        config: S5NodeConfig,
        registry: Option<Arc<dyn RegistryApi + Send + Sync>>,
    ) -> anyhow::Result<Self> {
        // Create iroh endpoint with optional stable secret key
        // Note: passes None for config_dir, so relative paths won't work here
        let mut builder = Endpoint::builder();
        if let Some(sec) = identity::load_secret_key(&config.identity, None) {
            builder = builder.secret_key(sec);
        }
        let endpoint = builder.bind().await?;

        Self::new_with_endpoint(config, registry, endpoint).await
    }

    /// Creates a new S5Node with a pre-created iroh endpoint.
    ///
    /// This is useful when the endpoint needs to exist before the registry
    /// can be created (e.g., for remote registries that need to connect
    /// to peers).
    pub async fn new_with_endpoint(
        config: S5NodeConfig,
        registry: Option<Arc<dyn RegistryApi + Send + Sync>>,
        endpoint: Endpoint,
    ) -> anyhow::Result<Self> {
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

/// Context needed to create registries that may require network access.
pub struct RegistryContext<'a> {
    /// Path for local registry storage (Redb, StoreLocal).
    pub registry_root: &'a Path,
    /// The iroh endpoint for creating remote connections.
    pub endpoint: &'a Endpoint,
    /// Peer configurations for resolving peer names to addresses.
    pub peers: &'a BTreeMap<String, NodeConfigPeer>,
}

/// Creates a registry from configuration.
///
/// For local backends (Redb, StoreLocal, Memory), this is synchronous.
/// For remote backends (Remote, Tee), this requires the endpoint to be available.
pub fn create_registry(
    backend: Option<NodeConfigRegistry>,
    ctx: &RegistryContext<'_>,
) -> anyhow::Result<Arc<dyn RegistryApi + Send + Sync>> {
    let backend = backend.unwrap_or(NodeConfigRegistry::Redb);
    create_registry_inner(backend, ctx)
}

fn create_registry_inner(
    backend: NodeConfigRegistry,
    ctx: &RegistryContext<'_>,
) -> anyhow::Result<Arc<dyn RegistryApi + Send + Sync>> {
    match backend {
        NodeConfigRegistry::Redb => {
            std::fs::create_dir_all(ctx.registry_root)?;
            let registry = RedbRegistry::open(ctx.registry_root)?;
            Ok(Arc::new(registry))
        }
        NodeConfigRegistry::StoreLocal { prefix } => {
            std::fs::create_dir_all(ctx.registry_root)?;
            let store = LocalStore::create(s5_store_local::LocalStoreConfig {
                base_path: ctx.registry_root.to_string_lossy().into_owned(),
            });
            let store_registry = StoreRegistry::new(Arc::new(store), prefix);
            Ok(Arc::new(store_registry))
        }
        NodeConfigRegistry::Memory => {
            let registry = MemoryRegistry::new();
            Ok(Arc::new(registry))
        }
        NodeConfigRegistry::Remote { peer } => {
            let peer_config = ctx
                .peers
                .get(&peer)
                .ok_or_else(|| anyhow!("registry remote peer '{}' not found in config", peer))?;
            if peer_config.id.is_empty() {
                return Err(anyhow!(
                    "registry remote peer '{}' has no id configured",
                    peer
                ));
            }
            let endpoint_id = EndpointId::from_str(&peer_config.id)
                .map_err(|e| anyhow!("invalid endpoint id for peer '{}': {}", peer, e))?;
            let remote = RemoteRegistry::connect(ctx.endpoint.clone(), endpoint_id);
            Ok(Arc::new(remote))
        }
        NodeConfigRegistry::Tee { local, remote_peer } => {
            // Create local backend recursively
            let local_registry = create_registry_inner(*local, ctx)?;

            // Create remote backend
            let peer_config = ctx.peers.get(&remote_peer).ok_or_else(|| {
                anyhow!(
                    "registry tee remote peer '{}' not found in config",
                    remote_peer
                )
            })?;
            if peer_config.id.is_empty() {
                return Err(anyhow!(
                    "registry tee remote peer '{}' has no id configured",
                    remote_peer
                ));
            }
            let endpoint_id = EndpointId::from_str(&peer_config.id)
                .map_err(|e| anyhow!("invalid endpoint id for peer '{}': {}", remote_peer, e))?;
            let remote_registry: Arc<dyn RegistryApi + Send + Sync> =
                Arc::new(RemoteRegistry::connect(ctx.endpoint.clone(), endpoint_id));

            let tee = TeeRegistry::new(local_registry, remote_registry);
            Ok(Arc::new(tee))
        }
        NodeConfigRegistry::Multi {
            backends,
            write_policy,
        } => {
            // Parse write policy
            let policy = match write_policy.as_deref() {
                None | Some("all") => WritePolicy::All,
                Some("any") => WritePolicy::Any,
                Some(s) if s.starts_with("quorum:") => {
                    let n: usize = s
                        .strip_prefix("quorum:")
                        .unwrap()
                        .parse()
                        .map_err(|_| anyhow!("invalid quorum value in write_policy: {}", s))?;
                    WritePolicy::Quorum(n)
                }
                Some(s) => return Err(anyhow!("unknown write_policy: {}", s)),
            };

            // Create all backends recursively
            let mut registry_backends: Vec<Arc<dyn RegistryApi + Send + Sync>> = Vec::new();
            for backend_config in backends {
                let backend = create_registry_inner(backend_config, ctx)?;
                registry_backends.push(backend);
            }

            let multi = MultiRegistry::with_policy(registry_backends, policy);
            Ok(Arc::new(multi))
        }
    }
}

pub async fn run_node(
    config_file_path: std::path::PathBuf,
    config: S5NodeConfig,
) -> anyhow::Result<()> {
    // Create iroh endpoint first (needed for remote registries)
    let config_dir = config_file_path.parent();
    let mut builder = Endpoint::builder();
    if let Some(sec) = identity::load_secret_key(&config.identity, config_dir) {
        builder = builder.secret_key(sec);
    }
    let endpoint = builder.bind().await?;

    // Determine registry path and create registry with context
    let registry_path = config::registry_path(&config_file_path, &config);
    let registry_ctx = RegistryContext {
        registry_root: &registry_path,
        endpoint: &endpoint,
        peers: &config.peer,
    };
    let registry = create_registry(config.registry.clone(), &registry_ctx)?;

    let node = S5Node::new_with_endpoint(config, Some(registry), endpoint).await?;

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
