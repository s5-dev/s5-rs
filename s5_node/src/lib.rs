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

use std::path::PathBuf;

use crate::config::{NodeConfigPeer, NodeConfigRegistry, NodeConfigStore, S5NodeConfig};
use anyhow::anyhow;
use iroh::{Endpoint, EndpointId, protocol::Router};
use s5_blobs::{ALPN as BLOBS_ALPN, BlobsServer};
use s5_core::blob::{BlobStore, BlobsRead};
use s5_core::{RegistryApi, StoreResult};
use s5_registry::MemoryRegistry;
use s5_registry_redb::RedbRegistry;
use s5_registry_store::StoreRegistry;
use s5_store_local::LocalStore;
use s5_store_local_links::LocalLinksStore;
use s5_store_memory::MemoryStore;
// use s5_store_pixeldrain::PixeldrainStore;  // TODO: add to workspace
use s5_node_api::ALPN as S5_NODE_ALPN;
use s5_node_api::connect::{ServiceLock, lock_path, remove_lock, write_lock};
use s5_store_s3::S3Store;
use s5_store_sia::SiaStore;
use std::{collections::BTreeMap, collections::HashMap, path::Path, str::FromStr, sync::Arc};
use tokio::sync::{RwLock, oneshot};
use tracing::info;

pub mod config;
pub mod fuse;
pub mod identity;
pub mod s5_server;
pub mod snapshot;
pub mod sync;
pub mod tasks;

pub use s5_registry::{
    ALPN as REGISTRY_ALPN, Client as RegistryClient, MultiRegistry, RegistryServer, RemoteRegistry,
    TeeRegistry, WritePolicy,
};
pub use sync::{SyncKeys, derive_sync_keys};

pub struct S5Node {
    pub config: S5NodeConfig,
    /// Full blob stores (local, s3, sia, etc.)
    pub stores: HashMap<String, BlobStore>,
    /// Local links stores - reference files by hash without copying.
    /// These implement BlobsRead and support import_file() but not full Store.
    pub link_stores: HashMap<String, Arc<LocalLinksStore>>,
    pub registry: Option<Arc<dyn RegistryApi + Send + Sync>>,
    pub endpoint: Endpoint,
    pub router: Router,
    /// Optional S5NodeServer for task orchestration RPC.
    pub s5_server: Option<s5_server::S5NodeServer>,
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
        // Local-only: no relay servers for security
        let mut builder = Endpoint::empty_builder();
        if let Some(sec) = identity::load_secret_key(&config.identity, None, &config.key) {
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
        Self::new_with_endpoint_and_config_dir(config, registry, endpoint, None, None).await
    }

    /// Creates a new S5Node with a pre-created iroh endpoint and config directory.
    ///
    /// The config_dir is used to resolve relative paths (e.g., for local_links.path).
    /// If s5_server is provided, it will be registered with the router.
    pub async fn new_with_endpoint_and_config_dir(
        config: S5NodeConfig,
        registry: Option<Arc<dyn RegistryApi + Send + Sync>>,
        endpoint: Endpoint,
        _config_dir: Option<&Path>,
        s5_server: Option<s5_server::S5NodeServer>,
    ) -> anyhow::Result<Self> {
        // Build stores from config, separating full stores from link stores
        let mut stores: HashMap<String, BlobStore> = HashMap::new();
        let mut link_stores: HashMap<String, Arc<LocalLinksStore>> = HashMap::new();

        for (name, store_config) in &config.store {
            match store_config {
                NodeConfigStore::LocalLinks(cfg) => {
                    let store = LocalLinksStore::open(&cfg.path)?;
                    tracing::info!(name, path = %cfg.path, "local_links store opened");
                    link_stores.insert(name.clone(), Arc::new(store));
                }
                other => {
                    let store = create_store(other.clone()).await?;
                    stores.insert(name.clone(), store);
                }
            }
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

        // Build read sources from link stores for BlobsServer
        let read_sources: HashMap<String, Arc<dyn BlobsRead>> = link_stores
            .iter()
            .map(|(name, store)| (name.clone(), store.clone() as Arc<dyn BlobsRead>))
            .collect();

        // TODO: expose vault meta blob stores as read sources when vault
        // system is wired up (replaces the old `config.fs` loop).

        let blobs_server =
            BlobsServer::with_read_sources(stores.clone(), read_sources, peer_cfg, pinner);
        let mut router_builder = Router::builder(endpoint.clone()).accept(BLOBS_ALPN, blobs_server);
        if let Some(registry_ref) = registry.as_ref() {
            // TODO: registry should forward set events to all connected peers
            // (push-based replication). Currently peers must poll to discover
            // new snapshot hashes.
            router_builder =
                router_builder.accept(REGISTRY_ALPN, RegistryServer::new(registry_ref.clone()));
        }
        // Register S5NodeServer for task orchestration RPC if provided.
        if let Some(server) = s5_server.as_ref() {
            router_builder = router_builder.accept(S5_NODE_ALPN, server.clone());
        }
        let router = router_builder.spawn();

        Ok(Self {
            config,
            stores,
            link_stores,
            registry,
            endpoint,
            router,
            s5_server,
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

/// Create the raw `Arc<dyn Store>` for a config entry.
///
/// `LocalLinks` is not a full `Store` — it is handled separately by the node.
pub async fn create_raw_store(
    config: NodeConfigStore,
) -> StoreResult<Arc<dyn s5_core::store::Store>> {
    let store: Box<dyn s5_core::store::Store + 'static> = match config {
        NodeConfigStore::SiaRenterd(config) => Box::new(SiaStore::create(config).await?),
        NodeConfigStore::Local(config) => Box::new(LocalStore::create(config)),
        NodeConfigStore::S3(config) => Box::new(S3Store::create(config)),
        // NodeConfigStore::Pixeldrain(config) => Box::new(PixeldrainStore::create(config)),  // TODO
        NodeConfigStore::Memory => Box::new(MemoryStore::new()),
        NodeConfigStore::LocalLinks(_) => {
            return Err(anyhow::anyhow!(
                "LocalLinks stores should be accessed via S5Node.link_stores"
            ));
        }
    };
    Ok(Arc::from(store))
}

/// Create a `BlobStore` from a config entry (convenience wrapper).
pub async fn create_store(config: NodeConfigStore) -> StoreResult<BlobStore> {
    let store = create_raw_store(config).await?;
    Ok(BlobStore::from_arc(store))
}

/// Context needed to create registries that may require network access.
pub struct RegistryContext<'a> {
    /// The iroh endpoint for creating remote connections.
    pub endpoint: &'a Endpoint,
    /// Peer configurations for resolving peer names to addresses.
    pub peers: &'a BTreeMap<String, NodeConfigPeer>,
    /// Pre-built raw stores for resolving store-backed registries.
    pub stores: &'a HashMap<String, Arc<dyn s5_core::store::Store>>,
}

/// Creates a registry from configuration.
pub fn create_registry(
    backend: NodeConfigRegistry,
    ctx: &RegistryContext<'_>,
) -> anyhow::Result<Arc<dyn RegistryApi + Send + Sync>> {
    create_registry_inner(backend, ctx)
}

fn create_registry_inner(
    backend: NodeConfigRegistry,
    ctx: &RegistryContext<'_>,
) -> anyhow::Result<Arc<dyn RegistryApi + Send + Sync>> {
    match backend {
        NodeConfigRegistry::Local { path } | NodeConfigRegistry::Redb { path } => {
            let registry_root = PathBuf::from(&path);
            std::fs::create_dir_all(&registry_root)?;
            let registry = RedbRegistry::open(&registry_root)?;
            Ok(Arc::new(registry))
        }
        NodeConfigRegistry::StoreLocal { path, prefix } => {
            let registry_root = PathBuf::from(&path);
            std::fs::create_dir_all(&registry_root)?;
            let store = LocalStore::create(s5_store_local::LocalStoreConfig { base_path: path });
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
        NodeConfigRegistry::Store { store, prefix } => {
            let raw_store = ctx.stores.get(&store).ok_or_else(|| {
                anyhow!("registry store '{}' not found in [store.*] config", store)
            })?;
            let store_registry = StoreRegistry::new(Arc::clone(raw_store), prefix);
            Ok(Arc::new(store_registry))
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
    // Local-only: no relay servers for security
    let config_dir = config_file_path.parent();
    let mut builder = Endpoint::empty_builder();
    if let Some(sec) = identity::load_secret_key(&config.identity, config_dir, &config.key) {
        builder = builder.secret_key(sec);
    }
    let endpoint = builder.bind().await?;

    // Pre-build raw stores so they can be shared between registries and blobs.
    let mut raw_stores: HashMap<String, Arc<dyn s5_core::store::Store>> = HashMap::new();
    for (name, store_config) in &config.store {
        match store_config {
            NodeConfigStore::LocalLinks(_) => {} // handled by the node separately
            other => {
                let store = create_raw_store(other.clone()).await?;
                raw_stores.insert(name.clone(), store);
            }
        }
    }

    // Wrap raw stores into BlobStores for the task executor.
    let stores: HashMap<String, BlobStore> = raw_stores
        .iter()
        .map(|(name, s)| (name.clone(), BlobStore::from_arc(Arc::clone(s))))
        .collect();

    // Create the default registry (if configured)
    let registry_ctx = RegistryContext {
        endpoint: &endpoint,
        peers: &config.peer,
        stores: &raw_stores,
    };
    let registry = match config.registry.get("default") {
        Some(reg_config) => Some(create_registry(reg_config.clone(), &registry_ctx)?),
        None => {
            tracing::warn!("no [registry.default] configured — snapshot publishing disabled");
            None
        }
    };

    // Create the task executor with pre-built stores.
    // Derive node secret from the endpoint's secret key for vault encryption.
    let node_secret =
        blake3::derive_key("s5/node/secret", endpoint.secret_key().to_bytes().as_ref());
    // Wrap config in Arc<RwLock> once — shared between the RPC server and the
    // task executor so that `patch_config` updates are visible to tasks.
    let config = Arc::new(RwLock::new(config));
    let executor_ctx = Arc::new(tasks::TaskExecutorContext {
        config: config.clone(),
        stores: stores.clone(),
        node_secret,
        registry: registry.clone(),
    });
    let executor = Arc::new(tasks::TaskExecutor::new(executor_ctx));

    // Create shutdown channel and S5NodeServer RPC.
    let endpoint_id = endpoint.id().to_string();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = s5_server::S5NodeServer::new(
        config.clone(),
        config_file_path.clone(),
        executor.clone(),
        endpoint_id,
        shutdown_tx,
    );

    let node = S5Node::new_with_endpoint_and_config_dir(
        config.read().await.clone(),
        registry,
        endpoint,
        config_dir,
        Some(server),
    )
    .await?;

    // Note: We skip `endpoint.online().await` because with `empty_builder()` (no relay),
    // it would block forever. The local addresses are available immediately for IPC.
    tracing::info!("s5_node started");
    // Single canonical endpoint id string used for both configs and ACLs.
    tracing::info!("endpoint id: {}", node.endpoint.id());
    tracing::info!("endpoint addr: {:?}", node.endpoint.addr());

    // Write the service lock file so clients can discover and connect.
    if let Err(e) = write_service_lockfile(&node) {
        tracing::warn!("failed to write service lock file: {e}");
    }

    // Spawn configured FUSE mounts (best-effort)
    if let Err(err) = crate::fuse::spawn_fuse_mounts(&node).await {
        tracing::warn!("failed to spawn FUSE mounts: {err}");
    }

    // Fire-and-forget one-shot sync; keep services alive
    if let Err(err) = crate::sync::run_file_sync(&node).await {
        tracing::warn!("file sync failed: {err}");
    }

    // Spawn configured snapshot cycles (background tasks)
    crate::snapshot::spawn_snapshot_cycles(&node).await;

    // Wait for either Ctrl+C or a shutdown request from the S5NodeServer.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("received Ctrl+C, shutting down");
        }
        result = shutdown_rx => {
            if result.is_ok() {
                info!("received shutdown request via RPC, shutting down");
            }
        }
    }

    // Clean up: remove the lock file and shut down the router.
    remove_lock();
    node.router.shutdown().await?;
    Ok(())
}

/// Writes the service lock file with the node's iroh endpoint address.
fn write_service_lockfile(node: &S5Node) -> anyhow::Result<()> {
    let endpoint_addr = node.endpoint.addr();

    let lock = ServiceLock {
        endpoint_addr,
        version: Some(s5_node_api::VERSION.to_string()),
        pid: Some(std::process::id()),
    };
    write_lock(&lock)?;

    info!(lock_path = %lock_path()?.display(), version = s5_node_api::VERSION, "service lock file written");
    Ok(())
}
