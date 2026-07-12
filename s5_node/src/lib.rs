//! The main S5 node implementation.
//!
//! This crate orchestrates the various S5 components into a running node:
//!
//! - **Storage management**: Initializes configured blob stores (local, S3, Sia, memory).
//! - **Networking**: Sets up the Iroh endpoint and router, registering protocol handlers
//!   for [`s5_blobs`] and [`s5_registry`].
//! - **FUSE**: Spawns FUSE mounts for configured filesystems via [`fuse::spawn_fuse_mounts`].
//!
//! # Usage
//!
//! This crate is primarily used by the `s5_cli` binary, but can be embedded in other
//! applications. See [`S5Node`] for the main entry point.

use std::path::PathBuf;

use crate::config::{NodeConfigRegistry, NodeConfigStore, NodeConfigStoreBackend, S5NodeConfig};
use anyhow::{Context as _, anyhow};
use iroh::{Endpoint, protocol::Router};
use s5_blobs::{ALPN_ACL as BLOBS_ALPN_ACL, ALPN_PUBLIC as BLOBS_ALPN_PUBLIC, BlobsServer};
use s5_core::blob::{BlobStore, Blobs, BlobsRead};
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
use s5_store_fjall::FjallStore;
use s5_store_s3::S3Store;
use s5_store_sia::SiaStore;
use std::{collections::HashMap, path::Path, sync::Arc};
use tokio::sync::{RwLock, oneshot};
use tracing::info;

pub mod admission;
pub mod bootstrap;
pub mod config;
pub mod config_vault;
pub mod device_keyset;
pub mod enroll;
pub mod export;
pub mod fuse;
pub mod health;
pub mod identity;
pub mod identity_anchor;
pub mod identity_secrets_vault;
pub mod identity_vault;
pub mod membership;
pub mod membership_subscribe;
pub mod mnemonic;
pub mod pair;
pub mod peer_observer;
pub mod s5_server;
pub mod share;
pub mod snapshot;
pub mod special_vaults;
pub mod store_registry;
pub mod tasks;
pub mod watch;

pub use store_registry::NodeStores;

pub use s5_registry::{
    ALPN as REGISTRY_ALPN, BroadcastingRegistry, Client as RegistryClient, MultiRegistry,
    RegistryServer, RemoteRegistry, TeeRegistry, WritePolicy,
};

/// Validate a vault label / share nickname: `[a-z0-9_-]{1,64}`, not starting
/// with a digit or `_`. Same rules as CLI vault names (kept here so
/// [`share`] can validate a URL's suggested label without depending on the
/// CLI). Returns a human-readable reason on rejection.
pub fn validate_share_label(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("label is empty".into());
    }
    if name.len() > 64 {
        return Err(format!("label '{name}' is longer than 64 chars"));
    }
    let first = name.chars().next().unwrap();
    if first.is_ascii_digit() {
        return Err(format!("label '{name}' cannot start with a digit"));
    }
    if first == '_' {
        return Err(format!("label '{name}' cannot start with '_'"));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
    {
        return Err(format!(
            "label '{name}' contains characters outside [a-z0-9_-]"
        ));
    }
    Ok(())
}

pub struct S5Node {
    pub config: S5NodeConfig,
    /// Path-backed blob stores (local, s3, fjall, …) as their
    /// path-`BlobStore` view — the blobs server's `provide` map, requested
    /// explicitly from the [`NodeStores`] registry (D15). Content-addressed
    /// backends (Sia `PackingStore`) are absent by design; the vault-facing
    /// `dyn Blobs` view lives in [`tasks::TaskExecutorContext::stores`].
    pub stores: HashMap<String, BlobStore>,
    /// Local links stores - reference files by hash without copying.
    /// These implement BlobsRead and support import_file() but not full Store.
    pub link_stores: HashMap<String, Arc<LocalLinksStore>>,
    /// Wrapped registry handle: the [`BroadcastingRegistry`] is the
    /// single fanout site for live subscriber events. Every writer in
    /// the daemon goes through this Arc, so the RPC server, the
    /// publish task, identity-bundle publishing, and inbound
    /// subscription event-apply all feed the same broadcast.
    pub registry: Option<Arc<BroadcastingRegistry>>,
    pub endpoint: Endpoint,
    pub router: Router,
    /// Optional S5NodeServer for task orchestration RPC.
    pub s5_server: Option<s5_server::S5NodeServer>,
    /// Loopback-only control plane serving the `s5/node/0` ALPN behind the
    /// lock-file cookie (F03 fix). `Some` iff `s5_server` is `Some`.
    pub control: Option<ControlPlane>,
}

/// The daemon's control plane: a SECOND iroh endpoint, bound to
/// `127.0.0.1` only, relay-free and never published to pkarr/DNS/mDNS
/// (`presets::Minimal` + `clear_ip_transports`), serving the control ALPN
/// behind [`s5_server::ControlAuthGate`]. Its address + per-run token go
/// into the 0600 service lock file — that file is the access-control
/// boundary (same user as the daemon, or root). The public endpoint never
/// serves the control ALPN.
pub struct ControlPlane {
    pub endpoint: Endpoint,
    pub router: Router,
    pub token: [u8; s5_node_api::CONTROL_TOKEN_LEN],
}

impl ControlPlane {
    async fn spawn(server: s5_server::S5NodeServer) -> anyhow::Result<Self> {
        use rand::Rng;
        let mut token = [0u8; s5_node_api::CONTROL_TOKEN_LEN];
        rand::rng().fill_bytes(&mut token);
        // Fresh random endpoint key each run: the control endpoint's id is
        // never persisted or published, it only travels via the lock file.
        let endpoint = Endpoint::builder(iroh::endpoint::presets::Minimal)
            .clear_ip_transports()
            .bind_addr("127.0.0.1:0")
            .map_err(|e| anyhow::anyhow!("control endpoint bind_addr: {e}"))?
            .bind()
            .await?;
        let gate = s5_server::ControlAuthGate::new(server, token);
        let router = Router::builder(endpoint.clone())
            .accept(S5_NODE_ALPN, gate)
            .spawn();
        Ok(Self {
            endpoint,
            router,
            token,
        })
    }
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
        registry: Option<Arc<BroadcastingRegistry>>,
    ) -> anyhow::Result<Self> {
        // Create iroh endpoint with optional stable secret key
        // Note: passes None for config_dir, so relative paths won't work here
        // Uses `presets::N0` for pkarr + DNS + mDNS discovery + ring crypto
        // + n0 relay fallback — see the run_node header for the rationale.
        let mut builder = Endpoint::builder(iroh::endpoint::presets::N0);
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
        registry: Option<Arc<BroadcastingRegistry>>,
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
        registry: Option<Arc<BroadcastingRegistry>>,
        endpoint: Endpoint,
        _config_dir: Option<&Path>,
        s5_server: Option<s5_server::S5NodeServer>,
    ) -> anyhow::Result<Self> {
        Self::new_with_stores(
            config, registry, endpoint, s5_server, None, None, None, None, None,
        )
        .await
    }

    /// Like `new_with_endpoint_and_config_dir` but accepts a pre-built
    /// store registry.
    ///
    /// When `pre_built_stores` is `Some`, its stores are used directly
    /// instead of re-opening from config. This avoids double-opening stores
    /// that use exclusive locks (e.g. fjall).
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_stores(
        config: S5NodeConfig,
        registry: Option<Arc<BroadcastingRegistry>>,
        endpoint: Endpoint,
        s5_server: Option<s5_server::S5NodeServer>,
        pre_built_stores: Option<&NodeStores>,
        registry_acl: Option<Arc<dyn s5_registry::RegistryAcl>>,
        blob_acl: Option<Arc<dyn s5_blobs::BlobAcl>>,
        pair_listener: Option<crate::pair::PairListener>,
        enroll_listener: Option<crate::enroll::EnrollListener>,
    ) -> anyhow::Result<Self> {
        // Build stores from config, separating full stores from link stores.
        // A pre-built registry hands over its path-`BlobStore` view here —
        // the blobs server (`provide`) is a genuine path-semantics site, so
        // the request is explicit (D15); content-addressed backends are
        // absent from the resulting map by design.
        let mut stores: HashMap<String, BlobStore> = pre_built_stores
            .map(NodeStores::path_stores)
            .unwrap_or_default();
        let mut link_stores: HashMap<String, Arc<LocalLinksStore>> = HashMap::new();

        for (name, store_config) in &config.store {
            match &store_config.backend {
                NodeConfigStoreBackend::LocalLinks(cfg) => {
                    let store = LocalLinksStore::open(&cfg.path)?;
                    tracing::info!(name, path = %cfg.path, "local_links store opened");
                    link_stores.insert(name.clone(), Arc::new(store));
                }
                // Content-addressed (Sia packing): no path-`BlobStore` view, so
                // it never enters the blobs-server map. Reached via `dyn Blobs`.
                NodeConfigStoreBackend::Indexd(_) => {}
                _ => {
                    // Skip if already pre-built
                    if !stores.contains_key(name) {
                        let store = create_store(store_config.clone()).await?;
                        stores.insert(name.clone(), store);
                    }
                }
            }
        }

        // No peer-keyed ACL plumbing yet — the Option A model uses
        // identity-bundle-based ACLs; until that lands, BlobsServer runs
        // without per-peer ACL configuration.
        let peer_cfg: HashMap<String, s5_blobs::PeerConfigBlobs> = HashMap::new();

        // Create and register protocol servers.
        // When a registry is available we also create a `RegistryPinner`
        // and pass it into the blobs server so that per-node blob pins
        // (PinContext::NodeId) can be maintained and enforced at the
        // transport layer.
        let pinner: Option<Arc<dyn s5_core::Pins>> = registry.as_ref().map(|r| {
            let dyn_reg: Arc<dyn RegistryApi + Send + Sync> = r.clone();
            Arc::new(s5_core::RegistryPinner::new(dyn_reg)) as Arc<dyn s5_core::Pins>
        });

        // Build read sources from link stores for BlobsServer
        let read_sources: HashMap<String, Arc<dyn BlobsRead>> = link_stores
            .iter()
            .map(|(name, store)| (name.clone(), store.clone() as Arc<dyn BlobsRead>))
            .collect();

        // TODO: expose vault meta blob stores as read sources when vault
        // system is wired up (replaces the old `config.fs` loop).

        // Build the shared BlobsServer template and clone it into two
        // ALPN-bound instances:
        //   * Public — no F02 challenge; serves only blobs in the
        //     daemon's `public_blob_hashes` set (identity bundles,
        //     advertised public-vault content).
        //   * ACL — requires the F02 challenge handshake; serves
        //     bound-principal-authorised blobs per
        //     `BlobAcl::allow_acl_read`.
        let blobs_server_template =
            BlobsServer::with_read_sources(stores.clone(), read_sources, peer_cfg, pinner);
        let blobs_server_template = match blob_acl {
            Some(acl) => blobs_server_template.with_acl(acl),
            None => blobs_server_template,
        };
        let local_iroh_pubkey: [u8; 32] = *endpoint.id().as_bytes();
        let blobs_public = blobs_server_template
            .clone()
            .with_mode(s5_blobs::ServerMode::Public)
            .with_local_iroh_pubkey(local_iroh_pubkey);
        let blobs_acl = blobs_server_template
            .with_mode(s5_blobs::ServerMode::Acl)
            .with_local_iroh_pubkey(local_iroh_pubkey);
        let mut router_builder = Router::builder(endpoint.clone())
            .accept(BLOBS_ALPN_PUBLIC, blobs_public)
            .accept(BLOBS_ALPN_ACL, blobs_acl);
        if let Some(registry_ref) = registry.as_ref() {
            // TODO: registry should forward set events to all connected peers
            // (push-based replication). Currently peers must poll to discover
            // new snapshot hashes.
            let server = match registry_acl {
                Some(acl) => RegistryServer::with_acl(registry_ref.clone(), acl),
                None => RegistryServer::new(registry_ref.clone()),
            };
            router_builder = router_builder.accept(REGISTRY_ALPN, server);
        }
        // Control RPC (`s5/node/0`) is deliberately NOT registered on this
        // (public) router: any peer that can reach the public endpoint could
        // otherwise drive tasks/config/export (F03). It lives on the
        // dedicated loopback [`ControlPlane`] built below instead.
        // Register the pair-handshake listener if provided. Distinct
        // ALPN so the local-CLI control surface is unaffected by
        // remote pair callers.
        if let Some(listener) = pair_listener {
            router_builder = router_builder.accept(crate::pair::PAIR_ALPN, listener);
        }
        // Register the device-enrollment listener if provided (D10) —
        // its own ALPN + one-time token, same isolation rationale.
        if let Some(listener) = enroll_listener {
            router_builder = router_builder.accept(crate::enroll::ENROLL_ALPN, listener);
        }
        let router = router_builder.spawn();

        let control = match s5_server.as_ref() {
            Some(server) => Some(ControlPlane::spawn(server.clone()).await?),
            None => None,
        };

        Ok(Self {
            config,
            stores,
            link_stores,
            registry,
            endpoint,
            router,
            s5_server,
            control,
        })
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        if let Some(control) = &self.control {
            control.router.shutdown().await?;
        }
        self.router.shutdown().await?;
        Ok(())
    }
}

/// Create the raw `Arc<dyn Store>` for a config entry.
///
/// `LocalLinks` is not a full `Store` — it is handled separately by the node.
///
/// A built raw store, plus — for backends that can cheaply back a durable
/// registry **in the same account** (indexd, via metadata pointers) — a registry
/// handle sharing the store's connection + cache. The durable registry binds to
/// `registry` instead of wrapping the packed blob store in a `StoreRegistry`, so
/// each HEAD update is a ~ms metadata re-pin (not a fresh erasure-coded slab) and
/// is rebuilt by the same `reconstruct_from_indexer` pass that rebuilds blobs.
pub struct CreatedStore {
    /// The path-store handle, for the blobs server (`provide`), the cold-store
    /// GC (`modified` / `blob_path_for_hash`), and the
    /// `BlobStore` server map. `None` for a content-addressed backend (the Sia
    /// `PackingStore`) that is not a `Store`.
    pub store: Option<Arc<dyn s5_core::store::Store>>,
    /// The vault-facing content-addressed handle (read + write + delete by
    /// hash). Path backends ride in here as their `BlobStore`; indexd plugs in
    /// its `PackingStore` directly. Always present.
    pub blobs: Arc<dyn Blobs>,
    pub registry: Option<Arc<dyn RegistryApi + Send + Sync>>,
}

pub async fn create_raw_store(
    config: NodeConfigStore,
    _resolved: &HashMap<String, Arc<dyn s5_core::store::Store>>,
) -> StoreResult<CreatedStore> {
    // Bound before the match consumes `config.backend`.
    let read_cache_bytes = config.read_cache_bytes;
    let outboard = config.outboard;
    // Set by backends that natively back a durable registry (indexd).
    let mut registry: Option<Arc<dyn RegistryApi + Send + Sync>> = None;
    let store: Arc<dyn s5_core::store::Store> = match config.backend {
        NodeConfigStoreBackend::SiaRenterd(config) => Arc::new(SiaStore::create(config).await?),
        NodeConfigStoreBackend::Local(config) => Arc::new(LocalStore::create(config)),
        NodeConfigStoreBackend::S3(config) => Arc::new(S3Store::create(config)),
        NodeConfigStoreBackend::Memory => Arc::new(MemoryStore::new()),
        NodeConfigStoreBackend::Fjall(config) => {
            let cache_bytes = config.cache_mib.unwrap_or(256) as u64 * 1024 * 1024;
            Arc::new(FjallStore::open_with_cache(&config.path, cache_bytes)?)
        }
        NodeConfigStoreBackend::LocalLinks(_) => {
            return Err(anyhow::anyhow!(
                "LocalLinks stores should be accessed via S5Node.link_stores"
            ));
        }
        NodeConfigStoreBackend::Indexd(cfg) => {
            // The Sia store is **packing over indexd**: small writes are bundled
            // into ~40 MiB content-addressed packs before they hit indexd, so each
            // upload is a full Sia slab. A raw tiny blob would otherwise waste a
            // whole erasure-coded slab (~40 s, ~0.002 MiB/s) — packing brings that
            // to ~1 MiB/s and keeps ingest memory bounded (chunks stage to disk,
            // not RAM). See `stores/packing` + `docs/reference/architecture-directions.md`.
            // AppKey is inline (like S3); `open` validates it against the indexer.
            let app_key = decode_app_key(&cfg.app_key)?;
            for suffix in ["", "-staging", "-manifests"] {
                let _ = std::fs::create_dir_all(format!("{}{suffix}", cfg.cache_path));
            }

            // The Sia store is ONE IndexdStore of self-describing packs. Bodies
            // are 10-of-30 (3x — the sia_storage/s3d default; a backup's
            // durability margin matters more than the ~17% a leaner 10-of-25
            // would save, and packed bodies already fill full slabs). There is NO
            // separate manifest object on Sia: each pack body carries its index
            // as a prepended header (see `stores/packing`), so a pack recovers
            // from its own header. `max_inflight` is the device RAM/throughput knob — upload
            // memory ≈ max_inflight × total_shards × 4 MiB — so phones lower it
            // and capable devices raise it for more concurrency.
            let max_inflight = cfg.max_inflight.unwrap_or(8);
            // The enumeration/download timeouts and their retries use the store's
            // generous defaults (60 s pages, 300 s reads) — they keep slow/bad
            // links working without config and never slow a healthy one. Only the
            // whole-pack upload deadline is tunable (see the packing_config below).
            let bodies = s5_store_indexd::IndexdStore::open(
                s5_store_indexd::IndexdConfig {
                    indexer_url: cfg.indexer_url.clone(),
                    upload_options: Some(s5_store_indexd::UploadOptionsBuilder {
                        data_shards: 10,
                        parity_shards: 20,
                        max_inflight,
                    }),
                    ..Default::default()
                },
                app_key,
                LocalStore::new(&cfg.cache_path),
                // Headless daemon connection — the S5 default branding.
                None,
            )
            .await?;

            // Background freshness: poll the indexer for objects written by other
            // devices on the same account (multi-device backup), incrementally
            // into the shared cache. Detached — lifetime is the daemon's; the
            // tokio runtime cancels it on shutdown. Shares the cache with the
            // packing/registry siblings, so one loop keeps everything current.
            tokio::spawn(
                bodies
                    .clone()
                    .run_sync_loop(std::time::Duration::from_secs(60)),
            );

            // Durable registry sibling: the SAME IndexdStore (shared connection +
            // cache via `with_upload_options`) exposed as a `RegistryApi`. Each
            // HEAD is an indexer metadata pointer — a ~ms re-pin per update, not a
            // fresh erasure-coded slab — landing in the same account, so the one
            // `reconstruct_from_indexer` pass rebuilds blobs *and* HEADs. The
            // placeholder bodies are 3-of-12 (smallest valid EC): the body is
            // throwaway; durability is the metadata pin record.
            registry = Some(Arc::new(bodies.with_upload_options(Some(
                s5_store_indexd::UploadOptionsBuilder {
                    data_shards: 3,
                    parity_shards: 9,
                    max_inflight,
                },
            ))));

            // The pack index is a LOCAL cache now (the durable copy is each pack
            // body's prepended header), so manifests live on a local store — no
            // Sia objects, no EC trade-off. On a cold/wiped device it's rebuilt
            // from the pack headers by the reconcile below.
            let manifests = LocalStore::new(format!("{}-manifests", cfg.cache_path));

            // Staging → local: blobs live here until a pack fills, then stream to
            // indexd and are deleted (re-enqueued on restart if unflushed).
            let staging = LocalStore::new(format!("{}-staging", cfg.cache_path));

            // Pack sizing is SLAB-aligned: one Sia slab carries
            // `data_shards × 4 MiB sectors` = 40 MiB of payload at the 10-of-30
            // EC above, so pack sizes that are multiples of 40 MiB fill whole
            // slabs — anything else pays for the padded remainder of the last
            // slab. Target 80 MiB (2 slabs) packs: big enough to amortize
            // per-object overhead, small enough that one pack uploads in seconds
            // even on a weak uplink, keeping the blast radius of a wedged upload
            // and the unit of incremental durability small. (s3d, the inspiration
            // for packing, groups to a waste threshold at slab granularity rather
            // than a fixed large target; big multi-slab packs were an s5 choice
            // that hurt slow connections, so keep them modest here.)
            //
            // `min_group_size` (40 MiB = 1 slab) is the MINIMUM the background
            // loop flushes; the 80 MiB *target* is `max_group_size` (the cap
            // `first_fit` fills toward). These MUST differ: the loop flushes only
            // when `total_size >= min_group_size`, but `first_fit` stops *before*
            // exceeding `max_group_size`, so a full group lands just under the
            // cap. If min == max the gate never trips and the loop flushes
            // nothing — the only flush left is publish's forced `sync()`, the
            // hang we saw on a fresh Sia account. Keep min below max. (Sub-minimum
            // tails still flush via `max_pending_age`, so small snaps never wait.)
            let packing_config = s5_store_packing::PackingConfig {
                min_group_size: 40 << 20,
                max_group_size: 80 << 20,
                slab_size: 40 << 20,
                upload_timeout_floor: std::time::Duration::from_secs(
                    cfg.upload_timeout_secs.unwrap_or(1200),
                ),
                ..s5_store_packing::PackingConfig::default()
            };
            // Keep a handle to the (sync-on-open) IndexdStore so we can enumerate
            // pack bodies for the cold-boot reconcile below; the original is
            // consumed as packing's content-addressed blob backend.
            let bodies_for_reconcile = bodies.clone();
            let packing = s5_store_packing::PackingStore::open(
                s5_core::blob::BlobStore::without_outboard(bodies),
                Arc::new(manifests),
                Arc::new(staging),
                packing_config,
            )
            .await?;

            // Cold-boot reconcile — CORRECTNESS, not an optimization. On a fresh
            // or wiped device the local manifest cache is empty, so the pack-
            // membership index is empty and packed vault roots/TNs would read as
            // "not found" (recovery silently broken). The IndexdStore is
            // sync-on-open, so its cache lists every pack body (`blob3/<hash>`).
            //
            // Discover cheaply + enrich in the background: `note_pack_hashes`
            // just records `todo` markers (no header reads, no blocking), then a
            // detached task drains them. Correctness doesn't depend on the task
            // finishing — any read/exists/dedup that would return a *negative*
            // blocks on enrichment first (a hit short-circuits), so `vup recover`
            // reading a packed root simply waits for the pack it needs. Boot stays
            // fast; a warm device's `note` is a no-op (every pack already known).
            let pack_hashes = s5_core::blob::BlobStore::without_outboard(bodies_for_reconcile)
                .list_hashes()
                .await?;
            packing.note_pack_hashes(pack_hashes).await?;
            tokio::spawn({
                let packing = packing.clone();
                async move {
                    match packing.enrich().await {
                        Ok(n) if n > 0 => tracing::info!(
                            enriched = n,
                            "packing: enriched pack-membership index from headers"
                        ),
                        Ok(_) => {}
                        Err(e) => tracing::warn!(
                            "packing: background enrich failed (reads will retry on demand): {e:?}"
                        ),
                    }
                }
            });

            // Background loop bundles + uploads packs; the publish path's
            // `sync()` barrier force-flushes pending packs before each HEAD.
            tokio::spawn(packing.clone().run_upload_loop());
            // PackingStore is content-addressed (`BlobsReadWrite`), not a
            // `Store`, so it returns as the vault `blobs` handle directly — no
            // path-store view (the blobs server / cold-GC don't serve packed
            // Sia blobs, and GC is deferred there). This also skips the
            // read-cache wrap below, which is a path-`Store` decorator; packing
            // has its own staging + in-memory index.
            return Ok(CreatedStore {
                store: None,
                blobs: packing,
                registry,
            });
        }
    };
    // Optional in-RAM read-through cache above the built store. Whole-blob
    // reads consult RAM first and populate on a miss; writes pass through to
    // the durable store (see `s5_core::CachingStore`). Bounded by `MemoryStore`'s
    // byte budget so the resident set self-evicts.
    let store = match read_cache_bytes {
        Some(n) if n > 0 => {
            tracing::info!(
                read_cache_bytes = n,
                "store: in-RAM read-through cache enabled"
            );
            let cache: Arc<dyn s5_core::store::Store> = Arc::new(MemoryStore::with_budget(n));
            Arc::new(s5_core::CachingStore::new(cache, store)) as Arc<dyn s5_core::store::Store>
        }
        _ => store,
    };
    // A path backend's vault handle is its `BlobStore` (per-store `outboard`).
    let blobs: Arc<dyn Blobs> =
        Arc::new(BlobStore::from_arc_with_outboard(store.clone(), outboard));
    Ok(CreatedStore {
        store: Some(store),
        blobs,
        registry,
    })
}

/// Decode a hex-encoded 32-byte indexd AppKey from config into raw bytes.
fn decode_app_key(hex_key: &str) -> StoreResult<[u8; 32]> {
    let bytes = hex::decode(hex_key.trim())
        .map_err(|e| anyhow::anyhow!("indexd app_key is not valid hex: {e}"))?;
    bytes.try_into().map_err(|v: Vec<u8>| {
        anyhow::anyhow!(
            "indexd app_key must be 32 bytes (64 hex chars), got {}",
            v.len()
        )
    })
}

/// Create a `BlobStore` from a config entry (convenience wrapper for
/// callers that operate on a single store — e.g. ad-hoc store ops).
pub async fn create_store(config: NodeConfigStore) -> StoreResult<BlobStore> {
    let outboard = config.outboard;
    let created = create_raw_store(config, &HashMap::new()).await?;
    match created.store {
        Some(store) => Ok(BlobStore::from_arc_with_outboard(store, outboard)),
        None => Err(anyhow::anyhow!(
            "this store backend is content-addressed (no BlobStore view); \
             use create_raw_store and its `blobs` (dyn Blobs) handle instead"
        )),
    }
}

/// Context needed to create registries.
pub struct RegistryContext<'a> {
    /// The unified store registry (D15). A `Store`-backed registry config
    /// prefers the store's native registry handle (indexd: metadata
    /// pointers) and otherwise wraps the raw path `Store` in a generic
    /// `StoreRegistry` — named-object writes are genuine path semantics,
    /// so the raw-store view is requested explicitly here.
    pub stores: &'a NodeStores,
}

/// Creates a registry from configuration, wrapped in a
/// [`BroadcastingRegistry`] so live subscribers see every write —
/// whether the write came over the RPC server or directly from a
/// local writer like the publish task.
pub fn create_registry(
    backend: NodeConfigRegistry,
    ctx: &RegistryContext<'_>,
) -> anyhow::Result<Arc<BroadcastingRegistry>> {
    let inner = create_registry_inner(backend, ctx)?;
    Ok(BroadcastingRegistry::wrap(inner))
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
        NodeConfigRegistry::Store { store, prefix } => {
            // Prefer the store's native registry handle (indexd: cheap
            // metadata-pointer HEADs, immediately durable, rebuilt by the same
            // reconstruct pass) when it has one; otherwise wrap it in a generic
            // StoreRegistry (value stored as object data). The pointer registry
            // uses the conventional `registry/` prefix; `prefix` applies only to
            // the StoreRegistry fallback.
            if let Some(reg) = ctx.stores.native_registry(&store) {
                return Ok(reg);
            }
            let raw_store = ctx.stores.raw_store(&store).ok_or_else(|| {
                anyhow!("registry store '{}' not found in [store.*] config", store)
            })?;
            let store_registry = StoreRegistry::new(raw_store, prefix);
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

// iroh notes informing the choices below:
//
// - `presets::N0` (iroh 0.97+) installs the rustls `ring` crypto
//   provider (required as of iroh 0.98 — no more implicit default),
//   pkarr publishing + resolution via n0's DNS, mDNS for local-network
//   peer discovery, and the n0 relay fallback. Address discovery is
//   iroh's job, not s5's: each `DidDocument` we publish lists this
//   device's iroh public key (and other devices' pubkeys are listed
//   in their bundles); pkarr/DNS/mDNS resolve those pubkeys to
//   current `EndpointAddr`s at dial time. The membership ACL
//   (`after_handshake` hook below) rejects unauthorised peers
//   regardless of how they reached us, so the relay's "anyone can
//   forward encrypted bytes to your endpoint" property is not a
//   meaningful new exposure.
//   For embedded targets where `ring`'s C assembly doesn't compile
//   (e.g. ESP32 / Xtensa, see the n0 ESP32 blog post Mar 2026), a
//   pluggable pure-Rust provider via `Builder::crypto_provider` is
//   the path forward.
//   Privacy caveat: pkarr in N0 *publishes* this daemon's
//   `pubkey → EndpointAddrs` to a DHT-backed DNS as a side effect
//   of running. That makes "this identity is online, here are its
//   current IPs" queryable by anyone who knows the pubkey — a real
//   metadata leak relative to `presets::Minimal`. We accept this for
//   now because it's the price of ed25519-keyed dialing without
//   out-of-band addressing, but a future config knob should let
//   operators opt out of *publishing* (resolution can stay on, since
//   it's lookup-only).
// - TODO(sovereignty): s5 should self-host every iroh service it
//   currently consumes from n0's hosted infra, so a fully-featured
//   deployment can survive n0 the company disappearing. Concretely:
//     * iroh-relay — embeddable since iroh 0.97; ship an
//       `s5 relay` subcommand and let operators point clients at
//       their own relay URLs instead of `relay.iroh.network`.
//     * pkarr publish + resolve — currently uses n0's
//       DNS-over-HTTPS endpoints; run our own pkarr relay(s) and
//       configure the endpoint resolver to prefer them.
//     * DNS-based pubkey lookup — replace `n0_dns()` /
//       `dns.iroh.link` with an s5-operated zone (or a fully
//       DHT-only mode) so address discovery has no n0 dependency.
//     * any telemetry/metrics endpoints baked into iroh's defaults
//       should be off by default and, if used, point at s5 infra.
//   None of this requires forking iroh — every piece is already a
//   pluggable component on `Endpoint::builder`. The work is mostly
//   ops + a small "hosting profile" config block that overrides the
//   N0 preset's URLs.
// - iroh 0.96 multipath: relay + direct UDP are first-class QUIC paths
//   with per-path congestion state. Free benefit; no s5 code change.
// - iroh 0.97 custom transports (`add_custom_transport`) — Tor, BLE,
//   Nym, WebRTC, InfiniBand etc. — map directly onto the privacy and
//   proximity scenarios described in `docs/reference/transport.md`.
//   To opt in, add the unstable-custom-transports feature and pull in
//   a transport crate. Architecture-directions Tor/BLE rows are the
//   product targets.
// - `EndpointHooks` (iroh 0.96+): only `after_handshake` is needed for
//   step 3a's connection-level peer ACL — `before_connect` is for
//   outgoing connections we initiate, and we don't yet have a use for
//   that hook. The auth-hook example in iroh's repo is the canonical
//   reference for hook-based authentication.
pub async fn run_node(
    config_file_path: std::path::PathBuf,
    config: S5NodeConfig,
) -> anyhow::Result<()> {
    run_node_with_local_client(config_file_path, config, None, None, None, None, None).await
}

/// Subset of the running daemon's state that in-process callers can
/// pull out at startup. Shipped through the `substrate_tx` arg of
/// [`run_node_with_local_client`] right after construction, before the
/// daemon enters its main loop.
///
/// Use cases: in-process consumers (like an embedding host) that need to
/// issue out-of-band calls — e.g. `load_peer_snapshot` against a
/// known peer — using the same blob store / registry / endpoint the
/// daemon is operating on. Keeps the consumer from having to open a
/// second view on the same on-disk data.
///
/// Cheap to clone: every field is already Arc-backed or `Endpoint`
/// (itself internally `Arc`).
#[derive(Clone)]
pub struct EmbeddedSubstrate {
    /// Every configured `[store.*]` as the vault-facing `dyn Blobs` handle
    /// (read + write + delete by hash). Same map the task executor reads from,
    /// so a Sia `PackingStore` is reached the same way as a path-store
    /// `BlobStore`.
    pub stores: std::collections::HashMap<String, Arc<dyn Blobs>>,
    /// The default registry (when configured). `None` mirrors the
    /// `S5Node::registry` field — runtime without a registry can't
    /// publish but is otherwise valid.
    pub registry: Option<Arc<dyn RegistryApi + Send + Sync>>,
    /// Iroh endpoint the daemon is dialing peers from. Pass to
    /// `s5_registry::Client::connect_to_peer` /
    /// `s5_blobs::Client::connect_to_peer_public` /
    /// `connect_to_peer_acl` in-process for ad-hoc dials that should
    /// share the warm connection cache.
    pub endpoint: Endpoint,
    /// This daemon's device ACL/read signing key. Embedded consumers
    /// (e.g. an embedding host's reactor) that dial peers on the **ACL**
    /// blobs ALPN must present the F02 challenge proof; this is the signer
    /// they use. Cloning the SigningKey is cheap (32-byte seed wrap).
    pub device_acl_signing_key: ed25519_dalek::SigningKey,
}

/// Same as [`run_node`], plus two opt-in extensions:
///
/// * `local_client_tx`: back-channel for an in-process irpc client.
///   After the RPC server is built, the local client is sent through
///   this oneshot — the caller can then issue RPCs against this node
///   from the same process without an iroh round-trip. See
///   [`s5_server::S5NodeServer::serve_local`] for the dispatch pattern.
///   Used by an embedding host's `bg_persist` hook.
///
/// * `blob_acl_override`: replace the default [`MembershipBlobAcl`]
///   that gates reads by vault membership. Pass
///   `Some(Arc::new(PermitAllBlobAcl))` for vaults that are
///   intentionally world-readable (e.g. a public compute mirror), or
///   any other `BlobAcl` for custom policies. `None` keeps the
///   default membership behaviour.
///
/// * `data_events`: optional broadcast sender that the embedded
///   [`crate::membership_subscribe::MembershipSubscriber`] fires
///   `DataVaultEvent` into on every data-vault HEAD change applied to
///   the local registry. The caller is expected to create the channel
///   (`tokio::sync::broadcast::channel`) and retain a clone of the
///   sender for `.subscribe()` calls. `None` disables fan-out.
///
/// * `substrate_tx`: optional one-shot for the in-process consumer
///   pattern — receives an [`EmbeddedSubstrate`] snapshot right after
///   stores + registry + endpoint are built, before the daemon enters
///   its main loop. Lets the caller share the daemon's blob store /
///   registry / endpoint for ad-hoc operations (e.g. `load_peer_snapshot`)
///   without opening a second view of the same on-disk state.
pub async fn run_node_with_local_client(
    config_file_path: std::path::PathBuf,
    config: S5NodeConfig,
    local_client_tx: Option<oneshot::Sender<irpc::Client<s5_node_api::S5NodeProto>>>,
    blob_acl_override: Option<Arc<dyn s5_blobs::BlobAcl>>,
    data_events: Option<
        tokio::sync::broadcast::Sender<crate::membership_subscribe::DataVaultEvent>,
    >,
    substrate_tx: Option<oneshot::Sender<EmbeddedSubstrate>>,
    // Optional metrics sink for the per-vault cold-store GC task. The
    // ingest publisher passes `Some(..)`; consumers pass `None`.
    // The GC task only spawns for vaults with `gc_enabled = true`, so a
    // `None` reporter just means "GC runs without metrics", not "no GC".
    gc_reporter: Option<Arc<dyn tasks::cold_gc::GcReporter>>,
) -> anyhow::Result<()> {
    // Create iroh endpoint first (needed for remote registries).
    // `presets::N0` enables pkarr/DNS/mDNS discovery so peers can dial
    // each other by ed25519 pubkey alone — see the run_node header.
    let config_dir = config_file_path.parent();
    // Shared membership state: filled below after stores+registry come up
    // and `build_membership_state` runs. Until then, the transport ACL
    // hook rejects every inbound connection.
    let membership_state = Arc::new(RwLock::new(crate::membership::MembershipState::default()));
    let membership_hook = crate::membership::MembershipHook::new(membership_state.clone());
    // Observation-only second hook: aggregates per-peer connection
    // events into a daemon-wide `RemoteMap`. Always Accept — the
    // membership hook before it does the actual policy. Cloned into
    // both the endpoint builder and the RPC server (DashMap-backed,
    // so all clones share the same map).
    let peer_observer = crate::peer_observer::PeerObserver::new();
    let mut builder = Endpoint::builder(iroh::endpoint::presets::N0)
        .hooks(membership_hook)
        .hooks(peer_observer.clone());
    // Per-device keyset (slice S2.5): three independent random ed25519
    // seeds (iroh transport + device signing + device ACL), age-encrypted
    // to `[key.main]`. Loaded once at boot; the iroh secret feeds the
    // endpoint builder, the other two are threaded through the task
    // executor + identity publish. When neither `keyset_file` nor
    // `secret_key_file` is configured, `device_keyset_for_boot` returns
    // `None` and we fall back to iroh's own ephemeral keygen plus blake3
    // derivation off it (the legacy in-RAM path, kept for tests and
    // inline-secret deployments).
    let device_keyset = device_keyset_for_boot(&config.identity, &config.key, config_dir);
    builder = builder.secret_key(device_keyset.iroh_secret_key());
    let endpoint = builder.bind().await?;

    // Build the unified store registry (D15): ONE map keyed by `[store.*]`
    // name. Every entry carries the vault-facing `dyn Blobs` view (including
    // the content-addressed Sia `PackingStore`); path-backed entries also
    // retain their raw `Store`, from which the path-`BlobStore` view is
    // requested EXPLICITLY at the two sites with genuine path semantics
    // (blobs-server `provide`, cold-GC) — never threaded as default
    // currency.
    let mut node_stores = NodeStores::default();
    for (name, store_config) in &config.store {
        match &store_config.backend {
            NodeConfigStoreBackend::LocalLinks(_) => {} // handled by the node separately
            _ => {
                let created = create_raw_store(store_config.clone(), &HashMap::new()).await?;
                node_stores.insert(name.clone(), created, store_config.outboard);
            }
        }
    }
    // Vault-facing `dyn Blobs` view of the registry — the default currency
    // the task executor, membership/identity plumbing, and the embedded
    // substrate operate on. Every backend is present, so a Sia
    // `PackingStore` is reached the same way as a path store.
    let vault_blobs: HashMap<String, Arc<dyn Blobs>> = node_stores.blobs_map();

    // Create the default registry (if configured)
    let registry_ctx = RegistryContext {
        stores: &node_stores,
    };
    let registry = match config.registry.get("default") {
        Some(reg_config) => Some(create_registry(reg_config.clone(), &registry_ctx)?),
        None => {
            tracing::warn!("no [registry.default] configured — snapshot publishing disabled");
            None
        }
    };

    // Create the task executor with pre-built stores.
    // The `node_secret` is the per-device signing key seed — used by
    // `tasks::publish::device_signing_key` to produce the SigningKey that
    // signs vault registry entries. Always sourced from the keyset
    // (slice S2.5/S2.6: independent random, no derivation off the iroh
    // secret).
    let node_secret = device_keyset.device_signing;
    // Wrap config in Arc<RwLock> once — shared between the RPC server and the
    // task executor so that `patch_config` updates are visible to tasks.
    let config = Arc::new(RwLock::new(config));
    let membership_refresh = Arc::new(tokio::sync::Notify::new());
    // Live-reconfig signal for the automation engine — mirrors
    // `membership_refresh` exactly: `handle_patch_config` fires it, the
    // automation coordinator reconciles on each notify (Stage 7).
    let automation_refresh = Arc::new(tokio::sync::Notify::new());
    let discovery_seed = Arc::new(std::sync::OnceLock::new());
    let executor_ctx = Arc::new(tasks::TaskExecutorContext {
        config: config.clone(),
        stores: vault_blobs.clone(),
        node_secret,
        registry: registry
            .as_ref()
            .map(|r| r.clone() as Arc<dyn RegistryApi + Send + Sync>),
        membership: Some(membership_state.clone()),
        membership_refresh: Some(membership_refresh.clone()),
        discovery_seed: discovery_seed.clone(),
    });
    let executor = Arc::new(tasks::TaskExecutor::new(executor_ctx));
    // The daemon's automation engine — reconciles `[task.*]` automations (and
    // the legacy `watch`/`snap_interval_secs` shim) into live loops. Shared
    // with the RPC server (for `GetStatus` liveness) and driven by the
    // coordinator task below.
    let automation_manager = Arc::new(crate::watch::AutomationManager::new(executor.clone()));
    let mount_manager = Arc::new(fuse::MountManager::new(executor.clone()));

    // ---- Per-vault cold-store GC ----
    // One periodic GC task per vault with `gc_enabled = true`. Needs a
    // registry (the published-TN reachability lookup); skips with a warning
    // otherwise. Reads are over the vault data store; deletion is
    // restricted to the named cold backend. See `tasks::cold_gc`.
    if let Some(reg) = registry.as_ref() {
        let pins: Arc<dyn s5_core::Pins> = Arc::new(s5_core::RegistryPinner::new(
            reg.clone() as Arc<dyn RegistryApi + Send + Sync>
        ));
        let self_pubkey = tasks::publish::device_signing_key(&node_secret)
            .verifying_key()
            .to_bytes();
        let gc_vaults: Vec<(String, s5_node_api::config::NodeConfigVault, Option<String>)> = {
            let cfg = config.read().await;
            cfg.vault
                .iter()
                .filter(|(_, v)| v.gc_enabled)
                .map(|(n, v)| {
                    let data = cfg.vault_data_store(n, v).ok().map(str::to_string);
                    (n.clone(), v.clone(), data)
                })
                .collect()
        };
        for (vault_name, vault, data_store_name) in gc_vaults {
            let Some(cold_name) = vault.gc_store.as_ref() else {
                tracing::warn!(vault = %vault_name, "gc_enabled but no gc_store set — cold-GC not started");
                continue;
            };
            // EXPLICIT path-view request (D15): deletion's mtime grace gate
            // needs `modified` — genuine path semantics. A content-addressed
            // gc_store has no path view and cannot host cold-GC deletion.
            let Some(cold_store) = node_stores.path_store(cold_name) else {
                tracing::warn!(vault = %vault_name, store = %cold_name, "gc_store names an unknown or content-addressed [store.*] — cold-GC not started");
                continue;
            };
            let Some(data_store) = data_store_name else {
                tracing::warn!(vault = %vault_name, "vault resolves no data store — cold-GC not started");
                continue;
            };
            // Reads (published-TN fetch + reachability walk) need no path
            // semantics — the capability view suffices for any backend.
            let Some(tiered_store) = node_stores.blobs(&data_store) else {
                tracing::warn!(vault = %vault_name, store = %data_store, "vault data store names an unknown [store.*] — cold-GC not started");
                continue;
            };
            tasks::cold_gc::spawn_cold_gc(tasks::cold_gc::ColdGcParams {
                vault_name,
                registry: reg.clone() as Arc<dyn RegistryApi + Send + Sync>,
                tiered_store,
                cold_store,
                pins: pins.clone(),
                membership: membership_state.clone(),
                self_pubkey,
                // Current deployments publish a plaintext TN
                // (`plaintext_published_tn`), so no age identity is needed
                // to read it. A vault with an
                // encrypted published TN + gc_enabled would need identity
                // plumbing here; until then the fetch fails safe (no deletions).
                identity_files: Vec::new(),
                interval: std::time::Duration::from_secs(vault.gc_interval_secs.unwrap_or(86_400)),
                min_age: std::time::Duration::from_secs(vault.gc_min_age_secs.unwrap_or(604_800)),
                dry_run: vault.gc_dry_run,
                reporter: gc_reporter.clone(),
            });
        }
    } else if config.read().await.vault.values().any(|v| v.gc_enabled) {
        tracing::warn!(
            "a vault has gc_enabled but no registry is configured — cold-GC not started"
        );
    }

    // Create shutdown channel and S5NodeServer RPC.
    let endpoint_id = endpoint.id().to_string();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = s5_server::S5NodeServer::new(
        config.clone(),
        config_file_path.clone(),
        executor.clone(),
        mount_manager,
        endpoint_id,
        shutdown_tx,
    );

    let registry_for_identity = registry.clone();
    let registry_acl: Arc<dyn s5_registry::RegistryAcl> = Arc::new(
        crate::membership::MembershipRegistryAcl::new(membership_state.clone()),
    );
    // Caller-supplied ACL takes precedence; otherwise default to the
    // membership-aware ACL (which denies everything when no members
    // are configured — the conservative default).
    let blob_acl: Arc<dyn s5_blobs::BlobAcl> = blob_acl_override.unwrap_or_else(|| {
        Arc::new(crate::membership::MembershipBlobAcl::new(
            membership_state.clone(),
            Arc::new(vault_blobs.clone()),
        ))
    });
    // Master signing key (DID-encoded), hoisted here so the pair listener +
    // RPC server can sign/verify the master-key proof-of-possession in the
    // pairing handshake (D8: everything anchored in the DID). Resolution:
    //   1. `[identity].master_key_file` set → use that path.
    //   2. Unset + `[identity].secret_key_file` set → sibling `identity_master.key`.
    //   3. Neither → ephemeral random master in RAM (DID won't survive restart).
    // On the disk paths the key is age-encrypted at rest when `[key.main]` is
    // configured; else plaintext + 0o600 with a warn.
    let master_signing_key = {
        let cfg_snapshot = config.read().await;
        let explicit = cfg_snapshot
            .identity
            .master_key_file
            .as_deref()
            .map(std::path::PathBuf::from);
        let resolved_path = explicit.or_else(|| {
            crate::identity_vault::default_master_key_path(&cfg_snapshot.identity, config_dir)
        });
        match resolved_path {
            Some(path) => match crate::identity_vault::load_or_generate_master_signing_key(
                &path,
                &cfg_snapshot.key,
                config_dir,
            ) {
                Ok(k) => k,
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        "identity: master key load/generate failed: {e:#} \
                         — using an ephemeral random master for this boot"
                    );
                    ephemeral_master_signing_key()
                }
            },
            None => {
                tracing::info!(
                    "identity: no master_key_file path resolvable — using an \
                     ephemeral random master (DID will not survive restart). \
                     Configure [identity].secret_key_file or master_key_file \
                     for persistence."
                );
                ephemeral_master_signing_key()
            }
        }
    };

    // D17 cold/warm: the shareable DID is the *cold* anchor pubkey, NOT
    // the (warm) signing key loaded above. Sources, in order:
    //   1. An anchor entry file (written by `vup onboard`/`recover`;
    //      `[identity].anchor_entry_file` or the default sibling
    //      `identity_anchor.entry`) → verify it names our warm key,
    //      DID = its embedded cold pubkey.
    //   2. None → SELF-ANCHORED dev mode: cold == warm; sign a pointer
    //      under the warm key so resolution stays uniformly two-hop.
    // Either way `anchor_entry` is republished to the registry below
    // (next to the bundle publish) and handed to the pairing plumbing
    // for in-band self-certification.
    let (self_did, anchor_entry) = {
        let cfg_snapshot = config.read().await;
        let explicit = cfg_snapshot
            .identity
            .anchor_entry_file
            .as_deref()
            .map(std::path::PathBuf::from);
        let resolved_path = explicit.or_else(|| {
            crate::identity_anchor::default_anchor_entry_path(&cfg_snapshot.identity, config_dir)
        });
        let from_file = resolved_path
            .filter(|p| p.exists())
            .map(|p| (crate::identity_anchor::load_anchor_entry(&p), p));
        match from_file {
            Some((Ok(entry), path)) => {
                let did = match &entry.key {
                    s5_core::StreamKey::Vault { pubkey, .. } => {
                        s5_core::identity::Did::from_pubkey(
                            s5_core::identity::DidMasterPubkey::new(*pubkey),
                        )
                    }
                    _ => unreachable!("anchor entries are always vault-keyed"),
                };
                match crate::identity_anchor::cold_pointer_from_entry(&did, &entry) {
                    Ok(pointer)
                        if pointer.warm_pub == master_signing_key.verifying_key().to_bytes() =>
                    {
                        (did, entry)
                    }
                    Ok(_) => {
                        tracing::error!(
                            path = %path.display(),
                            "identity: anchor entry names a DIFFERENT warm key than the one \
                             on disk — identity is inconsistent (re-run `vup recover`). \
                             Falling back to a self-anchored identity for this boot."
                        );
                        let entry =
                            crate::identity_anchor::self_anchored_entry(&master_signing_key)
                                .expect("self-anchor signing is infallible for a valid key");
                        let did = s5_core::identity::Did::from_pubkey(
                            s5_core::identity::DidMasterPubkey::from_verifying_key(
                                &master_signing_key.verifying_key(),
                            ),
                        );
                        (did, entry)
                    }
                    Err(e) => {
                        tracing::error!(
                            path = %path.display(),
                            "identity: anchor entry file is invalid: {e:#} — falling back \
                             to a self-anchored identity for this boot."
                        );
                        let entry =
                            crate::identity_anchor::self_anchored_entry(&master_signing_key)
                                .expect("self-anchor signing is infallible for a valid key");
                        let did = s5_core::identity::Did::from_pubkey(
                            s5_core::identity::DidMasterPubkey::from_verifying_key(
                                &master_signing_key.verifying_key(),
                            ),
                        );
                        (did, entry)
                    }
                }
            }
            Some((Err(e), path)) => {
                tracing::error!(
                    path = %path.display(),
                    "identity: reading anchor entry failed: {e:#} — falling back to a \
                     self-anchored identity for this boot."
                );
                let entry = crate::identity_anchor::self_anchored_entry(&master_signing_key)
                    .expect("self-anchor signing is infallible for a valid key");
                let did = s5_core::identity::Did::from_pubkey(
                    s5_core::identity::DidMasterPubkey::from_verifying_key(
                        &master_signing_key.verifying_key(),
                    ),
                );
                (did, entry)
            }
            None => {
                let entry = crate::identity_anchor::self_anchored_entry(&master_signing_key)
                    .expect("self-anchor signing is infallible for a valid key");
                let did = s5_core::identity::Did::from_pubkey(
                    s5_core::identity::DidMasterPubkey::from_verifying_key(
                        &master_signing_key.verifying_key(),
                    ),
                );
                tracing::info!(
                    "identity: no anchor entry file — running self-anchored \
                     (cold == warm; onboard for the full split)"
                );
                (did, entry)
            }
        }
    };
    tracing::info!(did = %self_did, "self did (share this to pair)");

    // Pair plumbing: the PendingPairs table is shared between the server (which
    // mints tokens via StartPair) and the iroh listener bound to `s5/pair/0`
    // (which redeems them). Both sign the warm PoP and ship the anchor entry
    // (D17), so the listener and server carry warm key + DID + anchor + iroh id.
    let self_iroh_id = *endpoint.id().as_bytes();
    let pending_pairs = crate::pair::PendingPairs::default();
    let pair_listener = crate::pair::PairListener::new(
        pending_pairs.clone(),
        master_signing_key.clone(),
        *self_did.pubkey(),
        anchor_entry.clone(),
        self_iroh_id,
    );

    // Device-enrollment plumbing (D10): the PendingEnrolls table is
    // shared between the server (`DeviceInvite` mints tokens) and the
    // `s5/enroll/0` listener (which redeems them and performs the full
    // §6.1 device-add). Requires a registry (the bundle edit is a
    // registry write) and a durable bootstrap store (the special vaults
    // the joiner completes its walk against live there); without either,
    // `vup device invite` reports the missing prerequisite.
    let pending_enrolls = crate::enroll::PendingEnrolls::default();
    let enroll_listener = {
        let cfg_snapshot = config.read().await;
        let bootstrap = cfg_snapshot.identity.bootstrap_store.clone();
        let escrow_store = bootstrap
            .as_deref()
            .and_then(|name| vault_blobs.get(name).cloned());
        match (registry.as_ref(), bootstrap, escrow_store) {
            (Some(reg), Some(store_name), Some(escrow_store)) => {
                match cfg_snapshot.store.get(&store_name).cloned() {
                    Some(store_cfg) => {
                        let recipients: Vec<String> = cfg_snapshot
                            .key
                            .values()
                            .map(|k| k.public_key.clone())
                            .collect();
                        let identity_files: Vec<String> = cfg_snapshot
                            .key
                            .values()
                            .filter_map(|k| k.identity_file.clone())
                            .collect();
                        let grant = crate::enroll::EnrollGrant {
                            store_name,
                            store: store_cfg,
                            recovery_recipient: cfg_snapshot
                                .key
                                .get("recovery")
                                .map(|k| k.public_key.clone()),
                        };
                        Some(crate::enroll::EnrollListener::new(
                            pending_enrolls.clone(),
                            Arc::new(crate::enroll::EnrollContext {
                                warm: master_signing_key.clone(),
                                anchor_entry: anchor_entry.clone(),
                                registry: reg.clone() as Arc<dyn RegistryApi + Send + Sync>,
                                stores: vault_blobs.clone(),
                                escrow_store,
                                recipients,
                                identity_files,
                                grant,
                            }),
                        ))
                    }
                    None => None,
                }
            }
            _ => {
                tracing::info!(
                    "enroll: no registry or no durable [identity].bootstrap_store — \
                     `vup device invite` unavailable on this daemon"
                );
                None
            }
        }
    };

    // The membership coordinator's Notify is created above (so the
    // task executor context can carry it). The RPC server gets the
    // same Arc so AddFriend / GrantVault / PatchConfig can fire it,
    // plus a clone of the pair + enroll plumbing.
    let server = server
        .with_membership_refresh(membership_refresh.clone())
        .with_automation_refresh(automation_refresh.clone())
        .with_automation_manager(automation_manager.clone())
        .with_pair_support(
            pending_pairs,
            endpoint.clone(),
            master_signing_key.clone(),
            *self_did.pubkey(),
            anchor_entry.clone(),
        )
        .with_enroll_support(enroll_listener.is_some().then(|| pending_enrolls.clone()))
        .with_peer_observer(peer_observer.clone());

    // If the caller asked for the in-process irpc back-channel, build
    // it now and send. The local sender is created from a fresh Arc<Self>
    // wrapper; the server we hand to S5Node::new_with_stores below is a
    // Clone (S5NodeServer is `#[derive(Clone)]` with Arc-internal state,
    // so the clones share state with the local-dispatch task).
    if let Some(client_tx) = local_client_tx {
        let server_arc = std::sync::Arc::new(server.clone());
        let local_sender = server_arc.serve_local();
        let local_client = irpc::Client::<s5_node_api::S5NodeProto>::local(local_sender);
        if client_tx.send(local_client).is_err() {
            tracing::warn!(
                "run_node_with_local_client: receiver dropped before client was sent — \
                 in-process RPC won't be available to the caller"
            );
        }
    }

    let node = S5Node::new_with_stores(
        config.read().await.clone(),
        registry,
        endpoint,
        Some(server),
        Some(&node_stores),
        Some(registry_acl),
        Some(blob_acl),
        Some(pair_listener),
        enroll_listener,
    )
    .await?;

    // Ship the substrate to any in-process consumer waiting for it.
    // We do this AFTER `new_with_stores` so the caller observes a
    // fully-constructed daemon (in particular, after the membership
    // hook and acl wiring are in place — so any out-of-band dials
    // the caller initiates use the same configuration).
    if let Some(tx) = substrate_tx {
        let substrate = EmbeddedSubstrate {
            stores: vault_blobs.clone(),
            registry: node
                .registry
                .as_ref()
                .map(|r| r.clone() as Arc<dyn RegistryApi + Send + Sync>),
            endpoint: node.endpoint.clone(),
            device_acl_signing_key: device_keyset.device_acl_key(),
        };
        if tx.send(substrate).is_err() {
            tracing::warn!("run_node_with_local_client: substrate_tx receiver dropped before send");
        }
    }

    // Note: We skip `endpoint.online().await` because with `empty_builder()` (no relay),
    // it would block forever. The local addresses are available immediately for IPC.
    tracing::info!("s5_node started");
    {
        // Reachability summary: print the iroh pubkey (= EndpointId, what
        // peers paste at pair time), the corresponding `did:s5:b...`,
        // and the locally-bound socket addresses. Pkarr/DNS publishing
        // happens automatically as a side effect of `presets::N0`; if
        // operators don't see this device come up under their pubkey
        // in pkarr after a few seconds, the local addresses logged
        // here are the fallback for explicit `EndpointAddr` config.
        let endpoint_id = node.endpoint.id();
        let iroh_pubkey = *endpoint_id.as_bytes();
        tracing::info!(iroh_pubkey = %hex::encode(iroh_pubkey), "iroh endpoint id");
        // NB: the shareable `did:s5:` encodes the MASTER signing pubkey,
        // not this iroh transport key (four-key model) — it's logged
        // below, once the master key is loaded.
        let addr = node.endpoint.addr();
        tracing::info!(?addr, "iroh endpoint local addresses");
        tracing::info!(
            "discovery: presets::N0 active — pkarr publish + DNS/mDNS \
             resolve + n0 relay fallback"
        );
    }

    // Write the service lock file so clients can discover and connect.
    if let Err(e) = write_service_lockfile(&node) {
        tracing::warn!("failed to write service lock file: {e}");
    }

    // Spawn configured FUSE mounts (best-effort)
    if let Err(err) = crate::fuse::spawn_fuse_mounts(&node).await {
        tracing::warn!("failed to spawn FUSE mounts: {err}");
    }

    // Spawn configured snapshot cycles (background tasks)
    crate::snapshot::spawn_snapshot_cycles(&node).await;

    // `master_signing_key` was hoisted above the pair plumbing (the pairing
    // handshake signs the master-key proof-of-possession). Reused here for the
    // identity bundle + membership subscriber.

    // Three device-scope pubkeys for the identity bundle, all from the
    // keyset (S2.5/S2.6: independent random seeds, no derivation off the
    // iroh secret).
    let device_signing_pubkey: [u8; 32] = device_keyset
        .device_signing_key()
        .verifying_key()
        .to_bytes();
    let device_acl_pubkey: [u8; 32] = device_keyset.device_acl_key().verifying_key().to_bytes();
    let iroh_pubkey: [u8; 32] = node.endpoint.id().as_bytes().to_owned();

    // Publish this daemon's self DidDocument and resolve vault.members
    // bundles into the in-memory membership state. Best-effort: any
    // failure logs and skips, never blocks daemon startup.
    if let Some(reg) = registry_for_identity {
        let cfg_snapshot = config.read().await.clone();
        let reg_dyn: Arc<dyn RegistryApi + Send + Sync> = reg.clone();

        // (Re)publish the cold-pointer anchor so peers resolving our DID
        // find it (D17 step 1 of 2). The entry is already signed —
        // onboard signed it with the cold key, or we self-anchored
        // above — so this is a plain idempotent set.
        if let Err(e) = reg.set(anchor_entry.clone()).await {
            tracing::warn!("identity: anchor republish failed: {e:#}");
        }

        let self_bundle_hash = crate::identity_vault::publish_self_on_startup(
            &cfg_snapshot,
            // The `dyn Blobs` view (D5): includes content-addressed backends
            // (Sia PackingStore) that have no path-`Store`/`BlobStore` view —
            // previously the bundle was silently NOT published on Sia.
            &vault_blobs,
            reg_dyn,
            &master_signing_key,
            device_signing_pubkey,
            device_acl_pubkey,
            iroh_pubkey,
        )
        .await;

        // Publish the synced config (store configs + vault directory + discovery
        // seed) into the durable config vault so a fresh device or paper recovery
        // can rebuild the node. Capture the seed so `publish` can mirror each
        // vault's HEAD under its discovery key. Best-effort: never blocks startup.
        match crate::config_vault::publish_bootstrap_config(
            &cfg_snapshot,
            &master_signing_key,
            &vault_blobs,
            reg.clone(),
        )
        .await
        {
            Ok(Some(seed)) => {
                let _ = discovery_seed.set(seed);
            }
            Ok(None) => {}
            Err(e) => tracing::warn!("bootstrap: config vault publish failed: {e:#}"),
        }

        // Escrow the warm master seed into the `identity_secrets` vault
        // (D17, identity-rotation.md §3/§8): the warm key is RANDOM — the
        // paper phrase re-derives only the cold key, then reads the warm
        // seed back from this vault — so without the escrow a recovered
        // identity cannot sign anything. Same durable host + recipient
        // set as the config vault; `publish` is read-first idempotent
        // (no churn on a quiescent boot). Best-effort: never blocks
        // startup.
        {
            let recipients: Vec<String> = cfg_snapshot
                .key
                .values()
                .map(|k| k.public_key.clone())
                .collect();
            let escrow_store = cfg_snapshot
                .identity
                .bootstrap_store
                .as_deref()
                .and_then(|name| vault_blobs.get(name).cloned());
            match (escrow_store, recipients.is_empty()) {
                (Some(store), false) => {
                    let identity_files: Vec<String> = cfg_snapshot
                        .key
                        .values()
                        .filter_map(|k| k.identity_file.clone())
                        .collect();
                    let vault = crate::identity_secrets_vault::IdentitySecretsVault::new(
                        master_signing_key.clone(),
                        store,
                        reg.clone(),
                        recipients,
                        identity_files,
                    );
                    if let Err(e) = vault.publish(&master_signing_key.to_bytes()).await {
                        tracing::warn!("identity_secrets: warm-seed escrow publish failed: {e:#}");
                    }
                }
                _ => {
                    tracing::info!(
                        "identity_secrets: no durable bootstrap store or no [key.*] \
                         recipients — warm-seed escrow skipped (paper recovery of \
                         the warm master unavailable)"
                    );
                }
            }
        }

        let resolved = crate::membership::build_membership_state(
            &self_did,
            &cfg_snapshot,
            reg.as_ref(),
            &vault_blobs,
        )
        .await;
        {
            // Refresh the shared state read by the transport ACL hook.
            // Carry over `public_blob_hashes` accumulated so far (e.g.
            // by an earlier subscriber pass) and add this daemon's own
            // identity-bundle hash so the public-ALPN handler (S3b)
            // serves it without challenge.
            let mut s = membership_state.write().await;
            let mut public_blobs = std::mem::take(&mut s.public_blob_hashes);
            if let Some(h) = self_bundle_hash {
                public_blobs.insert(h);
            }
            *s = resolved;
            s.public_blob_hashes = public_blobs;
        }
    }

    // Automation coordinator: long-lived task that reconciles the live loop
    // set (watch + scheduled backups) from config, then re-reconciles whenever
    // something fires `automation_refresh` (PatchConfig adding/pausing/removing
    // a `[task.*]` automation). The initial reconcile happens inline so watch
    // loops are up before we start serving; the loop then rides refreshes.
    // Mirrors the membership coordinator below.
    // Clone the config out from under the read guard BEFORE reconcile: reconcile
    // awaits cancelled loops' joins, which can await an in-flight backup that
    // itself re-acquires `config.read()` — holding the guard across that await
    // would deadlock the (write-preferring) config lock against any concurrent
    // `patch_config`. A cheap snapshot decouples them.
    {
        let cfg = config.read().await.clone();
        automation_manager.reconcile(&cfg).await;
    }
    let automation_cancel = tokio_util::sync::CancellationToken::new();
    let automation_handle = {
        let manager = automation_manager.clone();
        let config = config.clone();
        let refresh = automation_refresh.clone();
        let cancel = automation_cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = refresh.notified() => {
                        // Snapshot config, then reconcile — never hold the read
                        // guard across reconcile's join awaits (see above).
                        let cfg = config.read().await.clone();
                        manager.reconcile(&cfg).await;
                    }
                }
            }
        })
    };

    // Membership coordinator: long-lived task that subscribes to
    // peers' identity-vault and shared-vault data keys, and respawns
    // its per-peer subscriptions whenever something fires
    // `membership_refresh` (Pair / PatchConfig / publish registering
    // a new vault_id). Best-effort: no registry → no coordinator.
    let subscribe_cancel = tokio_util::sync::CancellationToken::new();
    let subscribe_handle = if let Some(reg) = node.registry.as_ref() {
        // The iroh transport pubkey is already what `node.endpoint.id()`
        // returns — same bytes as `device_keyset.iroh_secret_key()
        // .verifying_key()` since we passed keyset.iroh to the builder
        // above. Use that here rather than re-deriving from a raw secret.
        let self_iroh_pubkey: [u8; 32] = node.endpoint.id().as_bytes().to_owned();
        let reg_dyn: Arc<dyn RegistryApi + Send + Sync> = reg.clone();
        let subscriber = Arc::new(crate::membership_subscribe::MembershipSubscriber {
            // The daemon's anchored DID (cold pubkey) — resolved from the
            // anchor entry above, never derived from a key in hand (D17).
            self_did,
            // Iroh transport pubkey — used only for the self-skip in
            // `spawn_peer_tasks` when iterating `authorized_iroh_pubkeys`.
            self_iroh_pubkey,
            config: config.clone(),
            registry: reg_dyn,
            stores: vault_blobs.clone(),
            state: membership_state.clone(),
            endpoint: node.endpoint.clone(),
            data_events: data_events.clone(),
            refresh: membership_refresh.clone(),
        });
        let cancel = subscribe_cancel.clone();
        Some(tokio::spawn(async move {
            subscriber.run_lifecycle(cancel).await;
        }))
    } else {
        None
    };

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

    // Clean up: cancel subscriptions, watch loops, drain staged writes,
    // shut down the router. Every step is DEADLINED: the
    // 2026-07-02 drill produced a daemon that acked shutdown and then hung
    // forever behind a wedged upload — the endpoint key and service.lock
    // stayed held, so the replacement daemon could not start either. A
    // bounded, honest exit beats a perfect one that never happens; anything
    // un-drained survives in the staging WAL and is recovered on next start.
    subscribe_cancel.cancel();
    if let Some(h) = subscribe_handle
        && tokio::time::timeout(std::time::Duration::from_secs(5), h)
            .await
            .is_err()
    {
        tracing::warn!("shutdown: membership subscriber did not stop within 5s; abandoning it");
    }
    automation_cancel.cancel();
    automation_handle.abort();
    automation_manager.shutdown().await;
    // Best-effort drain: give staged packs one bounded chance to reach
    // durability (drill fix, layer 4). NOT a correctness requirement —
    // every published HEAD is already behind a blob_sync barrier, and the
    // WAL replays un-flushed staging on next start — so a slow backend must
    // not turn shutdown into a hang; 45 s covers a trailing pack on a
    // realistic uplink, then we exit honestly.
    let drain = async {
        for (name, store) in &vault_blobs {
            if let Err(e) = store.blob_sync().await {
                tracing::warn!(
                    store = name.as_str(),
                    "shutdown drain: blob_sync failed: {e:#}"
                );
            }
        }
    };
    if tokio::time::timeout(std::time::Duration::from_secs(45), drain)
        .await
        .is_err()
    {
        tracing::warn!(
            "shutdown drain exceeded 45s — exiting anyway; staged data stays in the \
             staging WAL and is re-enqueued on next start"
        );
    }
    remove_lock();
    if let Some(control) = node.control.as_ref()
        && tokio::time::timeout(std::time::Duration::from_secs(5), control.router.shutdown())
            .await
            .is_err()
    {
        tracing::warn!("shutdown: control router did not stop within 5s; exiting anyway");
    }
    match tokio::time::timeout(std::time::Duration::from_secs(10), node.router.shutdown()).await {
        Ok(result) => result?,
        Err(_) => tracing::warn!("shutdown: iroh router did not stop within 10s; exiting anyway"),
    }
    Ok(())
}

/// Resolve and load the device keyset for boot. Always returns a
/// `DeviceKeyset` — when no on-disk path is configured, generates an
/// ephemeral random keyset in RAM and emits an info log so operators
/// know the DID will not survive restart.
///
/// Path priority:
/// 1. `[identity].keyset_file` explicitly set → load-or-generate at that
///    path.
/// 2. Unset but `[identity].secret_key_file` set → default to the
///    sibling `device_keyset.cbor.age`; load-or-generate.
/// 3. Otherwise → ephemeral random in-RAM keyset.
fn device_keyset_for_boot(
    identity: &s5_node_api::config::NodeConfigIdentity,
    keys: &std::collections::BTreeMap<String, s5_node_api::config::NodeConfigKey>,
    config_dir: Option<&Path>,
) -> crate::device_keyset::DeviceKeyset {
    let explicit = identity
        .keyset_file
        .as_deref()
        .map(std::path::PathBuf::from);
    let resolved =
        explicit.or_else(|| crate::device_keyset::default_keyset_path(identity, config_dir));
    match resolved {
        Some(path) => {
            match crate::device_keyset::load_or_generate_device_keyset(&path, keys, config_dir) {
                Ok(ks) => ks,
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        "identity: device keyset load/generate failed: {e:#} \
                         — using an ephemeral in-RAM keyset for this boot"
                    );
                    crate::device_keyset::DeviceKeyset::generate()
                }
            }
        }
        None => {
            tracing::info!(
                "identity: no on-disk keyset configured — using an ephemeral \
                 in-RAM keyset (DID will not survive restart). Configure \
                 [identity].secret_key_file or [identity].keyset_file (and \
                 ideally [key.main] for at-rest encryption) for persistence."
            );
            crate::device_keyset::DeviceKeyset::generate()
        }
    }
}

/// Generate an ephemeral random ed25519 master signing key. Used at
/// boot when no `master_key_file` path is resolvable — the daemon runs
/// with a non-persistent DID until the operator configures persistence.
fn ephemeral_master_signing_key() -> ed25519_dalek::SigningKey {
    use rand::Rng;
    let mut seed = [0u8; 32];
    rand::rng().fill_bytes(&mut seed);
    ed25519_dalek::SigningKey::from_bytes(&seed)
}

/// Writes the service lock file with the CONTROL endpoint's address and
/// per-run auth token (never the public endpoint — clients must not even
/// try to reach control RPC there).
fn write_service_lockfile(node: &S5Node) -> anyhow::Result<()> {
    let control = node
        .control
        .as_ref()
        .context("no control plane — service lock file not written")?;

    let lock = ServiceLock {
        endpoint_addr: control.endpoint.addr(),
        version: Some(s5_node_api::VERSION.to_string()),
        pid: Some(std::process::id()),
        control_token: Some(hex::encode(control.token)),
    };
    write_lock(&lock)?;

    info!(lock_path = %lock_path()?.display(), version = s5_node_api::VERSION, "service lock file written");
    Ok(())
}
