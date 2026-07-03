//! Shared, store-agnostic harness for full-flow E2E tests
//! (recovery, sharing, …). The modular seam is [`DurableBackend`]: a flow is
//! written against it once and runs against `MemoryStore`, `LocalStore`, or
//! (env-gated, added separately) a live Sia/indexd backend — no per-store
//! branching in the flow itself.
//!
//! Included via `mod common;` in each integration-test binary; not every
//! binary exercises every helper, so silence the per-binary dead-code lint.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Result, anyhow};
use s5_core::blob::{BlobStore, Blobs};
use s5_core::store::Store;
use s5_core::{Hash, RegistryApi};
use s5_node::config::{
    NodeConfigIdentity, NodeConfigKey, NodeConfigSource, NodeConfigVault, S5NodeConfig, TaskSpec,
};
use s5_node::tasks::{TaskExecutor, TaskExecutorContext};
use s5_node_api::TaskState;
use s5_registry::MemoryRegistry;
use s5_registry_store::StoreRegistry;
use s5_store_local::{LocalStore, LocalStoreConfig};
use s5_store_memory::MemoryStore;
use tempfile::TempDir;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Pluggable durable backend (the modular seam)
// ---------------------------------------------------------------------------

/// A durable store + registry a flow rides on. `open` may be called more than
/// once — once for the publishing device, once for a cold consumer — and MUST
/// return handles over the *same* durable state each time (a cold device
/// shares the durable CAS + registry, not the publisher's RAM).
pub trait DurableBackend {
    fn label(&self) -> &'static str;
    fn open(&self) -> (Arc<dyn Blobs>, Arc<dyn RegistryApi + Send + Sync>);
}

/// In-memory backend: a shared `MemoryStore` + `MemoryRegistry` handed out by
/// clone, so re-opening sees the same data. Fast path for CI.
pub struct MemoryBackend {
    store: Arc<MemoryStore>,
    registry: Arc<MemoryRegistry>,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self {
            store: Arc::new(MemoryStore::new()),
            registry: Arc::new(MemoryRegistry::new()),
        }
    }
}

impl DurableBackend for MemoryBackend {
    fn label(&self) -> &'static str {
        "memory"
    }
    fn open(&self) -> (Arc<dyn Blobs>, Arc<dyn RegistryApi + Send + Sync>) {
        let blobs: Arc<dyn Blobs> =
            Arc::new(BlobStore::from_arc(self.store.clone() as Arc<dyn Store>));
        (blobs, self.registry.clone())
    }
}

/// On-disk backend: a `LocalStore` over a temp dir + a `StoreRegistry` over the
/// same dir. Re-opening builds fresh handles bound to the same path, so it
/// models a cold device that shares only the on-disk durable state.
pub struct LocalBackend {
    dir: TempDir,
}

impl Default for LocalBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalBackend {
    pub fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
    }
}

impl DurableBackend for LocalBackend {
    fn label(&self) -> &'static str {
        "local"
    }
    fn open(&self) -> (Arc<dyn Blobs>, Arc<dyn RegistryApi + Send + Sync>) {
        let raw: Arc<dyn Store> = Arc::new(LocalStore::create(LocalStoreConfig {
            base_path: self.dir.path().to_string_lossy().into_owned(),
        }));
        let blobs: Arc<dyn Blobs> = Arc::new(BlobStore::from_arc(raw.clone()));
        let registry: Arc<dyn RegistryApi + Send + Sync> =
            Arc::new(StoreRegistry::new(raw, Some("registry".to_string())));
        (blobs, registry)
    }
}

// ---------------------------------------------------------------------------
// Corpus + verification
// ---------------------------------------------------------------------------

/// A file corpus authored on disk, remembered by relative path → BLAKE3, so a
/// restore can be verified byte-for-byte without re-reading the source.
pub struct Corpus {
    pub dir: TempDir,
    pub hashes: BTreeMap<String, Hash>,
}

impl Corpus {
    /// Author `n` files of varied size + nested dirs, deterministic content
    /// (seeded by index, no RNG — reproducible across runs).
    pub fn author(n: usize) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        std::fs::create_dir_all(dir.path().join("nested/deep"))?;
        let mut hashes = BTreeMap::new();
        for i in 0..n {
            let rel = if i % 3 == 0 {
                format!("nested/file_{i}.bin")
            } else if i % 5 == 0 {
                format!("nested/deep/file_{i}.bin")
            } else {
                format!("file_{i}.txt")
            };
            let len = 1 + (i * 37) % (24 * 1024);
            let byte = (i % 251) as u8;
            let content = vec![byte; len];
            std::fs::write(dir.path().join(&rel), &content)?;
            hashes.insert(rel, Hash::new(&content));
        }
        Ok(Self { dir, hashes })
    }

    pub fn source_path(&self) -> String {
        self.dir.path().to_string_lossy().into_owned()
    }

    /// Assert every authored file exists under `restored_root` with matching
    /// bytes; returns the count verified.
    pub fn verify_restored(&self, restored_root: &std::path::Path) -> Result<usize> {
        for (rel, expected) in &self.hashes {
            let path = restored_root.join(rel);
            let got = std::fs::read(&path)
                .map_err(|e| anyhow!("restored file missing: {}: {e}", path.display()))?;
            let got_hash = Hash::new(&got);
            if &got_hash != expected {
                return Err(anyhow!(
                    "byte mismatch for {rel}: expected {expected}, restored {got_hash}"
                ));
            }
        }
        Ok(self.hashes.len())
    }
}

// ---------------------------------------------------------------------------
// Config + executor helpers
// ---------------------------------------------------------------------------

/// A single-vault config keyed to a paper recipient (recovery key) + a device
/// recipient, with the durable backend named "durable".
#[allow(clippy::too_many_arguments)]
pub fn make_config(
    vault_root: &str,
    paper_recipient: &str,
    paper_identity_file: &str,
    device_recipient: &str,
    device_identity_file: &str,
    source_path: &str,
) -> S5NodeConfig {
    let mut key = BTreeMap::new();
    key.insert(
        "paper".to_string(),
        NodeConfigKey {
            public_key: paper_recipient.to_string(),
            identity_file: Some(paper_identity_file.to_string()),
        },
    );
    key.insert(
        "device".to_string(),
        NodeConfigKey {
            public_key: device_recipient.to_string(),
            identity_file: Some(device_identity_file.to_string()),
        },
    );

    let mut source = BTreeMap::new();
    source.insert(
        "docs".to_string(),
        NodeConfigSource {
            paths: vec![source_path.to_string()],
            include_caches: false,
            skip_hidden: false,
            respect_ignore_files: false,
            exclude: vec![],
            one_file_system: false,
            max_concurrent_ops: None,
            follow_symlinks: false,
            detect_deletions: false,
        },
    );

    let mut vault = BTreeMap::new();
    vault.insert(
        "backup".to_string(),
        NodeConfigVault {
            root_path: vault_root.to_string(),
            key: "device".to_string(),
            data_store: Some("durable".to_string()),
            recipients: vec!["device".to_string(), "paper".to_string()],
            sources: vec!["docs".to_string()],
            ..Default::default()
        },
    );

    S5NodeConfig {
        identity: NodeConfigIdentity {
            secret_key_file: None,
            secret_key: None,
            encrypted_with: None,
            master_key_file: None,
            anchor_entry_file: None,
            keyset_file: None,
            bootstrap_store: Some("durable".to_string()),
        },
        key,
        store: BTreeMap::new(),
        default_store: Some("durable".to_string()),
        registry: BTreeMap::new(),
        source,
        vault,
        task: BTreeMap::new(),
        friend: BTreeMap::new(),
    }
}

/// Build a task executor context over one durable store named "durable".
pub fn build_ctx(
    config: S5NodeConfig,
    blobs: Arc<dyn Blobs>,
    registry: Arc<dyn RegistryApi + Send + Sync>,
    node_secret: [u8; 32],
) -> Arc<TaskExecutorContext> {
    let mut stores: HashMap<String, Arc<dyn Blobs>> = HashMap::new();
    stores.insert("durable".to_string(), blobs);
    Arc::new(TaskExecutorContext {
        config: Arc::new(RwLock::new(config)),
        stores,
        node_secret,
        registry: Some(registry),
        membership: None,
        membership_refresh: None,
        discovery_seed: Default::default(),
    })
}

/// Spawn a task and await terminal state.
pub async fn run_task(executor: &TaskExecutor, spec: TaskSpec) -> Result<()> {
    let (id, _) = executor.spawn(spec).await?;
    let mut rx = executor
        .watch_status(id)
        .await
        .ok_or_else(|| anyhow!("task {id} vanished after spawn"))?;
    loop {
        let state = rx.borrow().state.clone();
        match state {
            TaskState::Completed => return Ok(()),
            TaskState::Failed { error } => return Err(anyhow!("task {id} failed: {error}")),
            TaskState::Cancelled => return Err(anyhow!("task {id} cancelled")),
            _ => rx
                .changed()
                .await
                .map_err(|_| anyhow!("task {id} status channel closed"))?,
        }
    }
}

/// An authored age identity written to `dir/<name>.txt`; returns (recipient,
/// identity-file-path).
pub fn age_identity(dir: &std::path::Path, name: &str) -> (String, String) {
    use age::secrecy::ExposeSecret;
    let id = age::x25519::Identity::generate();
    let recipient = id.to_public().to_string();
    let path = dir.join(format!("{name}.txt"));
    std::fs::write(&path, id.to_string().expose_secret()).unwrap();
    (recipient, path.to_string_lossy().into_owned())
}
