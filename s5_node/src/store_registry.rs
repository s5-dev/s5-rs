//! The unified per-node store registry (D15).
//!
//! `run_node` used to thread TWO parallel maps keyed by `[store.*]` name —
//! a path-view `HashMap<String, BlobStore>` (blobs server + cold-GC) and a
//! vault-facing `HashMap<String, Arc<dyn Blobs>>` (everything else).
//! Membership in one map but not the other silently disabled features — the
//! D5 bug class (config-vault and identity-bundle publish silently skipped
//! on Sia). This module replaces the pair with ONE registry: every
//! configured backend has exactly one entry, and consumers request the
//! *view* they need:
//!
//! * capability traits ([`Blobs`] / [`BlobsRead`] / [`BlobsWrite`]) — the
//!   default currency. Every backend is present, including the
//!   content-addressed Sia `PackingStore`.
//! * the path-[`BlobStore`] view — requested EXPLICITLY, and only where
//!   path semantics genuinely exist: the blobs server (`provide`,
//!   store-relative paths) and the cold-store GC (`modified` mtime grace
//!   gate). Content-addressed backends return `None` here, meaning "this
//!   feature does not apply", never "store missing".
//!
//! Decided as D15, finishing the D5 trait migration.

use std::collections::HashMap;
use std::sync::Arc;

use s5_core::RegistryApi;
use s5_core::blob::{BlobStore, Blobs, BlobsRead, BlobsWrite};
use s5_core::store::Store;

use crate::CreatedStore;

/// One configured `[store.*]` backend, held once as its capability handles.
struct StoreEntry {
    /// Vault-facing content-addressed handle (read + write + delete by
    /// hash). Always present: a path backend rides in as its `BlobStore`,
    /// indexd as its `PackingStore`.
    blobs: Arc<dyn Blobs>,
    /// The raw path `Store` plus the per-store `outboard` config flag, kept
    /// so a path-`BlobStore` view can be handed out on explicit request.
    /// `None` for content-addressed backends (the Sia `PackingStore`).
    path: Option<(Arc<dyn Store>, bool)>,
    /// Native registry handle for backends that back one cheaply (indexd:
    /// metadata pointers sharing the store's connection + cache).
    registry: Option<Arc<dyn RegistryApi + Send + Sync>>,
}

/// The node's store registry: ONE map keyed by `[store.*]` name, exposing
/// capability-trait views, with the path view available only on explicit
/// request (D15).
#[derive(Default)]
pub struct NodeStores {
    entries: HashMap<String, StoreEntry>,
}

impl NodeStores {
    /// Register a built store under its `[store.*]` name. `outboard` is the
    /// per-store config flag any path-`BlobStore` view is created with.
    pub fn insert(&mut self, name: String, created: CreatedStore, outboard: bool) {
        self.entries.insert(
            name,
            StoreEntry {
                blobs: created.blobs,
                path: created.store.map(|s| (s, outboard)),
                registry: created.registry,
            },
        );
    }

    /// The vault-facing `dyn Blobs` view (read + write + delete by hash).
    /// Present for every configured backend.
    pub fn blobs(&self, name: &str) -> Option<Arc<dyn Blobs>> {
        self.entries.get(name).map(|e| e.blobs.clone())
    }

    /// Read-only capability view. Prefer this for signatures that only read.
    pub fn blobs_read(&self, name: &str) -> Option<Arc<dyn BlobsRead>> {
        self.blobs(name).map(|b| {
            let read: Arc<dyn BlobsRead> = b;
            read
        })
    }

    /// Write-only capability view. Prefer this for signatures that only write.
    pub fn blobs_write(&self, name: &str) -> Option<Arc<dyn BlobsWrite>> {
        self.blobs(name).map(|b| {
            let write: Arc<dyn BlobsWrite> = b;
            write
        })
    }

    /// Every store as its `dyn Blobs` view — the map currency the task
    /// executor / membership / identity / config-vault plumbing operate on.
    pub fn blobs_map(&self) -> HashMap<String, Arc<dyn Blobs>> {
        self.entries
            .iter()
            .map(|(name, e)| (name.clone(), e.blobs.clone()))
            .collect()
    }

    /// EXPLICIT path-view request (D15): the `BlobStore` over the raw path
    /// `Store`, for the sites with genuine path semantics (blobs-server
    /// `provide`, cold-GC `modified`). `None` for content-addressed
    /// backends — "this feature does not apply here", not "store missing";
    /// use [`Self::blobs`] for content access.
    pub fn path_store(&self, name: &str) -> Option<BlobStore> {
        self.entries.get(name).and_then(|e| {
            e.path.as_ref().map(|(store, outboard)| {
                BlobStore::from_arc_with_outboard(store.clone(), *outboard)
            })
        })
    }

    /// Every path-backed store as its `BlobStore` view — the blobs-server
    /// `provide` map. Content-addressed backends are absent by design (the
    /// server doesn't serve packed Sia blobs).
    pub fn path_stores(&self) -> HashMap<String, BlobStore> {
        self.entries
            .iter()
            .filter_map(|(name, e)| {
                e.path.as_ref().map(|(store, outboard)| {
                    (
                        name.clone(),
                        BlobStore::from_arc_with_outboard(store.clone(), *outboard),
                    )
                })
            })
            .collect()
    }

    /// The raw path `Store` handle — construction plumbing only:
    /// `Store`-backed registries (named-object writes are genuine path
    /// semantics).
    pub fn raw_store(&self, name: &str) -> Option<Arc<dyn Store>> {
        self.entries
            .get(name)
            .and_then(|e| e.path.as_ref().map(|(store, _)| store.clone()))
    }

    /// Every raw path `Store`, keyed by `[store.*]` name.
    pub fn raw_stores(&self) -> HashMap<String, Arc<dyn Store>> {
        self.entries
            .iter()
            .filter_map(|(name, e)| {
                e.path
                    .as_ref()
                    .map(|(store, _)| (name.clone(), store.clone()))
            })
            .collect()
    }

    /// The store's native registry handle, when the backend surfaces one
    /// (indexd: cheap metadata-pointer HEADs, rebuilt by the same
    /// reconstruct pass as blobs). Preferred by `create_registry` over
    /// wrapping the raw store in a generic `StoreRegistry`.
    pub fn native_registry(&self, name: &str) -> Option<Arc<dyn RegistryApi + Send + Sync>> {
        self.entries.get(name).and_then(|e| e.registry.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s5_store_memory::MemoryStore;

    /// A path-backed entry (`store: Some`) exposes every view; a
    /// content-addressed entry (`store: None`) exposes the trait views but
    /// NO path view — the D15 contract callers rely on.
    #[test]
    fn views_reflect_backend_capabilities() {
        let mut reg = NodeStores::default();

        let raw: Arc<dyn Store> = Arc::new(MemoryStore::new());
        let path_backed = CreatedStore {
            store: Some(raw.clone()),
            blobs: Arc::new(BlobStore::from_arc_with_outboard(raw, false)),
            registry: None,
        };
        reg.insert("local".to_string(), path_backed, false);

        let content_addressed = CreatedStore {
            store: None,
            blobs: Arc::new(BlobStore::without_outboard(MemoryStore::new())),
            registry: None,
        };
        reg.insert("sia".to_string(), content_addressed, false);

        // Trait views: every backend present.
        assert!(reg.blobs("local").is_some());
        assert!(reg.blobs("sia").is_some());
        assert!(reg.blobs_read("sia").is_some());
        assert!(reg.blobs_write("sia").is_some());
        assert_eq!(reg.blobs_map().len(), 2);

        // Path views: only the path-backed store.
        assert!(reg.path_store("local").is_some());
        assert!(reg.path_store("sia").is_none());
        assert_eq!(reg.path_stores().len(), 1);
        assert_eq!(reg.raw_stores().len(), 1);
        assert!(reg.raw_store("sia").is_none());

        // Unknown names are None everywhere, never a panic.
        assert!(reg.blobs("nope").is_none());
        assert!(reg.path_store("nope").is_none());
        assert!(reg.native_registry("nope").is_none());
    }
}
