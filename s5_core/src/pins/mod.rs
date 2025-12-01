#[cfg(not(target_arch = "wasm32"))]
pub mod registry_pinner;

use crate::Hash;
use minicbor::{Decode, Encode};
use std::collections::HashSet;

/// API for pinning and unpinning blobs in a particular context.
///
/// `Pins` is intentionally small and is typically backed by a registry
/// implementation, such as `RegistryPinner`.
#[async_trait::async_trait]
pub trait Pins: Sync + Send + std::fmt::Debug {
    /// Adds a pin for the given `hash` in the specified `context`.
    async fn pin_hash(&self, hash: Hash, context: PinContext) -> anyhow::Result<()>;

    /// Removes a pin for the given `hash` in the specified `context`.
    /// Returns `true` if the blob is now orphaned (no more pinners).
    async fn unpin_hash(&self, hash: Hash, context: PinContext) -> anyhow::Result<bool>;

    /// Removes all pins for a given hash, regardless of context.
    ///
    /// This effectively "clears" the pin set for the hash in the underlying registry.
    async fn unpin_hash_all(&self, hash: Hash) -> anyhow::Result<()>;

    /// Returns the set of pin contexts currently associated with the given `hash`.
    async fn get_pinners(&self, hash: Hash) -> anyhow::Result<HashSet<PinContext>>;

    /// Returns true if the given `hash` is pinned in the specified `context`.
    async fn is_pinned(&self, hash: Hash, context: PinContext) -> anyhow::Result<bool>;
}

/// Describes why or by whom a blob is pinned.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Encode, Decode)]
#[non_exhaustive]
pub enum PinContext {
    /// Pin owned by a remote node whose blobs we are storing.
    #[n(0)]
    NodeId(#[n(0)] [u8; 32]),

    /// Pin for the live head of the single local FS5 root
    /// whose metadata and registry are stored in this registry DB.
    ///
    /// This context is **local-only** and never exposed over the
    /// network; it is used to keep the current root snapshot and
    /// its reachable blobs alive.
    #[n(1)]
    LocalFsHead,

    /// Pin for a retained snapshot of the local FS5 root.
    ///
    /// Snapshots are addressed by the BLAKE3 hash of the
    /// `root.fs5.cbor` bytes at the time the snapshot was taken.
    /// This hash also identifies the corresponding `DirV1` blob in
    /// the meta blob store.
    #[n(2)]
    LocalFsSnapshot {
        #[n(0)]
        root_hash: [u8; 32],
    },
}
