pub mod registry_pinner;

use crate::Hash;
use minicbor::{Decode, Encode};
use std::collections::HashSet;

/// API for pinning and unpinning blobs in a particular context.
///
/// `Pins` is intentionally small and is typically backed by a registry
/// implementation, such as `RegistryPinner`.
#[async_trait::async_trait]
pub trait Pins: Sync + Send {
    /// Adds a pin for the given `hash` in the specified `context`.
    async fn pin_hash(&self, hash: Hash, context: PinContext) -> anyhow::Result<()>;

    /// Removes a pin for the given `hash` in the specified `context`.
    async fn unpin_hash(&self, hash: Hash, context: PinContext) -> anyhow::Result<()>;

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
    #[n(0)]
    NodeId(#[n(0)] [u8; 32]),
}
