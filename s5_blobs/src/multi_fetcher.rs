//! Multi-source blob fetcher with fallback support.
//!
//! `MultiFetcher` provides a unified interface for fetching blobs from
//! multiple sources (local stores, remote nodes) with automatic fallback.
//!
//! ## Use Cases
//!
//! - **Server-side pinning**: Pull blobs from remote nodes for `RequestPull` RPC
//! - **Light client reads**: Fetch blobs from multiple remote sources with fallback
//! - **Sync operations**: Download blobs during FS synchronization
//!
//! ## Example
//!
//! ```ignore
//! let fetcher = MultiFetcher::new()
//!     .with_local(local_store)
//!     .with_remote(client_a, node_a_id)
//!     .with_remote(client_b, node_b_id);
//!
//! let bytes = fetcher.fetch(hash).await?;
//! ```

use bytes::Bytes;
use s5_core::{BlobStore, Hash};
use std::sync::Arc;

use crate::Client;

/// Result of a fetch operation.
#[derive(Debug)]
pub enum FetchResult {
    /// Successfully fetched the blob.
    Ok(Bytes),
    /// Blob confirmed not to exist in any source (all sources returned "not found").
    NotFound,
    /// Some sources returned errors. The blob may or may not exist.
    /// Contains all errors encountered. If some sources returned "not found" while
    /// others errored, this variant is used (not `NotFound`).
    AllFailed(Vec<FetchError>),
}

/// Error from a single fetch attempt.
#[derive(Debug)]
pub struct FetchError {
    pub source_name: String,
    pub reason: String,
}

/// A source that can provide blobs.
#[derive(Clone)]
pub enum BlobSource {
    /// A local blob store.
    Local { name: String, store: Arc<BlobStore> },
    /// A remote node accessed via RPC client.
    Remote {
        name: String,
        client: Client,
        #[allow(dead_code)]
        node_id: [u8; 32],
    },
    // Future: HTTP URL sources, etc.
}

impl std::fmt::Debug for BlobSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobSource::Local { name, .. } => write!(f, "Local({})", name),
            BlobSource::Remote { name, node_id, .. } => {
                write!(f, "Remote({}, {:?})", name, &node_id[..4])
            }
        }
    }
}

/// Multi-source blob fetcher with fallback support.
///
/// Tries sources in order until one succeeds. Useful for:
/// - Fetching from multiple remote nodes with fallback
/// - Preferring local cache over remote sources
/// - Handling unreliable sources gracefully
#[derive(Debug, Clone, Default)]
pub struct MultiFetcher {
    sources: Vec<BlobSource>,
}

impl MultiFetcher {
    /// Creates an empty MultiFetcher.
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    /// Adds a local blob store as a source.
    pub fn with_local(mut self, name: impl Into<String>, store: Arc<BlobStore>) -> Self {
        self.sources.push(BlobSource::Local {
            name: name.into(),
            store,
        });
        self
    }

    /// Adds a remote node as a source.
    pub fn with_remote(
        mut self,
        name: impl Into<String>,
        client: Client,
        node_id: [u8; 32],
    ) -> Self {
        self.sources.push(BlobSource::Remote {
            name: name.into(),
            client,
            node_id,
        });
        self
    }

    /// Adds a source directly.
    pub fn with_source(mut self, source: BlobSource) -> Self {
        self.sources.push(source);
        self
    }

    /// Returns the number of sources.
    pub fn len(&self) -> usize {
        self.sources.len()
    }

    /// Returns true if there are no sources.
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    /// Fetches a blob by hash, trying each source in order.
    ///
    /// Returns:
    /// - `FetchResult::Ok(bytes)` if any source successfully returned the blob
    /// - `FetchResult::NotFound` if all sources confirmed the blob doesn't exist (no errors)
    /// - `FetchResult::AllFailed(errors)` if any source returned an error (even if others said "not found")
    pub async fn fetch(&self, hash: Hash) -> FetchResult {
        let mut errors = Vec::new();

        for source in &self.sources {
            match self.try_fetch_from(source, hash).await {
                Ok(Some(bytes)) => return FetchResult::Ok(bytes),
                Ok(None) => {
                    // Source confirmed blob doesn't exist, continue to next
                }
                Err(e) => {
                    errors.push(e);
                }
            }
        }

        if errors.is_empty() {
            // All sources responded, none had errors, blob not found anywhere
            FetchResult::NotFound
        } else {
            // At least one source errored - we can't be certain the blob doesn't exist
            FetchResult::AllFailed(errors)
        }
    }

    /// Fetches a blob and stores it in the destination store.
    ///
    /// Useful for pulling blobs to local storage.
    pub async fn fetch_to_store(&self, hash: Hash, dest: &BlobStore) -> Result<(), String> {
        // Check if destination already has it
        if let Ok(true) = dest.contains(hash).await {
            return Ok(());
        }

        match self.fetch(hash).await {
            FetchResult::Ok(bytes) => {
                dest.import_bytes(bytes)
                    .await
                    .map_err(|e| format!("failed to store blob: {e}"))?;
                Ok(())
            }
            FetchResult::NotFound => Err("blob not found in any source".into()),
            FetchResult::AllFailed(errors) => {
                let msgs: Vec<_> = errors.iter().map(|e| e.reason.as_str()).collect();
                Err(format!("all sources failed: {}", msgs.join(", ")))
            }
        }
    }

    /// Checks if any source has the blob.
    pub async fn exists(&self, hash: Hash) -> bool {
        for source in &self.sources {
            if self.check_exists(source, hash).await {
                return true;
            }
        }
        false
    }

    /// Checks if any source has the blob, using blinded hash for privacy.
    ///
    /// Returns the actual hash if found.
    pub async fn exists_blinded(&self, blinded_hash: [u8; 32]) -> Option<Hash> {
        for source in &self.sources {
            if let Some(actual) = self.check_exists_blinded(source, blinded_hash).await {
                return Some(actual);
            }
        }
        None
    }

    // --- Internal helpers ---

    async fn try_fetch_from(
        &self,
        source: &BlobSource,
        hash: Hash,
    ) -> Result<Option<Bytes>, FetchError> {
        match source {
            BlobSource::Local { name, store } => {
                match store.contains(hash).await {
                    Ok(true) => {}
                    Ok(false) => return Ok(None),
                    Err(e) => {
                        return Err(FetchError {
                            source_name: name.clone(),
                            reason: format!("contains check failed: {e}"),
                        });
                    }
                }

                match store.read_as_bytes(hash, 0, None).await {
                    Ok(bytes) => Ok(Some(bytes)),
                    Err(e) => Err(FetchError {
                        source_name: name.clone(),
                        reason: format!("read failed: {e}"),
                    }),
                }
            }
            BlobSource::Remote { name, client, .. } => {
                // First check if it exists
                let query_result = client
                    .query(hash, std::collections::BTreeSet::new())
                    .await
                    .map_err(|e| FetchError {
                        source_name: name.clone(),
                        reason: format!("query failed: {e}"),
                    })?;

                if !query_result.exists {
                    return Ok(None);
                }

                // Download the blob
                match client.download_bytes(hash, 0, None).await {
                    Ok(bytes) => Ok(Some(bytes)),
                    Err(e) => Err(FetchError {
                        source_name: name.clone(),
                        reason: format!("download failed: {e}"),
                    }),
                }
            }
        }
    }

    async fn check_exists(&self, source: &BlobSource, hash: Hash) -> bool {
        match source {
            BlobSource::Local { store, .. } => store.contains(hash).await.unwrap_or(false),
            BlobSource::Remote { client, .. } => client
                .query(hash, std::collections::BTreeSet::new())
                .await
                .map(|r| r.exists)
                .unwrap_or(false),
        }
    }

    async fn check_exists_blinded(
        &self,
        source: &BlobSource,
        blinded_hash: [u8; 32],
    ) -> Option<Hash> {
        match source {
            BlobSource::Local { store, .. } => {
                // For local stores, we need to iterate and check all hashes.
                // This is expensive but provides the same privacy guarantee.
                // Only available on native (list_hashes is native-only).
                #[cfg(not(target_arch = "wasm32"))]
                {
                    let hashes = store.list_hashes().await.ok()?;
                    for hash in hashes {
                        let computed = blake3::hash(hash.as_bytes());
                        if computed.as_bytes() == &blinded_hash {
                            return Some(hash);
                        }
                    }
                    None
                }
                #[cfg(target_arch = "wasm32")]
                {
                    // list_hashes not available on WASM; blinded local lookups unsupported
                    let _ = store;
                    None
                }
            }
            BlobSource::Remote { client, .. } => client
                .query_blinded(blinded_hash, std::collections::BTreeSet::new())
                .await
                .ok()
                .and_then(|r| {
                    if r.exists {
                        r.actual_hash.map(Hash::from)
                    } else {
                        None
                    }
                }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fetcher_builder() {
        let fetcher = MultiFetcher::new();
        assert!(fetcher.is_empty());
        assert_eq!(fetcher.len(), 0);
    }
}
