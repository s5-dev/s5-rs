//! iroh-based registry protocol for S5.
//!
//! This crate exposes a small RPC protocol and helpers
//! for accessing an [`s5_core::RegistryApi`] over an
//! iroh transport:
//!
//! - [`RegistryServer`]: serves any [`s5_core::RegistryApi`] implementation
//!   over an [`iroh::Endpoint`].
//! - [`Client`]: RPC client for the registry protocol.
//! - [`RemoteRegistry`]: adapter that implements
//!   [`s5_core::RegistryApi`] on top of [`Client`].
//!
//! The wire format is defined by [`RpcProto`] and the
//! ALPN identifier is [`ALPN`].

use std::{fmt, sync::Arc};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use iroh::{
    endpoint::{Connection, Endpoint},
    protocol::{AcceptError, ProtocolHandler},
};
use irpc::{Client as IrpcClient, channel::oneshot, rpc_requests};
use irpc_iroh::{IrohRemoteConnection, read_request};

use s5_core::{RegistryApi, StreamKey, StreamMessage};
use serde::{Deserialize, Serialize};
use tracing::warn;

pub const ALPN: &[u8] = b"s5/registry/0";

#[derive(Debug, Serialize, Deserialize)]
pub struct GetRequest {
    pub key_type: u8,
    pub key_data: [u8; 32],
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetResponse {
    pub message: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetRequest {
    pub message: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub key_type: u8,
    pub key_data: [u8; 32],
}

#[derive(Debug, Serialize, Deserialize)]
#[rpc_requests(message = RegistryRpcMessage)]
pub enum RpcProto {
    #[rpc(tx = oneshot::Sender<GetResponse>)]
    Get(GetRequest),
    #[rpc(tx = oneshot::Sender<Result<(), String>>)]
    Set(SetRequest),
    #[rpc(tx = oneshot::Sender<Result<(), String>>)]
    Delete(DeleteRequest),
}

/// Server that exposes a [`RegistryApi`] implementation over iroh.
///
/// Generic over any `R: RegistryApi`, allowing use with different
/// registry backends (e.g., `RedbRegistry` from `s5_registry_redb`,
/// `MemoryRegistry`, `StoreRegistry`, or custom implementations).
#[derive(Clone)]
pub struct RegistryServer<R> {
    registry: Arc<R>,
}

impl<R> fmt::Debug for RegistryServer<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryServer").finish()
    }
}

impl<R: RegistryApi + Send + Sync + 'static> RegistryServer<R> {
    pub fn new(registry: R) -> Self {
        Self {
            registry: Arc::new(registry),
        }
    }

    async fn handle_get(&self, req: GetRequest) -> GetResponse {
        let key = match StreamKey::from_bytes(req.key_type, &req.key_data) {
            Ok(key) => key,
            Err(err) => {
                warn!("registry get: invalid key: {err}");
                return GetResponse { message: None };
            }
        };

        match self.registry.get(&key).await {
            Ok(Some(message)) => GetResponse {
                message: Some(message.serialize().to_vec()),
            },
            Ok(None) => GetResponse { message: None },
            Err(err) => {
                warn!("registry get error: {err}");
                GetResponse { message: None }
            }
        }
    }

    async fn handle_set(&self, req: SetRequest) -> std::result::Result<(), String> {
        let message =
            StreamMessage::deserialize(Bytes::from(req.message)).map_err(|err| err.to_string())?;

        self.registry
            .set(message)
            .await
            .map_err(|err| err.to_string())
    }

    async fn handle_delete(&self, req: DeleteRequest) -> std::result::Result<(), String> {
        let key =
            StreamKey::from_bytes(req.key_type, &req.key_data).map_err(|err| err.to_string())?;

        self.registry
            .delete(&key)
            .await
            .map_err(|err| err.to_string())
    }
}

impl<R: RegistryApi + Send + Sync + 'static> ProtocolHandler for RegistryServer<R> {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        while let Some(msg) = read_request::<RpcProto>(&conn).await? {
            match msg {
                RegistryRpcMessage::Get(irpc::WithChannels { inner, tx, .. }) => {
                    let resp = self.handle_get(inner).await;
                    let _ = tx.send(resp).await;
                }
                RegistryRpcMessage::Set(irpc::WithChannels { inner, tx, .. }) => {
                    let result = self.handle_set(inner).await;
                    let _ = tx.send(result).await;
                }
                RegistryRpcMessage::Delete(irpc::WithChannels { inner, tx, .. }) => {
                    let result = self.handle_delete(inner).await;
                    let _ = tx.send(result).await;
                }
            }
        }
        conn.closed().await;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    inner: IrpcClient<RpcProto>,
}

impl Client {
    pub fn connect(endpoint: Endpoint, addr: impl Into<iroh::EndpointAddr>) -> Self {
        let conn = IrohRemoteConnection::new(endpoint, addr.into(), ALPN.to_vec());
        Client {
            inner: IrpcClient::boxed(conn),
        }
    }

    pub async fn get(&self, key: StreamKey) -> Result<Option<StreamMessage>> {
        let (key_type, key_bytes) = key.to_bytes();
        let mut key_data = [0u8; 32];
        key_data.copy_from_slice(key_bytes);

        let response = self.inner.rpc(GetRequest { key_type, key_data }).await?;

        if let Some(bytes) = response.message {
            let message = StreamMessage::deserialize(Bytes::from(bytes))
                .map_err(|err| anyhow!("failed to deserialize registry message: {err}"))?;
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }

    pub async fn set(&self, message: StreamMessage) -> Result<()> {
        let bytes = message.serialize();
        match self
            .inner
            .rpc(SetRequest {
                message: bytes.to_vec(),
            })
            .await?
        {
            Ok(()) => Ok(()),
            Err(err) => Err(anyhow!(err.to_string())),
        }
    }

    pub async fn delete(&self, key: StreamKey) -> Result<()> {
        let (key_type, key_bytes) = key.to_bytes();
        let mut key_data = [0u8; 32];
        key_data.copy_from_slice(key_bytes);

        match self.inner.rpc(DeleteRequest { key_type, key_data }).await? {
            Ok(()) => Ok(()),
            Err(err) => Err(anyhow!(err.to_string())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RemoteRegistry {
    client: Client,
}

impl RemoteRegistry {
    pub fn connect(endpoint: Endpoint, addr: impl Into<iroh::EndpointAddr>) -> Self {
        Self {
            client: Client::connect(endpoint, addr),
        }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }
}

#[async_trait::async_trait]
impl RegistryApi for RemoteRegistry {
    async fn get(&self, key: &StreamKey) -> Result<Option<StreamMessage>> {
        self.client.get(*key).await
    }

    async fn set(&self, message: StreamMessage) -> Result<()> {
        self.client.set(message).await
    }

    async fn delete(&self, key: &StreamKey) -> Result<()> {
        self.client.delete(*key).await
    }
}

// ============================================================================
// In-Memory Registry
// ============================================================================

use std::collections::HashMap;
use std::sync::RwLock;

/// In-memory registry implementation.
///
/// Useful for testing and as a local cache layer. Respects `should_store`
/// semantics to avoid regressing to older revisions.
#[derive(Debug, Default)]
pub struct MemoryRegistry {
    data: RwLock<HashMap<StreamKey, StreamMessage>>,
}

impl MemoryRegistry {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl RegistryApi for MemoryRegistry {
    async fn get(&self, key: &StreamKey) -> Result<Option<StreamMessage>> {
        let data = self.data.read().unwrap();
        Ok(data.get(key).cloned())
    }

    async fn set(&self, message: StreamMessage) -> Result<()> {
        let mut data = self.data.write().unwrap();
        let existing = data.get(&message.key);
        if message.should_store(existing) {
            data.insert(message.key, message);
        }
        Ok(())
    }

    async fn delete(&self, key: &StreamKey) -> Result<()> {
        let mut data = self.data.write().unwrap();
        data.remove(key);
        Ok(())
    }
}

// ============================================================================
// Tee Registry (writes to both local and remote)
// ============================================================================

/// A registry that writes to two underlying registries (local and remote).
///
/// ## Behavior
/// - **Reads**: Try local first, then fall back to remote
/// - **Writes**: Go to both local and remote
/// - **Delete**: Deletes from both
#[derive(Clone)]
pub struct TeeRegistry {
    local: Arc<dyn RegistryApi + Send + Sync>,
    remote: Arc<dyn RegistryApi + Send + Sync>,
}

impl fmt::Debug for TeeRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TeeRegistry").finish_non_exhaustive()
    }
}

impl TeeRegistry {
    pub fn new(
        local: Arc<dyn RegistryApi + Send + Sync>,
        remote: Arc<dyn RegistryApi + Send + Sync>,
    ) -> Self {
        Self { local, remote }
    }

    /// Get a reference to the local registry.
    pub fn local(&self) -> &Arc<dyn RegistryApi + Send + Sync> {
        &self.local
    }

    /// Get a reference to the remote registry.
    pub fn remote(&self) -> &Arc<dyn RegistryApi + Send + Sync> {
        &self.remote
    }
}

#[async_trait::async_trait]
impl RegistryApi for TeeRegistry {
    async fn get(&self, key: &StreamKey) -> Result<Option<StreamMessage>> {
        // Try local first
        if let Some(msg) = self.local.get(key).await? {
            return Ok(Some(msg));
        }
        // Fallback to remote
        self.remote.get(key).await
    }

    async fn set(&self, message: StreamMessage) -> Result<()> {
        // Write to both (sequentially for backward compat)
        self.local.set(message.clone()).await?;
        self.remote.set(message).await?;
        Ok(())
    }

    async fn delete(&self, key: &StreamKey) -> Result<()> {
        self.local.delete(key).await?;
        self.remote.delete(key).await?;
        Ok(())
    }
}

// ============================================================================
// Multi Registry (fan-out writes to N backends)
// ============================================================================

/// Write policy for multi-backend operations.
#[derive(Debug, Clone, Copy, Default)]
pub enum WritePolicy {
    /// All backends must succeed (fail-fast on first error).
    #[default]
    All,
    /// At least one backend must succeed.
    Any,
    /// At least N backends must succeed.
    Quorum(usize),
}

/// A registry that fans out writes to multiple backends in parallel.
///
/// ## Behavior
/// - **Reads**: Try each backend in order until one returns a value
/// - **Writes**: Fan out to all backends in parallel, respecting write policy
/// - **Delete**: Fan out to all backends in parallel
///
/// ## Use Case
/// Publishing to multiple remote registries for availability:
/// ```ignore
/// let multi = MultiRegistry::new(vec![
///     Arc::new(local_registry),
///     Arc::new(remote_a),
///     Arc::new(remote_b),
/// ]);
/// ```
#[derive(Clone)]
pub struct MultiRegistry {
    backends: Vec<Arc<dyn RegistryApi + Send + Sync>>,
    write_policy: WritePolicy,
}

impl fmt::Debug for MultiRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiRegistry")
            .field("backends", &self.backends.len())
            .field("write_policy", &self.write_policy)
            .finish()
    }
}

impl MultiRegistry {
    /// Creates a new MultiRegistry with default write policy (All).
    pub fn new(backends: Vec<Arc<dyn RegistryApi + Send + Sync>>) -> Self {
        Self {
            backends,
            write_policy: WritePolicy::All,
        }
    }

    /// Creates a new MultiRegistry with a specific write policy.
    pub fn with_policy(
        backends: Vec<Arc<dyn RegistryApi + Send + Sync>>,
        write_policy: WritePolicy,
    ) -> Self {
        Self {
            backends,
            write_policy,
        }
    }

    /// Returns the number of backends.
    pub fn len(&self) -> usize {
        self.backends.len()
    }

    /// Returns true if there are no backends.
    pub fn is_empty(&self) -> bool {
        self.backends.is_empty()
    }

    /// Returns a reference to the backends.
    pub fn backends(&self) -> &[Arc<dyn RegistryApi + Send + Sync>] {
        &self.backends
    }
}

#[async_trait::async_trait]
impl RegistryApi for MultiRegistry {
    async fn get(&self, key: &StreamKey) -> Result<Option<StreamMessage>> {
        // Try each backend in order until one returns a value
        for backend in &self.backends {
            match backend.get(key).await {
                Ok(Some(msg)) => return Ok(Some(msg)),
                Ok(None) => continue,
                Err(e) => {
                    // Log but continue to next backend
                    tracing::debug!("MultiRegistry get from backend failed: {e}");
                    continue;
                }
            }
        }
        Ok(None)
    }

    async fn set(&self, message: StreamMessage) -> Result<()> {
        if self.backends.is_empty() {
            return Ok(());
        }

        // Fan out writes in parallel
        let futures: Vec<_> = self
            .backends
            .iter()
            .map(|b| {
                let msg = message.clone();
                let backend = b.clone();
                async move { backend.set(msg).await }
            })
            .collect();

        let results: Vec<Result<()>> = futures::future::join_all(futures).await;

        match self.write_policy {
            WritePolicy::All => {
                // All must succeed
                for result in results {
                    result?;
                }
                Ok(())
            }
            WritePolicy::Any => {
                // At least one must succeed
                let success_count = results.iter().filter(|r| r.is_ok()).count();
                if success_count > 0 {
                    Ok(())
                } else {
                    // Return the last error
                    results.into_iter().last().unwrap_or(Ok(()))
                }
            }
            WritePolicy::Quorum(n) => {
                let success_count = results.iter().filter(|r| r.is_ok()).count();
                if success_count >= n {
                    Ok(())
                } else {
                    Err(anyhow!(
                        "quorum not met: {} succeeded, {} required",
                        success_count,
                        n
                    ))
                }
            }
        }
    }

    async fn delete(&self, key: &StreamKey) -> Result<()> {
        if self.backends.is_empty() {
            return Ok(());
        }

        // Fan out deletes in parallel
        let futures: Vec<_> = self
            .backends
            .iter()
            .map(|b| {
                let k = *key;
                let backend = b.clone();
                async move { backend.delete(&k).await }
            })
            .collect();

        let results: Vec<Result<()>> = futures::future::join_all(futures).await;

        // For deletes, we use the same write policy
        match self.write_policy {
            WritePolicy::All => {
                for result in results {
                    result?;
                }
                Ok(())
            }
            WritePolicy::Any => {
                if results.iter().any(|r| r.is_ok()) {
                    Ok(())
                } else {
                    results.into_iter().last().unwrap_or(Ok(()))
                }
            }
            WritePolicy::Quorum(n) => {
                let success_count = results.iter().filter(|r| r.is_ok()).count();
                if success_count >= n {
                    Ok(())
                } else {
                    Err(anyhow!(
                        "quorum not met for delete: {} succeeded, {} required",
                        success_count,
                        n
                    ))
                }
            }
        }
    }
}
