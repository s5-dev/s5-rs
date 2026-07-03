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
use irpc::{
    Client as IrpcClient,
    channel::{mpsc, oneshot},
    rpc_requests,
};
use irpc_iroh::{IrohLazyRemoteConnection, read_request};

use async_trait::async_trait;
use s5_core::{RegistryApi, StreamKey, StreamMessage};
use serde::{Deserialize, Serialize};
use tracing::warn;

/// ALPN bumped to `s5/registry/1` for the v3 wire format change
/// (`StreamKey::Vault` adds a 16-byte `vault_id` after the pubkey, so
/// keys are no longer fixed at 32 bytes).
pub const ALPN: &[u8] = b"s5/registry/1";

// TODO(audit): keys here are sent as raw `Vec<u8>` (the byte form
// returned by `StreamKey::storage_key()`) because StreamKey is
// variable-length. That pushes encode/decode to every caller and
// hides what the field actually is. A `#[serde(with = "…")]`
// adapter or a wire-only `StreamKeyBytes` newtype would let the
// field be typed `StreamKey` while still serialising as bytes.
// Cosmetic; not behavior.
#[derive(Debug, Serialize, Deserialize)]
pub struct GetRequest {
    /// Full storage key: `[type_tag, key_bytes...]` — produced by
    /// `StreamKey::storage_key()`. Variable length to accommodate
    /// `Vault` entries (49 bytes: 1 + 32 + 16) alongside legacy 33-byte
    /// keys for `Local` / `Blake3HashPin`.
    pub key: Vec<u8>,
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
    /// Full storage key — see `GetRequest::key`.
    pub key: Vec<u8>,
}

/// Subscribe to live updates on a fixed set of keys.
///
/// On open, the server replays the *current* value of each requested
/// key as a `RegistryEvent::Initial` (one event per key, including
/// keys with no entry — `message: None`). After that, the server
/// streams every subsequent SET / DELETE that lands on any of the
/// subscribed keys.
///
/// Push-based replication is the right shape here because the total
/// registry-entry cardinality is small — one entry per vault per
/// device + one per identity (typically tens). Polling for changes
/// would either be wasteful (frequent polls) or laggy (rare polls);
/// fanning every SET to all interested parties is cheap and keeps
/// the system reliable even after disconnect/reconnect cycles.
#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeRequest {
    /// Full storage keys to subscribe to. Each entry is the byte
    /// form returned by `StreamKey::storage_key()`.
    pub keys: Vec<Vec<u8>>,
}

/// Event delivered over the Subscribe stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegistryEvent {
    /// Initial value for one of the subscribed keys, sent once at
    /// subscription start. `message: None` indicates no entry yet.
    Initial {
        /// Storage key bytes (matches one of the request's `keys`).
        key: Vec<u8>,
        /// Serialised `StreamMessage`, or `None` if no entry.
        message: Option<Vec<u8>>,
    },
    /// A SET landed on one of the subscribed keys after the initial
    /// catch-up. The message body is the serialised `StreamMessage`
    /// — clients deserialise to extract the new value.
    Set {
        /// Storage key bytes.
        key: Vec<u8>,
        /// Serialised `StreamMessage`.
        message: Vec<u8>,
    },
    /// A DELETE landed on one of the subscribed keys.
    Delete {
        /// Storage key bytes.
        key: Vec<u8>,
    },
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
    #[rpc(tx = mpsc::Sender<RegistryEvent>)]
    Subscribe(SubscribeRequest),
}

/// Server that exposes a [`BroadcastingRegistry`] over iroh.
///
/// Wraps a `BroadcastingRegistry` so every `set`/`delete` — whether
/// it comes from a remote RPC or a local writer in the same daemon —
/// fires the same broadcast channel that `Subscribe` handlers listen
/// on. The server itself owns no broadcast channel: live event fanout
/// lives in the wrapped registry, not split across two code paths.
#[derive(Clone)]
pub struct RegistryServer {
    registry: Arc<BroadcastingRegistry>,
    acl: Option<Arc<dyn RegistryAcl>>,
}

/// Event broadcast on every successful set/delete on a
/// [`BroadcastingRegistry`]. `Subscribe` handlers fan these out to
/// interested clients (filtered by key). Public so callers can build
/// their own subscribers on the wrapper directly if needed.
#[derive(Debug, Clone)]
pub enum RegistryChange {
    Set {
        key_bytes: Vec<u8>,
        message_bytes: Vec<u8>,
    },
    Delete {
        key_bytes: Vec<u8>,
    },
}

impl fmt::Debug for RegistryServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryServer").finish()
    }
}

/// Wraps any [`RegistryApi`] with a broadcast channel that fires on
/// every successful `set` / `delete`. Plumbing this wrapper as the
/// single registry handle ensures that *every* writer in the daemon —
/// the RPC server, the publish task, identity bundle publishing,
/// inbound subscription event-apply — feeds the same fanout that
/// remote subscribers listen on. Without this wrapper the publish
/// task wrote directly to the inner registry and live subscribers
/// silently missed updates.
pub struct BroadcastingRegistry {
    inner: Arc<dyn RegistryApi + Send + Sync>,
    events: tokio::sync::broadcast::Sender<RegistryChange>,
}

impl BroadcastingRegistry {
    /// Wrap an existing registry. Capacity 256: well above the typical
    /// total entry cardinality (~tens) so we never lag in practice.
    pub fn wrap(inner: Arc<dyn RegistryApi + Send + Sync>) -> Arc<Self> {
        let (events, _) = tokio::sync::broadcast::channel(256);
        Arc::new(Self { inner, events })
    }

    /// Subscribe to live SET/DELETE events. Returned receiver yields
    /// `RegistryChange::{Set, Delete}` for every successful write that
    /// goes through this wrapper.
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<RegistryChange> {
        self.events.subscribe()
    }
}

impl fmt::Debug for BroadcastingRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BroadcastingRegistry").finish()
    }
}

#[async_trait]
impl RegistryApi for BroadcastingRegistry {
    async fn get(&self, key: &StreamKey) -> Result<Option<StreamMessage>> {
        self.inner.get(key).await
    }

    async fn set(&self, message: StreamMessage) -> Result<()> {
        let key_bytes = message.key.storage_key();
        let message_bytes = message.serialize().to_vec();
        self.inner.set(message).await?;
        // Best-effort broadcast — failure here just means no
        // subscribers; that's fine.
        let _ = self.events.send(RegistryChange::Set {
            key_bytes,
            message_bytes,
        });
        Ok(())
    }

    async fn delete(&self, key: &StreamKey) -> Result<()> {
        let key_bytes = key.storage_key();
        self.inner.delete(key).await?;
        let _ = self.events.send(RegistryChange::Delete { key_bytes });
        Ok(())
    }
}

/// Per-request authorisation hook for registry operations.
///
/// Inspiration: iroh-blobs upstream's provider-events `EventMask` +
/// `RequestMode::Intercept` pattern (iroh-blobs 0.93+) — a per-request
/// callback the server consults before dispatching, allowing s5_node to
/// gate registry access by vault membership without coupling
/// s5_registry to the identity layer.
///
/// Identity-vault entries (`StreamKey::Vault { vault_id == IDENTITY_VAULT_ID, .. }`)
/// MUST be permitted for read by any peer — they're public DID
/// bundles. Implementations should encode that exemption.
#[async_trait]
pub trait RegistryAcl: Send + Sync + 'static + std::fmt::Debug {
    /// Authorise a `get` from `peer_pubkey` against `key`. Returns
    /// false to deny. Default: allow.
    async fn allow_read(&self, _peer_pubkey: &[u8; 32], _key: &StreamKey) -> bool {
        true
    }
    /// Authorise a `set` from `peer_pubkey` against `key` — registry
    /// signature verification handles writer-key authentication; this
    /// hook gates by vault membership on top of that.
    async fn allow_write(&self, _peer_pubkey: &[u8; 32], _key: &StreamKey) -> bool {
        true
    }
}

impl RegistryServer {
    pub fn new(registry: Arc<BroadcastingRegistry>) -> Self {
        Self {
            registry,
            acl: None,
        }
    }

    /// Construct with a per-request ACL hook; the trait's default-allow
    /// methods are the stub used when no ACL is supplied.
    pub fn with_acl(registry: Arc<BroadcastingRegistry>, acl: Arc<dyn RegistryAcl>) -> Self {
        Self {
            registry,
            acl: Some(acl),
        }
    }

    async fn handle_get(&self, req: GetRequest, peer: &[u8; 32]) -> GetResponse {
        let key = match StreamKey::from_storage_key(&req.key) {
            Ok(key) => key,
            Err(err) => {
                warn!("registry get: invalid key: {err}");
                return GetResponse { message: None };
            }
        };

        if let Some(acl) = self.acl.as_ref()
            && !acl.allow_read(peer, &key).await
        {
            // Same response shape as "not found" — peer cannot
            // distinguish a denied lookup from a missing entry.
            return GetResponse { message: None };
        }

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

    async fn handle_set(
        &self,
        req: SetRequest,
        peer: &[u8; 32],
    ) -> std::result::Result<(), String> {
        let message =
            StreamMessage::deserialize(Bytes::from(req.message)).map_err(|err| err.to_string())?;

        if let Some(acl) = self.acl.as_ref()
            && !acl.allow_write(peer, &message.key).await
        {
            return Err("registry write denied by ACL".to_string());
        }

        // The wrapped `BroadcastingRegistry` fires the subscriber
        // fanout from inside its `set` — no separate broadcast call
        // here, so the RPC path and the local writer path share one
        // fanout site.
        self.registry
            .set(message)
            .await
            .map_err(|err| err.to_string())?;
        Ok(())
    }

    async fn handle_delete(
        &self,
        req: DeleteRequest,
        peer: &[u8; 32],
    ) -> std::result::Result<(), String> {
        let key = StreamKey::from_storage_key(&req.key).map_err(|err| err.to_string())?;

        if let Some(acl) = self.acl.as_ref()
            && !acl.allow_write(peer, &key).await
        {
            return Err("registry delete denied by ACL".to_string());
        }

        self.registry
            .delete(&key)
            .await
            .map_err(|err| err.to_string())?;
        Ok(())
    }

    async fn handle_subscribe(
        &self,
        req: SubscribeRequest,
        peer: &[u8; 32],
        tx: irpc::channel::mpsc::Sender<RegistryEvent>,
    ) {
        // ACL: filter the requested key set down to those the peer is
        // allowed to read. Keys that don't pass the ACL never appear
        // in the Initial replay nor in the live event stream.
        let mut authorised: Vec<(Vec<u8>, StreamKey)> = Vec::new();
        for raw in &req.keys {
            let Ok(parsed) = StreamKey::from_storage_key(raw) else {
                continue;
            };
            if let Some(acl) = self.acl.as_ref()
                && !acl.allow_read(peer, &parsed).await
            {
                continue;
            }
            authorised.push((raw.clone(), parsed));
        }
        if authorised.is_empty() {
            return;
        }

        // Subscribe to the broadcast BEFORE doing the catch-up reads
        // so we don't drop SETs that land between the GET and the
        // subscribe (small but real race).
        let mut rx = self.registry.subscribe_events();

        // Initial catch-up: emit the current value of each subscribed
        // key. `None` for absent keys lets the consumer learn "no
        // entry exists" without needing a separate Get.
        for (raw_key, parsed_key) in &authorised {
            let message = match self.registry.get(parsed_key).await {
                Ok(Some(msg)) => Some(msg.serialize().to_vec()),
                Ok(None) => None,
                Err(err) => {
                    warn!("subscribe initial get error: {err}");
                    None
                }
            };
            if tx
                .send(RegistryEvent::Initial {
                    key: raw_key.clone(),
                    message,
                })
                .await
                .is_err()
            {
                return; // subscriber gone
            }
        }

        // The authorised set can be small, so a linear scan per event
        // is fine. For larger subscriptions we'd swap to a HashSet.
        let key_set: std::collections::HashSet<Vec<u8>> =
            authorised.iter().map(|(k, _)| k.clone()).collect();

        loop {
            match rx.recv().await {
                Ok(change) => {
                    let event = match change {
                        RegistryChange::Set {
                            key_bytes,
                            message_bytes,
                        } => {
                            if !key_set.contains(&key_bytes) {
                                continue;
                            }
                            RegistryEvent::Set {
                                key: key_bytes,
                                message: message_bytes,
                            }
                        }
                        RegistryChange::Delete { key_bytes } => {
                            if !key_set.contains(&key_bytes) {
                                continue;
                            }
                            RegistryEvent::Delete { key: key_bytes }
                        }
                    };
                    if tx.send(event).await.is_err() {
                        return; // subscriber gone
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "registry subscriber lagged — events dropped");
                    // Continue receiving; the subscriber missed events
                    // but the channel is still usable.
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    }
}

impl ProtocolHandler for RegistryServer {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let peer: [u8; 32] = *conn.remote_id().as_bytes();
        while let Some(msg) = read_request::<RpcProto>(&conn).await? {
            match msg {
                RegistryRpcMessage::Get(irpc::WithChannels { inner, tx, .. }) => {
                    let resp = self.handle_get(inner, &peer).await;
                    let _ = tx.send(resp).await;
                }
                RegistryRpcMessage::Set(irpc::WithChannels { inner, tx, .. }) => {
                    let result = self.handle_set(inner, &peer).await;
                    let _ = tx.send(result).await;
                }
                RegistryRpcMessage::Delete(irpc::WithChannels { inner, tx, .. }) => {
                    let result = self.handle_delete(inner, &peer).await;
                    let _ = tx.send(result).await;
                }
                RegistryRpcMessage::Subscribe(irpc::WithChannels { inner, tx, .. }) => {
                    self.handle_subscribe(inner, &peer, tx).await;
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
        let conn = IrohLazyRemoteConnection::new(endpoint, addr.into(), ALPN.to_vec());
        Client {
            inner: IrpcClient::boxed(conn),
        }
    }

    /// Convenience: connect to a peer identified by their iroh
    /// pubkey (`[u8; 32]`). Mirrors `s5_blobs::Client::connect_to_peer`.
    pub fn connect_to_peer(endpoint: Endpoint, peer_pubkey: [u8; 32]) -> Result<Self> {
        let id = iroh::EndpointId::from_bytes(&peer_pubkey)
            .map_err(|e| anyhow!("invalid peer pubkey: {e}"))?;
        Ok(Self::connect(endpoint, iroh::EndpointAddr::from(id)))
    }

    pub async fn get(&self, key: StreamKey) -> Result<Option<StreamMessage>> {
        let response = self
            .inner
            .rpc(GetRequest {
                key: key.storage_key(),
            })
            .await?;

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
        match self
            .inner
            .rpc(DeleteRequest {
                key: key.storage_key(),
            })
            .await?
        {
            Ok(()) => Ok(()),
            Err(err) => Err(anyhow!(err.to_string())),
        }
    }

    /// Subscribe to a fixed set of `StreamKey`s.
    ///
    /// Returns an mpsc receiver that yields `RegistryEvent`s: first
    /// one `Initial` event per requested key (with the current value
    /// or `None`), then live `Set` / `Delete` events for any future
    /// changes on the subscribed keys.
    ///
    /// `capacity` is the per-subscriber buffer size — set higher than
    /// the expected event burst rate. For typical s5 deployments
    /// (tens of entries total, low rate) `64` is plenty.
    pub async fn subscribe(
        &self,
        keys: Vec<StreamKey>,
        capacity: usize,
    ) -> Result<irpc::channel::mpsc::Receiver<RegistryEvent>> {
        let raw_keys: Vec<Vec<u8>> = keys.iter().map(|k| k.storage_key()).collect();
        Ok(self
            .inner
            .server_streaming(SubscribeRequest { keys: raw_keys }, capacity)
            .await?)
    }
}

// TODO(audit): `RemoteRegistry` is a 15-line wrapper that exists
// only to hang `impl RegistryApi` off a different type. `Client`
// could implement `RegistryApi` directly and `RemoteRegistry`
// would go away. Mechanical.
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
