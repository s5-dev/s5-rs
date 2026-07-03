// TODO(step 3b-2): replace `PeerConfigBlobs`-based authorisation with a
// `BlobAcl` trait + `Option<Arc<dyn BlobAcl>>` field on `BlobsServer`,
// modelled on the `RegistryAcl` shape just added to `s5_registry`.
// Hook the trait at `handle_query` and `handle_download` (dropping the
// `cfg_for(node_key)` check entirely — that path is dead in the
// current daemon, where `peer_cfg` is always empty, so iroh-direct
// blob fetches silently deny today).
//
// `s5_node::membership::MembershipBlobAcl` becomes the impl: peer in
// some vault V where `hash[..16] ∈ V.reachable_chunks` → allow.
// `Snapshot::collect_reachable_chunks` (s5_fs_v2) is the per-vault
// builder; publish.rs needs a hook to call it after a successful snap
// and write into the shared `MembershipState`.
//
// Architecture inspiration: iroh-blobs upstream (0.93+) `EventSender`
// + `EventMask` + `RequestMode::Intercept` pattern — daemon
// subscribes to a per-request channel, replies `Ok(())` or
// `Err(AbortReason::Permission)`. Same shape, dyn-trait flavour for
// us so the ACL is a per-server field, not a separate task.
//
// Step 3a (transport-level peer ACL via iroh 0.98 `EndpointHooks`) is
// already in place upstream of this file in `s5_node::membership`.

use std::collections::HashMap;
use std::sync::Arc;

use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use irpc_iroh::read_request;
use s5_core::blob::BlobsRead;
use s5_core::pins::{PinContext, Pins};
use s5_core::{Hash, blob::BlobStore};

use crate::config::PeerConfigBlobs;
use crate::rpc::{
    AuthChallengeResponse, AuthProve, DeleteBlob, DownloadBlob, PinBlob, Query, QueryResponse,
    RpcMessage, RpcProto, UploadBlob,
};

const CHUNK_SIZE: usize = 64 * 1024; // 64k

/// Domain separator for the F02 binding-derivation step. Used with
/// `blake3::derive_key` so the binding is cryptographically separated
/// from any other use of blake3 in the system.
pub(crate) const F02_BINDING_DOMAIN: &str = "s5-blobs-acl-v1-binding";

/// Prefix for the ed25519 signature input on `AuthProve`. The full
/// signed message is `F02_SIG_PREFIX || binding` where `binding` is the
/// 32-byte blake3 derive of (nonce || client_iroh || server_iroh).
pub(crate) const F02_SIG_PREFIX: &[u8] = b"s5-blobs-acl-v1-auth:";

/// Compute the F02 channel-binding value. Both client and server
/// independently compute this from the server-issued `nonce` and the
/// two QUIC-authenticated transport pubkeys; mismatch on any input
/// (e.g. a replay onto a different connection) produces a different
/// binding and the signature won't verify.
pub(crate) fn f02_binding(
    nonce: &[u8; 32],
    client_iroh_pubkey: &[u8; 32],
    server_iroh_pubkey: &[u8; 32],
) -> [u8; 32] {
    let mut h = blake3::Hasher::new_derive_key(F02_BINDING_DOMAIN);
    h.update(nonce);
    h.update(client_iroh_pubkey);
    h.update(server_iroh_pubkey);
    *h.finalize().as_bytes()
}

/// Bytes to sign / verify in `AuthProve`: the domain-tagged binding.
pub(crate) fn f02_signed_message(binding: &[u8; 32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(F02_SIG_PREFIX.len() + 32);
    out.extend_from_slice(F02_SIG_PREFIX);
    out.extend_from_slice(binding);
    out
}

/// Which ALPN this server instance is bound to. Selects accept-loop
/// behaviour (challenge required vs not) and which `BlobAcl` method
/// gates reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerMode {
    /// Public ALPN (`ALPN_PUBLIC`). No challenge; serves only blobs
    /// approved by `BlobAcl::allow_public_read`.
    Public,
    /// ACL ALPN (`ALPN_ACL`). F02 challenge required before any other
    /// request; serves blobs approved by `BlobAcl::allow_acl_read`.
    Acl,
}

/// Principal identifying *who* is making a read request, post-handshake.
/// Used internally to dispatch the right `BlobAcl` method.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Principal {
    /// No authentication performed (public ALPN). Reads gated by
    /// `BlobAcl::allow_public_read(hash)`.
    Public,
    /// F02-challenge-bound ACL pubkey (ACL ALPN). Reads gated by
    /// `BlobAcl::allow_acl_read(principal, hash)`.
    AclPubkey([u8; 32]),
}

/// Per-request authorisation hook for blob queries / downloads.
///
/// Mirrors the design in `docs/reference/architecture-directions.md
/// § Bandwidth economy`: two orthogonal trait methods, one per ALPN.
/// The opaque principal bytes are deliberately small and untyped at
/// this layer — the higher-level daemon decides what the bytes mean
/// (today: device ACL pubkey; future: macaroon ID, blind-token hash).
#[async_trait::async_trait]
pub trait BlobAcl: Send + Sync + 'static + std::fmt::Debug {
    /// Authorise a read of `hash` on the **public** ALPN — no
    /// connection-bound principal exists (the public ALPN skips the
    /// F02 challenge). Returns true iff the hash belongs to the
    /// node's published-public set (e.g. identity bundles,
    /// `public_blob_hashes`). Default denies — operators must
    /// explicitly tag blobs as public to serve them anonymously.
    async fn allow_public_read(&self, _hash: &Hash) -> bool {
        false
    }

    /// Authorise a read of `hash` on the **ACL** ALPN given the
    /// F02-challenge-bound principal (the device's ed25519 ACL/read
    /// pubkey). Default denies — the daemon's MembershipBlobAcl impl
    /// is the only thing that authorises ACL reads in production.
    async fn allow_acl_read(&self, _principal: &[u8; 32], _hash: &Hash) -> bool {
        false
    }

    /// Check during the F02 challenge handshake whether `principal` is
    /// a recognised ACL pubkey at all — i.e. whether it appears in
    /// `authorized_acl_pubkeys` for *any* served vault. Used to reject
    /// unknown principals early, before they get to mint a connection
    /// they can't use anyway. Default denies.
    async fn allow_acl_principal(&self, _principal: &[u8; 32]) -> bool {
        false
    }
}

/// `BlobAcl` impl that approves every read on both ALPNs and accepts
/// every principal. Use for intentionally world-readable vaults
/// (in-DC compute pulling from a single-writer indexer; public mirrors;
/// archival reads) where membership-based ACLs aren't the right gate.
#[derive(Debug, Default, Clone, Copy)]
pub struct PermitAllBlobAcl;

#[async_trait::async_trait]
impl BlobAcl for PermitAllBlobAcl {
    async fn allow_public_read(&self, _hash: &Hash) -> bool {
        true
    }
    async fn allow_acl_read(&self, _p: &[u8; 32], _hash: &Hash) -> bool {
        true
    }
    async fn allow_acl_principal(&self, _p: &[u8; 32]) -> bool {
        true
    }
}

#[derive(Clone)]
pub struct BlobsServer {
    stores: Arc<HashMap<String, BlobStore>>, // named stores (read + write)
    /// Read-only sources that can be queried and downloaded from, but not written to.
    /// These are checked alongside `stores` when a peer has the source name in `readable_stores`.
    read_sources: Arc<HashMap<String, Arc<dyn BlobsRead>>>,
    // Keyed by stringified remote id (Display or Debug form).
    // The map may also contain a special "*" wildcard entry which
    // is used when no exact peer id match is found.
    //
    // TODO(post step 3b-2): drop entirely once all in-tree callers
    // (and tests) migrate to the membership-based `BlobAcl` shape.
    peer_cfg: Arc<HashMap<String, PeerConfigBlobs>>, // per-peer ACLs
    /// Per-request blob ACL hook. When `Some`, this takes precedence
    /// over `peer_cfg`: the daemon's own membership-aware ACL decides
    /// whether to serve, and approved requests search across ALL
    /// configured stores + read-only sources (membership doesn't
    /// carry per-store granularity).
    acl: Option<Arc<dyn BlobAcl>>,
    /// Optional pinning backend used to enforce that uploads,
    /// downloads and deletes are scoped to the calling node
    /// (`PinContext::NodeId`).
    pinner: Option<Arc<dyn Pins>>,
    /// This server's own iroh transport pubkey. Used as the
    /// server-side input to the F02 channel binding so the binding is
    /// asymmetric and unique per (client, server) pair. Set at
    /// construction time; cheap to clone in the per-connection accept
    /// loop.
    local_iroh_pubkey: [u8; 32],
    /// ALPN mode this server instance is bound to. Selects whether the
    /// accept loop requires the F02 challenge handshake (`Acl`) or
    /// runs anonymously over `public_blob_hashes` only (`Public`).
    mode: ServerMode,
}

impl std::fmt::Debug for BlobsServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobsServer")
            .field("stores", &self.stores.keys().collect::<Vec<_>>())
            .field(
                "read_sources",
                &self.read_sources.keys().collect::<Vec<_>>(),
            )
            .field("peer_cfg", &self.peer_cfg.keys().collect::<Vec<_>>())
            .field("pinner", &self.pinner.is_some())
            .finish()
    }
}

impl BlobsServer {
    pub fn new(
        stores: HashMap<String, BlobStore>,
        peer_cfg: HashMap<String, PeerConfigBlobs>,
        pinner: Option<Arc<dyn Pins>>,
    ) -> Self {
        Self {
            stores: Arc::new(stores),
            read_sources: Arc::new(HashMap::new()),
            peer_cfg: Arc::new(peer_cfg),
            acl: None,
            pinner,
            local_iroh_pubkey: [0u8; 32],
            mode: ServerMode::Acl,
        }
    }

    /// Creates a new BlobsServer with both read-write stores and read-only sources.
    ///
    /// Read-only sources (like `LocalLinksStore`) can be queried and downloaded from
    /// but cannot receive uploads. They are referenced by name in `readable_stores`.
    pub fn with_read_sources(
        stores: HashMap<String, BlobStore>,
        read_sources: HashMap<String, Arc<dyn BlobsRead>>,
        peer_cfg: HashMap<String, PeerConfigBlobs>,
        pinner: Option<Arc<dyn Pins>>,
    ) -> Self {
        Self {
            stores: Arc::new(stores),
            read_sources: Arc::new(read_sources),
            peer_cfg: Arc::new(peer_cfg),
            acl: None,
            pinner,
            local_iroh_pubkey: [0u8; 32],
            mode: ServerMode::Acl,
        }
    }

    /// Builder: bind this server instance to a specific ALPN mode
    /// (`Public` skips F02 challenge; `Acl` requires it).
    pub fn with_mode(mut self, mode: ServerMode) -> Self {
        self.mode = mode;
        self
    }

    /// Builder: set this node's iroh transport pubkey for use in the
    /// F02 channel-binding hash. Required when `mode = Acl`; for
    /// `Public` mode the value is unused.
    pub fn with_local_iroh_pubkey(mut self, pubkey: [u8; 32]) -> Self {
        self.local_iroh_pubkey = pubkey;
        self
    }

    /// Attach a per-request ACL hook. Once set, all reads are gated
    /// by `acl.allow_read(peer, hash)` and the legacy `peer_cfg` path
    /// is bypassed. Builder-style: pass-through self for chaining.
    pub fn with_acl(mut self, acl: Arc<dyn BlobAcl>) -> Self {
        self.acl = Some(acl);
        self
    }

    fn cfg_for(&self, node_key: &str) -> Option<&PeerConfigBlobs> {
        // First try an exact match for this peer's id; if not present,
        // fall back to a wildcard entry ("*") if configured.
        self.peer_cfg
            .get(node_key)
            .or_else(|| self.peer_cfg.get("*"))
    }

    /// Server-side verification of an `AuthProve` message. Returns the
    /// bound ACL pubkey on success, or an error string describing why
    /// the proof was rejected.
    ///
    /// Checks, in order:
    /// 1. A prior `AuthChallenge` exists (no challenge → no proof).
    /// 2. The ed25519 signature verifies under `acl_pubkey` over
    ///    `F02_SIG_PREFIX || binding`, where `binding` is computed from
    ///    `(nonce, client_iroh_pubkey, server_iroh_pubkey)`.
    /// 3. `acl_pubkey ∈` some served vault's `authorized_acl_pubkeys`
    ///    (via `BlobAcl::allow_acl_principal`).
    ///
    /// The channel binding (step 2) is what stops cross-connection
    /// replay: a sig minted under nonce N₁ + client A's iroh pubkey
    /// will not verify under nonce N₂ or under a different client iroh
    /// pubkey, even with the same `acl_pubkey`.
    async fn verify_auth_prove(
        &self,
        pending_nonce: Option<[u8; 32]>,
        client_iroh_pubkey: &[u8; 32],
        proof: AuthProve,
    ) -> Result<[u8; 32], String> {
        let Some(nonce) = pending_nonce else {
            return Err("AuthProve without prior AuthChallenge".to_string());
        };
        let binding = f02_binding(&nonce, client_iroh_pubkey, &self.local_iroh_pubkey);
        let signed = f02_signed_message(&binding);
        let vk = ed25519_dalek::VerifyingKey::from_bytes(&proof.acl_pubkey)
            .map_err(|e| format!("bad acl_pubkey: {e}"))?;
        let sig = ed25519_dalek::Signature::from_bytes(&proof.sig_bytes());
        vk.verify_strict(&signed, &sig)
            .map_err(|e| format!("F02 signature verify failed: {e}"))?;
        if let Some(acl) = self.acl.as_ref()
            && !acl.allow_acl_principal(&proof.acl_pubkey).await
        {
            return Err("acl_pubkey not in any served vault's acl_keys".to_string());
        }
        Ok(proof.acl_pubkey)
    }

    /// Decide which store/source names to search for a read request.
    ///
    /// When an `acl` is configured, the daemon's membership-aware
    /// hook gates the request; if approved, the search covers ALL
    /// configured stores and read-only sources (membership-based ACL
    /// has no per-store granularity — that's a deliberate property,
    /// see the architectural rationale in `s5_node::membership` and
    /// `docs/reference/iroh-inspirations.md`).
    ///
    /// The store-membership choice (vs a per-snapshot reachable-set)
    /// is what preserves access to older snapshots: a peer who
    /// retained an old snapshot's root hash can still fetch its blobs
    /// from the vault's store, because the blobs are still there even
    /// if not reachable from the *current* snapshot. The constraint
    /// is "do not mix vault meta blobs with unrelated risky data in
    /// the same store" — vault stores are scoped per-vault by
    /// convention.
    ///
    /// When no ACL is set, falls back to the legacy `peer_cfg`
    /// readable-stores list (preserves existing test setups).
    /// `None` return = denied.
    async fn resolve_readable_names(
        &self,
        node_key: &str,
        principal: &Principal,
        hash: &Hash,
    ) -> Option<Vec<String>> {
        if let Some(acl) = self.acl.as_ref() {
            let approved = match principal {
                Principal::Public => acl.allow_public_read(hash).await,
                Principal::AclPubkey(pk) => acl.allow_acl_read(pk, hash).await,
            };
            if !approved {
                return None;
            }
            return Some(
                self.stores
                    .keys()
                    .chain(self.read_sources.keys())
                    .cloned()
                    .collect(),
            );
        }
        // Legacy path: peer_cfg-based per-peer readable_stores. Used
        // by deployments and tests that haven't migrated to the
        // membership-based `BlobAcl` shape. Works regardless of
        // principal — the lookup is by `node_key` (stringified
        // transport pubkey), not by ACL principal.
        let _ = principal;
        self.cfg_for(node_key)
            .map(|cfg| cfg.readable_stores.clone())
    }
}

impl ProtocolHandler for BlobsServer {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let node_id = conn.remote_id();
        let node_key = node_id.to_string();
        let node_id_bytes: [u8; 32] = *node_id.as_bytes();

        tracing::info!(
            peer = %node_id.fmt_short(),
            mode = ?self.mode,
            "blobs: accepted connection"
        );

        // F02 connection state (ACL ALPN only):
        //   pending_nonce: the nonce we issued via AuthChallenge,
        //     awaiting an AuthProve. Consumed on first AuthProve.
        //   bound_acl_pubkey: the ACL pubkey the client proved
        //     possession of. Set on successful AuthProve; required for
        //     all subsequent read/write requests.
        let mut pending_nonce: Option<[u8; 32]> = None;
        let mut bound_acl_pubkey: Option<[u8; 32]> = None;

        let mut request_count = 0u64;
        while let Some(msg) = read_request::<RpcProto>(&conn).await? {
            request_count += 1;
            // Compute the current principal from connection state.
            let principal: Principal = match self.mode {
                ServerMode::Public => Principal::Public,
                ServerMode::Acl => match bound_acl_pubkey {
                    Some(pk) => Principal::AclPubkey(pk),
                    // Pre-authentication: only Auth* messages are
                    // permitted. All other handler branches below
                    // check `bound_acl_pubkey` and reject with
                    // "authentication required".
                    None => Principal::AclPubkey([0u8; 32]),
                },
            };

            match msg {
                RpcMessage::AuthChallenge(msg) => {
                    let irpc::WithChannels { inner: _, tx, .. } = msg;
                    if self.mode != ServerMode::Acl {
                        // Public ALPN doesn't run the challenge. Reply
                        // with a zero nonce; clients that mistakenly
                        // dial the public ALPN with an Authenticate
                        // flow will fail on AuthProve.
                        let _ = tx.send(AuthChallengeResponse { nonce: [0u8; 32] }).await;
                        continue;
                    }
                    use rand::Rng;
                    let mut nonce = [0u8; 32];
                    rand::rng().fill_bytes(&mut nonce);
                    pending_nonce = Some(nonce);
                    let _ = tx.send(AuthChallengeResponse { nonce }).await;
                }
                RpcMessage::AuthProve(msg) => {
                    let irpc::WithChannels { inner, tx, .. } = msg;
                    if self.mode != ServerMode::Acl {
                        let _ = tx.send(Err("AuthProve on public ALPN".to_string())).await;
                        continue;
                    }
                    let result = self
                        .verify_auth_prove(pending_nonce.take(), &node_id_bytes, inner)
                        .await;
                    if let Ok(pk) = result {
                        bound_acl_pubkey = Some(pk);
                        let _ = tx.send(Ok(())).await;
                    } else {
                        let _ = tx.send(Err(result.unwrap_err())).await;
                    }
                }
                _ if self.mode == ServerMode::Acl && bound_acl_pubkey.is_none() => {
                    // Reject any non-Auth request on the ACL ALPN
                    // before authentication. The bi-stream send paths
                    // differ per RPC variant — best-effort log + drop.
                    tracing::warn!(
                        peer = %node_id.fmt_short(),
                        "blobs: rejecting pre-auth request on ACL ALPN"
                    );
                    // Drop the message; the channel close signals the
                    // client that the request will not be served.
                    drop(msg);
                }
                RpcMessage::Query(msg) => {
                    let irpc::WithChannels { inner, tx, .. } = msg;
                    let _ =
                        handle_query(self, &node_key, &principal, node_id_bytes, inner, tx).await;
                }
                RpcMessage::UploadBlob(msg) => {
                    let irpc::WithChannels { inner, rx, tx, .. } = msg;
                    let _ = handle_upload(self, &node_key, node_id_bytes, inner, rx, tx).await;
                }
                RpcMessage::DownloadBlob(msg) => {
                    let irpc::WithChannels { inner, tx, .. } = msg;
                    let _ = handle_download(self, &node_key, &principal, node_id_bytes, inner, tx)
                        .await;
                }
                RpcMessage::DeleteBlob(msg) => {
                    let irpc::WithChannels { inner, tx, .. } = msg;
                    let _ = handle_delete(self, &node_key, node_id_bytes, inner, tx).await;
                }
                RpcMessage::PinBlob(msg) => {
                    let irpc::WithChannels { inner, tx, .. } = msg;
                    let _ = handle_pin(self, &node_key, node_id_bytes, inner, tx).await;
                }
            }
        }

        tracing::info!(
            peer = %node_id.fmt_short(),
            request_count,
            "blobs: connection closed"
        );
        conn.closed().await;
        Ok(())
    }
}

async fn handle_pin(
    server: &BlobsServer,
    node_key: &str,
    node_id_bytes: [u8; 32],
    req: PinBlob,
    tx: irpc::channel::oneshot::Sender<Result<bool, String>>,
) {
    let Some(cfg) = server.cfg_for(node_key) else {
        let _ = tx.send(Err("permission denied".into())).await;
        return;
    };
    // We use the same permission as upload for pinning
    let Some(store_name) = &cfg.store_uploads_in else {
        let _ = tx.send(Err("uploads (pinning) not allowed".into())).await;
        return;
    };
    let Some(store) = server.stores.get(store_name) else {
        let _ = tx.send(Err("invalid upload store".into())).await;
        return;
    };

    let hash: Hash = req.hash.into();

    // Check if blob exists
    match store.contains(hash).await {
        Ok(true) => {
            // Blob exists, try to pin it
            if let Some(pinner) = &server.pinner
                && let Err(e) = pinner
                    .pin_hash(hash, PinContext::NodeId(node_id_bytes))
                    .await
            {
                let _ = tx.send(Err(format!("pinning failed: {e}"))).await;
                return;
            }
            let _ = tx.send(Ok(true)).await;
        }
        Ok(false) => {
            // Blob not found
            let _ = tx.send(Ok(false)).await;
        }
        Err(e) => {
            let _ = tx.send(Err(format!("store error: {e}"))).await;
        }
    }
}

async fn handle_query(
    server: &BlobsServer,
    node_key: &str,
    principal: &Principal,
    _node_id_bytes: [u8; 32],
    query: Query,
    tx: irpc::channel::oneshot::Sender<QueryResponse>,
) {
    // TODO: If/when target_types is added, support additional targets (e.g. Obao6) in queries/answers.
    let mut resp = QueryResponse::default();

    if query.blinded {
        // Blinded queries probe by `blake3(actual_hash)` — used by the
        // opt-in cross-vault public CAS (architecture-directions §
        // "Cross-vault dedup via shared CAS"). The membership ACL is
        // hash-keyed, so it can't pre-authorise the unknown actual
        // hash; for now, blinded queries fall through to the legacy
        // `peer_cfg` path only. When `acl` is set, blinded queries
        // are denied (return an empty response).
        // TODO(step 3b followup): once public-CAS is wired, add a
        // `BlobAcl::allow_blinded` hook returning the search-store
        // list for that peer.
        if server.acl.is_some() {
            let _ = tx.send(resp).await;
            return;
        }
        let blinded_hash = query.hash;
        if let Some(cfg) = server.cfg_for(node_key) {
            for name in &cfg.readable_stores {
                if let Some(store) = server.stores.get(name)
                    && let Some(actual_hash) = find_blob_by_blinded_hash(store, blinded_hash).await
                {
                    resp.exists = true;
                    resp.actual_hash = Some(*actual_hash.as_bytes());

                    if resp.size.is_none()
                        && let Ok(sz) = store.size(actual_hash).await
                    {
                        resp.size = Some(sz);
                    }

                    if let Ok(mut locs) = store.provide(actual_hash).await {
                        resp.locations.append(&mut locs);
                    }
                    break; // Found it
                }
            }
        }
    } else {
        // Normal query: hash field is the actual hash
        let hash: Hash = query.hash.into();
        if let Some(names) = server
            .resolve_readable_names(node_key, principal, &hash)
            .await
        {
            for name in &names {
                // Check full stores first (they can provide locations)
                if let Some(store) = server.stores.get(name)
                    && let Ok(true) = store.contains(hash).await
                {
                    resp.exists = true;
                    if resp.size.is_none()
                        && let Ok(sz) = store.size(hash).await
                    {
                        resp.size = Some(sz);
                    }

                    if let Ok(mut locs) = store.provide(hash).await {
                        // TODO: optionally filter by query.location_types
                        resp.locations.append(&mut locs);
                    }
                }
                // Also check read-only sources
                else if let Some(source) = server.read_sources.get(name)
                    && let Ok(true) = source.blob_contains(hash).await
                {
                    resp.exists = true;
                    if resp.size.is_none()
                        && let Ok(sz) = source.blob_get_size(hash).await
                    {
                        resp.size = Some(sz);
                    }
                    // Read-only sources don't provide locations
                }
            }
        }
    }

    let _ = tx.send(resp).await;
}

/// Finds a blob by its blinded hash (blake3(actual_hash)).
///
/// This iterates over the store's blobs and computes blinded hashes on-the-fly.
/// For large stores, this could be slow - consider adding a blinded hash index
/// if this becomes a bottleneck.
async fn find_blob_by_blinded_hash(store: &BlobStore, blinded_hash: [u8; 32]) -> Option<Hash> {
    // Get list of all blob hashes in the store
    let hashes = match store.list_hashes().await {
        Ok(h) => h,
        Err(_) => return None,
    };

    for hash in hashes {
        // Compute blinded hash: blake3(hash)
        let computed_blinded = blake3::hash(hash.as_bytes());
        if computed_blinded.as_bytes() == &blinded_hash {
            return Some(hash);
        }
    }

    None
}

async fn handle_upload(
    server: &BlobsServer,
    node_key: &str,
    node_id_bytes: [u8; 32],
    req: UploadBlob,
    rx: irpc::channel::mpsc::Receiver<bytes::Bytes>,
    tx: irpc::channel::oneshot::Sender<Result<(), String>>,
) {
    let Some(cfg) = server.cfg_for(node_key) else {
        let _ = tx.send(Err("permission denied".into())).await;
        return;
    };
    let Some(store_name) = &cfg.store_uploads_in else {
        let _ = tx.send(Err("uploads not allowed".into())).await;
        return;
    };
    let Some(store) = server.stores.get(store_name) else {
        let _ = tx.send(Err("invalid upload store".into())).await;
        return;
    };

    // Adapt rx into the expected Stream type for import_stream, owning the receiver.
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Ok(Some(chunk)) => Some((Ok::<bytes::Bytes, std::io::Error>(chunk), rx)),
            _ => None,
        }
    });

    // TODO(remote-blobs): once RemoteBlobStore fully owns hashing and
    // outboard computation/verification, consider tightening this path
    // so the server can rely more directly on remote-side guarantees.
    match store.import_stream(Box::new(Box::pin(stream))).await {
        Ok(blob) => {
            let got_hash = blob.hash;
            let got_size = blob.size;
            if got_hash.as_bytes() != &req.expected_hash || got_size != req.size {
                let _ = store.delete(got_hash).await; // best-effort cleanup on mismatch
                let _ = tx.send(Err("hash/size mismatch".into())).await;
            } else {
                if let Some(pinner) = &server.pinner
                    && let Err(e) = pinner
                        .pin_hash(got_hash, PinContext::NodeId(node_id_bytes))
                        .await
                {
                    let _ = store.delete(got_hash).await;
                    let _ = tx.send(Err(format!("pinning failed: {e}"))).await;
                    return;
                }
                let _ = tx.send(Ok(())).await;
            }
        }
        Err(e) => {
            let _ = tx.send(Err(format!("upload failed: {e}"))).await;
        }
    }
}

async fn handle_download(
    server: &BlobsServer,
    node_key: &str,
    principal: &Principal,
    node_id_bytes: [u8; 32],
    req: DownloadBlob,
    tx: irpc::channel::mpsc::Sender<bytes::Bytes>,
) {
    let hash: Hash = req.hash.into();
    let hash_short = hash.fmt_short();

    tracing::info!(
        peer = node_key,
        hash = hash_short,
        "handle_download: request received"
    );

    let Some(names) = server
        .resolve_readable_names(node_key, principal, &hash)
        .await
    else {
        tracing::warn!(
            peer = node_key,
            hash = hash_short,
            "download denied: ACL or peer_cfg refused"
        );
        return;
    };

    // Find first readable source containing the blob (stores or read-only sources)
    let mut size_opt: Option<u64> = None;
    let mut source_opt: Option<Arc<dyn BlobsRead>> = None;
    let mut from_read_source = false;
    let mut source_name: Option<String> = None;

    for name in &names {
        // Check full stores first
        if let Some(store) = server.stores.get(name) {
            match store.contains(hash).await {
                Ok(true) => {
                    if let Ok(sz) = store.size(hash).await {
                        size_opt = Some(sz);
                    }
                    source_opt = Some(Arc::new(store.clone()) as Arc<dyn BlobsRead>);
                    source_name = Some(name.clone());
                    break;
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::info!(
                        store = name,
                        hash = hash_short,
                        error = %e,
                        "store.contains failed"
                    );
                }
            }
        }
        // Also check read-only sources
        if let Some(source) = server.read_sources.get(name) {
            match source.blob_contains(hash).await {
                Ok(true) => {
                    if let Ok(sz) = source.blob_get_size(hash).await {
                        size_opt = Some(sz);
                    }
                    source_opt = Some(source.clone());
                    from_read_source = true;
                    source_name = Some(name.clone());
                    break;
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::info!(
                        source = name,
                        hash = hash_short,
                        error = %e,
                        "read_source.blob_contains failed"
                    );
                }
            }
        }
    }

    // Pin check: only required for blobs from regular stores under
    // the legacy `peer_cfg` path. The membership-aware ACL path
    // (when `server.acl` is set) is strictly stronger than pin —
    // peer is in a vault, no need to additionally require a per-node
    // pin for vault content.
    if !from_read_source
        && server.acl.is_none()
        && let Some(legacy_cfg) = server.cfg_for(node_key)
        && !legacy_cfg.skip_pin_check
        && let Some(pinner) = &server.pinner
    {
        let is_pinned = pinner
            .is_pinned(hash, PinContext::NodeId(node_id_bytes))
            .await
            .unwrap_or(false);

        if !is_pinned {
            tracing::info!(
                peer = node_key,
                hash = hash_short,
                "download denied: not pinned"
            );
            return; // Not pinned by this user, deny download
        }
    }

    let Some(source) = source_opt else {
        tracing::info!(
            peer = node_key,
            hash = hash_short,
            readable_stores = ?names,
            num_stores = server.stores.len(),
            num_read_sources = server.read_sources.len(),
            "download: blob not found in any readable store"
        );
        return;
    };
    let Some(size) = size_opt else {
        tracing::warn!(
            peer = node_key,
            hash = hash_short,
            source = ?source_name,
            "download: blob exists but size unknown"
        );
        return;
    };

    if req.offset > size {
        tracing::warn!(
            peer = node_key,
            hash = hash_short,
            offset = req.offset,
            size,
            "download: offset beyond blob size"
        );
        return;
    }
    // TODO: If requests carry chunk bitmaps, use them to shape the read plan and coalesce chunks.
    let to_send = match req.max_len {
        Some(m) => m.min(size - req.offset),
        None => size - req.offset,
    };

    tracing::info!(
        peer = node_key,
        hash = hash_short,
        source = ?source_name,
        size,
        to_send,
        from_read_source,
        "download: sending blob"
    );

    let mut sent: u64 = 0;
    while sent < to_send {
        let want = std::cmp::min(CHUNK_SIZE as u64, to_send - sent);
        match source
            .blob_download_slice(hash, req.offset + sent, Some(want))
            .await
        {
            Ok(bytes) => {
                if bytes.is_empty() {
                    tracing::warn!(
                        hash = hash_short,
                        sent,
                        to_send,
                        offset = req.offset + sent,
                        "download: got empty slice mid-transfer"
                    );
                    break;
                }
                if tx.send(bytes.clone()).await.is_err() {
                    tracing::info!(hash = hash_short, sent, "download: peer disconnected");
                    break;
                }
                sent += bytes.len() as u64;
            }
            Err(e) => {
                tracing::warn!(
                    hash = hash_short,
                    sent,
                    to_send,
                    error = %e,
                    "download: slice read failed"
                );
                break;
            }
        }
    }
}

async fn handle_delete(
    server: &BlobsServer,
    node_key: &str,
    node_id_bytes: [u8; 32],
    req: DeleteBlob,
    tx: irpc::channel::oneshot::Sender<Result<bool, String>>,
) {
    let Some(cfg) = server.cfg_for(node_key) else {
        let _ = tx.send(Err("permission denied".into())).await;
        return;
    };
    // If user can upload, they can delete their own pins.
    if cfg.store_uploads_in.is_none() {
        let _ = tx.send(Err("delete not allowed".into())).await;
        return;
    }

    let hash: Hash = req.hash.into();

    if let Some(pinner) = &server.pinner {
        match pinner
            .unpin_hash(hash, PinContext::NodeId(node_id_bytes))
            .await
        {
            Ok(orphaned) => {
                if orphaned {
                    for store in server.stores.values() {
                        let _ = store.delete(hash).await;
                    }
                    let _ = tx.send(Ok(true)).await;
                } else {
                    let _ = tx.send(Ok(false)).await;
                }
            }
            Err(e) => {
                let _ = tx.send(Err(format!("unpin failed: {e}"))).await;
            }
        }
    } else {
        let _ = tx.send(Err("pinning not enabled".into())).await;
    }
}
