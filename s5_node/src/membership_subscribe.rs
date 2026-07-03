//! Step 4-b: daemon-side subscriber that keeps the local
//! `MembershipState` and registry in sync with peers' published bundles
//! and snapshots.
//!
//! For each known peer iroh pubkey resolved via `MembershipState`
//! (excluding ourselves), spawn a long-lived task that:
//!
//! 1. Dials the peer's `RegistryServer` over iroh.
//! 2. Subscribes to the peer's identity-vault key — `StreamKey::Vault {
//!    pubkey: peer, vault_id: IDENTITY_VAULT_ID }` — so bundle
//!    rotations propagate immediately.
//! 3. On each `Initial` / `Set` event, deserialises the
//!    `StreamMessage` and writes it into the local registry. The
//!    multi-registry on the daemon's side fans out to its `[redb,
//!    store]` backends, so subsequent reads (and the next
//!    membership-state rebuild) see the new value.
//! 4. After a successful write to a relevant key, kicks off a
//!    `build_membership_state` rebuild and replaces the shared
//!    `Arc<RwLock<MembershipState>>` if anything changed.
//!
//! Reconnect: tasks loop with a 5 s backoff on dial / stream
//! failures. Cancellation: the daemon's shutdown token tears down
//! every subscriber cleanly.
//!
//! Per peer, the subscription set is:
//!   * the peer's identity-vault key (bundle/DidDocument rotations), and
//!   * one data key `StreamKey::Vault { pubkey: peer, vault_id: V }`
//!     for every vault `V` shared with this daemon — i.e. every
//!     `vault_id` we have registered locally (via the publish path)
//!     that has the peer's pubkey in its `authorized_iroh_pubkeys`.
//!
//! The peer's data keys are how we learn about new HEADs (encrypted
//! Transparent Node revisions) on their side without polling. The
//! local `peer_load` consumer reads from the local registry, which
//! the subscriber fans events into; mount/UI layers can either watch
//! the registry or re-resolve on demand.
//!
//! Reachability: the daemon's iroh endpoint uses `presets::N0`, so
//! dialing a peer by ed25519 pubkey alone goes through iroh's
//! pkarr/DNS/mDNS resolution + n0 relay fallback. No address has to
//! be embedded in DidDocuments or carried out-of-band.
//
// Connection reuse: `run_for_peer` holds a persistent
// `s5_registry::Client` AND a persistent `s5_blobs::Client` per
// peer per connect-attempt — both stay warm for the lifetime of
// the subscription loop, and on registry recv error we drop
// both and reconnect the pair as a unit. iroh's idiomatic
// pattern is one `Endpoint` per pubkey with multiple ALPNs;
// QUIC binds each connection to one ALPN, but iroh shares the
// underlying UDP socket / NAT mapping / pathing across them, so
// the cost is essentially one TLS handshake per ALPN per peer
// (amortised over every subsequent stream on that connection).
// Subprotocol streams are bi-streams within the same connection
// — opening a new stream is cheap, dropping it doesn't tear
// down the connection.
//
// TODO(peer-connections-cache): elevate this to a daemon-wide
// `PeerConnections` (DashMap-keyed by peer pubkey) so non-
// subscriber code paths share the warm clients too. Today the
// only consumer is this subscriber, but `peer_load` (vup mount
// of a peer's view), a future sync director, and any future
// "talk to a peer for an ad-hoc query" command would each
// re-dial without the cache. Shape:
//
//     pub struct PeerConnections {
//         endpoint: iroh::Endpoint,
//         registry: DashMap<[u8; 32], s5_registry::Client>,
//         blobs:    DashMap<[u8; 32], s5_blobs::Client>,
//     }
//     impl PeerConnections {
//         fn registry(&self, peer) -> Result<s5_registry::Client>;
//         fn blobs(&self, peer)    -> Result<s5_blobs::Client>;
//         fn evict(&self, peer);   // call from recv-error branch
//     }
//
// Note: this is "one cache, many peers" — NOT "one
// `iroh::Endpoint` per peer". An iroh `Endpoint` is THIS
// daemon's local QUIC endpoint (one UDP socket, one ed25519
// keypair); a single `Endpoint` dials many peers. The thing
// that's per-peer is the `Connection` inside each cached
// `Client`, which already shares the daemon's `Endpoint` for
// pathing/NAT. Making `Endpoint` per-peer would mean a separate
// UDP socket and a separate identity for every peer, which is
// the opposite of what we want.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use s5_core::Hash;
use s5_core::{RegistryApi, StreamKey, StreamMessage};
use s5_registry::RegistryEvent;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::S5NodeConfig;
use crate::identity_vault::identity_vault_id;
use crate::membership::{MembershipState, build_membership_state};

/// The set of pubkeys a per-peer subscription task needs:
///
/// - `iroh` — QUIC dial target.
/// - `master` — identity-vault subscription key (where the peer's
///   `IdentityBundle` is published — `acl-and-revocation.md §1`).
/// - `device_signing` — vault-data subscription key (where the peer's
///   vault registry entries are written — they're signed by
///   `tasks::publish::device_signing_key`). `None` for read-only /
///   service-DID peers whose bundle has no `signers[]` entry.
#[derive(Clone, Copy, Debug)]
struct PeerSubKeys {
    iroh: [u8; 32],
    master: [u8; 32],
    device_signing: Option<[u8; 32]>,
}

/// One data-vault HEAD change observed by the membership subscriber.
///
/// Fired AFTER the local registry write succeeds, so any subscriber
/// reacting to this event can `registry.get(stream_key)` and see the
/// same `StreamMessage` the broadcast announced.
///
/// Identity-vault events are NOT broadcast here — they reshape
/// `MembershipState` and have a different downstream model. Subscribers
/// interested in identity rotations should watch `MembershipState`
/// directly.
#[derive(Debug, Clone)]
pub struct DataVaultEvent {
    /// The peer whose stream emitted this event.
    pub peer_pubkey: [u8; 32],
    /// The vault_id portion of the stream key — disambiguates between
    /// multiple data vaults shared with the same peer.
    pub vault_id: [u8; 16],
    /// Hash of the new published Transparent Node blob. Same value
    /// `registry.get(stream_key).await?.unwrap().hash` would return.
    pub blob_hash: Hash,
    /// Monotonic revision of the registry entry. Useful for de-duping
    /// when a subscriber lags or restarts mid-stream.
    pub revision: u64,
}

/// Inputs the per-peer subscription tasks need shared access to.
/// Wrapped in `Arc<>` and cloned cheaply across spawned tasks.
pub struct MembershipSubscriber {
    /// This daemon's own DID — the cold anchor pubkey (D17), resolved
    /// from the anchor entry at startup. NOT derivable from the warm
    /// signing key in hand; `build_membership_state` needs it to map
    /// `"self"` members.
    pub self_did: s5_core::identity::Did,
    /// Iroh transport pubkey — used only for self-skip when iterating
    /// `authorized_iroh_pubkeys` in `spawn_peer_tasks`. Not an
    /// authorisation principal; never signs anything.
    pub self_iroh_pubkey: [u8; 32],
    pub config: Arc<RwLock<S5NodeConfig>>,
    pub registry: Arc<dyn RegistryApi + Send + Sync>,
    pub stores: HashMap<String, Arc<dyn s5_core::blob::Blobs>>,
    pub state: Arc<RwLock<MembershipState>>,
    pub endpoint: iroh::Endpoint,
    /// Optional broadcast for data-vault HEAD changes. Fired AFTER the
    /// local registry write succeeds. `None` disables the broadcast —
    /// the subscriber still applies events to the local registry, just
    /// no fan-out. Slow consumers receiving `RecvError::Lagged` are
    /// expected to re-sync from the registry, not chase backlog.
    pub data_events: Option<broadcast::Sender<DataVaultEvent>>,
    /// Membership-changed signal. Fired by `tasks::publish::run_publish`
    /// after a fresh publish and by `handle_event` after an
    /// identity-vault bundle apply (the latter changes
    /// `authorized_iroh_pubkeys`, which determines which data-vault
    /// stream keys a peer subscribes to — without firing this, the
    /// initial subscribe stays stuck on the empty-authorized state
    /// forever).
    pub refresh: Arc<tokio::sync::Notify>,
}

impl MembershipSubscriber {
    /// Long-lived coordinator: rebuilds `MembershipState` and
    /// (re)spawns per-peer subscription tasks every time `refresh`
    /// fires (or once at startup). Cancellation tears down all
    /// per-peer tasks and returns.
    ///
    /// Why this shape: the per-peer subscription set depends on the
    /// resolved membership (which peers, which shared vault_ids) and
    /// changes at runtime — pair, publish (registers a new
    /// `vault_id`), patch_config can all rotate it. Owning the
    /// respawn here keeps the trigger sites (server handlers, publish
    /// task) decoupled from subscription mechanics.
    pub async fn run_lifecycle(self: Arc<Self>, cancel: CancellationToken) {
        let refresh = self.refresh.clone();
        // Initial pass on entry, then one per notify.
        let mut peer_cancel = CancellationToken::new();
        let mut peer_handles: Vec<JoinHandle<()>> =
            self.clone().spawn_peer_tasks(peer_cancel.clone()).await;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    peer_cancel.cancel();
                    for h in peer_handles.drain(..) {
                        let _ = h.await;
                    }
                    return;
                }
                _ = refresh.notified() => {
                    // Rebuild membership state from the latest config + registry,
                    // swap into the shared state, then bounce the per-peer tasks.
                    let cfg = self.config.read().await.clone();
                    let resolved = crate::membership::build_membership_state(
                        &self.self_did,
                        &cfg,
                        self.registry.as_ref(),
                        &self.stores,
                    )
                    .await;
                    let (before_peers, before_vaults, after_peers, after_vaults) = {
                        let mut cur = self.state.write().await;
                        let before_peers: usize = cur
                            .vaults
                            .values()
                            .map(|vm| vm.authorized_iroh_pubkeys.len())
                            .sum();
                        let before_vaults = cur.vaults.len();
                        let after_peers: usize = resolved
                            .vaults
                            .values()
                            .map(|vm| vm.authorized_iroh_pubkeys.len())
                            .sum();
                        let after_vaults = resolved.vaults.len();
                        // Merge vault_id_by_name: `build_membership_state`
                        // populates it from `[vault.<name>].vault_id` config
                        // hints; publish populates it at runtime via
                        // `register_vault_id`. Take the union so neither
                        // source clobbers the other.
                        let mut preserved = std::mem::take(&mut cur.vault_id_by_name);
                        *cur = resolved;
                        for (id, name) in preserved.drain() {
                            cur.vault_id_by_name.entry(id).or_insert(name);
                        }
                        (before_peers, before_vaults, after_peers, after_vaults)
                    };
                    tracing::info!(
                        before_peers, after_peers, before_vaults, after_vaults,
                        "membership refresh: state rebuilt, respawning peer subscribers"
                    );
                    peer_cancel.cancel();
                    for h in peer_handles.drain(..) {
                        let _ = h.await;
                    }
                    peer_cancel = CancellationToken::new();
                    peer_handles = self.clone().spawn_peer_tasks(peer_cancel.clone()).await;
                }
            }
        }
    }

    /// Compute the current peer set from `MembershipState` and spawn
    /// one task per peer. Internal — `run_lifecycle` is the public
    /// entry point.
    ///
    /// Each peer task gets three pubkeys: the iroh transport pubkey
    /// (QUIC dial target), the master pubkey (identity-vault
    /// subscription key — post-2d), and the device signing pubkey
    /// (vault-data subscription key, since vault writes are signed by
    /// the device signing key, not the iroh transport key). Peers
    /// whose master isn't yet known are skipped pending out-of-band
    /// bootstrap; a peer without a known device signing key is still
    /// subscribed for bundle rotations (identity-vault) but skips the
    /// vault-data path.
    async fn spawn_peer_tasks(self: Arc<Self>, cancel: CancellationToken) -> Vec<JoinHandle<()>> {
        let self_pubkey: [u8; 32] = self.self_iroh_pubkey;
        let peers: Vec<PeerSubKeys> = {
            let s = self.state.read().await;
            let mut seen: HashSet<[u8; 32]> = HashSet::new();
            let mut out: Vec<PeerSubKeys> = Vec::new();
            for vm in s.vaults.values() {
                for iroh in &vm.authorized_iroh_pubkeys {
                    if *iroh == self_pubkey || !seen.insert(*iroh) {
                        continue;
                    }
                    let Some(master) = s.master_for_peer.get(iroh).copied() else {
                        tracing::warn!(
                            peer = hex::encode(&iroh[..4]).as_str(),
                            "membership subscribe: skipping peer — no master_pubkey known yet \
                             (bundle not resolved; out-of-band bootstrap required post-2d)"
                        );
                        continue;
                    };
                    let device_signing = s.device_signing_for_peer.get(iroh).copied();
                    out.push(PeerSubKeys {
                        iroh: *iroh,
                        master,
                        device_signing,
                    });
                }
            }
            out
        };

        if peers.is_empty() {
            tracing::info!("membership subscribe: no peers to subscribe to");
            return Vec::new();
        }
        tracing::info!(
            count = peers.len(),
            "membership subscribe: spawning peer subscriptions"
        );

        let mut handles = Vec::with_capacity(peers.len());
        for keys in peers {
            let me = self.clone();
            let cancel = cancel.clone();
            handles.push(tokio::spawn(async move {
                me.run_for_peer(keys, cancel).await;
            }));
        }
        handles
    }

    /// Pull a peer's identity-vault bundle blob and cache it
    /// locally. Called from `handle_event` on every identity-vault
    /// SET/Initial — without it, the registry entry would land
    /// locally but `resolve_did` couldn't materialise the
    /// `DidDocument` (the blob lives in the peer's stores, not
    /// ours), and the next membership rebuild would clear out the
    /// peer's entry.
    async fn fetch_bundle_blob(
        &self,
        peer: [u8; 32],
        hash: s5_core::Hash,
        blobs_client: &s5_blobs::Client,
    ) -> anyhow::Result<()> {
        use anyhow::anyhow;
        use s5_core::BlobsRead;

        for store in self.stores.values() {
            if store.blob_contains(hash).await.unwrap_or(false) {
                // Already-cached bundle blob still belongs to the public set —
                // register it for the public-ALPN handler (S3a).
                self.state.write().await.public_blob_hashes.insert(hash);
                return Ok(());
            }
        }
        // Cache into the node default store when resolvable, else any store.
        let primary = {
            let cfg = self.config.read().await;
            cfg.default_store_name()
                .and_then(|n| self.stores.get(n))
                .or_else(|| self.stores.values().next())
                .cloned()
                .ok_or_else(|| anyhow!("no stores configured — cannot cache peer bundle"))?
        };
        let bytes = blobs_client
            .blob_download(hash)
            .await
            .map_err(|e| anyhow!("blob download from peer failed: {e}"))?;
        let blob_id = primary
            .blob_upload_bytes(bytes)
            .await
            .map_err(|e| anyhow!("upload to local store failed: {e}"))?;
        tracing::info!(
            peer = %hex::encode(&peer[..4]),
            hash = %blob_id.hash,
            "membership subscribe: cached peer bundle blob locally"
        );
        // Identity-bundle blobs are public by design — register the
        // hash so the public-ALPN handler (S3b) serves it without a
        // challenge to anyone resolving the peer's DID. Slice S3a only
        // populates the set; gating activates in S3b.
        self.state
            .write()
            .await
            .public_blob_hashes
            .insert(blob_id.hash);
        Ok(())
    }

    /// Long-lived loop for one peer: dial, subscribe, drain events,
    /// reconnect on failure, exit on cancel.
    ///
    /// `keys` carries the three pubkeys we need: iroh transport (the
    /// QUIC dial target), master (identity-vault subscription key,
    /// slice 2d), and device signing (vault-data subscription key,
    /// `tasks::publish::device_signing_key`). The device-signing key
    /// being `None` means the peer's bundle had no `signers[]` entry
    /// (read-only / service-DID provisioning) — vault-data subscription
    /// is skipped for that peer.
    async fn run_for_peer(&self, keys: PeerSubKeys, cancel: CancellationToken) {
        let peer = keys.iroh;
        let peer_master = keys.master;
        let peer_device_signing = keys.device_signing;
        let peer_short = hex::encode(&peer[..4]);
        loop {
            if cancel.is_cancelled() {
                return;
            }

            // One persistent client per ALPN per peer per connect-attempt.
            // Both stay alive for the lifetime of the subscription
            // loop; on registry recv error we drop both and reconnect
            // the pair as a unit. The `irpc::Client`s are lazy — the
            // first call on each opens a QUIC connection over the
            // shared `Endpoint`, subsequent calls reuse it (new bi-
            // streams over the same connection, no re-handshake).
            let registry_client =
                match s5_registry::Client::connect_to_peer(self.endpoint.clone(), peer) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(peer = peer_short.as_str(), "invalid peer pubkey: {e:#}");
                        return;
                    }
                };
            // Identity-bundle fetches go over the public ALPN — they're
            // explicitly part of `public_blob_hashes` by design (DID
            // resolution is the bootstrap layer and must work without
            // authentication).
            let blobs_client =
                match s5_blobs::Client::connect_to_peer_public(self.endpoint.clone(), peer) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(peer = peer_short.as_str(), "invalid peer pubkey: {e:#}");
                        return;
                    }
                };

            // Identity-vault key (bundle rotations) plus one data
            // key per vault shared with this peer. Vault data keys
            // are only present once the local publish path has
            // registered the vault_id; until then we get
            // bundle-only subscription, and a respawn after the
            // first publish picks up the data keys.
            let mut keys: Vec<StreamKey> = Vec::new();
            // Both halves of the D17 resolution chain: the cold
            // pointer lives under the DID (cold) pubkey — watching it
            // is what makes warm *rotation* propagate — and the bundle
            // lives under the warm pubkey the pointer names.
            keys.push(StreamKey::Vault {
                pubkey: peer_master,
                vault_id: crate::identity_anchor::identity_anchor_id(),
            });
            let warm = {
                let state = self.state.read().await;
                state.warm_for_master.get(&peer_master).copied()
            };
            if let Some(warm_pub) = warm {
                keys.push(StreamKey::Vault {
                    pubkey: warm_pub,
                    vault_id: identity_vault_id(),
                });
            } else {
                // Peer not yet resolved (bootstrap-from-config-hint
                // path): the anchor subscription above is what will
                // deliver the pointer; the next membership refresh
                // resolves the bundle and respawns us with both keys.
                tracing::info!(
                    peer = peer_short.as_str(),
                    "membership subscribe: warm key unknown — anchor-only subscription"
                );
            }
            // Vault-data subscriptions: vault registry entries are
            // signed by the peer's device signing key — `StreamKey::Vault
            // { pubkey: device_signing_pubkey, vault_id }`. Subscribe
            // there, NOT under the iroh pubkey (pre-2d the two were
            // conflated; 2c-acl+bugfix splits them). If we don't yet
            // know the peer's device signing pubkey (e.g. their bundle
            // had no signers entry, or the membership refresh hasn't
            // populated `device_signing_for_peer` yet) we skip the
            // vault-data subscription — bundle rotations still flow via
            // the identity-vault subscription above.
            if let Some(ds) = peer_device_signing {
                let state = self.state.read().await;
                for (vault_name, vm) in &state.vaults {
                    if !vm.authorized_iroh_pubkeys.contains(&peer) {
                        continue;
                    }
                    if let Some(vault_id) = state.vault_id_for_name(vault_name) {
                        keys.push(StreamKey::Vault {
                            pubkey: ds,
                            vault_id,
                        });
                    }
                }
            }

            let key_summary: Vec<String> = keys
                .iter()
                .map(|k| match k {
                    StreamKey::Vault { vault_id, .. } => hex::encode(&vault_id[..8]),
                    other => format!("{other:?}"),
                })
                .collect();
            tracing::info!(
                peer = peer_short.as_str(),
                key_count = keys.len(),
                keys = ?key_summary,
                "membership subscribe: subscribing"
            );

            match registry_client.subscribe(keys, 64).await {
                Ok(mut rx) => {
                    tracing::info!(
                        peer = peer_short.as_str(),
                        "membership subscribe: connected"
                    );
                    loop {
                        tokio::select! {
                            _ = cancel.cancelled() => return,
                            ev = rx.recv() => {
                                match ev {
                                    Ok(Some(event)) => self.handle_event(peer, event, &blobs_client).await,
                                    Ok(None) => {
                                        tracing::info!(peer = peer_short.as_str(), "membership subscribe: stream closed by peer");
                                        break;
                                    }
                                    Err(e) => {
                                        tracing::warn!(peer = peer_short.as_str(), "membership subscribe: recv error: {e}");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        peer = peer_short.as_str(),
                        "membership subscribe: connect failed: {e}"
                    );
                }
            }

            // Backoff before reconnect — keeps a flapping peer from
            // hammering us. Linear 5 s; exponential is overkill for
            // the cardinality we're dealing with.
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        }
    }

    /// Apply a received event: write/delete the local registry,
    /// fetch the bundle blob if applicable (using the same long-
    /// lived `s5_blobs::Client` the caller passes in, so we don't
    /// re-handshake per event), then rebuild `MembershipState` if
    /// the key was an identity-vault SET (data-vault SETs
    /// propagate to consumers via the registry itself; they don't
    /// change which peers are members).
    async fn handle_event(
        &self,
        peer: [u8; 32],
        event: RegistryEvent,
        blobs_client: &s5_blobs::Client,
    ) {
        let (key_bytes, message_bytes_opt, kind) = match event {
            RegistryEvent::Initial { key, message } => (key, message, "initial"),
            RegistryEvent::Set { key, message } => (key, Some(message), "set"),
            RegistryEvent::Delete { key } => {
                if let Ok(parsed) = StreamKey::from_storage_key(&key)
                    && let Err(e) = self.registry.delete(&parsed).await
                {
                    tracing::warn!("local registry delete failed: {e}");
                }
                return;
            }
        };
        let Some(message_bytes) = message_bytes_opt else {
            // Initial-no-entry: the peer's registry has no value under
            // this key (or the per-key ACL denied us). Bumped from
            // silent-return to a log line so a wedged subscribe (peer
            // doesn't know about the data-vault yet) shows up in the
            // journal instead of looking like "everything's fine, no
            // data flowing."
            let key_summary = StreamKey::from_storage_key(&key_bytes)
                .map(|k| match k {
                    StreamKey::Vault { pubkey, vault_id } => format!(
                        "Vault peer={} vault_id={}",
                        hex::encode(&pubkey[..4]),
                        hex::encode(&vault_id[..8])
                    ),
                    other => format!("{other:?}"),
                })
                .unwrap_or_else(|_| "unparseable".to_string());
            tracing::info!(
                kind,
                key = %key_summary,
                "membership subscribe: initial-no-entry (peer registry empty under this key)"
            );
            return;
        };

        let parsed_key = StreamKey::from_storage_key(&key_bytes).ok();
        let is_identity_vault_event = matches!(
            parsed_key.as_ref(),
            Some(StreamKey::Vault { vault_id, .. }) if *vault_id == identity_vault_id()
        );
        // Cold-pointer re-point (D17): the payload rides inline in the
        // entry, so there is no blob to fetch — but membership MUST be
        // rebuilt, since the peer's bundle now lives under a different
        // warm key.
        let is_anchor_event = matches!(
            parsed_key.as_ref(),
            Some(StreamKey::Vault { vault_id, .. })
                if *vault_id == crate::identity_anchor::identity_anchor_id()
        );
        let event_summary = match parsed_key.as_ref() {
            Some(StreamKey::Vault { pubkey, vault_id }) => {
                let role = if *vault_id == identity_vault_id() {
                    "identity"
                } else if *vault_id == crate::identity_anchor::identity_anchor_id() {
                    "anchor"
                } else {
                    "data"
                };
                format!(
                    "{role} vault peer={} vault_id={}",
                    hex::encode(&pubkey[..4]),
                    hex::encode(&vault_id[..4]),
                )
            }
            _ => "non-vault key".to_string(),
        };

        let msg = match StreamMessage::deserialize(Bytes::from(message_bytes)) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("decoding subscribed StreamMessage failed: {e}");
                return;
            }
        };
        let blob_hash = msg.hash;
        let msg_revision = msg.revision;
        if let Err(e) = self.registry.set(msg).await {
            tracing::warn!("local registry set from subscription failed: {e}");
            return;
        }
        tracing::info!(kind, %event_summary, "membership subscribe: applied event");

        // Pull the blob from the peer for identity-vault events.
        // The registry entry alone isn't enough — `resolve_did`
        // expects the bundle blob in one of the local stores. We
        // fetch from the same peer we got the event from over
        // `s5/blobs/0`. Best-effort: a failure here keeps the
        // bootstrap-fallback ACL entry alive so the next attempt
        // can still dial.
        if is_identity_vault_event
            && let Err(e) = self.fetch_bundle_blob(peer, blob_hash, blobs_client).await
        {
            tracing::warn!(
                peer = %hex::encode(&peer[..4]),
                hash = %blob_hash,
                "membership subscribe: bundle-blob fetch failed: {e:#}"
            );
        }

        if !is_identity_vault_event && !is_anchor_event {
            // Data-vault HEAD update: nothing membership-related to
            // recompute. Fire the broadcast so reactors elsewhere in
            // the process (e.g. an embedding host's snapshot watcher) can
            // react without polling the local registry.
            if let Some(tx) = self.data_events.as_ref()
                && let Some(StreamKey::Vault { pubkey, vault_id }) = parsed_key
            {
                // send() only fails when there are zero receivers;
                // that's the no-listeners case and a no-op for us.
                let _ = tx.send(DataVaultEvent {
                    peer_pubkey: pubkey,
                    vault_id,
                    blob_hash,
                    revision: msg_revision,
                });
            }
            return;
        }
        let cfg = self.config.read().await.clone();
        let resolved =
            build_membership_state(&self.self_did, &cfg, self.registry.as_ref(), &self.stores)
                .await;
        let mut current = self.state.write().await;
        if !state_equiv(&current, &resolved) {
            tracing::info!("membership subscribe: bundle changed → ACL state updated");
            // Merge vault_id_by_name from both sources (see swap in
            // `run_lifecycle` for the rationale).
            let mut preserved = std::mem::take(&mut current.vault_id_by_name);
            *current = resolved;
            for (id, name) in preserved.drain() {
                current.vault_id_by_name.entry(id).or_insert(name);
            }
            drop(current);
            // Kick the lifecycle loop to respawn per-peer tasks with
            // the freshly-resolved keys. Without this, `run_for_peer`
            // stays on its initial subscribe (typically identity-only
            // when started against an empty local registry) and never
            // picks up the data-vault stream key.
            self.refresh.notify_one();
        }
    }
}

/// Shallow comparison sufficient to decide whether to log+swap. Same
/// shape as the comparison previously sketched on the polling-based
/// path: comparing the operationally-important fields per vault.
fn state_equiv(a: &MembershipState, b: &MembershipState) -> bool {
    if a.vaults.len() != b.vaults.len() {
        return false;
    }
    for (name, va) in &a.vaults {
        let Some(vb) = b.vaults.get(name) else {
            return false;
        };
        if va.authorized_iroh_pubkeys != vb.authorized_iroh_pubkeys
            || va.age_recipients != vb.age_recipients
            || va.store_names != vb.store_names
            || va.member_dids.len() != vb.member_dids.len()
        {
            return false;
        }
    }
    true
}
