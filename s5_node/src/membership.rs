//! Resolve `vault.<name>.members` → IdentityBundles → derived per-vault state.
//!
//! For each member named in a vault's config (either `"self"` for this
//! daemon's own DID, or a `[friend.<nick>]` lookup), this module
//!
//! 1. queries the registry under `(master_pubkey, IDENTITY_VAULT_ID)`,
//! 2. downloads the bundle blob from the first reachable store
//!    (`BlobStore::blob_download` BLAKE3-verifies the bytes against
//!    `entry.hash`),
//! 3. CBOR-decodes the [`IdentityBundle`]. Integrity comes from
//!    F01-verified registry signature + content-addressed blob hash —
//!    no bundle-level signature is expected, per
//!    `identity-model.md` § Identity bundle.
//! 4. reads `iroh_pubkeys` and `age_recipients` from the bundle's flat
//!    keyset arrays,
//!
//! and accumulates them into a per-vault [`VaultMembership`].
//!
//! Step 3 (transport ACL) reads `authorized_iroh_pubkeys` to filter
//! inbound connections; step 5 reads `age_recipients` to derive the
//! encryption recipient set without hand-editing `vault.recipients`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use s5_core::identity::{Did, IdentityBundle};
use s5_core::{RegistryApi, StreamKey};

use crate::config::S5NodeConfig;
use crate::identity_vault::identity_vault_id;

/// Per-vault membership state, derived from resolving each member's
/// published IdentityBundle.
#[derive(Debug, Default, Clone)]
pub struct VaultMembership {
    /// DIDs that named this vault as a destination. Useful for
    /// diagnostics; the operational sets are below.
    pub member_dids: Vec<Did>,
    /// Iroh pubkeys allowed to talk to this daemon about this vault.
    /// Read by the transport-accept filter (step 3).
    pub authorized_iroh_pubkeys: HashSet<[u8; 32]>,
    /// ed25519 ACL/read pubkeys (`bundle.acl_keys[]`) for this vault's
    /// member identities. The F02 blob-fetch challenge (`acl-and-
    /// revocation.md §3a`) accepts a connection iff the client's
    /// presented `acl_pubkey ∈ authorized_acl_pubkeys` for some vault
    /// whose stores contain the requested blob. Populated by `merge_into`
    /// from every verified bundle; consumed by `MembershipBlobAcl`'s
    /// acl-ALPN check in slice S3b.
    pub authorized_acl_pubkeys: HashSet<[u8; 32]>,
    /// Age recipients to encrypt this vault's published TN to. Drives
    /// the encryption side once the auto-recipients pass lands (step 5).
    pub age_recipients: Vec<String>,
    /// Named stores that hold this vault's data and meta blobs —
    /// resolved `{data_store, meta_store}` set from config (D1).
    ///
    /// The per-blob ACL (`MembershipBlobAcl`) approves a fetch from
    /// peer `P` for hash `H` iff `P ∈ authorized_iroh_pubkeys` for
    /// some vault and `H` is contained in one of that vault's
    /// `store_names`. Coarser than a per-snapshot reachable-set but
    /// adequate for the typical "each vault has dedicated stores"
    /// configuration; cross-vault dedup (architecture-directions §
    /// "Cross-vault dedup via shared CAS") is an explicit opt-in
    /// feature that accepts the wider blast radius.
    ///
    /// `Snapshot::walk_hashes` / `Snapshot::collect_reachable_chunks`
    /// remain available for future tighter granularity — `vup check`
    /// / `vup repair` / GC tooling — without being on the ACL hot path.
    pub store_names: Vec<String>,
}

/// All resolved memberships, keyed by vault name.
#[derive(Debug, Default)]
pub struct MembershipState {
    pub vaults: HashMap<String, VaultMembership>,
    /// `iroh_pubkey → master_pubkey` for every peer the daemon knows
    /// about. The subscriber uses this to subscribe to each peer's
    /// identity-vault registry stream under the *master* pubkey (where
    /// the bundle is actually published — `acl-and-revocation.md §1`
    /// verify chain), since master ≠ iroh from slice 2d onwards.
    ///
    /// Populated during `merge_into` from each verified bundle: for
    /// every `iroh_pubkey ∈ bundle.iroh_pubkeys`, the entry maps to the
    /// DID's master pubkey. A device hosting multiple co-resident
    /// identities (per `identity-model.md` § Multi-identity) results in
    /// one entry per identity sharing that transport key; insertion
    /// order is which DID we'd subscribe under — not load-bearing for
    /// correctness since the subscriber spawns per peer regardless.
    pub master_for_peer: HashMap<[u8; 32], [u8; 32]>,
    /// `did_pubkey (cold) → warm_pubkey` for every resolved identity
    /// (D17). The subscriber needs both halves of the two-step chain:
    /// the anchor stream lives under the cold pubkey, the bundle
    /// stream under the warm pubkey it points at. Populated whenever
    /// `resolve_did` succeeds for a member.
    pub warm_for_master: HashMap<[u8; 32], [u8; 32]>,
    /// `iroh_pubkey → device_signing_pubkey` for every peer. The
    /// subscriber uses this to subscribe to each peer's *vault-data*
    /// registry entries — those are signed by the peer's device
    /// signing key (`tasks::publish::device_signing_key`), so
    /// `StreamKey::Vault.pubkey` carries the signing pubkey, not iroh.
    /// Populated alongside `master_for_peer` from `bundle.signers[0]`
    /// in each resolved bundle.
    pub device_signing_for_peer: HashMap<[u8; 32], [u8; 32]>,
    /// `device_signing_pubkey → master_pubkey` — the reverse index of
    /// every signer ever seen in a verified bundle's `signers[]`. The
    /// registry ACL consults this to decide whether to accept a
    /// non-identity vault registry write (F01 verify-chain level 3 /
    /// `acl-and-revocation.md §3b`): a write under
    /// `StreamKey::Vault.pubkey = X` is accepted only if `X` is a
    /// signer of some published bundle. Identity-vault writes are
    /// special-cased — they ARE the bundle and so have no prior
    /// bundle to check against; F01 signature verification on the
    /// master key (whose pubkey is the StreamKey.pubkey AND the DID)
    /// is the security gate there.
    pub did_for_device_signing: HashMap<[u8; 32], [u8; 32]>,
    /// Maps vault `vault_id` (16 bytes, derived from the vault root's
    /// `KEY_SLOT_RECOVERY` slot) to vault name. Populated by
    /// `publish::run_publish` on every successful publish — the publish
    /// path has `recovery_secret` in scope, so it can call
    /// `MembershipState::register_vault_id` cheaply. Empty until the
    /// first publish for each vault; until then,
    /// `is_authorized_for_vault_id` denies non-identity-vault registry
    /// requests from peers (we cannot verify membership without the
    /// mapping).
    pub vault_id_by_name: HashMap<[u8; 16], String>,
    /// Vault names whose published Transparent Node ships as raw CBOR
    /// (`plaintext_published_tn = true`). These vaults are intended for
    /// public read — any peer that knows the publisher's iroh pubkey +
    /// the vault_id should be able to subscribe to its registry stream
    /// key and fetch its blobs.
    ///
    /// Names rather than vault_ids because the vault_id is derived from
    /// `KEY_SLOT_RECOVERY` at publish time — when `build_membership_state`
    /// runs we may not have published yet, so the id isn't known.
    /// `is_public_read_vault_id` resolves the id via `vault_id_by_name`
    /// at lookup time.
    pub public_read_vault_names: HashSet<String>,
    /// BLAKE3 hashes of blobs that may be served on the public (no-
    /// challenge) blobs ALPN. Populated from two sources today (slice
    /// S3a):
    /// - Own identity-bundle blob hash, inserted by
    ///   `identity_vault::publish_self_on_startup` after a successful
    ///   bundle publish.
    /// - Peer identity-bundle blob hash, inserted by
    ///   `membership_subscribe::fetch_bundle_blob` after caching a
    ///   peer's bundle blob locally.
    ///
    /// Consumed by the public-ALPN handler (S3b): a blob fetch on the
    /// public ALPN is served iff its hash is in this set. The general
    /// `plaintext_published_tn = true` vault case (walking reachable
    /// blobs and tagging them all public) is a follow-up; today the
    /// override at `s5_node::lib::blob_acl_override = PermitAllBlobAcl`
    /// is the workaround for that case.
    pub public_blob_hashes: HashSet<s5_core::Hash>,
}

impl MembershipState {
    /// Record `vault_name → vault_id` so per-vault registry ACL checks
    /// can resolve the name from a wire-level `vault_id`. Idempotent.
    /// Returns `true` iff this changed the mapping (caller may want to
    /// trigger a refresh of subscribers that key off the mapping).
    pub fn register_vault_id(&mut self, vault_name: &str, vault_id: [u8; 16]) -> bool {
        match self.vault_id_by_name.get(&vault_id) {
            Some(existing) if existing == vault_name => false,
            _ => {
                self.vault_id_by_name
                    .insert(vault_id, vault_name.to_string());
                true
            }
        }
    }

    /// Inverse lookup: `vault_name → vault_id`. Linear scan over the
    /// (small) map. Returns `None` until publish has populated the
    /// entry for this vault.
    pub fn vault_id_for_name(&self, vault_name: &str) -> Option<[u8; 16]> {
        self.vault_id_by_name
            .iter()
            .find_map(|(id, name)| (name == vault_name).then_some(*id))
    }

    /// True iff `pubkey` appears in some served vault's `authorized_iroh_pubkeys`.
    /// This is the union ACL the transport-level hook checks on every inbound
    /// connection.
    pub fn is_authorized_iroh_pubkey(&self, pubkey: &[u8; 32]) -> bool {
        self.vaults
            .values()
            .any(|vm| vm.authorized_iroh_pubkeys.contains(pubkey))
    }

    /// True iff any served vault is configured for public reads via
    /// `plaintext_published_tn = true`. Used ONLY by the transport hook to
    /// decide whether to open `s5/registry/1` / `s5/blobs/0` to anonymous
    /// peers at all — a coarse "is anyone public here" gate. Per-blob and
    /// per-registry-key authorisation is the FINER
    /// [`Self::is_public_read_vault_id`] check (D14): opening the ALPN does
    /// not grant read of every vault.
    pub fn has_public_read_vault(&self) -> bool {
        !self.public_read_vault_names.is_empty()
    }

    /// True iff the specific `vault_id` names a `plaintext_published_tn`
    /// (public-read) vault. This is the per-vault gate (D14): a daemon that
    /// serves one public vault must NOT thereby serve *every* vault's
    /// registry/blobs to anyone — only the public vault's own `vault_id` is
    /// open. Resolves the id via `vault_id_by_name`; an unknown id (never
    /// published, or another vault entirely) is not public.
    pub fn is_public_read_vault_id(&self, vault_id: &[u8; 16]) -> bool {
        self.vault_id_by_name
            .get(vault_id)
            .map(|name| self.public_read_vault_names.contains(name))
            .unwrap_or(false)
    }

    /// True iff `pubkey` is authorised to access registry entries
    /// under the given `vault_id`. Identity-vault entries (constant
    /// `IDENTITY_VAULT_ID`) and cold-pointer anchors (constant
    /// `IDENTITY_ANCHOR_ID`, D17) are public lookup keys and always
    /// authorised — DID resolution must work for strangers. For
    /// other vault_ids, the vault name is looked up in
    /// `vault_id_by_name` and `pubkey` checked against that vault's
    /// `authorized_iroh_pubkeys`.
    pub fn is_authorized_for_vault_id(&self, pubkey: &[u8; 32], vault_id: &[u8; 16]) -> bool {
        if *vault_id == crate::identity_vault::identity_vault_id()
            || *vault_id == crate::identity_anchor::identity_anchor_id()
        {
            return true;
        }
        // Public-read is PER VAULT (D14): only the specific vault_id of a
        // `plaintext_published_tn` vault is open to anyone. Serving one public
        // vault does NOT open every other (private) vault's registry/blobs —
        // the previous blanket `has_public_read_vault()` short-circuit did,
        // silently collapsing the ACL for every vault on a mixed daemon.
        if self.is_public_read_vault_id(vault_id) {
            return true;
        }
        let Some(vault_name) = self.vault_id_by_name.get(vault_id) else {
            return false;
        };
        self.vaults
            .get(vault_name)
            .map(|vm| vm.authorized_iroh_pubkeys.contains(pubkey))
            .unwrap_or(false)
    }
}

/// `RegistryAcl` impl reading the same shared `MembershipState` the
/// transport-level `MembershipHook` consults. Used for the
/// per-request authorisation hook on `s5_registry::RegistryServer`.
///
/// Inspired by iroh-blobs upstream's provider-events `EventMask` +
/// `RequestMode::Intercept` pattern (iroh-blobs 0.93+) — the daemon
/// gets to say "this peer cannot read this entry" without coupling
/// `s5_registry` to identity/membership concerns.
#[derive(Debug, Clone)]
pub struct MembershipRegistryAcl {
    state: std::sync::Arc<tokio::sync::RwLock<MembershipState>>,
}

impl MembershipRegistryAcl {
    pub fn new(state: std::sync::Arc<tokio::sync::RwLock<MembershipState>>) -> Self {
        Self { state }
    }
}

/// `BlobAcl` impl reading the same shared `MembershipState` the
/// transport-level `MembershipHook` and the `MembershipRegistryAcl`
/// consult. Approves a blob fetch from peer `P` for hash `H` iff
/// some served vault `V` has `P ∈ V.authorized_iroh_pubkeys` AND
/// `H` is contained in one of `V.store_names`.
///
/// Why store-membership instead of a per-snapshot reachable-set:
/// the chosen design preserves access to *older* snapshots — a peer
/// who retained an old snapshot's root hash can still walk it and
/// fetch its blobs, which would be blocked under a strict
/// reachable-set ACL even though the peer was already authorised
/// when they originally walked. ("If you know the hash, you know"
/// — the architecture-directions § "Cross-vault dedup via shared
/// CAS" framing.) The constraint is "do not mix vault meta blobs
/// with unrelated risky data in the same store" — vault config
/// stores are scoped per-vault by convention.
///
/// `Snapshot::walk_hashes` / `collect_reachable_chunks` remain
/// available for tools that DO want per-snapshot granularity (GC,
/// `vup check`, `vup repair`) — separate use cases, not on the
/// ACL path.
#[derive(Clone)]
pub struct MembershipBlobAcl {
    state: std::sync::Arc<tokio::sync::RwLock<MembershipState>>,
    stores:
        std::sync::Arc<std::collections::HashMap<String, std::sync::Arc<dyn s5_core::blob::Blobs>>>,
}

impl std::fmt::Debug for MembershipBlobAcl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MembershipBlobAcl")
            .field("stores", &self.stores.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl MembershipBlobAcl {
    pub fn new(
        state: std::sync::Arc<tokio::sync::RwLock<MembershipState>>,
        stores: std::sync::Arc<
            std::collections::HashMap<String, std::sync::Arc<dyn s5_core::blob::Blobs>>,
        >,
    ) -> Self {
        Self { state, stores }
    }
}

#[async_trait::async_trait]
impl s5_blobs::BlobAcl for MembershipBlobAcl {
    /// Public-ALPN gate: serve iff the requested hash is in
    /// `MembershipState.public_blob_hashes`. The set is currently
    /// populated only with own + peer identity-bundle blob hashes
    /// (S3a) — i.e. the DID-resolution bootstrap layer is the only
    /// thing reachable without authentication.
    ///
    /// TODO(S4-public-vault): extend `public_blob_hashes` with the
    /// reachable-blob set of every vault whose config has
    /// `plaintext_published_tn = true`. Walk happens at publish time
    /// (`tasks::publish::run_publish` already has the snapshot in
    /// scope; emit reachable hashes via
    /// `Snapshot::walk_hashes` / `collect_reachable_chunks`). With
    /// that in place, deployments currently relying on a
    /// `PermitAllBlobAcl` blanket override can drop the override
    /// and use the proper per-vault gate — they keep the same
    /// world-readable behaviour for their public vault, but mixed
    /// public/private vaults on the same daemon stop leaking private
    /// blobs across the gate.
    async fn allow_public_read(&self, hash: &s5_core::Hash) -> bool {
        let state = self.state.read().await;
        state.public_blob_hashes.contains(hash)
    }

    /// ACL-ALPN gate: the connection is already bound to `principal`
    /// (the device ACL pubkey) via the F02 challenge. We approve the
    /// read iff (a) some served vault includes `principal` in its
    /// `authorized_acl_pubkeys`, AND (b) one of that vault's stores
    /// actually contains the requested hash. The two-clause check is
    /// what stops a member of vault X from reading vault Y's blobs by
    /// hash alone.
    async fn allow_acl_read(&self, principal: &[u8; 32], hash: &s5_core::Hash) -> bool {
        let store_names: Vec<String> = {
            let state = self.state.read().await;
            let mut seen = std::collections::HashSet::<String>::new();
            let mut out = Vec::new();
            for vm in state.vaults.values() {
                if vm.authorized_acl_pubkeys.contains(principal) {
                    for s in &vm.store_names {
                        if seen.insert(s.clone()) {
                            out.push(s.clone());
                        }
                    }
                }
            }
            out
        };
        if store_names.is_empty() {
            return false;
        }
        for name in &store_names {
            if let Some(store) = self.stores.get(name)
                && store.blob_contains(*hash).await.unwrap_or(false)
            {
                return true;
            }
        }
        false
    }

    /// F02 challenge-time gate: accept the principal iff it appears in
    /// `authorized_acl_pubkeys` for *any* served vault. This rejects
    /// unknown ACL pubkeys at connection-open, before they can mint a
    /// useless bound connection. Per-blob authorisation still goes
    /// through `allow_acl_read` (which adds the store-contains check).
    async fn allow_acl_principal(&self, principal: &[u8; 32]) -> bool {
        let state = self.state.read().await;
        state
            .vaults
            .values()
            .any(|vm| vm.authorized_acl_pubkeys.contains(principal))
    }
}

#[async_trait::async_trait]
impl s5_registry::RegistryAcl for MembershipRegistryAcl {
    async fn allow_read(&self, peer_pubkey: &[u8; 32], key: &s5_core::StreamKey) -> bool {
        match key {
            s5_core::StreamKey::Vault { vault_id, .. } => {
                let state = self.state.read().await;
                state.is_authorized_for_vault_id(peer_pubkey, vault_id)
            }
            // Legacy non-vault stream keys (s5_fs DirActor): no
            // membership to map; allow if the peer is in any vault
            // (transport-level check).
            _ => {
                let state = self.state.read().await;
                state.is_authorized_iroh_pubkey(peer_pubkey)
            }
        }
    }

    async fn allow_write(&self, peer_pubkey: &[u8; 32], key: &s5_core::StreamKey) -> bool {
        let state = self.state.read().await;
        match key {
            s5_core::StreamKey::Vault {
                pubkey: writer_pubkey,
                vault_id,
            } => {
                // Transport ACL gate (which connections may even write).
                if !state.is_authorized_for_vault_id(peer_pubkey, vault_id) {
                    return false;
                }
                // F01 verify-chain level 3 (`acl-and-revocation.md §3b`):
                // for non-identity vault writes, the writer pubkey
                // carried in the StreamKey must be a recognised device
                // signing key — `∈ did_for_device_signing` (i.e. some
                // published bundle's signers[]). Identity-vault writes
                // and cold-pointer anchors (D17) are special-cased:
                // they ARE the resolution chain, so there is no prior
                // bundle to check against; F01 signature verification
                // (`StreamMessage::new`) under the StreamKey's own
                // embedded pubkey is the security gate there — any key
                // may publish *its own* anchor/bundle, and consumers
                // decide which pubkey to trust by walking the chain
                // from the DID.
                if *vault_id == crate::identity_vault::identity_vault_id()
                    || *vault_id == crate::identity_anchor::identity_anchor_id()
                {
                    true
                } else {
                    state.did_for_device_signing.contains_key(writer_pubkey)
                }
            }
            // Non-vault stream keys (Local / Blake3HashPin): same
            // envelope as transport ACL — they have no signature and
            // no per-key authorisation principal.
            _ => state.is_authorized_iroh_pubkey(peer_pubkey),
        }
    }
}

#[cfg(test)]
mod acl_tests {
    use super::*;

    #[test]
    fn empty_state_authorises_no_one() {
        let s = MembershipState::default();
        assert!(!s.is_authorized_iroh_pubkey(&[0u8; 32]));
        assert!(!s.is_authorized_iroh_pubkey(&[42u8; 32]));
    }

    #[test]
    fn pubkey_in_any_vault_is_authorised() {
        let mut s = MembershipState::default();
        let alice = [11u8; 32];
        let bob = [22u8; 32];
        let mut va = VaultMembership::default();
        va.authorized_iroh_pubkeys.insert(alice);
        s.vaults.insert("vault_a".into(), va);
        let mut vb = VaultMembership::default();
        vb.authorized_iroh_pubkeys.insert(bob);
        s.vaults.insert("vault_b".into(), vb);

        assert!(s.is_authorized_iroh_pubkey(&alice));
        assert!(s.is_authorized_iroh_pubkey(&bob));
        assert!(!s.is_authorized_iroh_pubkey(&[33u8; 32]));
    }

    /// D14: a public-read vault opens ONLY its own vault_id to anonymous
    /// reads — a private vault on the same daemon stays gated. The previous
    /// blanket `has_public_read_vault()` short-circuit exposed every vault.
    #[test]
    fn public_read_is_scoped_to_the_public_vaults_id() {
        let mut s = MembershipState::default();
        let public_id = [1u8; 16];
        let private_id = [2u8; 16];
        let stranger = [99u8; 32];

        // "pub" is a plaintext_published_tn vault; "priv" is a normal one
        // whose only authorised reader is `member`.
        s.public_read_vault_names.insert("pub".into());
        s.register_vault_id("pub", public_id);
        s.register_vault_id("priv", private_id);
        let member = [7u8; 32];
        let mut vpriv = VaultMembership::default();
        vpriv.authorized_iroh_pubkeys.insert(member);
        s.vaults.insert("priv".into(), vpriv);

        // Anyone may read the public vault's id...
        assert!(s.is_authorized_for_vault_id(&stranger, &public_id));
        // ...but NOT the private vault's id, even though a public vault exists.
        assert!(!s.is_authorized_for_vault_id(&stranger, &private_id));
        // The private vault's member still reads it.
        assert!(s.is_authorized_for_vault_id(&member, &private_id));
        // An unknown vault_id is never public.
        assert!(!s.is_authorized_for_vault_id(&stranger, &[3u8; 16]));
    }

    /// F01 verify-chain level 3: a vault registry write under
    /// `StreamKey::Vault.pubkey = X` is accepted iff `X` is a
    /// recognised device signing key (∈ `did_for_device_signing`),
    /// once the transport ACL has already passed. Identity-vault
    /// writes are explicitly NOT gated by this check (their
    /// authorisation is the F01 master signature itself).
    #[tokio::test]
    async fn allow_write_gates_vault_data_on_did_for_device_signing() {
        use s5_core::StreamKey;
        use s5_registry::RegistryAcl;
        use std::sync::Arc;
        use tokio::sync::RwLock;

        let mut s = MembershipState::default();
        let peer_iroh = [1u8; 32];
        let recognised_signer = [2u8; 32];
        let recognised_master = [3u8; 32];
        let unrecognised_signer = [99u8; 32];
        let vault_id = [0x42u8; 16];

        // Authorise the connection + name a vault → vault_id.
        let mut vm = VaultMembership::default();
        vm.authorized_iroh_pubkeys.insert(peer_iroh);
        s.vaults.insert("vault_a".into(), vm);
        s.vault_id_by_name.insert(vault_id, "vault_a".into());
        s.did_for_device_signing
            .insert(recognised_signer, recognised_master);

        let acl = MembershipRegistryAcl::new(Arc::new(RwLock::new(s)));

        // Recognised signer ⇒ accept.
        let good = StreamKey::Vault {
            pubkey: recognised_signer,
            vault_id,
        };
        assert!(acl.allow_write(&peer_iroh, &good).await);

        // Unrecognised signer ⇒ reject even though the connection is
        // authorised (sig is valid per F01 but signer isn't in any
        // known bundle.signers[]).
        let bad = StreamKey::Vault {
            pubkey: unrecognised_signer,
            vault_id,
        };
        assert!(!acl.allow_write(&peer_iroh, &bad).await);

        // Identity-vault writes bypass the level-3 check — the master
        // signature is the authorisation primitive there. Connection
        // ACL has its own always-allow branch for identity_vault, so
        // an unrecognised signer still goes through.
        let id_write = StreamKey::Vault {
            pubkey: unrecognised_signer,
            vault_id: crate::identity_vault::identity_vault_id(),
        };
        assert!(acl.allow_write(&peer_iroh, &id_write).await);
    }

    /// D11: `merge_into` admits a member's signers as vault writers ONLY when
    /// the member is write-capable. A read-only member still gets the read
    /// ACL (iroh + acl_keys) and age recipients, but its signers are NOT
    /// added to `did_for_device_signing` — so its registry writes are
    /// rejected by `allow_write`.
    #[test]
    fn capability_is_keyset_membership() {
        use s5_core::identity::{Did, DidMasterPubkey, IdentityBundle};

        let bundle = IdentityBundle {
            version: IdentityBundle::CURRENT_VERSION,
            revision: 1,
            signers: vec![[0x11u8; 32]],
            acl_keys: vec![[0x22u8; 32]],
            iroh_pubkeys: vec![[0x33u8; 32]],
            age_recipients: vec!["age1reader".to_string()],
        };
        let did = Did::from_pubkey(DidMasterPubkey::new([0x44u8; 32]));

        // Read-only member.
        {
            let mut state = VaultMembership::default();
            let mut did_ds = HashMap::new();
            let mut master = HashMap::new();
            let mut warm = HashMap::new();
            let mut ds = HashMap::new();
            let mut pk = PeerKeyMaps {
                master_for_peer: &mut master,
                warm_for_master: &mut warm,
                device_signing_for_peer: &mut ds,
                did_for_device_signing: &mut did_ds,
            };
            merge_into(&mut state, &mut pk, &did, &bundle, false);
            // Read access: yes. Write authority: no.
            assert!(state.authorized_iroh_pubkeys.contains(&[0x33u8; 32]));
            assert!(state.authorized_acl_pubkeys.contains(&[0x22u8; 32]));
            assert_eq!(state.age_recipients, vec!["age1reader".to_string()]);
            assert!(
                did_ds.is_empty(),
                "read-only member must not become a recognised writer"
            );
        }

        // Writer.
        {
            let mut state = VaultMembership::default();
            let mut did_ds = HashMap::new();
            let mut master = HashMap::new();
            let mut warm = HashMap::new();
            let mut ds = HashMap::new();
            let mut pk = PeerKeyMaps {
                master_for_peer: &mut master,
                warm_for_master: &mut warm,
                device_signing_for_peer: &mut ds,
                did_for_device_signing: &mut did_ds,
            };
            merge_into(&mut state, &mut pk, &did, &bundle, true);
            assert_eq!(
                did_ds.get(&[0x11u8; 32]),
                Some(&[0x44u8; 32]),
                "writer's signer is admitted under its master"
            );
        }
    }
}

/// `EndpointHooks` implementation that rejects inbound connections from
/// peers not in any served vault's `authorized_iroh_pubkeys`.
///
/// Uses iroh 0.96+'s `after_handshake` interception point: we get the
/// remote's verified `EndpointId` (== ed25519 pubkey, 32 bytes) and the
/// negotiated ALPN, and can return `AfterHandshakeOutcome::Reject` with
/// a close-frame error code. Per the iroh 0.96 release notes the hook
/// is intended for exactly this — "authentication, authorization, rate
/// limiting at the connection layer" — keeping the protocol handlers
/// (BlobsServer, RegistryServer, S5NodeServer) focused on protocol
/// logic. iroh's `auth-hook` example demonstrates the same pattern.
///
/// Bootstrap order: `MembershipState` is shared via `Arc<RwLock<>>` so the
/// hook can be installed at endpoint creation (before any membership has
/// been resolved) and refreshed once `build_membership_state` runs after
/// the registry+stores are up. During the brief window where the state is
/// empty all inbound connections are rejected; peers will retry.
#[derive(Debug, Clone)]
pub struct MembershipHook {
    state: std::sync::Arc<tokio::sync::RwLock<MembershipState>>,
}

impl MembershipHook {
    pub fn new(state: std::sync::Arc<tokio::sync::RwLock<MembershipState>>) -> Self {
        Self { state }
    }
}

impl iroh::endpoint::EndpointHooks for MembershipHook {
    async fn after_handshake(
        &self,
        conn: &iroh::endpoint::Connection,
    ) -> iroh::endpoint::AfterHandshakeOutcome {
        // The control-RPC ALPN (`s5/node/0`) is deliberately ABSENT here:
        // since the F03 fix it is served exclusively on the dedicated
        // loopback-bound control endpoint behind the lock-file cookie (see
        // `ControlPlane` in lib.rs). A control dial arriving at THIS public
        // endpoint is an unknown ALPN — it falls through to the membership
        // checks below and its router has no handler for it either.
        //
        // The pair handshake ALPN (`s5/pair/0`) is bypassed — any
        // peer must be able to dial it to redeem a token. The
        // protocol's auth is the one-time secret carried in the
        // request body, validated server-side by the listener.
        if conn.alpn() == crate::pair::PAIR_ALPN {
            return iroh::endpoint::AfterHandshakeOutcome::Accept;
        }
        // Same for the device-enrollment ALPN (`s5/enroll/0`, D10): the
        // joining device is by definition not yet a member of anything;
        // the one-time invite secret is the auth.
        if conn.alpn() == crate::enroll::ENROLL_ALPN {
            return iroh::endpoint::AfterHandshakeOutcome::Accept;
        }
        // Public read shortcut: when a served vault is configured
        // `plaintext_published_tn = true`, the read ALPNs accept any peer at
        // the CONNECTION level. This only opens the pipe — per-message
        // authorisation is the finer gate above: `MembershipRegistryAcl`
        // authorises a registry read only for the SPECIFIC public vault's
        // `vault_id` (`is_public_read_vault_id`, D14), so a mixed daemon does
        // NOT expose its private vaults just because it also serves a public
        // one; and `MembershipBlobAcl` gates blobs per-hash (a fully-public
        // publisher opts into `PermitAllBlobAcl` explicitly). Non-read ALPNs
        // (control RPC, future write paths) still require membership.
        let pubkey = *conn.remote_id().as_bytes();
        if conn.alpn() == s5_registry::ALPN
            || conn.alpn() == s5_blobs::ALPN_PUBLIC
            || conn.alpn() == s5_blobs::ALPN_ACL
        {
            let any_public = {
                let state = self.state.read().await;
                state.has_public_read_vault()
            };
            if any_public {
                tracing::debug!(
                    peer = hex::encode(pubkey).as_str(),
                    alpn = String::from_utf8_lossy(conn.alpn()).as_ref(),
                    "accepting public-read connection"
                );
                return iroh::endpoint::AfterHandshakeOutcome::Accept;
            }
        }
        let authorized = {
            let state = self.state.read().await;
            state.is_authorized_iroh_pubkey(&pubkey)
        };
        if authorized {
            tracing::info!(
                peer = hex::encode(pubkey).as_str(),
                alpn = String::from_utf8_lossy(conn.alpn()).as_ref(),
                "accepting authorised peer connection"
            );
            iroh::endpoint::AfterHandshakeOutcome::Accept
        } else {
            tracing::warn!(
                peer = hex::encode(pubkey).as_str(),
                alpn = String::from_utf8_lossy(conn.alpn()).as_ref(),
                "rejecting unauthorised iroh connection (peer not a member of any served vault)"
            );
            iroh::endpoint::AfterHandshakeOutcome::Reject {
                error_code: iroh::endpoint::VarInt::from_u32(0x10),
                reason: b"s5: peer not a member of any served vault".to_vec(),
            }
        }
    }
}

/// A DID resolved through the two-step chain: the bundle plus the warm
/// pubkey the cold pointer named on the way.
pub struct ResolvedIdentity {
    pub bundle: IdentityBundle,
    /// The warm master pubkey the DID's cold pointer currently names —
    /// where the bundle stream lives, and the key its entries are
    /// signed under.
    pub warm_pub: [u8; 32],
}

/// Resolve `did` to its [`IdentityBundle`] via the two-step walk
/// (D17, `identity-rotation.md §4.3`): cold pointer at
/// `(did_pubkey, IDENTITY_ANCHOR_ID)` names the current **warm**
/// master; the bundle lives at `(warm_pub, IDENTITY_VAULT_ID)` and is
/// warm-signed. There is deliberately no single-hop fallback — that
/// would reopen the downgrade path the split closes.
///
/// Integrity comes from three upstream checks, not from anything in
/// this function:
/// 1. F01 on the cold-pointer entry (signed under the DID pubkey) +
///    the anchor-shape checks in `cold_pointer_from_entry`.
/// 2. F01 on the bundle registry entry (signed under `warm_pub`).
/// 3. `BlobStore::blob_download` BLAKE3-verifies the bytes against
///    `entry.hash`.
///
/// `Ok(None)` means the DID has an anchor but no published bundle yet
/// (legitimate state on first run). A missing *anchor* is an error —
/// an unanchored DID is unresolvable by design.
pub async fn resolve_did(
    did: &Did,
    registry: &dyn RegistryApi,
    stores: &HashMap<String, Arc<dyn s5_core::blob::Blobs>>,
) -> Result<Option<ResolvedIdentity>> {
    let (pointer, _anchor_revision) =
        crate::identity_anchor::resolve_cold_pointer(registry, did).await?;
    let stream_key = StreamKey::Vault {
        pubkey: pointer.warm_pub,
        vault_id: identity_vault_id(),
    };
    let Some(entry) = registry.get(&stream_key).await? else {
        return Ok(None);
    };

    let mut bytes = None;
    for store in stores.values() {
        if let Ok(b) = store.blob_download(entry.hash).await {
            bytes = Some(b);
            break;
        }
    }
    let bytes = bytes.with_context(|| {
        format!(
            "identity bundle blob {} not present in any configured store",
            entry.hash
        )
    })?;
    let bundle =
        IdentityBundle::decode_cbor(&bytes).map_err(|e| anyhow!("decoding IdentityBundle: {e}"))?;
    // Sentinel log for cutover observability — single line carries
    // everything an operator needs to verify "the four-key bundle
    // resolved correctly for this peer". Grep target.
    let peer_short = bundle
        .iroh_pubkeys
        .first()
        .map(|pk| hex::encode(&pk[..4]))
        .unwrap_or_else(|| "<no-iroh>".to_string());
    tracing::info!(
        peer = peer_short.as_str(),
        did = did.to_string().as_str(),
        warm = %hex::encode(&pointer.warm_pub[..4]),
        signers = bundle.signers.len(),
        acl_keys = bundle.acl_keys.len(),
        iroh_pubkeys = bundle.iroh_pubkeys.len(),
        bundle_hash = %hex::encode(&entry.hash.as_bytes()[..4]),
        revision = bundle.revision,
        "identity resolved"
    );
    Ok(Some(ResolvedIdentity {
        bundle,
        warm_pub: pointer.warm_pub,
    }))
}

/// Map a member name (`"self"` or a `[friend.<nick>]` key) to a DID
/// and (optionally) the friend's hardcoded iroh transport pubkey.
///
/// Returns `(did, iroh_pubkey)`. The iroh pubkey is only ever `Some`
/// for non-`self` friends with `[friend.<nick>].iroh_pubkey_hex` set
/// in config — it's the bootstrap-from-cold-cache hint that lets the
/// daemon dial the peer before its identity bundle has been fetched.
/// `self` returns `None` for iroh (caller doesn't need it; iroh
/// dialing is between distinct peers).
fn resolve_name(
    name: &str,
    self_did: &Did,
    config: &S5NodeConfig,
) -> Option<(Did, Option<[u8; 32]>)> {
    if name == "self" {
        return Some((*self_did, None));
    }
    let friend = config.friend.get(name)?;
    let did = match Did::parse(&friend.id) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                friend = name,
                "[friend.{name}].id is not a valid did:s5: {e}"
            );
            return None;
        }
    };
    let iroh_pubkey = friend.iroh_pubkey_hex.as_deref().and_then(|s| {
        let bytes = match hex::decode(s) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    friend = name,
                    "[friend.{name}].iroh_pubkey_hex is not valid hex: {e}"
                );
                return None;
            }
        };
        match <[u8; 32]>::try_from(bytes.as_slice()) {
            Ok(arr) => Some(arr),
            Err(_) => {
                tracing::warn!(
                    friend = name,
                    len = bytes.len(),
                    "[friend.{name}].iroh_pubkey_hex must decode to 32 bytes"
                );
                None
            }
        }
    });
    Some((did, iroh_pubkey))
}

/// Cross-vault, per-peer pubkey maps populated as a side effect by
/// `resolve_vault_membership`. The caller (`build_membership_state`)
/// carries one of these across every vault so the subscriber has the
/// full peer→{master, device_signing, …} mapping at hand. Future
/// per-peer key types (e.g. ACL on the consumer side) extend this
/// struct, not the call signature.
pub struct PeerKeyMaps<'a> {
    pub master_for_peer: &'a mut HashMap<[u8; 32], [u8; 32]>,
    pub warm_for_master: &'a mut HashMap<[u8; 32], [u8; 32]>,
    pub device_signing_for_peer: &'a mut HashMap<[u8; 32], [u8; 32]>,
    pub did_for_device_signing: &'a mut HashMap<[u8; 32], [u8; 32]>,
}

/// Resolve every member of a single vault into a [`VaultMembership`].
/// Per-member resolution failures are logged and skipped; one bad
/// member must not poison the others.
pub async fn resolve_vault_membership(
    vault_name: &str,
    members: &[String],
    self_did: &Did,
    config: &S5NodeConfig,
    registry: &dyn RegistryApi,
    stores: &HashMap<String, Arc<dyn s5_core::blob::Blobs>>,
    peer_keys: &mut PeerKeyMaps<'_>,
) -> VaultMembership {
    let mut state = VaultMembership::default();
    // Snapshot the vault's resolved store set (data + meta primaries,
    // de-duplicated — D1). The per-blob ACL consults this list to decide
    // whether a peer can fetch a given hash.
    let writers: &[String] = config
        .vault
        .get(vault_name)
        .map(|v| v.writers.as_slice())
        .unwrap_or(&[]);
    if let Some(vault_cfg) = config.vault.get(vault_name) {
        for s in config
            .vault_read_stores(vault_name, vault_cfg)
            .unwrap_or_default()
        {
            if !state.store_names.iter().any(|n| n == s) {
                state.store_names.push(s.to_string());
            }
        }
    }
    for member_name in members {
        let Some((did, friend_iroh)) = resolve_name(member_name, self_did, config) else {
            tracing::warn!(
                vault = vault_name,
                member = member_name.as_str(),
                "vault member not found in [friend.*] (and is not 'self')"
            );
            continue;
        };
        // Capability = keyset membership (D11): `self` and members named in
        // `writers` are write-capable (their signers[] become accepted vault
        // writers); everyone else is read-only.
        let is_writer = member_name == "self" || writers.iter().any(|w| w == member_name);
        match resolve_did(&did, registry, stores).await {
            Ok(Some(resolved)) => {
                // Record the cold→warm mapping so the subscriber can
                // watch both halves of the resolution chain (D17).
                peer_keys
                    .warm_for_master
                    .insert(*did.pubkey(), resolved.warm_pub);
                merge_into(&mut state, peer_keys, &did, &resolved.bundle, is_writer);
            }
            Ok(None) | Err(_) => {
                // No bundle available locally yet. Two recoverable
                // paths from here:
                //   * `friend.iroh_pubkey_hex` configured → seed the
                //     iroh pubkey directly into VaultMembership so
                //     `MembershipSubscriber` can dial the peer + pull
                //     their bundle. Next refresh will replace this
                //     bootstrap entry with the bundle's full keysets.
                //   * Unset → peer is unreachable from cold cache (the
                //     pre-pair / pre-bundle state). Once the bundle
                //     reaches the local registry via any out-of-band
                //     channel, the next refresh resolves it normally.
                if let Some(iroh) = friend_iroh {
                    state.authorized_iroh_pubkeys.insert(iroh);
                    peer_keys
                        .master_for_peer
                        .entry(iroh)
                        .or_insert(*did.pubkey());
                    tracing::info!(
                        vault = vault_name,
                        member = member_name.as_str(),
                        did = did.to_string().as_str(),
                        iroh = %hex::encode(&iroh[..4]),
                        "friend bootstrap: seeded iroh pubkey from config; \
                         subscriber will dial + pull bundle"
                    );
                } else {
                    tracing::warn!(
                        vault = vault_name,
                        member = member_name.as_str(),
                        did = did.to_string().as_str(),
                        "no bundle published + no [friend.{member_name}].iroh_pubkey_hex \
                         configured — peer unreachable until bundle arrives out-of-band \
                         (since master ≠ iroh in the four-key model, the DID alone is \
                         not a dial target)"
                    );
                }
                state.member_dids.push(did);
            }
        }
    }
    state
}

fn merge_into(
    state: &mut VaultMembership,
    peer_keys: &mut PeerKeyMaps<'_>,
    did: &Did,
    bundle: &IdentityBundle,
    is_writer: bool,
) {
    state.member_dids.push(*did);
    let master_pubkey = *did.pubkey();
    // The bundle's primary device signing pubkey. `None` (empty
    // signers[]) means a read-only / service-DID provisioning per
    // `identity-model.md` § Per-device keys — the daemon legitimately
    // has no write authority and we skip the data-subscription mapping.
    // `is_writer == false` (a read-only MEMBER, D11) has the same effect:
    // we do not admit its signers as vault writers and do not subscribe to
    // its (non-existent) HEAD stream.
    let device_signing_pubkey = is_writer.then(|| bundle.signers.first().copied()).flatten();
    // F01 level 3 reverse index: every signer in a WRITER's bundle is now a
    // recognised write principal under its master. Read-only members are
    // deliberately skipped — this is what makes `grant --read` read-only.
    if is_writer {
        for s in &bundle.signers {
            peer_keys
                .did_for_device_signing
                .entry(*s)
                .or_insert(master_pubkey);
        }
    }
    for pk in &bundle.iroh_pubkeys {
        state.authorized_iroh_pubkeys.insert(*pk);
        // First insertion wins for the peer→master direction. A repeat
        // iroh pubkey across DIDs (co-resident identities sharing a
        // transport key) means we'd subscribe under whichever DID was
        // resolved first; the subscriber still runs one task per iroh
        // peer regardless, so the choice doesn't affect coverage.
        peer_keys
            .master_for_peer
            .entry(*pk)
            .or_insert(master_pubkey);
        if let Some(ds) = device_signing_pubkey {
            peer_keys.device_signing_for_peer.entry(*pk).or_insert(ds);
        }
    }
    for ak in &bundle.acl_keys {
        state.authorized_acl_pubkeys.insert(*ak);
    }
    for r in &bundle.age_recipients {
        state.age_recipients.push(r.clone());
    }
}

/// Architectural note for step 4 (live bundle refresh):
///
/// Polling re-resolution every N seconds was rejected — the total
/// registry-entry cardinality is small (one per vault per device,
/// one per identity), so push-based fanout is both cheaper and more
/// reliable. The plan:
///
/// 1. `RegistryServer` exposes a `Subscribe` RPC. Server keeps a
///    `tokio::sync::broadcast` of SET events; subscribers receive a
///    stream filtered by interested `StreamKey`s.
/// 2. Each daemon, for every vault it serves, dials every member's
///    iroh endpoint (already known via `MembershipState` resolved at
///    startup) and subscribes to:
///    - The member's identity-vault key (`StreamKey::Vault {
///      pubkey: master_pubkey, vault_id: IDENTITY_VAULT_ID }`) so
///      bundle rotations propagate.
///    - The member's vault data keys for vaults shared with this
///      daemon, so HEAD updates propagate.
/// 3. On subscription open, the server replays the current value of
///    each requested key (catch-up for state missed while
///    disconnected).
/// 4. On received event: server-side this daemon writes into its own
///    registry (multi-registry fans out to redb + store backends);
///    `MembershipState` is rebuilt from the local registry on
///    relevant key changes.
///
/// The push design lives at the registry layer (TODO at
/// `s5_registry::RegistryServer` and the construction site in
/// `s5_node::S5Node::new_with_stores`); membership is just one
/// consumer. Other consumers (peer-snapshot live mounts, future
/// vault-data sync) reuse the same primitive.
const _STEP4_DESIGN_NOTE: () = ();

/// Resolve every vault declared with `members`. Vaults without
/// `members` are absent from the resulting state — step 3 falls back
/// to legacy `[peer.*]` ACL for those, until they're migrated.
///
/// `self_did` is the daemon's own identity — the **cold** anchor
/// pubkey under D17, resolved from the anchor entry at startup, NOT
/// derivable from the (warm) signing key in hand. Callers MUST pass
/// the anchored DID, never a pubkey recovered from a transport or
/// warm key.
pub async fn build_membership_state(
    self_did: &Did,
    config: &S5NodeConfig,
    registry: &dyn RegistryApi,
    stores: &HashMap<String, Arc<dyn s5_core::blob::Blobs>>,
) -> MembershipState {
    let self_did = *self_did;
    let mut state = MembershipState::default();

    // Record which vaults are public-read. The id lookup happens later
    // via `is_public_read_vault_id` against `vault_id_by_name`.
    for (vault_name, vault_cfg) in &config.vault {
        if vault_cfg.plaintext_published_tn {
            state.public_read_vault_names.insert(vault_name.clone());
        }
    }

    for (vault_name, vault_cfg) in &config.vault {
        if vault_cfg.members.is_empty() {
            continue;
        }
        let mut peer_keys = PeerKeyMaps {
            master_for_peer: &mut state.master_for_peer,
            warm_for_master: &mut state.warm_for_master,
            device_signing_for_peer: &mut state.device_signing_for_peer,
            did_for_device_signing: &mut state.did_for_device_signing,
        };
        let vm = resolve_vault_membership(
            vault_name,
            &vault_cfg.members,
            &self_did,
            config,
            registry,
            stores,
            &mut peer_keys,
        )
        .await;
        tracing::info!(
            vault = vault_name.as_str(),
            members = vm.member_dids.len(),
            iroh_pubkeys = vm.authorized_iroh_pubkeys.len(),
            age_recipients = vm.age_recipients.len(),
            "vault membership resolved"
        );
        state.vaults.insert(vault_name.clone(), vm);
    }

    // Seed `vault_id_by_name` from any explicit `[vault.<name>].vault_id`
    // hints. Read-only members never publish — so the publisher-side
    // `register_vault_id` path doesn't fire on this daemon — but
    // `MembershipSubscriber::run_for_peer` needs the mapping to know
    // which `(peer, vault_id)` data keys to subscribe to. Without this,
    // a member-only vault would get identity-vault subscriptions only,
    // and the data-vault stream we actually care about goes ignored.
    //
    // Publishers should leave `vault_id` unset and let
    // `register_vault_id` populate the mapping on first publish; setting
    // both a publisher-side hint and a derived-from-recovery value just
    // requires they match exactly (defensive but harmless).
    for (vault_name, vault_cfg) in &config.vault {
        let Some(hex_str) = vault_cfg.vault_id.as_deref() else {
            continue;
        };
        let bytes = match hex::decode(hex_str.trim()) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    vault = vault_name.as_str(),
                    error = %e,
                    "ignoring [vault.{vault_name}].vault_id — not valid hex"
                );
                continue;
            }
        };
        if bytes.len() != 16 {
            tracing::warn!(
                vault = vault_name.as_str(),
                got_bytes = bytes.len(),
                "ignoring [vault.{vault_name}].vault_id — must decode to exactly 16 bytes"
            );
            continue;
        }
        let mut vault_id = [0u8; 16];
        vault_id.copy_from_slice(&bytes);
        if state.register_vault_id(vault_name, vault_id) {
            tracing::info!(
                vault = vault_name.as_str(),
                vault_id = %hex::encode(vault_id),
                "registered vault_id from config hint"
            );
        }
    }

    state
}
