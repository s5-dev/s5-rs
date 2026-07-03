//! The cold pointer — the DID's identity anchor (D17, `identity-rotation.md`).
//!
//! Under the cold/warm split an s5 DID is the **cold** master pubkey
//! (`K_cold.pub`, paper-only), and all day-to-day identity state is
//! signed by a rotatable **warm** master. The binding between the two
//! is exactly one registry v3 entry — the *cold pointer*:
//!
//! ```text
//! PUBKEY   = K_cold.pub                            (the DID)
//! VAULT_ID = IDENTITY_ANCHOR_ID
//!          = blake3("s5/identity-anchor/v1")[..16] (constant)
//! PAYLOAD  = warm_pub(32) ‖ next_cold_pub(32)      (64 B, inline, UNENCRYPTED)
//! SIG      = ed25519(K_cold, canonical v3 input)
//! ```
//!
//! The payload rides *inline* in the entry (`StreamMessage::data`), so
//! resolution needs no blob fetch, and — because the v3 signature
//! covers `hash ‖ inline data` and `StreamMessage::new` is the F01
//! chokepoint every receive path funnels through — a cold-pointer entry
//! is **self-certifying**: anyone holding the DID string can verify it
//! offline. Pairing exploits that by carrying the entry inside the
//! handshake instead of requiring a mutual registry round-trip.
//!
//! `next_cold_pub` is the pre-committed successor cold key
//! (`identity-rotation.md §6.3`), derived and committed at onboard time
//! when the mnemonic is in hand. Rotation *enforcement* is deferred
//! (D17); the format carries the commitment from day one so enabling it
//! later needs no wire change.
//!
//! DID resolution (spec §4.3) is always two registry lookups:
//!
//! ```text
//! 1. cold  ← registry.get(K_cold.pub, IDENTITY_ANCHOR_ID)   ; MUST exist
//! 2. bundle← registry.get(warm_pub,  IDENTITY_VAULT_ID)     ; signed by warm
//! ```

use anyhow::{Result, anyhow, bail};
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use s5_core::identity::Did;
use s5_core::{Hash, RegistryApi, StreamKey, StreamMessage};

/// Domain string for the identity-anchor vault id (spec §10).
pub const IDENTITY_ANCHOR_DOMAIN: &str = "s5/identity-anchor/v1";

/// 16-byte vault id reserved for cold pointers across all DIDs —
/// `blake3("s5/identity-anchor/v1")[..16]`, same scheme as the other
/// well-known special-vault ids.
pub fn identity_anchor_id() -> [u8; 16] {
    crate::tasks::publish::well_known_vault_id(IDENTITY_ANCHOR_DOMAIN)
}

/// Decoded cold-pointer payload: the current warm master pubkey and the
/// pre-committed successor cold pubkey.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColdPointer {
    /// The current operational (warm) master ed25519 pubkey. The
    /// identity bundle lives at `(warm_pub, IDENTITY_VAULT_ID)`.
    pub warm_pub: [u8; 32],
    /// `K_cold[epoch+1].pub` — the committed successor (spec §6.3).
    /// Carried from day one; not yet enforced (D17 deferral).
    pub next_cold_pub: [u8; 32],
}

impl ColdPointer {
    /// Canonical 64-byte payload: `warm_pub ‖ next_cold_pub`.
    pub fn encode(&self) -> [u8; 64] {
        let mut out = [0u8; 64];
        out[..32].copy_from_slice(&self.warm_pub);
        out[32..].copy_from_slice(&self.next_cold_pub);
        out
    }

    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != 64 {
            bail!(
                "cold pointer payload must be 64 bytes (warm_pub ‖ next_cold_pub), got {}",
                payload.len()
            );
        }
        let mut warm_pub = [0u8; 32];
        let mut next_cold_pub = [0u8; 32];
        warm_pub.copy_from_slice(&payload[..32]);
        next_cold_pub.copy_from_slice(&payload[32..]);
        Ok(Self {
            warm_pub,
            next_cold_pub,
        })
    }
}

/// Sign a cold-pointer entry under `cold` at `revision`. The payload is
/// carried inline; `hash = blake3(payload)` per the v3 tie-breaker
/// contract.
pub fn sign_cold_pointer(
    cold: &SigningKey,
    pointer: &ColdPointer,
    revision: u64,
) -> Result<StreamMessage> {
    let payload = pointer.encode();
    StreamMessage::sign_ed25519_registry_with_data(
        cold,
        identity_anchor_id(),
        Hash::new(payload),
        revision,
        Some(Bytes::copy_from_slice(&payload)),
    )
    .map_err(|e| anyhow!("signing cold pointer: {e}"))
}

/// Extract + validate the [`ColdPointer`] from a registry entry that
/// claims to be `did`'s anchor.
///
/// The ed25519 signature itself was already verified under the entry's
/// embedded pubkey by `StreamMessage::new` (F01 chokepoint) — what this
/// checks is everything *around* that: the embedded pubkey is the DID's,
/// the vault id is the anchor id, the payload is present inline, and it
/// hashes to the signed `hash`. Suitable for entries received out of
/// band (e.g. inside the pairing handshake), not just from a registry.
pub fn cold_pointer_from_entry(did: &Did, entry: &StreamMessage) -> Result<ColdPointer> {
    let StreamKey::Vault { pubkey, vault_id } = &entry.key else {
        bail!("cold pointer entry has a non-vault stream key");
    };
    if pubkey != did.pubkey() {
        bail!("cold pointer entry pubkey does not match the DID");
    }
    if *vault_id != identity_anchor_id() {
        bail!("cold pointer entry vault_id is not IDENTITY_ANCHOR_ID");
    }
    let payload = entry
        .data
        .as_deref()
        .ok_or_else(|| anyhow!("cold pointer entry carries no inline payload"))?;
    if Hash::new(payload) != entry.hash {
        bail!("cold pointer inline payload does not match the signed hash");
    }
    ColdPointer::decode(payload)
}

/// Resolve `did` to its current cold pointer: step 1 of the two-step
/// resolution (spec §4.3). Errors if no anchor entry exists — a DID
/// without a cold pointer is unresolvable by design (there is no
/// single-key fallback; that would reopen the downgrade path the split
/// closes).
pub async fn resolve_cold_pointer(
    registry: &dyn RegistryApi,
    did: &Did,
) -> Result<(ColdPointer, u64)> {
    let key = StreamKey::Vault {
        pubkey: *did.pubkey(),
        vault_id: identity_anchor_id(),
    };
    let entry = registry
        .get(&key)
        .await
        .map_err(|e| anyhow!("registry get for cold pointer: {e}"))?
        .ok_or_else(|| anyhow!("no cold pointer published for {did} — identity unresolvable"))?;
    let pointer = cold_pointer_from_entry(did, &entry)?;
    Ok((pointer, entry.revision))
}

/// Publish (or re-point) the cold pointer: reads the previous revision
/// (if any) and writes `revision + 1`. This is the **only** operation
/// the cold key ever signs.
pub async fn publish_cold_pointer(
    registry: &dyn RegistryApi,
    cold: &SigningKey,
    pointer: &ColdPointer,
) -> Result<u64> {
    let key = StreamKey::Vault {
        pubkey: cold.verifying_key().to_bytes(),
        vault_id: identity_anchor_id(),
    };
    let revision = match registry.get(&key).await {
        Ok(Some(prev)) => prev.revision + 1,
        Ok(None) => 1,
        Err(e) => bail!("registry get before cold pointer publish: {e}"),
    };
    let entry = sign_cold_pointer(cold, pointer, revision)?;
    registry
        .set(entry)
        .await
        .map_err(|e| anyhow!("registry set for cold pointer: {e}"))?;
    Ok(revision)
}

/// Default path for the anchor entry file when
/// `[identity].anchor_entry_file` is unset: a sibling of the warm key
/// file named `identity_anchor.entry`. `None` when no key-file path is
/// resolvable (fully ephemeral daemon).
pub fn default_anchor_entry_path(
    identity: &s5_node_api::config::NodeConfigIdentity,
    config_dir: Option<&std::path::Path>,
) -> Option<std::path::PathBuf> {
    crate::identity_vault::default_master_key_path(identity, config_dir)
        .and_then(|p| p.parent().map(|d| d.join("identity_anchor.entry")))
}

/// Load a serialized cold-pointer entry from disk (the file `vup
/// onboard`/`vup recover` writes). Public data — the entry is exactly
/// what sits in the registry; confidentiality is not a goal, the
/// signature carries the authority.
pub fn load_anchor_entry(path: &std::path::Path) -> Result<StreamMessage> {
    let bytes = std::fs::read(path)
        .map_err(|e| anyhow!("reading anchor entry file {}: {e}", path.display()))?;
    StreamMessage::deserialize(Bytes::from(bytes))
        .map_err(|e| anyhow!("deserializing anchor entry {}: {e}", path.display()))
}

/// Build a **self-anchored** cold pointer: cold == warm, DID = the warm
/// pubkey, no committed successor (all-zero `next_cold_pub` sentinel).
///
/// Dev/test-daemon mode only (no onboarding ceremony): it keeps DID
/// resolution uniformly two-hop without requiring a mnemonic, at the
/// cost of the split's security properties — warm compromise of a
/// self-anchored identity IS identity compromise, exactly the pre-D17
/// semantics. Onboarded identities never use this.
pub fn self_anchored_entry(warm: &SigningKey) -> Result<StreamMessage> {
    let pointer = ColdPointer {
        warm_pub: warm.verifying_key().to_bytes(),
        next_cold_pub: [0u8; 32],
    };
    sign_cold_pointer(warm, &pointer, 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use s5_core::identity::DidMasterPubkey;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn sample_pointer() -> ColdPointer {
        ColdPointer {
            warm_pub: key(2).verifying_key().to_bytes(),
            next_cold_pub: key(3).verifying_key().to_bytes(),
        }
    }

    #[test]
    fn payload_round_trips() {
        let p = sample_pointer();
        assert_eq!(ColdPointer::decode(&p.encode()).unwrap(), p);
        assert!(ColdPointer::decode(&[0u8; 63]).is_err());
    }

    #[test]
    fn signed_entry_verifies_offline_under_the_did() {
        let cold = key(1);
        let did = Did::from_pubkey(DidMasterPubkey::from_verifying_key(&cold.verifying_key()));
        let p = sample_pointer();

        let entry = sign_cold_pointer(&cold, &p, 7).unwrap();
        // Round-trip through the wire form — the exact shape pairing
        // ships — and re-admit through the F01 chokepoint.
        let bytes = entry.serialize();
        let received = StreamMessage::deserialize(bytes).unwrap();
        assert_eq!(cold_pointer_from_entry(&did, &received).unwrap(), p);
        assert_eq!(received.revision, 7);
    }

    #[test]
    fn entry_for_a_different_did_is_rejected() {
        let cold = key(1);
        let other = key(9);
        let other_did =
            Did::from_pubkey(DidMasterPubkey::from_verifying_key(&other.verifying_key()));
        let entry = sign_cold_pointer(&cold, &sample_pointer(), 1).unwrap();
        assert!(cold_pointer_from_entry(&other_did, &entry).is_err());
    }

    #[test]
    fn entry_at_a_different_vault_id_is_rejected() {
        let cold = key(1);
        let did = Did::from_pubkey(DidMasterPubkey::from_verifying_key(&cold.verifying_key()));
        let p = sample_pointer();
        // Same signer, same payload — but published under the identity
        // *vault* id instead of the anchor id.
        let entry = StreamMessage::sign_ed25519_registry_with_data(
            &cold,
            crate::identity_vault::identity_vault_id(),
            Hash::new(p.encode()),
            1,
            Some(Bytes::copy_from_slice(&p.encode())),
        )
        .unwrap();
        assert!(cold_pointer_from_entry(&did, &entry).is_err());
    }

    #[test]
    fn payload_hash_mismatch_is_rejected() {
        let cold = key(1);
        let did = Did::from_pubkey(DidMasterPubkey::from_verifying_key(&cold.verifying_key()));
        let p = sample_pointer();
        // Sign hash-of-payload but attach no inline data: consumers must
        // refuse rather than fetch-and-hope.
        let entry = StreamMessage::sign_ed25519_registry_with_data(
            &cold,
            identity_anchor_id(),
            Hash::new(p.encode()),
            1,
            None,
        )
        .unwrap();
        assert!(cold_pointer_from_entry(&did, &entry).is_err());
    }
}
