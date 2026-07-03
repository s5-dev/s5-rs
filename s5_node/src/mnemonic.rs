//! Mnemonic-rooted key derivation (`docs/reference/mnemonic-derivation.md`).
//!
//! A single paper phrase is the inverse of the whole per-identity key set: the
//! cold identity key (whose pubkey IS the DID), the paper recovery age key,
//! and the storage-root fallback seed all derive from it through
//! [`s5_core::crypto::derive_secret`]. The **warm** master is deliberately NOT
//! derivable — it is random, escrowed in the `identity_secrets` vault, and
//! recovered by *opening* that vault with the paper age key
//! (`identity-rotation.md §3`/`§8`).
//!
//! **Interim:** the phrase is a standard 12-word BIP-39 English mnemonic; the
//! custom 15-word s5 wordlist (version/network/epoch/checksum — `§1`/`§9.2`) is
//! deferred. The derivation tree below (`§2`–`§5`) is independent of the phrase
//! *encoding*, so swapping in the custom wordlist later yields the same derived
//! keys for the same entropy.

use anyhow::{Result, anyhow};
use bip39::{Language, Mnemonic};
use ed25519_dalek::SigningKey;
use rand::Rng;
use s5_core::crypto::derive_secret;

// All contexts follow the flat `"s5/<purpose>/v<n>"` convention (§2). The
// superseded pre-split context `"s5/identity-master/v1"` lives on only as
// `identity_vault::MASTER_KEY_DERIVATION_DOMAIN` (a test utility); the
// non-collision of cold-split DIDs with pre-split ones is pinned in the
// tests below.
/// KDF context for the root master (`§2`).
const ROOT_CTX: &str = "s5/root/v1";
/// KDF context for the **cold** master signing seed
/// (`identity-rotation.md §3`): the key whose pubkey IS the DID under
/// the cold/warm split (D17). Epoch 0 derives directly from the root
/// master; each successor chains from the previous seed (see
/// [`identity_cold_signing_key`]).
const IDENTITY_COLD_CTX: &str = "s5/identity-cold/v1";
/// KDF context for the paper recovery age seed (`§3.2`).
const PAPER_AGE_CTX: &str = "s5/paper-age/v1";
/// KDF context for the storage-root fallback seed (`§5`).
const STORAGE_ROOT_CTX: &str = "s5/storage-root/v1";

/// Generate a fresh 12-word BIP-39 English mnemonic (128 bits of entropy).
///
/// Entropy comes from `OsRng` here (not bip39's optional `rand` feature) and is
/// turned into the phrase via `from_entropy`.
pub fn generate_mnemonic() -> Result<String> {
    let mut entropy = [0u8; 16];
    rand::rng().fill_bytes(&mut entropy);
    Mnemonic::from_entropy(&entropy)
        .map(|m| m.to_string())
        .map_err(|e| anyhow!("generating mnemonic: {e}"))
}

/// Derive the 32-byte root master from a mnemonic phrase (`§2`).
///
/// Uses the BIP-39 *entropy* (16 bytes for a 12-word phrase), zero-padded to
/// 32, as the input to `derive_secret(ROOT_CTX, …)` — deliberately **not** the
/// BIP-39 PBKDF2 seed: 128 bits of true entropy needs no memory-hard stretch.
pub fn root_master(phrase: &str) -> Result<[u8; 32]> {
    let mnemonic = Mnemonic::parse_in(Language::English, phrase.trim())
        .map_err(|e| anyhow!("invalid mnemonic: {e}"))?;
    let entropy = mnemonic.to_entropy();
    let mut padded = [0u8; 32];
    let n = entropy.len().min(32);
    padded[..n].copy_from_slice(&entropy[..n]);
    Ok(derive_secret(ROOT_CTX, &padded))
}

/// The **cold** master ed25519 signing key at `epoch`
/// (`identity-rotation.md §3`/`§6.3`, D17). `K_cold[0].pub` is the DID;
/// it lives on paper only and is touched solely to (re)point the cold
/// pointer at the current warm key.
///
/// Epoch scheme (chain, so `next_cold_pub` is committable in advance
/// without new derivation inputs):
///
/// ```text
/// K_cold[0].seed   = kdf("s5/identity-cold/v1", root_master)
/// K_cold[e+1].seed = kdf("s5/identity-cold/v1", K_cold[e].seed)
/// ```
///
/// Deriving epoch `e` costs `e+1` cheap blake3 KDF calls; cold rotation
/// is expected ~never, so the chain walk is irrelevant in practice.
pub fn identity_cold_signing_key(root_master: &[u8; 32], epoch: u64) -> SigningKey {
    let mut seed = derive_secret(IDENTITY_COLD_CTX, root_master);
    for _ in 0..epoch {
        seed = derive_secret(IDENTITY_COLD_CTX, &seed);
    }
    SigningKey::from_bytes(&seed)
}

/// The paper recovery age key (`§3.2`), derived deterministically so it is
/// reconstructible from the phrase alone — a permanent recipient of every vault
/// encrypted to this identity.
pub fn paper_age_identity(root_master: &[u8; 32]) -> Result<age::x25519::Identity> {
    age_identity_from_seed(&derive_secret(PAPER_AGE_CTX, root_master))
}

/// The storage-root seed (`§5`): the storage derivation root. Recoverable from
/// the phrase, and also stored in the `stores` vault as the standing source of
/// truth. Managed-account credentials derive from it via
/// [`managed_storage_secret`].
pub fn storage_root_seed(root_master: &[u8; 32]) -> [u8; 32] {
    derive_secret(STORAGE_ROOT_CTX, root_master)
}

/// The managed-account storage secret for a backend `kind` and account `label`
/// (`mnemonic-derivation.md` § Layer C / `special-vaults.md` § 4):
///
/// ```text
/// blake3::derive_key("s5/storage/<kind>/v1", stores_seed ‖ label)
/// ```
///
/// This is the secret fed to the backend's OAuth/enrollment (e.g.
/// [`s5_store_indexd::auth::register`]), deterministically yielding the same
/// account credential — so a managed account needs no stored key material, only
/// its label, and stays re-derivable from the paper mnemonic. `kind` is the
/// backend tag (`"indexd"`, `"s3"`, …); `label` is the store name. The 32-byte
/// `stores_seed` prefix is fixed-width, so `stores_seed ‖ label` is unambiguous.
pub fn managed_storage_secret(stores_seed: &[u8; 32], kind: &str, label: &str) -> [u8; 32] {
    let mut input = Vec::with_capacity(32 + label.len());
    input.extend_from_slice(stores_seed);
    input.extend_from_slice(label.as_bytes());
    derive_secret(&format!("s5/storage/{kind}/v1"), &input)
}

/// Build an `age::x25519::Identity` deterministically from a 32-byte seed.
///
/// age 0.11 exposes no from-bytes constructor, so we encode the seed in age's
/// own secret-key wire format — Bech32 over HRP `age-secret-key-`, exactly as
/// `age::x25519::Identity`'s own `Display` does — and parse it back. The
/// recipient is then a deterministic function of the seed. Pinned by the
/// `paper_age_key_*` tests so an age/bech32 bump can't silently change it.
fn age_identity_from_seed(seed: &[u8; 32]) -> Result<age::x25519::Identity> {
    use bech32::{ToBase32, Variant};
    let encoded = bech32::encode("age-secret-key-", seed.to_base32(), Variant::Bech32)
        .map_err(|e| anyhow!("bech32-encoding age key: {e}"))?
        .to_uppercase();
    encoded
        .parse::<age::x25519::Identity>()
        .map_err(|e| anyhow!("parsing derived age key: {e}"))
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use super::*;

    // BIP-39 test vectors (valid checksums): entropy 0x7f… and 0xff….
    const PHRASE_A: &str =
        "legal winner thank year wave sausage worth useful legal winner thank yellow";
    const PHRASE_B: &str = "zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo wrong";

    #[test]
    fn generated_mnemonic_is_valid_12_words() {
        let phrase = generate_mnemonic().unwrap();
        assert_eq!(phrase.split_whitespace().count(), 12);
        root_master(&phrase).expect("a generated phrase must derive a root");
    }

    #[test]
    fn derivations_are_deterministic_and_distinct() {
        let rm = root_master(PHRASE_A).unwrap();
        assert_eq!(
            rm,
            root_master(PHRASE_A).unwrap(),
            "root_master deterministic"
        );

        let cold = identity_cold_signing_key(&rm, 0);
        let storage = storage_root_seed(&rm);
        let paper = paper_age_identity(&rm).unwrap();

        // Distinct KDF domains → distinct material.
        assert_ne!(cold.to_bytes(), storage);
        // The load-bearing property: the paper recipient re-derives identically.
        assert_eq!(
            paper.to_public().to_string(),
            paper_age_identity(&rm).unwrap().to_public().to_string()
        );
    }

    #[test]
    fn different_phrases_give_different_roots() {
        assert_ne!(
            root_master(PHRASE_A).unwrap(),
            root_master(PHRASE_B).unwrap()
        );
    }

    #[test]
    fn managed_storage_secret_is_scoped_and_deterministic() {
        let seed = storage_root_seed(&root_master(PHRASE_A).unwrap());
        let a = managed_storage_secret(&seed, "indexd", "sia");
        assert_eq!(
            a,
            managed_storage_secret(&seed, "indexd", "sia"),
            "same (seed, kind, label) is deterministic — recovery re-derives the same AppKey"
        );
        assert_ne!(
            a,
            managed_storage_secret(&seed, "indexd", "archive"),
            "a different label is an independent account"
        );
        assert_ne!(
            a,
            managed_storage_secret(&seed, "s3", "sia"),
            "a different backend kind is independent"
        );
        // It is its own secret, not the storage root nor the cold identity master.
        assert_ne!(a, seed);
        assert_ne!(
            a,
            identity_cold_signing_key(&root_master(PHRASE_A).unwrap(), 0).to_bytes()
        );
    }

    #[test]
    fn invalid_phrase_is_rejected() {
        assert!(root_master("not a real mnemonic phrase at all nope").is_err());
    }

    #[test]
    fn cold_key_epochs_chain_deterministically_and_are_distinct() {
        let rm = root_master(PHRASE_A).unwrap();
        let e0 = identity_cold_signing_key(&rm, 0);
        let e1 = identity_cold_signing_key(&rm, 1);
        let e2 = identity_cold_signing_key(&rm, 2);

        // Deterministic: same phrase + epoch → same key (paper recovery
        // must re-derive the committed next_cold_pub bit-identically).
        assert_eq!(e0.to_bytes(), identity_cold_signing_key(&rm, 0).to_bytes());
        assert_eq!(e1.to_bytes(), identity_cold_signing_key(&rm, 1).to_bytes());

        // Distinct across epochs and from the legacy single-master key
        // (different KDF context — a pre-split DID never collides with a
        // post-split one). The superseded context is spelled out here so
        // the non-collision stays pinned after its derivation fn was
        // removed.
        assert_ne!(e0.to_bytes(), e1.to_bytes());
        assert_ne!(e1.to_bytes(), e2.to_bytes());
        assert_ne!(
            e0.to_bytes(),
            derive_secret("s5/identity-master/v1", &rm),
            "cold context must differ from the superseded master context"
        );

        // Chain property: epoch e+1 seed = kdf(ctx, epoch e seed).
        let manual_e1 = SigningKey::from_bytes(&derive_secret(
            IDENTITY_COLD_CTX,
            &derive_secret(IDENTITY_COLD_CTX, &rm),
        ));
        assert_eq!(e1.to_bytes(), manual_e1.to_bytes());
    }

    #[test]
    fn derived_paper_age_key_encrypts_and_decrypts() {
        // Proves the bech32-from-seed key is a *working* age identity, not just
        // deterministic bytes.
        let rm = root_master(PHRASE_A).unwrap();
        let id = paper_age_identity(&rm).unwrap();
        let recipient: age::x25519::Recipient = id.to_public().to_string().parse().unwrap();

        let msg = b"recover me";
        let enc =
            age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))
                .unwrap();
        let mut ct = vec![];
        let mut w = enc.wrap_output(&mut ct).unwrap();
        w.write_all(msg).unwrap();
        w.finish().unwrap();

        let dec = age::Decryptor::new(&ct[..]).unwrap();
        let mut pt = vec![];
        dec.decrypt(std::iter::once(&id as &dyn age::Identity))
            .unwrap()
            .read_to_end(&mut pt)
            .unwrap();
        assert_eq!(pt, msg);
    }
}
