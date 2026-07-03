//! Identity-vault publishing.
//!
//! On startup the daemon publishes an [`IdentityBundle`] — a flat
//! CBOR record carrying the identity's four keysets (signers, ACL/read,
//! iroh transport, age recipients). Other nodes resolve the DID via
//! the registry, F01-verify the registry entry's master signature,
//! fetch the bundle blob, BLAKE3-check the bytes, and read the four
//! keysets directly. See `docs/reference/identity-model.md` § Identity
//! bundle and `acl-and-revocation.md §1` for the verify chain.
//!
//! The identity vault uses a constant `vault_id` so any consumer can
//! locate it with only a DID in hand. Single writer = the DID master
//! signing key; no recipients (the bundle is public, content-addressed,
//! and integrity-protected by the master signature on its registry
//! entry — no envelope on the blob itself).
//!
//! The master signing key is cryptographically distinct from the iroh
//! transport key. Two sources, caller's choice:
//!
//! - [`derive_master_signing_key`] — blake3 from `iroh_secret` under a
//!   dedicated domain tag. Deterministic across boots; the DID is
//!   stable so long as the iroh secret is. Used as the **fallback** when
//!   no `[identity].secret_key_file` is configured (i.e. an inline
//!   iroh secret or no secret at all); compromise of the iroh secret
//!   then reconstructs the master.
//! - [`load_or_generate_master_signing_key`] — generate random + persist
//!   to a file on first boot, read back on subsequent boots. Default for
//!   any daemon whose iroh secret lives in a file (the master persists
//!   alongside it); opt-out by setting `[identity].master_key_file =
//!   ""`. The DID then survives iroh-secret rotation. **Age-encrypted at
//!   rest** when a `[key.main]` recipient is configured (slice S2);
//!   falls back to plaintext + warn otherwise. Paper-recovery
//!   passphrase wrap (`key-recovery.md`) layers on top later.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use ed25519_dalek::SigningKey;
use s5_core::identity::IdentityBundle;
use s5_core::{Hash, RegistryApi};
use s5_node_api::config::NodeConfigKey;

/// Magic header prefix of the age v1 ASCII format. Anything starting
/// with this byte sequence is treated as an age ciphertext; anything
/// else is treated as a raw 32-byte seed (legacy path).
const AGE_V1_HEADER: &[u8] = b"age-encryption.org/v1\n";

/// Domain tag for the per-identity master signing key derivation. Any
/// future format change bumps the `/v1` suffix so old derivations are
/// not accidentally reused.
const MASTER_KEY_DERIVATION_DOMAIN: &str = "s5/identity-master/v1";

/// Derive the per-identity master ed25519 signing key from an
/// arbitrary 32-byte seed. **Test utility only** — production daemons
/// go through [`load_or_generate_master_signing_key`] (a persistent
/// random seed in a file, age-encrypted to `[key.main]`). Kept public
/// so tests can manufacture stable test-DIDs from a known input.
pub fn derive_master_signing_key(seed_input: &[u8; 32]) -> SigningKey {
    let seed = s5_core::crypto::derive_secret(MASTER_KEY_DERIVATION_DOMAIN, seed_input);
    SigningKey::from_bytes(&seed)
}

/// Load the master ed25519 signing key from `path`, or — if the file
/// doesn't exist — generate a fresh random one, persist it (age-encrypted
/// when a `[key.main]` recipient is configured), and return it.
///
/// On-disk format, in order of precedence:
///
/// 1. **Age ciphertext** (preferred, prefix `b"age-encryption.org/v1\n"`).
///    Decrypted with `keys["main"].identity_file`. The 32-byte plaintext
///    is the ed25519 seed.
/// 2. **Raw 32 bytes** (legacy / no-recipient fallback). The bytes are
///    the ed25519 seed directly.
///
/// On first boot the function writes shape (1) when `keys["main"]` has
/// a parseable `public_key`; otherwise it writes shape (2) and emits a
/// `tracing::warn!` recommending the operator configure `[key.main]`.
/// Either way the file permissions are tightened to owner-only on unix
/// (`0o600`) as defense in depth; the age layer is the load-bearing
/// confidentiality control.
///
/// `config_dir` is used to resolve a relative `identity_file` path on
/// the decryption side (same shape as `identity::load_secret_key`).
///
/// Slice S2 (2026-05-20). Paper-recovery passphrase wrap
/// (`docs/reference/key-recovery.md`) layers on top in a later slice —
/// add the recovery age recipient to the wrap, re-encrypt.
pub fn load_or_generate_master_signing_key(
    path: &std::path::Path,
    keys: &std::collections::BTreeMap<String, NodeConfigKey>,
    config_dir: Option<&std::path::Path>,
) -> Result<SigningKey> {
    use rand::Rng;

    let key_main = keys.get("main");

    if path.exists() {
        let raw_bytes = std::fs::read(path)
            .map_err(|e| anyhow!("reading master key file {}: {e}", path.display()))?;

        let seed_bytes: Vec<u8> = if raw_bytes.starts_with(AGE_V1_HEADER) {
            // Age-encrypted: decrypt via [key.main].identity_file.
            let id_file_rel = key_main
                .and_then(|k| k.identity_file.as_ref())
                .ok_or_else(|| {
                    anyhow!(
                        "master key file {} is age-encrypted, but \
                         [key.main].identity_file is not configured — \
                         cannot decrypt",
                        path.display()
                    )
                })?;
            let resolved = {
                let p = std::path::Path::new(id_file_rel);
                if p.is_relative() {
                    config_dir
                        .map(|d| d.join(p))
                        .unwrap_or_else(|| p.to_path_buf())
                } else {
                    p.to_path_buf()
                }
            };
            crate::tasks::vault_persist::age_decrypt_with_identity_files(
                &raw_bytes,
                &[resolved.to_string_lossy().into_owned()],
            )
            .map_err(|e| anyhow!("age-decrypting master key file {}: {e}", path.display()))?
        } else {
            raw_bytes
        };

        let seed: [u8; 32] = seed_bytes.as_slice().try_into().map_err(|_| {
            anyhow!(
                "master key seed from {} has wrong size: {} (expected 32)",
                path.display(),
                seed_bytes.len()
            )
        })?;
        return Ok(SigningKey::from_bytes(&seed));
    }

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow!("creating master key parent dir {}: {e}", parent.display()))?;
    }
    let mut seed = [0u8; 32];
    rand::rng().fill_bytes(&mut seed);

    let bytes_to_write: Vec<u8> = match key_main {
        Some(k) => {
            // Encrypt to [key.main].public_key. age::Recipient parsing
            // is delegated to age_encrypt_for_recipients.
            crate::tasks::vault_persist::age_encrypt_for_recipients(
                &seed,
                std::slice::from_ref(&k.public_key),
            )
            .map_err(|e| anyhow!("age-encrypting master key for [key.main]: {e}"))?
        }
        None => {
            tracing::warn!(
                path = %path.display(),
                "[key.main] is not configured — writing master key file as plaintext. \
                 Configure [key.main] with an age public_key + identity_file to enable \
                 at-rest encryption."
            );
            seed.to_vec()
        }
    };
    std::fs::write(path, &bytes_to_write)
        .map_err(|e| anyhow!("writing master key file {}: {e}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(SigningKey::from_bytes(&seed))
}

/// Default path for the master key file when `[identity].master_key_file`
/// is unset. Resolved as a sibling of `[identity].secret_key_file` —
/// i.e. the iroh secret's directory — under the name
/// `identity_master.key`. Returns `None` when no iroh-secret file is
/// configured (inline secret or no key at all), in which case the
/// caller should fall back to blake3-deriving the master from the iroh
/// secret.
pub fn default_master_key_path(
    identity: &s5_node_api::config::NodeConfigIdentity,
    config_dir: Option<&std::path::Path>,
) -> Option<std::path::PathBuf> {
    let secret_file = identity.secret_key_file.as_deref()?;
    let p = std::path::Path::new(secret_file);
    let resolved = if p.is_relative() {
        config_dir
            .map(|d| d.join(p))
            .unwrap_or_else(|| p.to_path_buf())
    } else {
        p.to_path_buf()
    };
    resolved.parent().map(|d| d.join("identity_master.key"))
}

/// 16-byte vault id reserved for identity vaults across all DIDs.
/// Derived deterministically so any consumer can compute it without
/// resolving config: `blake3("s5/identity-vault/v1")[..16]` — the
/// [`well_known_vault_id`](crate::tasks::publish::well_known_vault_id) of the
/// identity-vault domain (same scheme as the `stores` / `identity_secrets`
/// special vaults).
pub fn identity_vault_id() -> [u8; 16] {
    crate::tasks::publish::well_known_vault_id("s5/identity-vault/v1")
}

/// Build the daemon's self-published [`IdentityBundle`] at the given
/// revision.
///
/// Four distinct ed25519 pubkeys (single-device today; one entry per
/// keyset array):
///
/// - `device_signing_pubkey` → `signers[]`. The per-device
///   write-authority key; matches `tasks::publish::device_signing_key`
///   so a verifier checking `vault-write-pubkey ∈ signers[]` (F01
///   verify-chain level 3) succeeds.
/// - `acl_pubkey` → `acl_keys[]`. The per-device read-access key; a
///   blob-fetch connection challenge proves possession of it
///   (`acl-and-revocation.md §3a`).
/// - `iroh_node_id` → `iroh_pubkeys[]`. The QUIC transport pubkey;
///   never an authorisation principal (`identity-model.md` § Per-device
///   keys).
/// - The DID-encoded master pubkey is set by the caller (it's the
///   StreamKey::Vault.pubkey under which the bundle is published); it
///   does NOT appear in the bundle blob — the master signature lives on
///   the registry entry that points at this blob, not inside it.
///
/// Single-device shape only — a test/bootstrap convenience. The live
/// bundle is maintained by the read-merge-write admission core
/// ([`crate::admission`], D9/D10): sibling devices' keys are unioned in
/// by `admit_device_keys`, never rebuilt from one device's view.
pub fn build_self_identity_bundle(
    device_signing_pubkey: [u8; 32],
    acl_pubkey: [u8; 32],
    iroh_node_id: [u8; 32],
    age_recipients: Vec<String>,
    revision: u64,
) -> IdentityBundle {
    IdentityBundle {
        version: IdentityBundle::CURRENT_VERSION,
        revision,
        signers: vec![device_signing_pubkey],
        acl_keys: vec![acl_pubkey],
        iroh_pubkeys: vec![iroh_node_id],
        age_recipients,
    }
}

/// Every `[key.*]` public key — the age recipients advertised in this device's
/// identity bundle: the device's own age key(s), the paper recovery key
/// (`[key.recovery]`), and any further decryption-capable key (e.g. a YubiKey)
/// the user adds. Because publishers resolve a vault's recipients from the
/// member bundles' `age_recipients`, advertising the recovery key here makes
/// *every* writer — own devices and co-members alike — always encrypt for it, so
/// paper recovery of any vault's content is guaranteed without per-vault
/// recipient config. Deterministic order (`config.key` is a `BTreeMap`) keeps
/// the bundle idempotent across boots.
pub fn bundle_age_recipients(
    keys: &std::collections::BTreeMap<String, s5_node_api::config::NodeConfigKey>,
) -> Vec<String> {
    keys.values().map(|k| k.public_key.clone()).collect()
}

/// Top-level entry point called from daemon startup: **ensure this
/// device's keys are present** in the identity bundle. Best-effort: any
/// failure logs and skips, never blocks the daemon from coming up.
///
/// Built ON TOP of the read-merge-write admission core
/// ([`crate::admission::ensure_device_present`], D9): this device's
/// three pubkeys and every `[key.*]` age recipient are UNIONED into the
/// current bundle — sibling devices' entries survive a boot; the
/// pre-D10 behavior of rebuilding a fresh single-device bundle (which
/// clobbered siblings) is gone. The registry entry's ed25519 signature
/// is the *only* warm-key signature in the chain; the bundle blob
/// itself is unsigned (`identity-model.md` § Identity bundle).
///
/// Idempotency: if this device's keys are already all present, nothing
/// is republished (revision does not bump — bundle content drives
/// revision, not daemon restarts) and the current bundle blob hash is
/// returned for `MembershipState.public_blob_hashes` (S3a).
pub async fn publish_self_on_startup(
    config: &crate::config::S5NodeConfig,
    stores: &std::collections::HashMap<String, Arc<dyn s5_core::blob::Blobs>>,
    registry: Arc<dyn RegistryApi + Send + Sync>,
    master_signing_key: &SigningKey,
    device_signing_pubkey: [u8; 32],
    acl_pubkey: [u8; 32],
    iroh_pubkey: [u8; 32],
) -> Option<Hash> {
    let age_recipients = bundle_age_recipients(&config.key);
    if age_recipients.is_empty() {
        tracing::info!("identity publish: no [key.*] recipients configured, skipping");
        return None;
    }

    if stores.is_empty() {
        tracing::info!("identity publish: no blob stores configured, skipping");
        return None;
    }

    // Four pubkeys, four roles. Caller is responsible for sourcing
    // each consistently with how the daemon will actually use them:
    //   * `master_signing_key` — the WARM master (D17): the bundle
    //     stream lives at `(warm_pub, IDENTITY_VAULT_ID)` and is
    //     warm-signed; resolvers reach it via the DID's cold pointer.
    //   * `device_signing_pubkey` — matches the key
    //     `tasks::publish::device_signing_key` will sign vault registry
    //     entries with. Today: keyset.device_signing from S2.5; falls
    //     back to blake3-of-iroh in legacy mode.
    //   * `acl_pubkey` — populates `bundle.acl_keys[]` for the F02
    //     blob-fetch challenge. Today: keyset.device_acl from S2.5;
    //     legacy fallback identical to device_signing.
    //   * `iroh_pubkey` — the iroh transport pubkey; QUIC handshake
    //     only, never an authorisation principal.
    let warm_pubkey = master_signing_key.verifying_key().to_bytes();
    // Log label only — the bundle's stream key is the warm pubkey; the
    // DID (cold) is upstream in the anchor, not derivable here.
    let did = format!("warm:{}", hex::encode(&warm_pubkey[..8]));

    match crate::admission::ensure_device_present(
        master_signing_key,
        registry.as_ref(),
        stores,
        device_signing_pubkey,
        acl_pubkey,
        iroh_pubkey,
        &age_recipients,
    )
    .await
    {
        Ok(outcome) if outcome.changed => {
            tracing::info!(
                did = did.as_str(),
                revision = outcome.revision,
                blob = %outcome.blob_hash,
                "identity bundle published (device keys merged)"
            );
            Some(outcome.blob_hash)
        }
        Ok(outcome) => {
            tracing::info!(
                did = did.as_str(),
                revision = outcome.revision,
                "identity publish: device keys already present, skipping"
            );
            Some(outcome.blob_hash)
        }
        Err(e) => {
            tracing::warn!(did = did.as_str(), "identity publish failed: {e:#}");
            None
        }
    }
}
