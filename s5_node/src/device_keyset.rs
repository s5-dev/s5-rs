//! Per-device keyset.
//!
//! Holds the device-scope ed25519 secrets — three **independent random**
//! 32-byte seeds, one per orthogonal capability the device exercises:
//!
//! | Field           | Role                                         |
//! |-----------------|----------------------------------------------|
//! | `iroh`          | iroh transport secret (QUIC handshake)       |
//! | `device_signing`| signs vault registry entries (writes)        |
//! | `device_acl`    | proves read access (F02 blob-fetch challenge)|
//!
//! **Master is not here.** The identity-master signing key is
//! per-identity (one DID may be carried by multiple devices), so it
//! lives in its own file / future `identity_secrets` vault. See
//! [`crate::identity_vault::load_or_generate_master_signing_key`].
//!
//! ## On-disk format
//!
//! CBOR map (4-byte keys for compactness), age-encrypted to
//! `[key.main].public_key` when that key is configured. Magic header
//! `b"age-encryption.org/v1\n"` is the load-time detection signal;
//! plaintext CBOR is the legacy/no-recipient fallback for deployments
//! that haven't configured `[key.main]` yet. File permissions are
//! tightened to `0o600` on unix as defense in depth.
//!
//! ## Threat model
//!
//! - **Leak of the keyset file alone** (without `[key.main]` identity
//!   file) → attacker holds opaque ciphertext, learns nothing.
//! - **Leak of `[key.main]` identity file alone** (without keyset file)
//!   → no device secrets exposed.
//! - **Leak of both files** → full device compromise. Same blast radius
//!   as today's "leak of iroh secret"; the change is that an attacker
//!   needs **two** files instead of one.
//!
//! ## Why three independent seeds, not a single seed + blake3 domains
//!
//! The previous design derived `device_signing` and `device_acl` from
//! the iroh secret via blake3 with distinct domain tags. That gives
//! *mathematically* independent keys but **operationally identical
//! compromise**: a single file leak reconstructs all three. The split
//! exposure that the four-key identity model promises only exists when
//! the bytes themselves are independent. Hence: three random seeds,
//! one file, one encryption envelope.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use ed25519_dalek::SigningKey;
use minicbor::{Decode, Encode};
use rand::Rng;

use s5_node_api::config::{NodeConfigIdentity, NodeConfigKey};

/// Magic header prefix of the age v1 ASCII format. Anything starting
/// with this byte sequence is treated as an age ciphertext; anything
/// else is treated as plaintext CBOR (fallback / no-recipient path).
const AGE_V1_HEADER: &[u8] = b"age-encryption.org/v1\n";

/// CBOR-serialised on-disk form of the keyset.
///
/// Numeric CBOR map keys per minicbor convention; tight encoding keeps
/// the plaintext at ~120 bytes (~340 bytes after the age envelope).
#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
#[cbor(map)]
struct OnDiskKeyset {
    /// Format version. Bumped on incompatible schema changes.
    #[n(0)]
    v: u8,
    /// iroh transport secret (raw 32 bytes).
    #[n(1)]
    iroh: [u8; 32],
    /// device signing secret — signs vault registry entries.
    #[n(2)]
    sign: [u8; 32],
    /// device ACL/read secret — blob-fetch challenge responder.
    #[n(3)]
    acl: [u8; 32],
}

const KEYSET_VERSION: u8 = 1;

/// In-memory device keyset. All three fields are independent random
/// ed25519 seeds.
#[derive(Debug, Clone)]
pub struct DeviceKeyset {
    pub iroh: [u8; 32],
    pub device_signing: [u8; 32],
    pub device_acl: [u8; 32],
}

impl DeviceKeyset {
    /// Generate a fresh keyset with three independent random seeds.
    pub fn generate() -> Self {
        let mut iroh = [0u8; 32];
        let mut sign = [0u8; 32];
        let mut acl = [0u8; 32];
        rand::rng().fill_bytes(&mut iroh);
        rand::rng().fill_bytes(&mut sign);
        rand::rng().fill_bytes(&mut acl);
        Self {
            iroh,
            device_signing: sign,
            device_acl: acl,
        }
    }

    /// iroh transport SecretKey constructed from `iroh`.
    pub fn iroh_secret_key(&self) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&self.iroh)
    }

    /// Device signing key (writes vault registry entries).
    pub fn device_signing_key(&self) -> SigningKey {
        SigningKey::from_bytes(&self.device_signing)
    }

    /// Device ACL/read key (blob-fetch challenge responder).
    pub fn device_acl_key(&self) -> SigningKey {
        SigningKey::from_bytes(&self.device_acl)
    }

    fn to_disk(&self) -> OnDiskKeyset {
        OnDiskKeyset {
            v: KEYSET_VERSION,
            iroh: self.iroh,
            sign: self.device_signing,
            acl: self.device_acl,
        }
    }

    fn from_disk(d: OnDiskKeyset) -> Result<Self> {
        if d.v != KEYSET_VERSION {
            return Err(anyhow!(
                "device keyset has unknown version {} (expected {})",
                d.v,
                KEYSET_VERSION
            ));
        }
        Ok(Self {
            iroh: d.iroh,
            device_signing: d.sign,
            device_acl: d.acl,
        })
    }
}

/// Load the keyset from `path`, or generate a fresh one and persist it
/// when the file is missing.
///
/// At-rest format: age-encrypted CBOR when `keys["main"]` has a
/// parseable recipient; plaintext CBOR (with a warn) otherwise. The
/// magic prefix `b"age-encryption.org/v1\n"` is detected on load to
/// pick the right decode path — i.e. the format is auto-discovered, no
/// extra bookkeeping in config.
pub fn load_or_generate_device_keyset(
    path: &Path,
    keys: &BTreeMap<String, NodeConfigKey>,
    config_dir: Option<&Path>,
) -> Result<DeviceKeyset> {
    let key_main = keys.get("main");

    if path.exists() {
        let raw_bytes = std::fs::read(path)
            .map_err(|e| anyhow!("reading device keyset file {}: {e}", path.display()))?;

        let cbor_bytes: Vec<u8> = if raw_bytes.starts_with(AGE_V1_HEADER) {
            let id_file_rel = key_main
                .and_then(|k| k.identity_file.as_ref())
                .ok_or_else(|| {
                    anyhow!(
                        "device keyset file {} is age-encrypted, but \
                         [key.main].identity_file is not configured — \
                         cannot decrypt",
                        path.display()
                    )
                })?;
            let resolved = {
                let p = Path::new(id_file_rel);
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
            .map_err(|e| anyhow!("age-decrypting device keyset {}: {e}", path.display()))?
        } else {
            raw_bytes
        };

        let on_disk: OnDiskKeyset = minicbor::decode(&cbor_bytes)
            .map_err(|e| anyhow!("decoding device keyset CBOR from {}: {e}", path.display()))?;
        return DeviceKeyset::from_disk(on_disk);
    }

    // Fresh generation — three independent random seeds.
    let ks = DeviceKeyset::generate();
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|e| {
            anyhow!(
                "creating device keyset parent dir {}: {e}",
                parent.display()
            )
        })?;
    }

    let cbor = minicbor::to_vec(ks.to_disk())
        .map_err(|e| anyhow!("encoding device keyset to CBOR: {e}"))?;

    let bytes_to_write: Vec<u8> = match key_main {
        Some(k) => crate::tasks::vault_persist::age_encrypt_for_recipients(
            &cbor,
            std::slice::from_ref(&k.public_key),
        )
        .map_err(|e| anyhow!("age-encrypting device keyset to [key.main]: {e}"))?,
        None => {
            tracing::warn!(
                path = %path.display(),
                "[key.main] is not configured — writing device keyset as plaintext CBOR. \
                 Configure [key.main] with an age public_key + identity_file to enable \
                 at-rest encryption."
            );
            cbor
        }
    };

    std::fs::write(path, &bytes_to_write)
        .map_err(|e| anyhow!("writing device keyset file {}: {e}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(ks)
}

/// Default path for the device keyset file when
/// `[identity].keyset_file` is unset. Resolved as a sibling of
/// `[identity].secret_key_file` (its directory, with name
/// `device_keyset.cbor.age`). Returns `None` when no iroh-secret file
/// is configured.
pub fn default_keyset_path(
    identity: &NodeConfigIdentity,
    config_dir: Option<&Path>,
) -> Option<PathBuf> {
    let secret_file = identity.secret_key_file.as_deref()?;
    let p = Path::new(secret_file);
    let resolved = if p.is_relative() {
        config_dir
            .map(|d| d.join(p))
            .unwrap_or_else(|| p.to_path_buf())
    } else {
        p.to_path_buf()
    };
    resolved.parent().map(|d| d.join("device_keyset.cbor.age"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use age::secrecy::ExposeSecret;
    use tempfile::tempdir;

    fn keymain(dir: &Path) -> (BTreeMap<String, NodeConfigKey>, age::x25519::Identity) {
        let identity = age::x25519::Identity::generate();
        let id_path = dir.join("main.age");
        std::fs::write(&id_path, identity.to_string().expose_secret()).unwrap();
        let mut keys = BTreeMap::new();
        keys.insert(
            "main".to_string(),
            NodeConfigKey {
                public_key: identity.to_public().to_string(),
                identity_file: Some(id_path.to_string_lossy().into_owned()),
            },
        );
        (keys, identity)
    }

    #[test]
    fn fresh_generation_yields_three_independent_seeds() {
        let dir = tempdir().unwrap();
        let (keys, _) = keymain(dir.path());
        let path = dir.path().join("device_keyset.cbor.age");
        let ks = load_or_generate_device_keyset(&path, &keys, None).unwrap();
        // With 32-byte random seeds, collision is astronomically
        // unlikely — any equality between fields is a derivation bug.
        assert_ne!(ks.iroh, ks.device_signing);
        assert_ne!(ks.iroh, ks.device_acl);
        assert_ne!(ks.device_signing, ks.device_acl);
    }

    #[test]
    fn round_trip_age_encrypted() {
        let dir = tempdir().unwrap();
        let (keys, _) = keymain(dir.path());
        let path = dir.path().join("device_keyset.cbor.age");
        let ks1 = load_or_generate_device_keyset(&path, &keys, None).unwrap();

        let on_disk = std::fs::read(&path).unwrap();
        assert!(
            on_disk.starts_with(AGE_V1_HEADER),
            "with [key.main] configured, the keyset file must be age ciphertext"
        );

        let ks2 = load_or_generate_device_keyset(&path, &keys, None).unwrap();
        assert_eq!(ks1.iroh, ks2.iroh);
        assert_eq!(ks1.device_signing, ks2.device_signing);
        assert_eq!(ks1.device_acl, ks2.device_acl);
    }

    #[test]
    fn round_trip_plaintext_when_no_keymain() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("device_keyset.cbor.age");
        let empty: BTreeMap<String, NodeConfigKey> = BTreeMap::new();
        let ks1 = load_or_generate_device_keyset(&path, &empty, None).unwrap();

        let on_disk = std::fs::read(&path).unwrap();
        assert!(
            !on_disk.starts_with(AGE_V1_HEADER),
            "with no [key.main], the keyset file is plaintext CBOR"
        );

        let ks2 = load_or_generate_device_keyset(&path, &empty, None).unwrap();
        assert_eq!(ks1.iroh, ks2.iroh);
        assert_eq!(ks1.device_signing, ks2.device_signing);
        assert_eq!(ks1.device_acl, ks2.device_acl);
    }

    #[test]
    fn wrong_identity_file_fails_to_decrypt() {
        let dir = tempdir().unwrap();
        let (keys, _) = keymain(dir.path());
        let path = dir.path().join("device_keyset.cbor.age");
        let _ks = load_or_generate_device_keyset(&path, &keys, None).unwrap();

        // Substitute the recipient + identity_file with an unrelated
        // age key. Decrypt must fail (not silently fall through to
        // plaintext-CBOR decode, which would be a security regression).
        let other = age::x25519::Identity::generate();
        let other_path = dir.path().join("other.age");
        std::fs::write(&other_path, other.to_string().expose_secret()).unwrap();
        let mut wrong = BTreeMap::new();
        wrong.insert(
            "main".to_string(),
            NodeConfigKey {
                public_key: other.to_public().to_string(),
                identity_file: Some(other_path.to_string_lossy().into_owned()),
            },
        );
        let err = load_or_generate_device_keyset(&path, &wrong, None).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("decrypt") || msg.contains("identity"),
            "expected age decrypt failure, got: {msg}"
        );
    }

    #[test]
    fn default_path_is_sibling_of_secret_key_file() {
        let identity = NodeConfigIdentity {
            secret_key: None,
            secret_key_file: Some("/var/lib/s5/iroh.secret".to_string()),
            encrypted_with: None,
            master_key_file: None,
            anchor_entry_file: None,
            keyset_file: None,
            bootstrap_store: None,
        };
        let p = default_keyset_path(&identity, None).unwrap();
        assert_eq!(p, Path::new("/var/lib/s5/device_keyset.cbor.age"));
    }

    #[test]
    fn default_path_none_when_inline_only() {
        let identity = NodeConfigIdentity {
            secret_key: Some("inline".to_string()),
            secret_key_file: None,
            encrypted_with: None,
            master_key_file: None,
            anchor_entry_file: None,
            keyset_file: None,
            bootstrap_store: None,
        };
        assert!(default_keyset_path(&identity, None).is_none());
    }
}
