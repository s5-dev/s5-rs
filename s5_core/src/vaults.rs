//! Wire types for the special vaults' KV leaves.
//!
//! The special vaults (`identity_secrets`, `stores`; see
//! `docs/reference/special-vaults.md`) are ordinary single-leaf `s5_fs_v2`
//! prolly-tree vaults used as tiny key→value stores. This module pins the CBOR
//! byte layout of the *values* stored under their keys — the device-independent
//! part that must round-trip exactly. The vault container, its encryption, and
//! the locator (`vault_id`) derivation live in the `s5_node` orchestration
//! layer, not here.

use std::collections::BTreeMap;

use minicbor::{Decode, Encode};

/// The backend kind of a [`StoreEntry`], mirrored by the small integer stored
/// in [`StoreEntry::kind`]. Kept `u8`-valued and open-ended so a new backend is
/// a forward-compatible addition: an unknown value still decodes, and an older
/// client simply treats it as unrecognised rather than failing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum StoreKind {
    Indexd = 0,
    S3 = 1,
    Sia = 2,
    Local = 3,
}

impl StoreKind {
    /// The `u8` written into [`StoreEntry::kind`].
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Recognise a stored `kind` byte, or `None` if a newer client wrote a
    /// backend kind this client doesn't know.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Indexd),
            1 => Some(Self::S3),
            2 => Some(Self::Sia),
            3 => Some(Self::Local),
            _ => None,
        }
    }
}

/// One vault-managed storage backend, stored in the warm `stores` vault under
/// `stores/<name>` (the canonical store name is the tree key, not a field).
///
/// **Static state only.** A `credential` of `None` marks a **managed** account
/// whose secret is *derived* from `stores_seed ‖ <store name>` (the store name
/// is the derivation label, so no key is stored); `Some` is an **imported**
/// external key. The per-device sync checkpoint is deliberately *not* here — it
/// is local cache progress. See `special-vaults.md` § 4.
#[derive(Clone, Debug, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct StoreEntry {
    /// Backend kind — see [`StoreKind`]. Stored as `u8` for forward-compat.
    #[n(0)]
    pub kind: u8,
    /// Non-secret backend config as kind-specific string keys (indexd:
    /// `{"url": …}`; s3: `{"bucket", "region", "endpoint"}`).
    #[n(1)]
    pub config: BTreeMap<String, String>,
    /// The stored secret, or `None` for a derived (managed) account.
    #[n(2)]
    #[cbor(with = "minicbor::bytes")]
    pub credential: Option<Vec<u8>>,
}

impl StoreEntry {
    /// A **managed** account — its credential derives from `stores_seed ‖ <store
    /// name>`, so nothing secret is stored.
    pub fn managed(kind: StoreKind, config: BTreeMap<String, String>) -> Self {
        Self {
            kind: kind.as_u8(),
            config,
            credential: None,
        }
    }

    /// An **imported** account with a stored external `credential`.
    pub fn imported(
        kind: StoreKind,
        config: BTreeMap<String, String>,
        credential: Vec<u8>,
    ) -> Self {
        Self {
            kind: kind.as_u8(),
            config,
            credential: Some(credential),
        }
    }

    pub fn encode_cbor(&self) -> Vec<u8> {
        minicbor::to_vec(self).expect("CBOR encoding into Vec is infallible")
    }

    pub fn decode_cbor(bytes: &[u8]) -> Result<Self, minicbor::decode::Error> {
        minicbor::decode(bytes)
    }
}

// The vault discovery index (`name → {vault_id, stores}`) lives in the warm
// `config` vault as `s5_node::config_vault::VaultDirEntry`, reached from the
// same master anchor; it is not a cold-vault catalogue keyed by writers.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_entry_round_trips_managed_and_imported() {
        let imported = StoreEntry::imported(
            StoreKind::Indexd,
            BTreeMap::from([("url".to_string(), "https://sia.storage".to_string())]),
            vec![7u8; 32],
        );
        assert_eq!(
            StoreEntry::decode_cbor(&imported.encode_cbor()).unwrap(),
            imported
        );

        let managed = StoreEntry::managed(StoreKind::Indexd, BTreeMap::new());
        assert_eq!(
            StoreEntry::decode_cbor(&managed.encode_cbor()).unwrap(),
            managed
        );
        assert!(managed.credential.is_none());
    }

    #[test]
    fn store_kind_round_trips_and_rejects_unknown() {
        for k in [
            StoreKind::Indexd,
            StoreKind::S3,
            StoreKind::Sia,
            StoreKind::Local,
        ] {
            assert_eq!(StoreKind::from_u8(k.as_u8()), Some(k));
        }
        assert_eq!(StoreKind::from_u8(200), None);
    }

    #[test]
    fn encoding_is_deterministic() {
        // Stable bytes from stable inputs — the leaf value is content-addressed.
        let e = StoreEntry::imported(
            StoreKind::Indexd,
            BTreeMap::from([("a".to_string(), "1".to_string())]),
            vec![1, 2, 3],
        );
        assert_eq!(e.encode_cbor(), e.encode_cbor());
    }
}
