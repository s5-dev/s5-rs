//! Defines the core data structures for S5 streams and the registry.
//!
//! This module unifies the implementation for both S5 Streams and the S5 Registry,
//! a significant simplification from the original spec where they used different
//! byte constants (0x07 for registry, 0x08 for streams) and different endianness
//! for revision numbers.
//!
//! ## Design Decisions
//!
//! **Unified Implementation**: Both streams and registry now share the same underlying
//! data structure (`StreamMessage`). This reduces code duplication and simplifies
//! the protocol. The only difference is behavioral:
//! - For a `Stream` (type 0), nodes store all messages forming an append-only log.
//! - For a `Registry` (type 1), nodes only keep the entry with the highest revision,
//!   making it a mutable key-value store.
//!
//! **Eventual Consistency**: To handle network partitions and concurrent updates,
//! we use a deterministic tie-breaking rule: when two messages have the same revision,
//! the one with the lexicographically smaller payload “value” wins (intuitively,
//! “starts with more zeros” wins). Practically, since not all peers will carry inline
//! payload bytes, we must use a value that is available everywhere to avoid divergence.
//! Therefore, the canonical comparator is the BLAKE3 payload hash (`hash` field).
//! If inline payload is present, nodes SHOULD verify it hashes to `hash` before accepting.
//!
//! ## Wire Format Summary
//!
//! Registry v3 (`MessageType::Registry`, `StreamKey::Vault`):
//!
//! | Field | Size (bytes) | Description |
//! |---|---|---|
//! | Message Type | 1 | `0x5c` = Registry v3 (with VAULT_ID) |
//! | Key Type | 1 | `0xed` = ed25519 vault entry |
//! | Public Key | 32 | The device's ed25519 pubkey (reused across vaults) |
//! | Vault ID | 16 | Per-vault namespace tag |
//! | Revision | 8 | Big-endian `u64` |
//! | Hash | 32 | BLAKE3 hash of the payload |
//! | Signature | 64 | Ed25519 signature |
//! | Data | Variable | Inline payload (optional, max 1024 bytes) |
//!
//! Other variants (legacy / non-vault):
//!
//! | Field | Size (bytes) | Description |
//! |---|---|---|
//! | Message Type | 1 | `0x00` (Stream — append-only log) |
//! | Key Type | 1 | `0x00` (Local) or `0x03` (Blake3HashPin) |
//! | Key Data | 32 | The identifier |
//! | Revision | 8 | Big-endian `u64` |
//! | Hash | 32 | BLAKE3 hash of the payload |
//! | Signature | 0 | (none for these key types) |
//! | Data | Variable | Inline payload (optional, max 1024 bytes) |
//!
//! Per `docs/reference/snapshot-publication.md` § Registry entry format (v3):
//! the lookup key for a vault entry is `(PUBKEY, VAULT_ID)` — same writer
//! pubkey distinguishes vaults via `VAULT_ID`. Non-vault uses (pin
//! tracking, local actor state, append-only streams) keep their existing
//! key types.

use crate::Hash;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};

mod order;

/// Maximum allowed data size for inline data in a message (1024 bytes).
/// This keeps messages small enough for efficient P2P gossip while still
/// allowing for small metadata.
pub const MAX_INLINE_DATA_SIZE: usize = 1024;

/// Size of an Ed25519 public key in bytes.
pub const KEY_SIZE: usize = 32;

/// Size of a BLAKE3 hash in bytes.
pub const HASH_SIZE: usize = 32;

/// Size of an Ed25519 signature in bytes.
pub const SIGNATURE_SIZE: usize = 64;

/// Size of a vault ID (the per-vault namespace tag in `StreamKey::Vault`).
/// 16 bytes = 128 bits — collision-resistant at any plausible vault count.
/// See `docs/reference/snapshot-publication.md` § Vault ID derivation.
pub const VAULT_ID_SIZE: usize = 16;

// Re-export the shared multihash constant so call sites in this module
// can spell it `MULTIHASH_BLAKE3` without importing `crate::blob::...`.
pub use crate::blob::identifier::MULTIHASH_BLAKE3;

/// Maximum size of a v3 vault registry payload. `LEN` is a single
/// byte, so the payload (multihash tag + hash + any inline tail) cannot
/// exceed 255 bytes. Legacy `Local` / `Blake3HashPin` entries keep the
/// pre-v3 framing and are bounded by `MAX_INLINE_DATA_SIZE` instead.
pub const MAX_VAULT_PAYLOAD_LEN: usize = u8::MAX as usize;

/// Domain-separation tag for v3 vault-entry signing bytes.
/// Per `docs/reference/snapshot-publication.md` § Registry entry format
/// (v3): `SIG = ed25519(SIG_DOMAIN_TAG_V3 || PUBKEY || VAULT_ID || REVISION || PAYLOAD)`.
pub const SIG_DOMAIN_TAG_V3: &[u8] = b"s5-reg-v3:";

/// A type alias for a 32-byte Ed25519 public key.
pub type PublicKeyEd25519 = [u8; KEY_SIZE];

/// Represents the key for a stream or registry entry.
///
/// A key identifies the "owner" or "topic" of a stream or registry entry.
/// It can either be a per-vault Ed25519-signed entry (the dominant case
/// for the registry layer), a local randomly-generated identifier for
/// internal/ephemeral use, or a BLAKE3 hash used for pin metadata.
#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug, PartialOrd, Ord)]
#[non_exhaustive]
pub enum StreamKey {
    /// A local, 32-byte identifier, not tied to a cryptographic keypair.
    /// Useful for local-only or ephemeral streams where cryptographic
    /// authentication is not required (e.g., internal node communication,
    /// actor-state checkpoints).
    Local([u8; KEY_SIZE]),

    /// An Ed25519-signed registry entry scoped to a specific vault.
    /// `pubkey` is the device's ed25519 transport key (reused across every
    /// vault that device writes to); `vault_id` is the 16-byte per-vault
    /// namespace tag derived from the vault root's `KEY_SLOT_RECOVERY`
    /// slot (see `docs/reference/snapshot-publication.md` § Vault ID
    /// derivation). Lookup is by the pair `(pubkey, vault_id)`.
    /// Entries must be signed with the corresponding private key.
    Vault {
        pubkey: PublicKeyEd25519,
        vault_id: [u8; VAULT_ID_SIZE],
    },

    /// A non-vault Ed25519-signed registry entry. Used by legacy
    /// callers (s5_fs DirActor, the Flutter/WASM bindings) that pre-date
    /// the per-vault namespace tag. Wire format keeps the v2 layout
    /// (no LEN/PAYLOAD framing, raw 32-byte HASH field) so existing
    /// data is read back unchanged.
    ///
    /// **Not for new code.** New ed25519-signed registry use should be
    /// `Vault { pubkey, vault_id }`.
    PublicKeyEd25519(PublicKeyEd25519),

    /// A 32-byte BLAKE3 hash, used for pin metadata.
    /// For this key type, larger inline payloads are allowed so that
    /// large pin sets can be stored without being constrained by the
    /// default inline data limit.
    Blake3HashPin([u8; KEY_SIZE]),
}

/// The type of a message on the wire, distinguishing between a Stream and
/// a Registry entry. The on-wire byte for `Registry` was bumped to `0x5c`
/// in the v3 format change (which also added the `VAULT_ID` field for
/// `StreamKey::Vault` entries) — old v2 registry bytes (type `0x01`) are
/// rejected on parse. `0x5c` sits in the s5 magic-byte cluster, where the
/// low nibble is a mnemonic letter: `0x5b` = blobs ("b"), `0x5c` =
/// current/registry ("c"), `0x5e` historically encryption ("e").
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum MessageType {
    /// A message that is part of a persistent, append-only log.
    Stream = 0,
    /// A key-value entry where only the latest version is kept.
    /// Wire byte `0x5c` (v3 — entries with `StreamKey::Vault` carry a
    /// 16-byte `VAULT_ID` after the public key).
    Registry = 0x5c,
}

/// Represents a single message in a Stream or an update to a Registry entry.
///
/// This unified structure replaces separate `SignedRegistryEntry` and stream message
/// types from the original spec and contains all the necessary information for a node to validate, store, and route the message.
///
/// The `Ord` implementation is crucial for resolving conflicts in a distributed setting
/// and ensuring eventual consistency across the network.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMessage {
    /// The type of the message (Stream or Registry).
    pub type_id: MessageType,

    /// The key identifying the stream or registry entry.
    pub key: StreamKey,

    /// A 64-bit revision number.
    /// - For streams: typically a timestamp (upper 32 bits) + sequence (lower 32 bits)
    /// - For registry: monotonically increasing version number
    ///
    /// Using u64 instead of separate timestamp/sequence fields provides flexibility.
    pub revision: u64,

    /// The BLAKE3 hash of the data associated with this message.
    /// This serves both as a content identifier and as the tie-breaker for
    /// eventual consistency (lexicographically smaller hash wins).
    ///
    /// Rationale: not all peers will have inline data; using the hash guarantees
    /// that all peers can compute the same ordering and converge.
    pub hash: Hash,

    /// The Ed25519 signature proving ownership of the key.
    /// - For `StreamKey::Local`, this MUST be empty.
    /// - For `StreamKey::PublicKeyEd25519`, this MUST be a valid 64-byte signature.
    pub signature: Box<[u8]>,

    /// Optional inline data. If `Some`, contains the actual data (max 1024 bytes).
    /// If `None`, the data must be fetched from the network using the `hash`.
    /// Nodes SHOULD verify that `BLAKE3(data) == hash` when present.
    pub data: Option<Bytes>,
}

/// Errors that can occur during StreamMessage operations.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamMessageError {
    #[error("invalid key: {0}")]
    InvalidKey(#[from] StreamKeyDeserializeError),

    #[error("invalid message type: {0}")]
    InvalidMessageType(#[from] MessageTypeTryFromError),

    #[error("signature required for public key")]
    SignatureRequired,

    #[error("invalid signature length: expected {expected}, got {actual}")]
    InvalidSignatureLength { expected: usize, actual: usize },

    #[error("inline data too large: {size} bytes (max: {max})")]
    DataTooLarge { size: usize, max: usize },

    #[error("vault payload exceeds {max}-byte LEN limit: {size} bytes")]
    VaultPayloadTooLarge { size: usize, max: usize },

    #[error("unknown multihash tag in vault payload: {0:#x}")]
    UnknownMultihashTag(u8),

    #[error("insufficient bytes for deserialization")]
    InsufficientBytes,

    #[error("signature verification failed")]
    InvalidSignature,
}

/// Errors that can occur when deserializing a `StreamKey`.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamKeyDeserializeError {
    #[error("invalid data length: expected {expected}, got {actual}")]
    InvalidLength { expected: usize, actual: usize },

    #[error("unknown stream key type: {0}")]
    UnknownId(u8),
}

impl StreamKey {
    /// Identifier for the `Local` variant.
    pub const LOCAL_ID: u8 = 0x00;

    /// Identifier for the `PublicKeyEd25519` variant — non-vault
    /// ed25519-signed entry. Kept at the legacy v2 wire byte so existing
    /// data round-trips.
    pub const PUBLIC_KEY_ED25519_ID: u8 = 0x01;

    /// Identifier for the `Vault` variant — ed25519 vault entry. The `0xed`
    /// value is mnemonic for "ed25519".
    pub const VAULT_ID_KEYTYPE: u8 = 0xed;

    /// Identifier for the `Blake3HashPin` variant.
    pub const BLAKE3_HASH_PIN_ID: u8 = 0x03;

    /// Returns the serialized form suitable as a backend storage key for
    /// this `StreamKey` — a single byte vector that encodes both the
    /// variant tag and any per-variant data (including the 16-byte
    /// `vault_id` for `Vault` entries). Backends use this to key their
    /// internal maps; the encoding is stable and includes enough
    /// information to round-trip via `from_storage_key`.
    pub fn storage_key(&self) -> Vec<u8> {
        match self {
            StreamKey::Local(data) => {
                let mut buf = Vec::with_capacity(1 + KEY_SIZE);
                buf.push(Self::LOCAL_ID);
                buf.extend_from_slice(data);
                buf
            }
            StreamKey::PublicKeyEd25519(data) => {
                let mut buf = Vec::with_capacity(1 + KEY_SIZE);
                buf.push(Self::PUBLIC_KEY_ED25519_ID);
                buf.extend_from_slice(data);
                buf
            }
            StreamKey::Vault { pubkey, vault_id } => {
                let mut buf = Vec::with_capacity(1 + KEY_SIZE + VAULT_ID_SIZE);
                buf.push(Self::VAULT_ID_KEYTYPE);
                buf.extend_from_slice(pubkey);
                buf.extend_from_slice(vault_id);
                buf
            }
            StreamKey::Blake3HashPin(data) => {
                let mut buf = Vec::with_capacity(1 + KEY_SIZE);
                buf.push(Self::BLAKE3_HASH_PIN_ID);
                buf.extend_from_slice(data);
                buf
            }
        }
    }

    /// Inverse of `storage_key`: parse the backend storage key back into
    /// a `StreamKey`. Returns an error for unknown tags or wrong lengths.
    pub fn from_storage_key(bytes: &[u8]) -> Result<Self, StreamKeyDeserializeError> {
        let (&tag, rest) = bytes
            .split_first()
            .ok_or(StreamKeyDeserializeError::InvalidLength {
                expected: 1,
                actual: 0,
            })?;
        match tag {
            Self::LOCAL_ID => {
                let arr: [u8; KEY_SIZE] =
                    rest.try_into()
                        .map_err(|_| StreamKeyDeserializeError::InvalidLength {
                            expected: KEY_SIZE,
                            actual: rest.len(),
                        })?;
                Ok(StreamKey::Local(arr))
            }
            Self::PUBLIC_KEY_ED25519_ID => {
                let arr: [u8; KEY_SIZE] =
                    rest.try_into()
                        .map_err(|_| StreamKeyDeserializeError::InvalidLength {
                            expected: KEY_SIZE,
                            actual: rest.len(),
                        })?;
                Ok(StreamKey::PublicKeyEd25519(arr))
            }
            Self::VAULT_ID_KEYTYPE => {
                if rest.len() != KEY_SIZE + VAULT_ID_SIZE {
                    return Err(StreamKeyDeserializeError::InvalidLength {
                        expected: KEY_SIZE + VAULT_ID_SIZE,
                        actual: rest.len(),
                    });
                }
                let mut pubkey = [0u8; KEY_SIZE];
                pubkey.copy_from_slice(&rest[..KEY_SIZE]);
                let mut vault_id = [0u8; VAULT_ID_SIZE];
                vault_id.copy_from_slice(&rest[KEY_SIZE..]);
                Ok(StreamKey::Vault { pubkey, vault_id })
            }
            Self::BLAKE3_HASH_PIN_ID => {
                let arr: [u8; KEY_SIZE] =
                    rest.try_into()
                        .map_err(|_| StreamKeyDeserializeError::InvalidLength {
                            expected: KEY_SIZE,
                            actual: rest.len(),
                        })?;
                Ok(StreamKey::Blake3HashPin(arr))
            }
            other => Err(StreamKeyDeserializeError::UnknownId(other)),
        }
    }

    /// Deserializes the per-message wire form of a `StreamKey` (just the
    /// key data, not including the surrounding `MessageType`). For `Vault`
    /// entries the caller must have already read the keytype byte and
    /// supply both the 32-byte pubkey and the 16-byte vault_id concatenated.
    pub fn from_bytes(id: u8, data: &[u8]) -> Result<Self, StreamKeyDeserializeError> {
        match id {
            Self::LOCAL_ID => {
                let arr: [u8; KEY_SIZE] =
                    data.try_into()
                        .map_err(|_| StreamKeyDeserializeError::InvalidLength {
                            expected: KEY_SIZE,
                            actual: data.len(),
                        })?;
                Ok(StreamKey::Local(arr))
            }
            Self::PUBLIC_KEY_ED25519_ID => {
                let arr: [u8; KEY_SIZE] =
                    data.try_into()
                        .map_err(|_| StreamKeyDeserializeError::InvalidLength {
                            expected: KEY_SIZE,
                            actual: data.len(),
                        })?;
                Ok(StreamKey::PublicKeyEd25519(arr))
            }
            Self::VAULT_ID_KEYTYPE => {
                if data.len() != KEY_SIZE + VAULT_ID_SIZE {
                    return Err(StreamKeyDeserializeError::InvalidLength {
                        expected: KEY_SIZE + VAULT_ID_SIZE,
                        actual: data.len(),
                    });
                }
                let mut pubkey = [0u8; KEY_SIZE];
                pubkey.copy_from_slice(&data[..KEY_SIZE]);
                let mut vault_id = [0u8; VAULT_ID_SIZE];
                vault_id.copy_from_slice(&data[KEY_SIZE..]);
                Ok(StreamKey::Vault { pubkey, vault_id })
            }
            Self::BLAKE3_HASH_PIN_ID => {
                let arr: [u8; KEY_SIZE] =
                    data.try_into()
                        .map_err(|_| StreamKeyDeserializeError::InvalidLength {
                            expected: KEY_SIZE,
                            actual: data.len(),
                        })?;
                Ok(StreamKey::Blake3HashPin(arr))
            }
            _ => Err(StreamKeyDeserializeError::UnknownId(id)),
        }
    }

    /// Returns true if this key requires a signature for valid messages.
    pub fn requires_signature(&self) -> bool {
        self.signature_len() > 0
    }

    /// Returns the signature size required for valid messages by this key.
    pub fn signature_len(&self) -> usize {
        match &self {
            Self::Local(_) => 0,
            Self::PublicKeyEd25519(_) => SIGNATURE_SIZE,
            Self::Vault { .. } => SIGNATURE_SIZE,
            Self::Blake3HashPin(_) => 0,
        }
    }

    /// Returns true if the inline data size limit should be enforced for this key.
    ///
    /// For `Blake3HashPin` keys, larger payloads are allowed to support
    /// arbitrarily large pin sets.
    pub fn enforce_inline_limit(&self) -> bool {
        !matches!(self, StreamKey::Blake3HashPin(_))
    }

    /// Returns the keytype byte for this variant, as it appears on the wire.
    pub fn keytype_byte(&self) -> u8 {
        match self {
            StreamKey::Local(_) => Self::LOCAL_ID,
            StreamKey::PublicKeyEd25519(_) => Self::PUBLIC_KEY_ED25519_ID,
            StreamKey::Vault { .. } => Self::VAULT_ID_KEYTYPE,
            StreamKey::Blake3HashPin(_) => Self::BLAKE3_HASH_PIN_ID,
        }
    }
}

/// An error that can occur when converting a `u8` to a `MessageType`.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
#[error("invalid message type: {0}")]
pub struct MessageTypeTryFromError(pub u8);

impl TryFrom<u8> for MessageType {
    type Error = MessageTypeTryFromError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(MessageType::Stream),
            0x5c => Ok(MessageType::Registry),
            _ => Err(MessageTypeTryFromError(value)),
        }
    }
}

/// Build the canonical signing input for a v3 vault registry entry and
/// sign it. Returns the 64-byte ed25519 signature.
///
/// Signing input:
/// `SIG_DOMAIN_TAG_V3 || pub_key(32) || vault_id(16) || revision(8 BE) || PAYLOAD`
///
/// where `PAYLOAD = MULTIHASH_BLAKE3 (1) || hash(32) || data` — the
/// exact same bytes that go on the wire after the LEN field.
fn sign_vault_registry_payload(
    signing_key: &SigningKey,
    pub_key: &[u8; KEY_SIZE],
    vault_id: &[u8; VAULT_ID_SIZE],
    revision: u64,
    hash: &[u8; HASH_SIZE],
    inline_data: &[u8],
) -> [u8; SIGNATURE_SIZE] {
    let mut sign_bytes = Vec::with_capacity(
        SIG_DOMAIN_TAG_V3.len() + KEY_SIZE + VAULT_ID_SIZE + 8 + 1 + HASH_SIZE + inline_data.len(),
    );
    sign_bytes.extend_from_slice(SIG_DOMAIN_TAG_V3);
    sign_bytes.extend_from_slice(pub_key);
    sign_bytes.extend_from_slice(vault_id);
    sign_bytes.extend_from_slice(&revision.to_be_bytes());
    sign_bytes.push(MULTIHASH_BLAKE3);
    sign_bytes.extend_from_slice(hash);
    sign_bytes.extend_from_slice(inline_data);
    signing_key.sign(&sign_bytes).to_bytes()
}

impl StreamMessage {
    /// Creates a new StreamMessage with validation.
    ///
    /// - Local keys must have an empty signature.
    /// - Ed25519 keys must have a 64-byte signature; if missing entirely, returns `SignatureRequired`.
    /// - For `StreamKey::Vault`, the on-wire PAYLOAD (`1 + HASH_SIZE +
    ///   inline data`) must fit in `MAX_VAULT_PAYLOAD_LEN` (255 B).
    /// - For non-vault keys with `enforce_inline_limit`, inline data
    ///   must not exceed `MAX_INLINE_DATA_SIZE` (1024 B).
    ///
    /// Note: This function does not perform cryptographic verification.
    pub fn new(
        type_id: MessageType,
        key: StreamKey,
        revision: u64,
        hash: Hash,
        signature: Box<[u8]>,
        data: Option<Bytes>,
    ) -> Result<Self, StreamMessageError> {
        // Enforce presence for keys that require signatures
        if key.requires_signature() && signature.is_empty() {
            return Err(StreamMessageError::SignatureRequired);
        }

        // Enforce exact signature length per key type (including requiring empty for Local)
        let expected = key.signature_len();
        if signature.len() != expected {
            return Err(StreamMessageError::InvalidSignatureLength {
                expected,
                actual: signature.len(),
            });
        }

        match &key {
            StreamKey::Vault { .. } => {
                let data_len = data.as_ref().map_or(0, |d| d.len());
                let payload_len = 1 + HASH_SIZE + data_len;
                if payload_len > MAX_VAULT_PAYLOAD_LEN {
                    return Err(StreamMessageError::VaultPayloadTooLarge {
                        size: payload_len,
                        max: MAX_VAULT_PAYLOAD_LEN,
                    });
                }
            }
            _ => {
                // Validate inline data size (except for Blake3HashPin keys which allow
                // arbitrarily large inline payloads to support large pin sets).
                if key.enforce_inline_limit()
                    && let Some(ref d) = data
                    && d.len() > MAX_INLINE_DATA_SIZE
                {
                    return Err(StreamMessageError::DataTooLarge {
                        size: d.len(),
                        max: MAX_INLINE_DATA_SIZE,
                    });
                }
            }
        }

        Ok(Self {
            type_id,
            key,
            revision,
            hash,
            signature,
            data,
        })
    }

    /// Create a signed Ed25519 *non-vault* registry entry — for the
    /// legacy s5_fs DirActor path that uses
    /// [`StreamKey::PublicKeyEd25519`]. New code should use
    /// [`Self::sign_ed25519_registry`] instead, which produces a v3
    /// vault entry with `VAULT_ID` framing.
    ///
    /// Signing bytes (no domain tag — matches the pre-v3 layout for
    /// data round-trip):
    /// `[Registry(0x5c), 0x01 (PublicKeyEd25519), pub_key(32),
    ///   revision(8 BE), 0x21 (Multihash Blake3), hash(32)]`
    pub fn sign_ed25519_legacy(
        signing_key: &SigningKey,
        hash: Hash,
        revision: u64,
    ) -> Result<Self, StreamMessageError> {
        let verifying_key: VerifyingKey = signing_key.into();
        let pub_key_bytes = verifying_key.to_bytes();

        let mut sign_bytes = Vec::with_capacity(1 + 1 + KEY_SIZE + 8 + 1 + HASH_SIZE);
        sign_bytes.push(MessageType::Registry as u8);
        sign_bytes.push(StreamKey::PUBLIC_KEY_ED25519_ID);
        sign_bytes.extend_from_slice(&pub_key_bytes);
        sign_bytes.extend_from_slice(&revision.to_be_bytes());
        sign_bytes.push(MULTIHASH_BLAKE3);
        sign_bytes.extend_from_slice(hash.as_bytes());

        let signature = signing_key.sign(&sign_bytes);

        Self::new(
            MessageType::Registry,
            StreamKey::PublicKeyEd25519(pub_key_bytes),
            revision,
            hash,
            signature.to_bytes().to_vec().into_boxed_slice(),
            None,
        )
    }

    /// Create a signed Ed25519 vault registry entry (v3).
    ///
    /// Derives the public key from `signing_key`, constructs the canonical
    /// signing bytes, signs them, and returns a ready-to-publish message
    /// keyed by `(pubkey, vault_id)` with the hash carried as a
    /// multihash-prefixed payload.
    ///
    /// TODO: Long-term, move Ed25519 signing internals into the s5_registry
    /// crate so s5_core doesn't depend on ed25519-dalek directly.
    ///
    /// Wire payload: `0x1e (blake3 multihash) || hash[32]` (33 bytes →
    /// `LEN = 0x21`).
    ///
    /// Signing bytes:
    /// `b"s5-reg-v3:" || pub_key(32) || vault_id(16) || revision(8 BE) || PAYLOAD`
    pub fn sign_ed25519_registry(
        signing_key: &SigningKey,
        vault_id: [u8; VAULT_ID_SIZE],
        hash: Hash,
        revision: u64,
    ) -> Result<Self, StreamMessageError> {
        let verifying_key: VerifyingKey = signing_key.into();
        let pub_key_bytes = verifying_key.to_bytes();

        let signature = sign_vault_registry_payload(
            signing_key,
            &pub_key_bytes,
            &vault_id,
            revision,
            hash.as_bytes(),
            &[],
        );

        Self::new(
            MessageType::Registry,
            StreamKey::Vault {
                pubkey: pub_key_bytes,
                vault_id,
            },
            revision,
            hash,
            signature.into(),
            None,
        )
    }

    /// Serializes the message for wire transport.
    ///
    /// Wire format (Registry / `StreamKey::Vault`, the v3 vault layout):
    ///
    /// ```text
    /// TYPE(1=0x5c) | KEYTYPE(1=0xed) | PUBKEY(32) | VAULT_ID(16)
    ///   | REVISION(8 BE) | LEN(1) | PAYLOAD(LEN) | SIG(64)
    /// ```
    ///
    /// PAYLOAD = `MULTIHASH_BLAKE3(0x1e) || hash[32] || data` —
    /// for typical hash-only entries this is 33 bytes, so `LEN = 0x21`.
    /// Optional inline data (`StreamMessage::data`) is appended inside
    /// PAYLOAD; total PAYLOAD must fit in `MAX_VAULT_PAYLOAD_LEN` (255).
    /// SIG is computed by `sign_ed25519_registry` over the canonical
    /// signing bytes (`SIG_DOMAIN_TAG_V3 || PUBKEY || VAULT_ID || REVISION || PAYLOAD`).
    ///
    /// Wire format (legacy / non-`Vault`, unchanged from v2):
    ///
    /// ```text
    /// TYPE(1) | KEYTYPE(1) | KEY(32) | REVISION(8 BE) | HASH(32)
    ///   | SIG(0 or 64) | optional inline data (consumes the rest)
    /// ```
    ///
    /// `Local` and `Blake3HashPin` keep the original framing — they
    /// don't carry a `vault_id`, their signatures are empty, and inline
    /// data is bounded by `MAX_INLINE_DATA_SIZE` (1024 B) without an
    /// on-wire LEN field.
    pub fn serialize(&self) -> Bytes {
        match &self.key {
            StreamKey::Vault { pubkey, vault_id } => self.serialize_vault(pubkey, vault_id),
            _ => self.serialize_legacy(),
        }
    }

    fn serialize_vault(&self, pubkey: &[u8; KEY_SIZE], vault_id: &[u8; VAULT_ID_SIZE]) -> Bytes {
        let data_len = self.data.as_ref().map_or(0, |d| d.len());
        let payload_len = 1 + HASH_SIZE + data_len;
        debug_assert!(
            payload_len <= MAX_VAULT_PAYLOAD_LEN,
            "vault payload exceeds 255-byte LEN limit (StreamMessage::new should have rejected this)"
        );

        let mut buf = BytesMut::with_capacity(
            1 + 1 + KEY_SIZE + VAULT_ID_SIZE + 8 + 1 + payload_len + self.signature.len(),
        );
        buf.put_u8(self.type_id as u8);
        buf.put_u8(StreamKey::VAULT_ID_KEYTYPE);
        buf.put_slice(pubkey);
        buf.put_slice(vault_id);
        buf.put_u64(self.revision);
        buf.put_u8(payload_len as u8);
        buf.put_u8(MULTIHASH_BLAKE3);
        buf.put_slice(self.hash.as_ref());
        if let Some(ref data) = self.data {
            buf.put_slice(data);
        }
        buf.put_slice(&self.signature);
        buf.freeze()
    }

    fn serialize_legacy(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(
            1 + 1
                + KEY_SIZE
                + 8
                + HASH_SIZE
                + self.signature.len()
                + self.data.as_ref().map_or(0, |d| d.len()),
        );
        buf.put_u8(self.type_id as u8);
        buf.put_u8(self.key.keytype_byte());
        match &self.key {
            StreamKey::Local(data) => buf.put_slice(data),
            StreamKey::PublicKeyEd25519(data) => buf.put_slice(data),
            StreamKey::Blake3HashPin(data) => buf.put_slice(data),
            StreamKey::Vault { .. } => unreachable!("dispatched via serialize_vault"),
        }
        buf.put_u64(self.revision);
        buf.put_slice(self.hash.as_ref());
        buf.put_slice(&self.signature);
        if let Some(ref data) = self.data {
            buf.put_slice(data);
        }
        buf.freeze()
    }

    /// Deserializes a message from wire format. Dispatches on `KEYTYPE`
    /// to handle the v3 vault format (LEN-prefixed payload) vs the
    /// legacy format (raw HASH field) — see `serialize` for the layouts.
    pub fn deserialize(mut bytes: Bytes) -> Result<Self, StreamMessageError> {
        if bytes.remaining() < 2 {
            return Err(StreamMessageError::InsufficientBytes);
        }

        let type_id = MessageType::try_from(bytes.get_u8())?;
        let key_id = bytes.get_u8();

        if key_id == StreamKey::VAULT_ID_KEYTYPE {
            // v3 vault layout
            let header_remaining = KEY_SIZE + VAULT_ID_SIZE + 8 + 1;
            if bytes.remaining() < header_remaining {
                return Err(StreamMessageError::InsufficientBytes);
            }
            let mut pubkey = [0u8; KEY_SIZE];
            bytes.copy_to_slice(&mut pubkey);
            let mut vault_id = [0u8; VAULT_ID_SIZE];
            bytes.copy_to_slice(&mut vault_id);
            let revision = bytes.get_u64();
            let payload_len = bytes.get_u8() as usize;

            if bytes.remaining() < payload_len + SIGNATURE_SIZE {
                return Err(StreamMessageError::InsufficientBytes);
            }
            // Payload = MULTIHASH_BLAKE3 || hash[32] || optional data
            if payload_len < 1 + HASH_SIZE {
                return Err(StreamMessageError::InsufficientBytes);
            }
            let multihash_tag = bytes.get_u8();
            if multihash_tag != MULTIHASH_BLAKE3 {
                return Err(StreamMessageError::UnknownMultihashTag(multihash_tag));
            }
            let mut hash_bytes = [0u8; HASH_SIZE];
            bytes.copy_to_slice(&mut hash_bytes);
            let hash = Hash::from(hash_bytes);

            let trailing_data_len = payload_len - 1 - HASH_SIZE;
            let data = if trailing_data_len > 0 {
                Some(bytes.copy_to_bytes(trailing_data_len))
            } else {
                None
            };

            let mut sig = vec![0u8; SIGNATURE_SIZE];
            bytes.copy_to_slice(&mut sig);

            Self::new(
                type_id,
                StreamKey::Vault { pubkey, vault_id },
                revision,
                hash,
                sig.into_boxed_slice(),
                data,
            )
        } else {
            // Legacy layout (Local / Blake3HashPin)
            if bytes.remaining() < KEY_SIZE + 8 + HASH_SIZE {
                return Err(StreamMessageError::InsufficientBytes);
            }
            let mut key_bytes = [0u8; KEY_SIZE];
            bytes.copy_to_slice(&mut key_bytes);
            let key = StreamKey::from_bytes(key_id, &key_bytes)?;

            let revision = bytes.get_u64();

            let mut hash_bytes = [0u8; HASH_SIZE];
            bytes.copy_to_slice(&mut hash_bytes);
            let hash = Hash::from(hash_bytes);

            let sig_len = key.signature_len();
            if bytes.remaining() < sig_len {
                return Err(StreamMessageError::InsufficientBytes);
            }

            let signature = if sig_len > 0 {
                let mut sig = vec![0u8; sig_len];
                bytes.copy_to_slice(&mut sig);
                sig.into_boxed_slice()
            } else {
                Box::new([])
            };

            let data = if !bytes.is_empty() {
                Some(bytes.copy_to_bytes(bytes.remaining()))
            } else {
                None
            };

            Self::new(type_id, key, revision, hash, signature, data)
        }
    }

    /// Returns true if this message should be stored persistently.
    ///
    /// - **Stream messages**: always stored (append-only).
    /// - **Registry messages**: stored only if strictly newer
    ///   (`self.revision > existing.revision`). Idempotent re-writes
    ///   of the same `(revision, hash)` are also stored (no-op write).
    ///
    /// **Same-revision-different-hash is rejected.** This is the strict-CAS
    /// semantic that lets a concurrent writer reliably detect "I lost
    /// the race" via verify-after-set: if our set was rejected, the
    /// existing entry stays unchanged, our verify sees the other
    /// writer's hash, and we retry at `existing.revision + 1` with
    /// merged content. Without strict-CAS the "smaller hash wins"
    /// tie-break that used to live here silently overwrote earlier
    /// writers — once the loser's verify-after-set exited, a later
    /// smaller-hash writer could still bury them and the loser
    /// wouldn't notice.
    ///
    /// The ordering rule in [`Ord`] (which still tie-breaks on hash)
    /// is unchanged — it's about which message is "canonically newer"
    /// for read-side reconciliation across replicas — but for the
    /// write path the answer is "first writer at a revision wins; all
    /// others retry."
    pub fn should_store(&self, existing: Option<&Self>) -> bool {
        match self.type_id {
            MessageType::Stream => true, // Always store stream messages
            MessageType::Registry => match existing {
                None => true,
                Some(e) => {
                    self.revision > e.revision
                        || (self.revision == e.revision && self.hash == e.hash)
                }
            },
        }
    }
}
