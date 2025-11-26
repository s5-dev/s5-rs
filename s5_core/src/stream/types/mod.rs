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
//! | Field | Size (bytes) | Description |
//! |---|---|---|
//! | Message Type | 1 | `0x00` (Stream) or `0x01` (Registry) |
//! | Key Type | 1 | `0x00` (Local), `0x01` (Ed25519), `0x03` (Pin) |
//! | Key Data | 32 | The public key or identifier |
//! | Revision | 8 | Big-endian `u64` |
//! | Hash | 32 | BLAKE3 hash of the payload |
//! | Signature | 0 or 64 | Ed25519 signature (if required by key type) |
//! | Data | Variable | Inline payload (optional, max 1024 bytes) |

use crate::Hash;
use bytes::{Buf, BufMut, Bytes, BytesMut};

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

/// A type alias for a 32-byte Ed25519 public key.
pub type PublicKeyEd25519 = [u8; KEY_SIZE];

/// Represents the key for a stream or registry entry.
///
/// A key identifies the "owner" or "topic" of a stream. It can either be a
/// standard Ed25519 public key for signed, public entries, a local,
/// randomly generated identifier for private or local-only use cases where
/// cryptographic identity is not required, or a BLAKE3 hash used for pins.
#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug, PartialOrd, Ord)]
#[non_exhaustive]
pub enum StreamKey {
    /// A local, 32-byte identifier, not tied to a cryptographic keypair.
    /// Useful for local-only or ephemeral streams where cryptographic
    /// authentication is not required (e.g., internal node communication).
    Local([u8; KEY_SIZE]),

    /// An Ed25519 public key. Entries with this key type must be signed
    /// with the corresponding private key to be valid.
    PublicKeyEd25519(PublicKeyEd25519),

    /// A 32-byte BLAKE3 hash, used for pin metadata.
    /// For this key type, larger inline payloads are allowed so that
    /// large pin sets can be stored without being constrained by the
    /// default inline data limit.
    Blake3HashPin([u8; KEY_SIZE]),
}

/// The type of a message on the wire, distinguishing between a Stream and a Registry entry.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum MessageType {
    /// A message that is part of a persistent, append-only log.
    Stream = 0,
    /// A key-value entry where only the latest version is kept.
    Registry = 1,
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
    pub const LOCAL_ID: u8 = 0;

    /// Identifier for the `PublicKeyEd25519` variant.
    pub const PUBLIC_KEY_ED25519_ID: u8 = 1;

    /// Identifier for the `Blake3HashPin` variant.
    pub const BLAKE3_HASH_PIN_ID: u8 = 3;

    /// Serializes the `StreamKey` into its type ID and raw bytes.
    ///
    /// Returns a tuple containing the `u8` identifier and a slice of the 32-byte key.
    pub fn to_bytes(&self) -> (u8, &[u8]) {
        match self {
            StreamKey::Local(data) => (Self::LOCAL_ID, data),
            StreamKey::PublicKeyEd25519(data) => (Self::PUBLIC_KEY_ED25519_ID, data),
            StreamKey::Blake3HashPin(data) => (Self::BLAKE3_HASH_PIN_ID, data),
        }
    }

    /// Deserializes a byte slice into a `StreamKey`.
    ///
    /// - `id` is the key type identifier.
    /// - `data` must be 32 bytes.
    pub fn from_bytes(id: u8, data: &[u8]) -> Result<Self, StreamKeyDeserializeError> {
        let data_array: [u8; KEY_SIZE] =
            data.try_into()
                .map_err(|_| StreamKeyDeserializeError::InvalidLength {
                    expected: KEY_SIZE,
                    actual: data.len(),
                })?;

        match id {
            Self::LOCAL_ID => Ok(StreamKey::Local(data_array)),
            Self::PUBLIC_KEY_ED25519_ID => Ok(StreamKey::PublicKeyEd25519(data_array)),
            Self::BLAKE3_HASH_PIN_ID => Ok(StreamKey::Blake3HashPin(data_array)),
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
}

/// An error that can occur when converting a `u8` to a `MessageType`.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
#[error("invalid message type: {0}")]
pub struct MessageTypeTryFromError(pub u8);

impl TryFrom<u8> for MessageType {
    type Error = MessageTypeTryFromError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MessageType::Stream),
            1 => Ok(MessageType::Registry),
            _ => Err(MessageTypeTryFromError(value)),
        }
    }
}

impl StreamMessage {
    /// Creates a new StreamMessage with validation.
    ///
    /// - Local keys must have an empty signature.
    /// - Ed25519 keys must have a 64-byte signature; if missing entirely, returns `SignatureRequired`.
    /// - Inline data must not exceed `MAX_INLINE_DATA_SIZE`.
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

        Ok(Self {
            type_id,
            key,
            revision,
            hash,
            signature,
            data,
        })
    }

    /// Serializes the message for wire transport.
    ///
    /// Wire format:
    /// - 1 byte: message type
    /// - 1 byte: key type
    /// - 32 bytes: key data
    /// - 8 bytes: revision (big-endian)
    /// - 32 bytes: hash
    /// - N bytes: signature (length depends on key type)
    /// - If data present:
    ///   - N bytes: data (no length prefix; consumes remaining bytes)
    pub fn serialize(&self) -> Bytes {
        let (key_id, key_bytes) = self.key.to_bytes();

        let mut buf = BytesMut::with_capacity(
            1 + 1
                + KEY_SIZE
                + 8
                + HASH_SIZE
                + self.signature.len()
                + self.data.as_ref().map_or(0, |d| d.len()),
        );

        buf.put_u8(self.type_id as u8);
        buf.put_u8(key_id);
        buf.put_slice(key_bytes);
        buf.put_u64(self.revision);
        buf.put_slice(self.hash.as_ref());
        buf.put_slice(&self.signature);

        if let Some(ref data) = self.data {
            buf.put_slice(data);
        }

        buf.freeze()
    }

    /// Deserializes a message from wire format.
    pub fn deserialize(mut bytes: Bytes) -> Result<Self, StreamMessageError> {
        if bytes.remaining() < 1 + 1 + KEY_SIZE + 8 + HASH_SIZE {
            return Err(StreamMessageError::InsufficientBytes);
        }

        let type_id = MessageType::try_from(bytes.get_u8())?;
        let key_id = bytes.get_u8();

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

    /// Returns true if this message should be stored persistently.
    /// Registry entries are only stored if they have the highest revision (or win the tie).
    pub fn should_store(&self, existing: Option<&Self>) -> bool {
        match self.type_id {
            MessageType::Stream => true, // Always store stream messages
            MessageType::Registry => existing.is_none_or(|e| self > e),
        }
    }
}
