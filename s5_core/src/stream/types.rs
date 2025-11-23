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

use crate::Hash;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cmp::Ordering;

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
    const LOCAL_ID: u8 = 0;

    /// Identifier for the `PublicKeyEd25519` variant.
    const PUBLIC_KEY_ED25519_ID: u8 = 1;

    /// Identifier for the `Blake3HashPin` variant.
    const BLAKE3_HASH_PIN_ID: u8 = 3;

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

// Manual implementation of Ord to enforce the tie-breaking rule for eventual consistency.
impl Ord for StreamMessage {
    /// Compares two `StreamMessage` instances to determine their canonical order.
    ///
    /// Ordering:
    /// 1. Higher `revision` number wins.
    /// 2. If revisions are equal, the message with the lexicographically smaller
    ///    payload value wins. To guarantee global determinism (even without inline data),
    ///    we use the BLAKE3 payload hash (`hash`) as the canonical comparator.
    /// 3. If both equal, they are identical for ordering purposes.
    ///
    /// Note: We reverse the hash comparison so that a smaller hash results in a
    /// greater ordering value, making it the "winner" in conflict resolution.
    fn cmp(&self, other: &Self) -> Ordering {
        self.revision
            .cmp(&other.revision)
            .then_with(|| other.hash.cmp(&self.hash)) // Reversed for tie-breaker
    }
}

impl PartialOrd for StreamMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    // Helper function to create a dummy message for testing.
    fn create_test_message(revision: u64, hash_byte: u8) -> StreamMessage {
        let key = StreamKey::Local([0; KEY_SIZE]);
        let hash = [hash_byte; HASH_SIZE];
        StreamMessage {
            type_id: MessageType::Registry,
            key,
            revision,
            hash: hash.into(),
            signature: Box::new([]),
            data: None,
        }
    }

    #[test]
    fn test_stream_key_serialization_roundtrip() {
        let local_key = StreamKey::Local([1; KEY_SIZE]);
        let (id, bytes) = local_key.to_bytes();
        assert_eq!(id, StreamKey::LOCAL_ID);
        assert_eq!(bytes.len(), KEY_SIZE);
        let deserialized = StreamKey::from_bytes(id, bytes).unwrap();
        assert_eq!(local_key, deserialized);

        let ed_key = StreamKey::PublicKeyEd25519([2; KEY_SIZE]);
        let (id, bytes) = ed_key.to_bytes();
        assert_eq!(id, StreamKey::PUBLIC_KEY_ED25519_ID);
        assert_eq!(bytes.len(), KEY_SIZE);
        let deserialized = StreamKey::from_bytes(id, bytes).unwrap();
        assert_eq!(ed_key, deserialized);

        let pin_key = StreamKey::Blake3HashPin([3; KEY_SIZE]);
        let (id, bytes) = pin_key.to_bytes();
        assert_eq!(id, StreamKey::BLAKE3_HASH_PIN_ID);
        assert_eq!(bytes.len(), KEY_SIZE);
        let deserialized = StreamKey::from_bytes(id, bytes).unwrap();
        assert_eq!(pin_key, deserialized);
    }

    #[test]
    fn test_stream_key_deserialization_errors() {
        // Wrong length
        assert_eq!(
            StreamKey::from_bytes(StreamKey::LOCAL_ID, &[0; 31]).unwrap_err(),
            StreamKeyDeserializeError::InvalidLength {
                expected: KEY_SIZE,
                actual: 31
            }
        );

        // Unknown ID
        assert_eq!(
            StreamKey::from_bytes(99, &[0; KEY_SIZE]).unwrap_err(),
            StreamKeyDeserializeError::UnknownId(99)
        );
    }

    #[test]
    fn test_stream_key_requires_signature() {
        let local = StreamKey::Local([0; KEY_SIZE]);
        assert!(!local.requires_signature());

        let ed = StreamKey::PublicKeyEd25519([0; KEY_SIZE]);
        assert!(ed.requires_signature());

        let pin = StreamKey::Blake3HashPin([0; KEY_SIZE]);
        assert!(!pin.requires_signature());
    }

    #[test]
    fn test_message_type_try_from() {
        assert_eq!(MessageType::try_from(0).unwrap(), MessageType::Stream);
        assert_eq!(MessageType::try_from(1).unwrap(), MessageType::Registry);
        assert_eq!(
            MessageType::try_from(2).unwrap_err(),
            MessageTypeTryFromError(2)
        );
    }

    #[test]
    fn test_stream_message_ordering_by_revision() {
        let msg_rev1 = create_test_message(1, 1);
        let msg_rev2 = create_test_message(2, 1);
        assert!(msg_rev2 > msg_rev1);
        assert_eq!(msg_rev2.cmp(&msg_rev1), Ordering::Greater);
    }

    #[test]
    fn test_stream_message_ordering_tie_breaker_by_hash() {
        // Same revision, different hashes.
        let msg_hash1 = create_test_message(5, 1); // hash is [1, 1, ...]
        let msg_hash2 = create_test_message(5, 2); // hash is [2, 2, ...]

        // msg_hash1 has a smaller hash, so it should be considered "greater".
        assert!(msg_hash1 > msg_hash2);
        assert_eq!(msg_hash1.cmp(&msg_hash2), Ordering::Greater);
    }

    #[test]
    fn test_stream_message_ordering_equal() {
        let msg1 = create_test_message(10, 5);
        let msg2 = create_test_message(10, 5);
        assert_eq!(msg1.cmp(&msg2), Ordering::Equal);
    }

    #[test]
    fn test_stream_message_sorting() {
        let msg1 = create_test_message(100, 2); // rev 100, hash 2
        let msg2 = create_test_message(101, 1); // rev 101, hash 1 (highest rev)
        let msg3 = create_test_message(100, 1); // rev 100, hash 1 (wins tie-breaker)

        let mut messages = vec![msg1.clone(), msg2.clone(), msg3.clone()];
        messages.sort();

        // The sorted order should be: msg1 < msg3 < msg2
        // msg1 (rev 100, hash 2) < msg3 (rev 100, hash 1) < msg2 (rev 101, hash 1)
        assert_eq!(messages, vec![msg1.clone(), msg3.clone(), msg2.clone()]);

        // The "best" message is the one with highest revision, or smallest hash if tied
        let best = vec![msg1, msg2, msg3].into_iter().max().unwrap();
        assert_eq!(best, create_test_message(101, 1));
    }

    #[test]
    fn test_stream_message_validation() {
        // Valid message with local key (no signature required)
        let msg = StreamMessage::new(
            MessageType::Stream,
            StreamKey::Local([0; KEY_SIZE]),
            1,
            [0; HASH_SIZE].into(),
            Box::new([]),
            None,
        );
        assert!(msg.is_ok());

        // Ed25519 key without signature should fail
        let msg = StreamMessage::new(
            MessageType::Registry,
            StreamKey::PublicKeyEd25519([0; KEY_SIZE]),
            1,
            [0; HASH_SIZE].into(),
            Box::new([]),
            None,
        );
        assert_eq!(msg.unwrap_err(), StreamMessageError::SignatureRequired);

        // Ed25519 key with proper signature should succeed
        let msg = StreamMessage::new(
            MessageType::Registry,
            StreamKey::PublicKeyEd25519([0; KEY_SIZE]),
            1,
            [0; HASH_SIZE].into(),
            Box::new([0; SIGNATURE_SIZE]), // 64-byte signature
            None,
        );
        assert!(msg.is_ok());

        // Data too large should fail for Local keys
        let large_data = Bytes::from(vec![0; 2000]); // > 1024 bytes
        let msg = StreamMessage::new(
            MessageType::Stream,
            StreamKey::Local([0; KEY_SIZE]),
            1,
            [0; HASH_SIZE].into(),
            Box::new([]),
            Some(large_data.clone()),
        );
        assert!(matches!(
            msg.unwrap_err(),
            StreamMessageError::DataTooLarge { .. }
        ));

        // But should be allowed for Blake3HashPin keys
        let msg = StreamMessage::new(
            MessageType::Registry,
            StreamKey::Blake3HashPin([0; KEY_SIZE]),
            1,
            [0; HASH_SIZE].into(),
            Box::new([]),
            Some(large_data),
        );
        assert!(msg.is_ok());
    }

    #[test]
    fn test_message_serialization_roundtrip() {
        let original = StreamMessage::new(
            MessageType::Registry,
            StreamKey::PublicKeyEd25519([42; KEY_SIZE]),
            0xDEADBEEF,
            [0xAB; HASH_SIZE].into(),
            Box::new([0xFF; SIGNATURE_SIZE]),
            Some(Bytes::from(vec![1, 2, 3, 4])),
        )
        .unwrap();

        let serialized = original.serialize();
        let deserialized = StreamMessage::deserialize(serialized).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_message_serialization_without_data() {
        let original = StreamMessage::new(
            MessageType::Stream,
            StreamKey::Local([7; KEY_SIZE]),
            999,
            [0x55; HASH_SIZE].into(),
            Box::new([]),
            None,
        )
        .unwrap();

        let serialized = original.serialize();
        let deserialized = StreamMessage::deserialize(serialized).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_should_store() {
        let msg1 = create_test_message(10, 1);
        let msg2 = create_test_message(11, 1);

        // Stream messages should always be stored
        let mut stream_msg = msg1.clone();
        stream_msg.type_id = MessageType::Stream;
        assert!(stream_msg.should_store(None));
        assert!(stream_msg.should_store(Some(&msg2)));

        // Registry messages only stored if newer
        assert!(msg1.should_store(None)); // No existing = store
        assert!(!msg1.should_store(Some(&msg2))); // Older = don't store
        assert!(msg2.should_store(Some(&msg1))); // Newer = store

        // Test tie-breaker scenario
        let msg_tie1 = create_test_message(10, 1); // Same rev, smaller hash
        let msg_tie2 = create_test_message(10, 2); // Same rev, larger hash
        assert!(msg_tie1.should_store(Some(&msg_tie2))); // Smaller hash wins
        assert!(!msg_tie2.should_store(Some(&msg_tie1))); // Larger hash loses
    }

    #[test]
    fn test_eventual_consistency_scenario() {
        // Simulate a network partition where two nodes create different entries
        // for the same revision
        let node_a_msg = StreamMessage::new(
            MessageType::Registry,
            StreamKey::PublicKeyEd25519([1; KEY_SIZE]),
            100,
            [0x00; HASH_SIZE].into(), // Smaller hash - should win
            Box::new([0xAA; SIGNATURE_SIZE]),
            Some(Bytes::from(b"Node A data".to_vec())),
        )
        .unwrap();

        let node_b_msg = StreamMessage::new(
            MessageType::Registry,
            StreamKey::PublicKeyEd25519([1; KEY_SIZE]),
            100,
            [0xFF; HASH_SIZE].into(), // Larger hash - should lose
            Box::new([0xBB; SIGNATURE_SIZE]),
            Some(Bytes::from(b"Node B data".to_vec())),
        )
        .unwrap();

        // Both nodes should converge to node_a_msg
        assert!(node_a_msg > node_b_msg);
        assert!(node_a_msg.should_store(Some(&node_b_msg)));
        assert!(!node_b_msg.should_store(Some(&node_a_msg)));

        // Verify that a vector of both messages, when sorted and taking the max,
        // yields the expected winner
        let winner = vec![node_a_msg.clone(), node_b_msg]
            .into_iter()
            .max()
            .unwrap();
        assert_eq!(winner, node_a_msg);
    }

    #[test]
    fn test_revision_is_big_endian_on_wire() {
        let msg = StreamMessage::new(
            MessageType::Stream,
            StreamKey::Local([0; KEY_SIZE]),
            0x0102_0304_0506_0708u64,
            [0x11; KEY_SIZE].into(),
            Box::new([]),
            None,
        )
        .unwrap();

        let bytes = msg.serialize();
        // Offsets: 1(type) + 1(keytype) + 32(key) = 34
        // Next 8 bytes are revision in big-endian.
        let rev_be = &bytes[34..42];
        assert_eq!(rev_be, &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }
}
