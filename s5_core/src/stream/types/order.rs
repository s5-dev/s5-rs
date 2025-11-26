use super::StreamMessage;
use std::cmp::Ordering;

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
    use crate::stream::types::{
        HASH_SIZE, KEY_SIZE, MessageType, MessageTypeTryFromError, SIGNATURE_SIZE, StreamKey,
        StreamKeyDeserializeError, StreamMessageError,
    };
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
