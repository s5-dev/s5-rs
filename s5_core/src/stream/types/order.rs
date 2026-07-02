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
        StreamKeyDeserializeError, StreamMessageError, VAULT_ID_SIZE,
    };
    use bytes::Bytes;

    /// Convenience: build a `StreamKey::Vault` for tests using a placeholder
    /// (zero) `vault_id`. Tests that exercise the per-vault distinguishing
    /// behaviour pass a non-zero value explicitly.
    fn vault_key(pubkey_byte: u8) -> StreamKey {
        StreamKey::Vault {
            pubkey: [pubkey_byte; KEY_SIZE],
            vault_id: [0; VAULT_ID_SIZE],
        }
    }

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
        // All variants round-trip uniformly via storage_key /
        // from_storage_key. (The fixed-length variants — Local /
        // Blake3HashPin — produce 33-byte storage keys; Vault
        // produces 49.)
        let local_key = StreamKey::Local([1; KEY_SIZE]);
        let storage = local_key.storage_key();
        assert_eq!(storage[0], StreamKey::LOCAL_ID);
        assert_eq!(storage.len(), 1 + KEY_SIZE);
        assert_eq!(StreamKey::from_storage_key(&storage).unwrap(), local_key);

        let pin_key = StreamKey::Blake3HashPin([3; KEY_SIZE]);
        let storage = pin_key.storage_key();
        assert_eq!(storage[0], StreamKey::BLAKE3_HASH_PIN_ID);
        assert_eq!(storage.len(), 1 + KEY_SIZE);
        assert_eq!(StreamKey::from_storage_key(&storage).unwrap(), pin_key);

        let vault_key = StreamKey::Vault {
            pubkey: [2; KEY_SIZE],
            vault_id: [9; VAULT_ID_SIZE],
        };
        let storage = vault_key.storage_key();
        assert_eq!(storage[0], StreamKey::VAULT_ID_KEYTYPE);
        assert_eq!(storage.len(), 1 + KEY_SIZE + VAULT_ID_SIZE);
        assert_eq!(StreamKey::from_storage_key(&storage).unwrap(), vault_key);
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

        let vault = vault_key(0);
        assert!(vault.requires_signature());

        let pin = StreamKey::Blake3HashPin([0; KEY_SIZE]);
        assert!(!pin.requires_signature());
    }

    #[test]
    fn test_message_type_try_from() {
        assert_eq!(MessageType::try_from(0x00).unwrap(), MessageType::Stream);
        assert_eq!(MessageType::try_from(0x5c).unwrap(), MessageType::Registry);
        // Old v2 registry tag (0x01) is now rejected.
        assert_eq!(
            MessageType::try_from(0x01).unwrap_err(),
            MessageTypeTryFromError(0x01)
        );
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

        // Vault key without signature should fail
        let msg = StreamMessage::new(
            MessageType::Registry,
            vault_key(0),
            1,
            [0; HASH_SIZE].into(),
            Box::new([]),
            None,
        );
        assert_eq!(msg.unwrap_err(), StreamMessageError::SignatureRequired);

        // Vault key with a real signature should succeed. (F01 rejects
        // any not-actually-verifying signature — see the dedicated
        // `new_rejects_v3_vault_entry_with_*` tests below.)
        use crate::Hash;
        use ed25519_dalek::SigningKey;
        let signing = SigningKey::from_bytes(&[1u8; 32]);
        let hash: Hash = [0u8; HASH_SIZE].into();
        let msg = StreamMessage::sign_ed25519_registry(&signing, [0; VAULT_ID_SIZE], hash, 1);
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
        // F01: deserialize funnels through `new` which now verifies the
        // signature, so the round-trip must start from a properly-signed
        // message. Uses `sign_ed25519_registry_with_data` to cover the
        // inline-data path.
        use crate::Hash;
        use ed25519_dalek::SigningKey;
        let signing = SigningKey::from_bytes(&[42u8; 32]);
        let hash: Hash = [0xAB; HASH_SIZE].into();
        let original = StreamMessage::sign_ed25519_registry_with_data(
            &signing,
            [0xCD; VAULT_ID_SIZE],
            hash,
            0xDEADBEEF,
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

        // Registry messages: stored only if strictly newer
        assert!(msg1.should_store(None)); // No existing = store
        assert!(!msg1.should_store(Some(&msg2))); // Older = don't store
        assert!(msg2.should_store(Some(&msg1))); // Newer = store

        // Strict-CAS at same revision: same hash is idempotent
        // (re-writing the exact same registry entry is a no-op write
        // and should "succeed"); different hash is rejected so the
        // losing writer can detect the race and retry.
        let msg_same_a = create_test_message(10, 1);
        let msg_same_b = create_test_message(10, 1);
        assert!(msg_same_a.should_store(Some(&msg_same_b)));
        assert!(msg_same_b.should_store(Some(&msg_same_a)));

        let msg_diff_smaller = create_test_message(10, 1);
        let msg_diff_larger = create_test_message(10, 2);
        assert!(
            !msg_diff_smaller.should_store(Some(&msg_diff_larger)),
            "same-rev-different-hash must reject (formerly: smaller hash silently won, \
             which let later writers bury earlier ones unnoticed)"
        );
        assert!(
            !msg_diff_larger.should_store(Some(&msg_diff_smaller)),
            "same-rev-different-hash must reject regardless of which side is smaller"
        );
    }

    #[test]
    fn test_eventual_consistency_ordering_preserved() {
        // Simulate two nodes that independently produced entries at the
        // same revision (network-partition CRDT scenario). The `Ord`
        // tie-break (smaller hash wins for read-side reconciliation) is
        // preserved — it's how a future sync layer could pick a
        // canonical winner across replicas.
        //
        // The *write* side, by contrast, rejects same-revision-
        // different-hash via `should_store` (strict-CAS) so a local
        // concurrent writer can detect their loss and retry. Without
        // that, the loser would be silently overwritten and would
        // never know to merge their changes back in.
        //
        // When P2P registry replication lands and needs to splice a
        // peer's entry into the local view, it'll use `Ord` directly
        // (or a dedicated "merge from peer" code path) rather than
        // `should_store` — which would correctly reject same-rev
        // overwrites coming from local code paths.
        // Same signing key + vault_id ⇒ same `(pubkey, vault_id)` for both
        // messages; the ordering/CAS behaviour we test is purely about
        // `(revision, hash, data)`. F01 requires real sigs to construct.
        use crate::Hash;
        use ed25519_dalek::SigningKey;
        let signing = SigningKey::from_bytes(&[1u8; 32]);
        let vault_id = [0u8; VAULT_ID_SIZE];
        let hash_a: Hash = [0x00; HASH_SIZE].into();
        let hash_b: Hash = [0xFF; HASH_SIZE].into();
        let node_a_msg = StreamMessage::sign_ed25519_registry_with_data(
            &signing,
            vault_id,
            hash_a,
            100,
            Some(Bytes::from(b"Node A data".to_vec())),
        )
        .unwrap();
        let node_b_msg = StreamMessage::sign_ed25519_registry_with_data(
            &signing,
            vault_id,
            hash_b,
            100,
            Some(Bytes::from(b"Node B data".to_vec())),
        )
        .unwrap();

        // Read-side: Ord still picks the smaller-hash entry as the
        // canonical "winner" for any reconciliation that needs a
        // single answer. Unchanged from the prior tie-break.
        assert!(node_a_msg > node_b_msg);
        let winner = vec![node_a_msg.clone(), node_b_msg.clone()]
            .into_iter()
            .max()
            .unwrap();
        assert_eq!(winner, node_a_msg);

        // Write-side: strict-CAS — same-rev-different-hash is rejected
        // so the losing local writer can detect the race and retry.
        assert!(!node_a_msg.should_store(Some(&node_b_msg)));
        assert!(!node_b_msg.should_store(Some(&node_a_msg)));
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

    /// Asserts the **exact byte layout** of a v3 vault registry entry on
    /// the wire (per docs/reference/snapshot-publication.md § Registry
    /// entry format). Catches drift the symmetric serialize/deserialize
    /// round-trip can't see — earlier versions of this code wrote
    /// `... | REVISION | HASH(32) | SIG | data` and round-tripped fine
    /// because both ends used the wrong (matching) layout.
    ///
    /// Expected wire format:
    /// `TYPE(1) | KEYTYPE(1) | PUBKEY(32) | VAULT_ID(16) | REVISION(8 BE)
    ///   | LEN(1) | PAYLOAD(LEN) | SIG(64)`
    /// where `PAYLOAD = MULTIHASH_BLAKE3(0x1e) || hash[32]` for a
    /// hash-only payload (33 bytes → LEN = 0x21).
    #[test]
    fn vault_registry_wire_layout_matches_spec() {
        use crate::Hash;
        use crate::stream::types::{
            MAX_VAULT_PAYLOAD_LEN, MULTIHASH_BLAKE3, SIG_DOMAIN_TAG_V3, VAULT_ID_SIZE,
        };
        use ed25519_dalek::{SigningKey, VerifyingKey};

        // Real signature now required (F01). The wire layout the test
        // pins is independent of the sig bytes themselves — we assert
        // position and length, not content.
        let signing = SigningKey::from_bytes(&[0xAAu8; 32]);
        let vk: VerifyingKey = (&signing).into();
        let pubkey = vk.to_bytes();
        let vault_id = [0xBBu8; VAULT_ID_SIZE];
        let hash_bytes = [0xCCu8; HASH_SIZE];
        let hash: Hash = hash_bytes.into();
        let revision: u64 = 0x0102_0304_0506_0708;

        let msg = StreamMessage::sign_ed25519_registry(&signing, vault_id, hash, revision).unwrap();

        let bytes = msg.serialize();

        // Layout offsets: 0  1  2..34         34..50     50..58       58   59      60..92       92..156
        //                 T  K  PUBKEY(32)    VID(16)    REV(8 BE)    LEN  MH(0x1e) HASH(32)     SIG(64)
        assert_eq!(
            bytes.len(),
            1 + 1 + KEY_SIZE + VAULT_ID_SIZE + 8 + 1 + 33 + SIGNATURE_SIZE
        );

        assert_eq!(bytes[0], 0x5c, "TYPE byte (Registry v3)");
        assert_eq!(bytes[1], 0xed, "KEYTYPE byte (Vault / ed25519)");
        assert_eq!(&bytes[2..2 + KEY_SIZE], &pubkey, "PUBKEY");
        assert_eq!(
            &bytes[2 + KEY_SIZE..2 + KEY_SIZE + VAULT_ID_SIZE],
            &vault_id,
            "VAULT_ID"
        );
        let rev_off = 2 + KEY_SIZE + VAULT_ID_SIZE;
        assert_eq!(
            &bytes[rev_off..rev_off + 8],
            &revision.to_be_bytes(),
            "REVISION big-endian"
        );
        let len_off = rev_off + 8;
        assert_eq!(bytes[len_off], 0x21, "LEN byte (= 33 = 1 + 32)");
        let payload_off = len_off + 1;
        assert_eq!(
            bytes[payload_off], MULTIHASH_BLAKE3,
            "first PAYLOAD byte = 0x1e (blake3 multihash)"
        );
        assert_eq!(
            &bytes[payload_off + 1..payload_off + 1 + HASH_SIZE],
            &hash_bytes,
            "PAYLOAD hash[32]"
        );
        // SIG occupies the final 64 bytes — assert position+length only;
        // content is the real ed25519 sig, not test-supplied bytes.
        let sig_off = payload_off + 33;
        assert_eq!(
            bytes[sig_off..].len(),
            SIGNATURE_SIZE,
            "SIG occupies the final SIGNATURE_SIZE bytes"
        );

        // And the canonical signing-bytes layout (for SIG verification).
        let mut expected_sign = Vec::new();
        expected_sign.extend_from_slice(SIG_DOMAIN_TAG_V3);
        expected_sign.extend_from_slice(&pubkey);
        expected_sign.extend_from_slice(&vault_id);
        expected_sign.extend_from_slice(&revision.to_be_bytes());
        expected_sign.push(MULTIHASH_BLAKE3);
        expected_sign.extend_from_slice(&hash_bytes);
        // SIG_DOMAIN_TAG_V3 (10) + 32 + 16 + 8 + 1 + 32 = 99
        assert_eq!(expected_sign.len(), 10 + 32 + 16 + 8 + 1 + 32);
        assert_eq!(MAX_VAULT_PAYLOAD_LEN, 255);
    }

    /// Round-trip a vault entry with inline data (LEN > 33).
    #[test]
    fn vault_registry_round_trip_with_inline_data() {
        use crate::Hash;
        use ed25519_dalek::SigningKey;

        let signing = SigningKey::from_bytes(&[0x11u8; 32]);
        let vault_id = [0x22u8; 16];
        let hash_bytes = [0x33u8; HASH_SIZE];
        let hash: Hash = hash_bytes.into();
        let inline = Bytes::from(vec![0x55u8; 64]);

        let msg = StreamMessage::sign_ed25519_registry_with_data(
            &signing,
            vault_id,
            hash,
            7,
            Some(inline.clone()),
        )
        .unwrap();

        let bytes = msg.serialize();
        // LEN should now be 33 + 64 = 97 = 0x61
        let len_off = 2 + KEY_SIZE + 16 + 8;
        assert_eq!(bytes[len_off], 1 + HASH_SIZE as u8 + 64);

        let decoded = StreamMessage::deserialize(bytes).unwrap();
        assert_eq!(decoded.data, Some(inline));
        assert_eq!(decoded.hash.as_bytes(), &hash_bytes);
    }

    /// A vault payload exceeding 255 B must be rejected at construction.
    #[test]
    fn vault_payload_over_255_bytes_is_rejected() {
        let pubkey = [0x11u8; KEY_SIZE];
        let vault_id = [0x22u8; 16];
        let hash_bytes = [0x33u8; HASH_SIZE];
        let sig_bytes = [0x44u8; SIGNATURE_SIZE];
        // 1 (multihash) + 32 (hash) + 223 (data) = 256 > 255
        let oversized = Bytes::from(vec![0u8; 223]);

        let err = StreamMessage::new(
            MessageType::Registry,
            StreamKey::Vault { pubkey, vault_id },
            1,
            hash_bytes.into(),
            Box::new(sig_bytes),
            Some(oversized),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            StreamMessageError::VaultPayloadTooLarge { .. }
        ));
    }

    /// A wire blob with an unknown multihash tag in the payload prefix
    /// must be rejected on deserialize.
    #[test]
    fn vault_payload_with_unknown_multihash_tag_is_rejected() {
        use crate::stream::types::VAULT_ID_SIZE;

        let mut bytes = Vec::new();
        bytes.push(0x5c); // TYPE
        bytes.push(0xed); // KEYTYPE
        bytes.extend_from_slice(&[0x11u8; KEY_SIZE]);
        bytes.extend_from_slice(&[0x22u8; VAULT_ID_SIZE]);
        bytes.extend_from_slice(&7u64.to_be_bytes()); // REVISION
        bytes.push(0x21); // LEN
        bytes.push(0xff); // bogus multihash tag
        bytes.extend_from_slice(&[0x33u8; HASH_SIZE]);
        bytes.extend_from_slice(&[0x44u8; SIGNATURE_SIZE]);

        let err = StreamMessage::deserialize(Bytes::from(bytes)).unwrap_err();
        assert!(matches!(err, StreamMessageError::UnknownMultihashTag(0xff)));
    }

    // ---- F01: cryptographic signature verification ----
    //
    // The signing helper `sign_ed25519_registry` produces correctly-signed
    // v3 vault messages; these tests assert that `new` REJECTS messages
    // whose signature does not verify under the embedded pubkey. Closes
    // the F01 hole where length-only validation made every downstream ACL
    // decision hollow (`acl-and-revocation.md §1`).

    #[test]
    fn new_rejects_v3_vault_entry_with_tampered_signature() {
        use crate::Hash;
        use crate::stream::types::MessageType;
        use ed25519_dalek::SigningKey;

        let signing = SigningKey::from_bytes(&[7u8; 32]);
        let hash: Hash = [0x11u8; HASH_SIZE].into();
        let vault_id = [0xAB; VAULT_ID_SIZE];

        let signed = StreamMessage::sign_ed25519_registry(&signing, vault_id, hash, 1).unwrap();
        let mut tampered = signed.signature.to_vec();
        tampered[0] ^= 0x01; // flip one bit

        let err = StreamMessage::new(
            MessageType::Registry,
            signed.key,
            signed.revision,
            signed.hash,
            tampered.into_boxed_slice(),
            signed.data.clone(),
        )
        .unwrap_err();
        assert_eq!(err, StreamMessageError::InvalidSignature);
    }

    #[test]
    fn new_rejects_v3_vault_entry_with_pubkey_mismatch() {
        use crate::Hash;
        use crate::stream::types::MessageType;
        use ed25519_dalek::{SigningKey, VerifyingKey};

        let signer = SigningKey::from_bytes(&[1u8; 32]);
        let other = SigningKey::from_bytes(&[2u8; 32]);
        let hash: Hash = [0x22u8; HASH_SIZE].into();
        let vault_id = [0xCD; VAULT_ID_SIZE];

        // Real signature under `signer`, but present `other`'s pubkey on
        // the vault key — verifier must reject (signing bytes embed pubkey
        // AND verification runs against the presented key).
        let signed = StreamMessage::sign_ed25519_registry(&signer, vault_id, hash, 1).unwrap();
        let other_pk: VerifyingKey = (&other).into();
        let wrong_key = StreamKey::Vault {
            pubkey: other_pk.to_bytes(),
            vault_id,
        };

        let err = StreamMessage::new(
            MessageType::Registry,
            wrong_key,
            signed.revision,
            signed.hash,
            signed.signature.clone(),
            signed.data.clone(),
        )
        .unwrap_err();
        assert_eq!(err, StreamMessageError::InvalidSignature);
    }

    #[test]
    fn new_accepts_legitimately_signed_v3_vault_entry() {
        use crate::Hash;
        use ed25519_dalek::SigningKey;

        let signing = SigningKey::from_bytes(&[9u8; 32]);
        let hash: Hash = [0x44u8; HASH_SIZE].into();
        let vault_id = [0xEF; VAULT_ID_SIZE];
        // Round-trip via the helper — signer self-check must succeed.
        let msg = StreamMessage::sign_ed25519_registry(&signing, vault_id, hash, 7).unwrap();
        assert_eq!(msg.revision, 7);
        assert_eq!(msg.hash, hash);
    }

    #[test]
    fn new_accepts_local_key_with_no_signature() {
        use crate::Hash;
        use crate::stream::types::MessageType;

        // Local keys carry no signature; F01 verification must not touch
        // them. Regression guard.
        let hash: Hash = [0u8; HASH_SIZE].into();
        let msg = StreamMessage::new(
            MessageType::Stream,
            StreamKey::Local([0; KEY_SIZE]),
            1,
            hash,
            Box::new([]),
            None,
        );
        assert!(msg.is_ok());
    }
}
