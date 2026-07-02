//! Integration tests that don't need a live indexd.
//!
//! The capability format (`serde_json` of `SealedObject`) is stable and
//! reversible. The SealedObject is the trustless handle stored as the value of
//! each `p/<path>` cache entry, so its serialization must round-trip back to an
//! equal `SealedObject`.

#[test]
fn sealed_object_json_round_trips() {
    let sealed = sia_storage::SealedObject {
        encrypted_data_key: (0u8..32).collect(),
        slabs: Vec::new(),
        data_signature: Default::default(),
        encrypted_metadata_key: vec![9, 9, 9],
        encrypted_metadata: vec![1, 1, 1, 2, 2, 2],
        metadata_signature: Default::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let a = serde_json::to_vec(&sealed).unwrap();
    let decoded: sia_storage::SealedObject = serde_json::from_slice(&a).unwrap();
    assert_eq!(decoded, sealed, "capability bytes must round-trip");
}
