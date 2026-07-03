//! Identity-vault bootstrap test: `publish_self_on_startup` writes an
//! `IdentityBundle` blob to every store and a signed registry entry
//! under `(master_pubkey, identity_vault_id())`. The entry's hash
//! resolves to a CBOR-encoded `IdentityBundle` whose keysets match the
//! inputs.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use age::secrecy::ExposeSecret;
use ed25519_dalek::SigningKey;
use s5_core::RegistryApi;
use s5_core::StreamKey;
use s5_core::blob::BlobStore;
use s5_core::identity::IdentityBundle;
use s5_node::config::{NodeConfigIdentity, NodeConfigSource, NodeConfigVault, S5NodeConfig};
use s5_node::identity_vault::{
    build_self_identity_bundle, bundle_age_recipients, derive_master_signing_key,
    identity_vault_id, load_or_generate_master_signing_key, publish_self_on_startup,
};
use s5_node_api::config::NodeConfigKey;
use s5_registry::MemoryRegistry;
use s5_store_local::{LocalStore, LocalStoreConfig};
use tempfile::tempdir;

const AGE_RECIPIENT: &str = "age1a4z5qsqlthanhgp5fj2dszsvmxhrpvw35n7an69zjr0nlukfj9lsxsmney";

fn make_config() -> S5NodeConfig {
    let mut keys = BTreeMap::new();
    keys.insert(
        "main".to_string(),
        NodeConfigKey {
            public_key: AGE_RECIPIENT.to_string(),
            identity_file: None,
        },
    );
    S5NodeConfig {
        identity: NodeConfigIdentity {
            secret_key_file: None,
            secret_key: None,
            encrypted_with: None,
            master_key_file: None,
            anchor_entry_file: None,
            keyset_file: None,
            bootstrap_store: None,
        },
        key: keys,
        store: BTreeMap::new(),
        default_store: None,
        registry: BTreeMap::new(),
        source: BTreeMap::<String, NodeConfigSource>::new(),
        vault: BTreeMap::<String, NodeConfigVault>::new(),
        task: BTreeMap::new(),
        friend: BTreeMap::new(),
    }
}

#[tokio::test]
async fn publishes_identity_bundle_to_all_stores_and_registry() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();

    let store_a = BlobStore::from_arc(Arc::new(LocalStore::create(LocalStoreConfig {
        base_path: dir_a.path().to_string_lossy().into_owned(),
    })));
    let store_b = BlobStore::from_arc(Arc::new(LocalStore::create(LocalStoreConfig {
        base_path: dir_b.path().to_string_lossy().into_owned(),
    })));

    let mut stores: HashMap<String, Arc<dyn s5_core::blob::Blobs>> = HashMap::new();
    stores.insert("a".to_string(), Arc::new(store_a.clone()));
    stores.insert("b".to_string(), Arc::new(store_b.clone()));

    let config = make_config();
    let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());

    // Slice S2.5 keyset is the production source — three independent
    // random ed25519 seeds. Combined with a separate test-deterministic
    // master, this gives the four-pubkey shape the assertions check.
    let master_seed = [42u8; 32];
    let master_pubkey = derive_master_signing_key(&master_seed)
        .verifying_key()
        .to_bytes();
    let keyset = s5_node::device_keyset::DeviceKeyset::generate();
    let device_signing_pubkey = keyset.device_signing_key().verifying_key().to_bytes();
    let acl_pubkey = keyset.device_acl_key().verifying_key().to_bytes();
    let iroh_pubkey = SigningKey::from_bytes(&keyset.iroh)
        .verifying_key()
        .to_bytes();

    let returned_hash = publish_self_on_startup(
        &config,
        &stores,
        registry.clone(),
        &derive_master_signing_key(&master_seed),
        device_signing_pubkey,
        acl_pubkey,
        iroh_pubkey,
    )
    .await;
    // S3a: publish returns the bundle blob hash so the caller can
    // register it in MembershipState.public_blob_hashes.
    assert!(
        returned_hash.is_some(),
        "publish_self_on_startup must return the published bundle hash"
    );
    // All four must be pairwise distinct — independent blake3 domains
    // (slice 2c-acl).
    for (a, b, label) in [
        (master_pubkey, iroh_pubkey, "master vs iroh"),
        (device_signing_pubkey, iroh_pubkey, "device_signing vs iroh"),
        (acl_pubkey, iroh_pubkey, "acl vs iroh"),
        (
            master_pubkey,
            device_signing_pubkey,
            "master vs device_signing",
        ),
        (master_pubkey, acl_pubkey, "master vs acl"),
        (device_signing_pubkey, acl_pubkey, "device_signing vs acl"),
    ] {
        assert_ne!(a, b, "{label} must differ");
    }

    // ---- registry entry is keyed by the master pubkey ----
    let stream_key = StreamKey::Vault {
        pubkey: master_pubkey,
        vault_id: identity_vault_id(),
    };
    let entry = registry
        .get(&stream_key)
        .await
        .unwrap()
        .expect("registry should contain identity entry");
    assert_eq!(entry.revision, 1);

    // ---- the same blob lives in both stores ----
    let bytes_a = store_a.read_as_bytes(entry.hash, 0, None).await.unwrap();
    let bytes_b = store_b.read_as_bytes(entry.hash, 0, None).await.unwrap();
    assert_eq!(bytes_a, bytes_b);

    // ---- S3a: returned hash matches the registry entry hash ----
    assert_eq!(
        returned_hash.unwrap(),
        entry.hash,
        "publish_self_on_startup must return the same hash that lands \
         in the registry entry — caller uses this to populate \
         MembershipState.public_blob_hashes"
    );

    // ---- the blob decodes to an IdentityBundle with four populated keysets ----
    let bundle = IdentityBundle::decode_cbor(&bytes_a).unwrap();
    let expected = build_self_identity_bundle(
        device_signing_pubkey,
        acl_pubkey,
        iroh_pubkey,
        bundle_age_recipients(&config.key),
        1,
    );
    assert_eq!(bundle, expected);
    assert_eq!(bundle.revision, 1);
    assert_eq!(bundle.signers, vec![device_signing_pubkey]);
    assert_eq!(bundle.acl_keys, vec![acl_pubkey]);
    assert_eq!(bundle.iroh_pubkeys, vec![iroh_pubkey]);
    assert_eq!(bundle.age_recipients, vec![AGE_RECIPIENT.to_string()]);
}

#[tokio::test]
async fn second_run_with_unchanged_inputs_keeps_revision_at_one() {
    let dir = tempdir().unwrap();
    let store = BlobStore::from_arc(Arc::new(LocalStore::create(LocalStoreConfig {
        base_path: dir.path().to_string_lossy().into_owned(),
    })));
    let mut stores: HashMap<String, Arc<dyn s5_core::blob::Blobs>> = HashMap::new();
    stores.insert("only".to_string(), Arc::new(store));

    let config = make_config();
    let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
    let master_seed = [7u8; 32];

    // Stable keyset across both calls so the idempotency check sees
    // identical inputs (the production daemon would persist the same
    // file across boots, giving the same property).
    let keyset = s5_node::device_keyset::DeviceKeyset::generate();
    let device_signing_pubkey = keyset.device_signing_key().verifying_key().to_bytes();
    let acl_pubkey = keyset.device_acl_key().verifying_key().to_bytes();
    let iroh_pubkey = SigningKey::from_bytes(&keyset.iroh)
        .verifying_key()
        .to_bytes();

    let mut hashes = Vec::new();
    for _ in 0..2 {
        let h = publish_self_on_startup(
            &config,
            &stores,
            registry.clone(),
            &derive_master_signing_key(&master_seed),
            device_signing_pubkey,
            acl_pubkey,
            iroh_pubkey,
        )
        .await;
        hashes.push(h);
    }
    // Both calls must return Some(hash), and the second (idempotent
    // skip) must return the *same* hash as the first — the bundle blob
    // identity is invariant under idempotent republish.
    assert_eq!(
        hashes[0], hashes[1],
        "idempotent republish must return the same bundle hash"
    );
    assert!(
        hashes[0].is_some(),
        "publish_self_on_startup must return the bundle hash"
    );

    let master_pubkey = derive_master_signing_key(&master_seed)
        .verifying_key()
        .to_bytes();
    let stream_key = StreamKey::Vault {
        pubkey: master_pubkey,
        vault_id: identity_vault_id(),
    };
    let entry = registry.get(&stream_key).await.unwrap().unwrap();
    assert_eq!(
        entry.revision, 1,
        "idempotent publish must not bump revision when keysets are unchanged"
    );
}

#[tokio::test]
async fn master_key_file_generates_and_persists_plaintext_when_no_keymain() {
    // No `[key.main]` configured → falls back to plaintext storage with
    // a warn. 32 bytes on disk; round-trip yields the same key.
    let dir = tempdir().unwrap();
    let path = dir.path().join("identity_master.key");
    assert!(!path.exists());

    let empty_keys: BTreeMap<String, NodeConfigKey> = BTreeMap::new();
    let k1 = load_or_generate_master_signing_key(&path, &empty_keys, None).unwrap();
    assert!(path.exists());
    let bytes_after_first = std::fs::read(&path).unwrap();
    assert_eq!(
        bytes_after_first.len(),
        32,
        "no [key.main] → plaintext fallback writes raw 32-byte seed"
    );

    let k2 = load_or_generate_master_signing_key(&path, &empty_keys, None).unwrap();
    assert_eq!(
        k1.verifying_key().to_bytes(),
        k2.verifying_key().to_bytes(),
        "load-then-load must return the same key"
    );

    let derived = derive_master_signing_key(&[42u8; 32]);
    assert_ne!(
        k1.verifying_key().to_bytes(),
        derived.verifying_key().to_bytes(),
        "random-generated master must differ from blake3-derived one"
    );
}

#[tokio::test]
async fn master_key_file_age_encrypted_round_trip_with_keymain() {
    // With `[key.main]` carrying recipient + identity_file, the master
    // file is age-encrypted at rest. Compromise of the file alone (without
    // the identity_file) does NOT yield the key.
    let dir = tempdir().unwrap();
    let path = dir.path().join("identity_master.key");

    // Generate a fresh age key pair for [key.main].
    let identity = age::x25519::Identity::generate();
    let recipient = identity.to_public().to_string();
    let identity_file_path = dir.path().join("main.age");
    std::fs::write(&identity_file_path, identity.to_string().expose_secret()).unwrap();

    let mut keys = BTreeMap::new();
    keys.insert(
        "main".to_string(),
        NodeConfigKey {
            public_key: recipient,
            identity_file: Some(identity_file_path.to_string_lossy().into_owned()),
        },
    );

    let k1 = load_or_generate_master_signing_key(&path, &keys, None).unwrap();
    let on_disk = std::fs::read(&path).unwrap();
    assert!(
        on_disk.starts_with(b"age-encryption.org/v1\n"),
        "with [key.main] configured, the master file must be age ciphertext, \
         got first 40 bytes: {:?}",
        &on_disk[..on_disk.len().min(40)]
    );

    let k2 = load_or_generate_master_signing_key(&path, &keys, None).unwrap();
    assert_eq!(
        k1.verifying_key().to_bytes(),
        k2.verifying_key().to_bytes(),
        "age-encrypted load-then-load must return the same key"
    );

    // Wrong identity file fails to decrypt cleanly.
    let other = age::x25519::Identity::generate();
    let other_path = dir.path().join("other.age");
    std::fs::write(&other_path, other.to_string().expose_secret()).unwrap();
    let mut wrong_keys = BTreeMap::new();
    wrong_keys.insert(
        "main".to_string(),
        NodeConfigKey {
            public_key: other.to_public().to_string(),
            identity_file: Some(other_path.to_string_lossy().into_owned()),
        },
    );
    let err = load_or_generate_master_signing_key(&path, &wrong_keys, None).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("age-decrypting") || msg.contains("identity"),
        "expected decrypt failure, got: {msg}"
    );
}

#[tokio::test]
async fn master_key_file_rejects_wrong_size_in_plaintext_path() {
    // 16-byte plaintext (no age magic) → wrong-size error.
    let dir = tempdir().unwrap();
    let path = dir.path().join("bad.key");
    std::fs::write(&path, [0u8; 16]).unwrap();
    let empty_keys: BTreeMap<String, NodeConfigKey> = BTreeMap::new();
    let err = load_or_generate_master_signing_key(&path, &empty_keys, None).unwrap_err();
    assert!(
        err.to_string().contains("wrong size"),
        "expected size-check error, got: {err}"
    );
}

/// The identity bundle advertises every `[key.*]` recipient — the device's own
/// age key *and* the paper recovery key — so any publisher (own devices or a
/// co-member) resolving recipients from the bundle always encrypts for paper.
/// This is what makes paper-only content recovery work without per-vault config.
#[test]
fn bundle_advertises_device_and_paper_recipients() {
    let mut keys = BTreeMap::new();
    keys.insert(
        "main".to_string(),
        NodeConfigKey {
            public_key: "age1device".to_string(),
            identity_file: None,
        },
    );
    keys.insert(
        "recovery".to_string(),
        NodeConfigKey {
            public_key: "age1paper".to_string(),
            identity_file: None,
        },
    );
    // Deterministic BTreeMap order: "main" < "recovery".
    assert_eq!(
        bundle_age_recipients(&keys),
        vec!["age1device".to_string(), "age1paper".to_string()]
    );
}
