//! Step 2 end-to-end: two daemons (A and B) publish their DidDocuments
//! to a shared registry + store. A's config has `[friend.b] = {id=B's DID}`
//! and `vault.X.members = ["self", "b"]`. After running
//! `build_membership_state`, A's `MembershipState` for vault X contains
//! both A's and B's iroh pubkeys + age recipients.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use ed25519_dalek::SigningKey;
use s5_core::RegistryApi;
use s5_core::blob::BlobStore;
use s5_core::identity::Did;
use s5_node::config::{NodeConfigIdentity, NodeConfigSource, NodeConfigVault, S5NodeConfig};
use s5_node::device_keyset::DeviceKeyset;
use s5_node::identity_vault::{derive_master_signing_key, publish_self_on_startup};

/// Manufacture a deterministic-but-independent pubkey triple from a
/// 32-byte test seed. Each pubkey is derived under its own blake3
/// domain so the three are distinct — mirrors the production keyset
/// shape (independent random secrets) while keeping tests reproducible.
fn pubkeys_from_seed(seed: &[u8; 32]) -> ([u8; 32], [u8; 32], [u8; 32]) {
    let device_signing = blake3::derive_key("test/device_signing", seed);
    let device_acl = blake3::derive_key("test/device_acl", seed);
    let iroh = blake3::derive_key("test/iroh", seed);
    (
        SigningKey::from_bytes(&device_signing)
            .verifying_key()
            .to_bytes(),
        SigningKey::from_bytes(&device_acl)
            .verifying_key()
            .to_bytes(),
        SigningKey::from_bytes(&iroh).verifying_key().to_bytes(),
    )
}

/// Compile-time check that DeviceKeyset is accessible from the test
/// crate (slice S2.5 keyset is the production source — tests use the
/// `pubkeys_from_seed` helper above for reproducibility, but the
/// keyset itself is what feeds the daemon at boot).
#[allow(dead_code)]
fn _keyset_type_check() -> DeviceKeyset {
    DeviceKeyset::generate()
}
use s5_node::membership::{VaultMembership, build_membership_state};
use s5_node_api::config::{NodeConfigFriend, NodeConfigKey};
use s5_registry::MemoryRegistry;
use s5_store_local::{LocalStore, LocalStoreConfig};
use tempfile::tempdir;

const AGE_PUB_A: &str = "age1a4z5qsqlthanhgp5fj2dszsvmxhrpvw35n7an69zjr0nlukfj9lsxsmney";
const AGE_PUB_B: &str = "age1m5y8j727k9gqqwwu32sgx0wrquxhq2svxepuv5xgq685umr8pytsqzkt36";

fn vault_with_members(members: Vec<String>) -> NodeConfigVault {
    NodeConfigVault {
        root_path: "/tmp/unused".to_string(),
        key: "main".to_string(),
        data_store: None,
        preset: None,
        recipients: Vec::new(),
        sources: Vec::new(),
        meta_store: None,
        plaintext_tree: false,
        plaintext_published_tn: false,
        watch: false,
        members,
        pipelines: Vec::new(),
        vault_id: None,
        ..Default::default()
    }
}

fn keys_with_main(age_pub: &str) -> BTreeMap<String, NodeConfigKey> {
    let mut keys = BTreeMap::new();
    keys.insert(
        "main".to_string(),
        NodeConfigKey {
            public_key: age_pub.to_string(),
            identity_file: None,
        },
    );
    keys
}

fn config_with(
    age_pub: &str,
    vault_members: Option<(&str, Vec<String>)>,
    friends: BTreeMap<String, NodeConfigFriend>,
) -> S5NodeConfig {
    let mut vaults = BTreeMap::new();
    if let Some((name, members)) = vault_members {
        vaults.insert(name.to_string(), vault_with_members(members));
    }
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
        key: keys_with_main(age_pub),
        store: BTreeMap::new(),
        default_store: None,
        registry: BTreeMap::new(),
        source: BTreeMap::<String, NodeConfigSource>::new(),
        vault: vaults,
        task: BTreeMap::new(),
        friend: friends,
    }
}

#[tokio::test]
async fn resolves_self_and_friend() {
    let dir = tempdir().unwrap();
    let store = BlobStore::from_arc(Arc::new(LocalStore::create(LocalStoreConfig {
        base_path: dir.path().to_string_lossy().into_owned(),
    })));
    let mut stores: HashMap<String, Arc<dyn s5_core::blob::Blobs>> = HashMap::new();
    stores.insert("only".to_string(), Arc::new(store));

    let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());

    // Distinct seeds for A and B → distinct masters → distinct DIDs.
    // `pubkeys_from_seed` produces three independent pubkeys per seed
    // (matching the production keyset shape); we extract the iroh
    // entry for the ACL assertion below.
    let secret_a = [11u8; 32];
    let secret_b = [22u8; 32];
    let master_a = derive_master_signing_key(&secret_a);
    let did_a = Did::from_pubkey(s5_core::identity::DidMasterPubkey::from_verifying_key(
        &master_a.verifying_key(),
    ));
    let pubkey_a = pubkeys_from_seed(&secret_a).2; // iroh pubkey
    let pubkey_b = pubkeys_from_seed(&secret_b).2; // iroh pubkey
    // D17: identities resolve via their cold pointer. Tests run
    // self-anchored (cold == warm) — publish one pointer per identity.
    for seed in [&secret_a, &secret_b] {
        let key = derive_master_signing_key(seed);
        registry
            .set(s5_node::identity_anchor::self_anchored_entry(&key).unwrap())
            .await
            .unwrap();
    }
    let did_b = Did::from_pubkey(s5_core::identity::DidMasterPubkey::from_verifying_key(
        &derive_master_signing_key(&secret_b).verifying_key(),
    ));

    // A and B both publish their bundles using the standard daemon path.
    publish_self_on_startup(
        &config_with(AGE_PUB_A, None, BTreeMap::new()),
        &stores,
        registry.clone(),
        &derive_master_signing_key(&secret_a),
        pubkeys_from_seed(&secret_a).0,
        pubkeys_from_seed(&secret_a).1,
        pubkeys_from_seed(&secret_a).2,
    )
    .await;
    publish_self_on_startup(
        &config_with(AGE_PUB_B, None, BTreeMap::new()),
        &stores,
        registry.clone(),
        &derive_master_signing_key(&secret_b),
        pubkeys_from_seed(&secret_b).0,
        pubkeys_from_seed(&secret_b).1,
        pubkeys_from_seed(&secret_b).2,
    )
    .await;

    // A's config: vault X has members = ["self", "b"]; [friend.b].id = B's DID.
    let mut friends = BTreeMap::new();
    friends.insert(
        "b".to_string(),
        NodeConfigFriend {
            id: did_b.to_string(),
            iroh_pubkey_hex: None,
        },
    );
    let cfg_a = config_with(
        AGE_PUB_A,
        Some(("X", vec!["self".to_string(), "b".to_string()])),
        friends,
    );

    let state = build_membership_state(&did_a, &cfg_a, registry.as_ref(), &stores).await;

    let vm: &VaultMembership = state.vaults.get("X").expect("vault X must be resolved");
    assert_eq!(vm.member_dids.len(), 2);
    assert!(vm.authorized_iroh_pubkeys.contains(&pubkey_a));
    assert!(vm.authorized_iroh_pubkeys.contains(&pubkey_b));
    assert_eq!(vm.age_recipients.len(), 2);
    assert!(vm.age_recipients.iter().any(|r| r == AGE_PUB_A));
    assert!(vm.age_recipients.iter().any(|r| r == AGE_PUB_B));

    // S3a: every member's `bundle.acl_keys[]` must land in the resolved
    // VaultMembership.authorized_acl_pubkeys — the F02 blob-fetch
    // challenge consumer (S3b) needs this set populated.
    let acl_a = pubkeys_from_seed(&secret_a).1;
    let acl_b = pubkeys_from_seed(&secret_b).1;
    assert!(
        vm.authorized_acl_pubkeys.contains(&acl_a),
        "A's published ACL pubkey must appear in the resolved vault's \
         authorized_acl_pubkeys"
    );
    assert!(
        vm.authorized_acl_pubkeys.contains(&acl_b),
        "B's published ACL pubkey must appear in the resolved vault's \
         authorized_acl_pubkeys"
    );
    assert_eq!(
        vm.authorized_acl_pubkeys.len(),
        2,
        "exactly the two members' ACL pubkeys — no extras"
    );
}

#[tokio::test]
async fn missing_friend_skipped_logged() {
    let dir = tempdir().unwrap();
    let store = BlobStore::from_arc(Arc::new(LocalStore::create(LocalStoreConfig {
        base_path: dir.path().to_string_lossy().into_owned(),
    })));
    let mut stores: HashMap<String, Arc<dyn s5_core::blob::Blobs>> = HashMap::new();
    stores.insert("only".to_string(), Arc::new(store));

    let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
    let secret_a = [33u8; 32];
    let master_a = derive_master_signing_key(&secret_a);
    let did_a = Did::from_pubkey(s5_core::identity::DidMasterPubkey::from_verifying_key(
        &master_a.verifying_key(),
    ));
    registry
        .set(s5_node::identity_anchor::self_anchored_entry(&master_a).unwrap())
        .await
        .unwrap();

    // A publishes its bundle, but member "ghost" is not in [friend.*].
    publish_self_on_startup(
        &config_with(AGE_PUB_A, None, BTreeMap::new()),
        &stores,
        registry.clone(),
        &master_a,
        pubkeys_from_seed(&secret_a).0,
        pubkeys_from_seed(&secret_a).1,
        pubkeys_from_seed(&secret_a).2,
    )
    .await;
    let cfg_a = config_with(
        AGE_PUB_A,
        Some(("X", vec!["self".to_string(), "ghost".to_string()])),
        BTreeMap::new(),
    );

    let state = build_membership_state(&did_a, &cfg_a, registry.as_ref(), &stores).await;

    let vm = state.vaults.get("X").expect("vault X must be resolved");
    assert_eq!(
        vm.member_dids.len(),
        1,
        "ghost must be skipped, only self remains"
    );
    assert_eq!(vm.authorized_iroh_pubkeys.len(), 1);
    assert_eq!(vm.age_recipients.len(), 1);
}

#[tokio::test]
async fn unpublished_friend_keeps_did_but_empty_acl() {
    let dir = tempdir().unwrap();
    let store = BlobStore::from_arc(Arc::new(LocalStore::create(LocalStoreConfig {
        base_path: dir.path().to_string_lossy().into_owned(),
    })));
    let mut stores: HashMap<String, Arc<dyn s5_core::blob::Blobs>> = HashMap::new();
    stores.insert("only".to_string(), Arc::new(store));

    let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());

    let secret_a = [44u8; 32];
    let secret_b = [55u8; 32];
    let master_a = derive_master_signing_key(&secret_a);
    let did_a = Did::from_pubkey(s5_core::identity::DidMasterPubkey::from_verifying_key(
        &master_a.verifying_key(),
    ));
    registry
        .set(s5_node::identity_anchor::self_anchored_entry(&master_a).unwrap())
        .await
        .unwrap();
    let did_b = Did::from_pubkey(s5_core::identity::DidMasterPubkey::from_verifying_key(
        &derive_master_signing_key(&secret_b).verifying_key(),
    ));

    // Only A publishes; B exists as a friend reference but has no bundle yet.
    publish_self_on_startup(
        &config_with(AGE_PUB_A, None, BTreeMap::new()),
        &stores,
        registry.clone(),
        &master_a,
        pubkeys_from_seed(&secret_a).0,
        pubkeys_from_seed(&secret_a).1,
        pubkeys_from_seed(&secret_a).2,
    )
    .await;

    let mut friends = BTreeMap::new();
    friends.insert(
        "b".to_string(),
        NodeConfigFriend {
            id: did_b.to_string(),
            iroh_pubkey_hex: None,
        },
    );
    let cfg_a = config_with(
        AGE_PUB_A,
        Some(("X", vec!["self".to_string(), "b".to_string()])),
        friends,
    );

    let state = build_membership_state(&did_a, &cfg_a, registry.as_ref(), &stores).await;

    let vm = state.vaults.get("X").expect("vault X must be resolved");
    assert_eq!(
        vm.member_dids.len(),
        2,
        "both DIDs are listed even when one bundle is unpublished"
    );
    // Slice 2d retired the master==iroh bootstrap fallback: when a
    // friend's bundle hasn't reached the local registry yet, we have
    // no out-of-band iroh pubkey to seed and the DID master pubkey is
    // NOT an iroh transport key. So only self contributes to the iroh
    // ACL — the unpublished friend is unreachable until their bundle
    // arrives via some other channel.
    assert_eq!(
        vm.authorized_iroh_pubkeys.len(),
        1,
        "only self's iroh pubkey — unpublished friend has no known transport key"
    );
    // Age recipients still need a real bundle — only self contributes.
    assert_eq!(vm.age_recipients.len(), 1);
}
