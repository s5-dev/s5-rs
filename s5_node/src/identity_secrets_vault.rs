//! The `identity_secrets` vault — the daemon's escrow of the warm master
//! signing seed.
//!
//! A master-anchored special vault ([`crate::special_vaults`],
//! `docs/reference/special-vaults.md` § 3) located at `(master_pubkey,
//! identity_secrets_vault_id())`, holding:
//!
//! - `master.key` — the warm master signing seed, escrowed so a paired device
//!   (holding only its device age key, not the mnemonic) can sign without paper.
//! - `devices` — the device catalogue: label → the device's four pubkeys +
//!   age recipient (D10). UI/bookkeeping only; see [`DEVICES_KEY`].
//!
//! Sealed to the paper recovery key + the device age keys and published durably,
//! and master-signed (admin cadence: device add / rotation). Vault **discovery**
//! — the `vault_id` directory and the discovery `seed` — lives in the warm
//! `config` vault ([`crate::config_vault`]), reached from the same master
//! anchor; this vault carries only the truly-secret escrow, so adding a vault
//! never touches it.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use ed25519_dalek::SigningKey;
use s5_core::RegistryApi;
use s5_core::blob::Blobs;

use crate::admission::DeviceKeys;
use crate::special_vaults::{identity_secrets_vault_id, publish_vault_entries, read_vault_entries};

/// Tree key for the escrowed warm master signing seed. Public so paper
/// recovery (which has no warm key yet, hence no [`IdentitySecretsVault`]
/// handle) can read the entry via
/// [`crate::special_vaults::read_vault_entries`] at `(warm_pub,
/// identity_secrets_vault_id())`.
pub const MASTER_KEY: &str = "master.key";

/// Tree key for the device catalogue: a CBOR map `label →`
/// [`DeviceKeys`] recording every enrolled device's four pubkeys + age
/// recipient. Written on admission (`vup device invite`); read by
/// `vup device ls` and D18's `vup device revoke` (label → which keys
/// to drop).
///
/// **Labels are UI-only — NEVER an authorization input.** Authorization
/// is exclusively keyset membership in the signed identity bundle
/// (D9/D11); the catalogue is unsigned-at-this-layer bookkeeping inside
/// an encrypted vault, and nothing anywhere may grant or deny anything
/// based on a label string.
pub const DEVICES_KEY: &str = "devices";

/// The device catalogue: petname label → the device's four public keys.
pub type DeviceCatalogue = BTreeMap<String, DeviceKeys>;

/// Decode the catalogue from the raw `devices` entry bytes.
pub fn decode_device_catalogue(bytes: &[u8]) -> Result<DeviceCatalogue> {
    minicbor::decode(bytes).map_err(|e| anyhow!("decoding device catalogue CBOR: {e}"))
}

/// Encode the catalogue to the raw `devices` entry bytes.
pub fn encode_device_catalogue(catalogue: &DeviceCatalogue) -> Vec<u8> {
    minicbor::to_vec(catalogue).expect("CBOR encoding into Vec is infallible")
}

/// A handle to the identity's `identity_secrets` vault. Cheap to construct; each
/// method is a fresh registry + blob round trip.
pub struct IdentitySecretsVault {
    /// Identity master key — signs the HEAD; its pubkey locates the vault.
    master: SigningKey,
    /// Durable, recoverable blob store the vault lives in — the `dyn Blobs`
    /// view so content-addressed backends (Sia `PackingStore`, no path-`Store`
    /// view) qualify, same as the config vault.
    store: Arc<dyn Blobs>,
    registry: Arc<dyn RegistryApi + Send + Sync>,
    /// age recipients the vault is re-sealed to (the device set + paper).
    recipients: Vec<String>,
    /// age identity files tried when decrypting on read.
    identity_files: Vec<String>,
}

impl IdentitySecretsVault {
    pub fn new(
        master: SigningKey,
        store: Arc<dyn Blobs>,
        registry: Arc<dyn RegistryApi + Send + Sync>,
        recipients: Vec<String>,
        identity_files: Vec<String>,
    ) -> Self {
        Self {
            master,
            store,
            registry,
            recipients,
            identity_files,
        }
    }

    /// Read the escrowed warm master signing seed, or `None` if the vault has
    /// never been published.
    pub async fn read(&self) -> Result<Option<[u8; 32]>> {
        Ok(self
            .read_raw()
            .await?
            .get(MASTER_KEY)
            .and_then(|v| v.as_slice().try_into().ok()))
    }

    /// Publish (escrow) the warm master signing seed, republishing only if it
    /// changed (idempotent — no churn on a quiescent boot).
    pub async fn publish(&self, master_key: &[u8; 32]) -> Result<()> {
        let before = self.read_raw().await.unwrap_or_default();
        let mut raw = before.clone();
        raw.insert(MASTER_KEY.to_string(), master_key.to_vec());
        if raw == before {
            return Ok(());
        }
        publish_vault_entries(
            raw,
            identity_secrets_vault_id(),
            &self.master,
            self.store.clone(),
            self.registry.as_ref(),
            &self.recipients,
        )
        .await
    }

    /// Read the device catalogue, or an empty one if never written.
    pub async fn read_devices(&self) -> Result<DeviceCatalogue> {
        match self.read_raw().await?.get(DEVICES_KEY) {
            Some(bytes) => decode_device_catalogue(bytes),
            None => Ok(DeviceCatalogue::new()),
        }
    }

    /// Upsert one catalogue entry under `label` and republish
    /// (revision++), preserving every other identity-secrets entry
    /// (`master.key`, other devices' records). Idempotent: an identical
    /// record already stored under `label` republishes nothing.
    ///
    /// The label is a UI petname only — never an authorization input
    /// (see [`DEVICES_KEY`]). Admission to the *bundle*
    /// ([`crate::admission::admit_device_keys`]) is what grants
    /// authority; this records the human-readable inventory.
    pub async fn upsert_device(&self, label: &str, keys: &DeviceKeys) -> Result<()> {
        let before = self.read_raw().await.unwrap_or_default();
        let mut catalogue = match before.get(DEVICES_KEY) {
            Some(bytes) => decode_device_catalogue(bytes)?,
            None => DeviceCatalogue::new(),
        };
        if catalogue.get(label) == Some(keys) {
            return Ok(());
        }
        catalogue.insert(label.to_string(), keys.clone());
        let mut raw = before;
        raw.insert(DEVICES_KEY.to_string(), encode_device_catalogue(&catalogue));
        publish_vault_entries(
            raw,
            identity_secrets_vault_id(),
            &self.master,
            self.store.clone(),
            self.registry.as_ref(),
            &self.recipients,
        )
        .await
    }

    /// Remove one catalogue entry by `label` and republish (revision++),
    /// preserving every other identity-secrets entry (`master.key`, the
    /// other devices' records). No-op when the label is absent
    /// (idempotent revoke). Returns the removed record, if any.
    ///
    /// D18's bookkeeping half: authority is revoked by dropping the keys
    /// from the *bundle* ([`crate::admission::remove_device_keys`]); this
    /// removes the inventory record. The republish seals to THIS handle's
    /// `recipients` — construct it with the **surviving** set.
    pub async fn remove_device(&self, label: &str) -> Result<Option<DeviceKeys>> {
        let before = self.read_raw().await.unwrap_or_default();
        let mut catalogue = match before.get(DEVICES_KEY) {
            Some(bytes) => decode_device_catalogue(bytes)?,
            None => DeviceCatalogue::new(),
        };
        let Some(removed) = catalogue.remove(label) else {
            return Ok(None);
        };
        let mut raw = before;
        raw.insert(DEVICES_KEY.to_string(), encode_device_catalogue(&catalogue));
        publish_vault_entries(
            raw,
            identity_secrets_vault_id(),
            &self.master,
            self.store.clone(),
            self.registry.as_ref(),
            &self.recipients,
        )
        .await?;
        Ok(Some(removed))
    }

    async fn read_raw(&self) -> Result<BTreeMap<String, Vec<u8>>> {
        read_vault_entries(
            self.master.verifying_key().to_bytes(),
            identity_secrets_vault_id(),
            self.store.clone(),
            self.registry.as_ref(),
            &self.identity_files,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use s5_core::StreamKey;
    use s5_core::blob::BlobStore;
    use s5_registry::MemoryRegistry;
    use s5_store_memory::MemoryStore;

    use super::*;

    fn age_identity(dir: &Path, name: &str) -> (String, String) {
        use age::secrecy::ExposeSecret;
        let identity = age::x25519::Identity::generate();
        let recipient = identity.to_public().to_string();
        let path = dir.join(format!("{name}.txt"));
        std::fs::write(&path, identity.to_string().expose_secret()).unwrap();
        (recipient, path.to_string_lossy().into_owned())
    }

    /// Device A escrows the warm master seed; device B — same identity, its own
    /// age key, no enrollment — recovers it. This is what lets a paired device
    /// sign without the paper phrase.
    #[tokio::test]
    async fn publish_then_recover_master_key() {
        let dir = tempfile::tempdir().unwrap();
        let (rec_a, id_a) = age_identity(dir.path(), "a");
        let (rec_b, id_b) = age_identity(dir.path(), "b");
        let store = Arc::new(BlobStore::new(MemoryStore::new()));
        let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
        let master = SigningKey::from_bytes(&[7u8; 32]);
        let stream_key = StreamKey::Vault {
            pubkey: master.verifying_key().to_bytes(),
            vault_id: identity_secrets_vault_id(),
        };

        let warm = [3u8; 32];
        let a = IdentitySecretsVault::new(
            master.clone(),
            store.clone(),
            registry.clone(),
            vec![rec_a, rec_b],
            vec![id_a],
        );
        a.publish(&warm).await.unwrap();
        let rev1 = registry.get(&stream_key).await.unwrap().map(|e| e.revision);
        assert!(rev1.is_some());

        let b = IdentitySecretsVault::new(
            master.clone(),
            store.clone(),
            registry.clone(),
            vec![],
            vec![id_b],
        );
        assert_eq!(b.read().await.unwrap(), Some(warm));

        // Idempotent: republishing the same seed does not bump the revision.
        a.publish(&warm).await.unwrap();
        let rev2 = registry.get(&stream_key).await.unwrap().map(|e| e.revision);
        assert_eq!(rev2, rev1, "unchanged escrow must not republish");
    }

    #[tokio::test]
    async fn read_on_unpublished_vault_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let (_rec, id) = age_identity(dir.path(), "a");
        let store = Arc::new(BlobStore::new(MemoryStore::new()));
        let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
        let v = IdentitySecretsVault::new(
            SigningKey::from_bytes(&[1u8; 32]),
            store,
            registry,
            vec![],
            vec![id],
        );
        assert_eq!(v.read().await.unwrap(), None);
    }

    fn device_keys(seed: u8) -> DeviceKeys {
        DeviceKeys {
            signing: [seed; 32],
            acl: [seed.wrapping_add(1); 32],
            iroh: [seed.wrapping_add(2); 32],
            age_recipient: format!("age1device{seed}"),
        }
    }

    /// Admission writes a catalogue entry; a second device's entry
    /// coexists with the first AND with the master.key escrow (one
    /// vault, merged entries); re-upserting an identical record is a
    /// no-op (no HEAD bump).
    #[tokio::test]
    async fn catalogue_upsert_merges_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let (rec, id) = age_identity(dir.path(), "a");
        let store = Arc::new(BlobStore::new(MemoryStore::new()));
        let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
        let master = SigningKey::from_bytes(&[7u8; 32]);
        let stream_key = StreamKey::Vault {
            pubkey: master.verifying_key().to_bytes(),
            vault_id: identity_secrets_vault_id(),
        };
        let v =
            IdentitySecretsVault::new(master.clone(), store, registry.clone(), vec![rec], vec![id]);

        v.publish(&[3u8; 32]).await.unwrap();
        v.upsert_device("laptop", &device_keys(10)).await.unwrap();
        v.upsert_device("phone", &device_keys(20)).await.unwrap();

        let catalogue = v.read_devices().await.unwrap();
        assert_eq!(catalogue.len(), 2);
        assert_eq!(catalogue.get("laptop"), Some(&device_keys(10)));
        assert_eq!(catalogue.get("phone"), Some(&device_keys(20)));
        // The escrow survived the catalogue writes.
        assert_eq!(v.read().await.unwrap(), Some([3u8; 32]));

        // Idempotent re-upsert: no HEAD bump.
        let rev = registry
            .get(&stream_key)
            .await
            .unwrap()
            .map(|e| e.revision)
            .unwrap();
        v.upsert_device("phone", &device_keys(20)).await.unwrap();
        let rev2 = registry
            .get(&stream_key)
            .await
            .unwrap()
            .map(|e| e.revision)
            .unwrap();
        assert_eq!(rev, rev2, "identical record must not republish");
    }

    /// D18's catalogue half: `remove_device` drops exactly the target's
    /// record (siblings + the escrow survive), removing an absent label
    /// is a no-op (no HEAD bump), and the removed record is returned.
    #[tokio::test]
    async fn catalogue_remove_drops_only_the_target() {
        let dir = tempfile::tempdir().unwrap();
        let (rec, id) = age_identity(dir.path(), "a");
        let store = Arc::new(BlobStore::new(MemoryStore::new()));
        let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
        let master = SigningKey::from_bytes(&[7u8; 32]);
        let stream_key = StreamKey::Vault {
            pubkey: master.verifying_key().to_bytes(),
            vault_id: identity_secrets_vault_id(),
        };
        let v =
            IdentitySecretsVault::new(master.clone(), store, registry.clone(), vec![rec], vec![id]);

        v.publish(&[3u8; 32]).await.unwrap();
        v.upsert_device("laptop", &device_keys(10)).await.unwrap();
        v.upsert_device("phone", &device_keys(20)).await.unwrap();

        let removed = v.remove_device("phone").await.unwrap();
        assert_eq!(removed, Some(device_keys(20)));
        let catalogue = v.read_devices().await.unwrap();
        assert_eq!(catalogue.len(), 1);
        assert_eq!(catalogue.get("laptop"), Some(&device_keys(10)));
        // The escrow survived the removal republish.
        assert_eq!(v.read().await.unwrap(), Some([3u8; 32]));

        // Idempotent: removing an absent label neither errors nor bumps.
        let rev = registry
            .get(&stream_key)
            .await
            .unwrap()
            .map(|e| e.revision)
            .unwrap();
        assert_eq!(v.remove_device("phone").await.unwrap(), None);
        let rev2 = registry
            .get(&stream_key)
            .await
            .unwrap()
            .map(|e| e.revision)
            .unwrap();
        assert_eq!(rev, rev2, "absent-label removal must not republish");
    }

    /// The §6.1 re-wrap: after `rewrap_special_vaults` expands the
    /// recipient set, the NEW device's age key opens the vault — and the
    /// catalogue + escrow contents came through the re-seal intact.
    #[tokio::test]
    async fn catalogue_survives_rewrap_and_new_device_can_read() {
        let dir = tempfile::tempdir().unwrap();
        let (rec_a, id_a) = age_identity(dir.path(), "a");
        let (rec_b, id_b) = age_identity(dir.path(), "b");
        let (rec_paper, _id_paper) = age_identity(dir.path(), "paper");
        let store = Arc::new(BlobStore::new(MemoryStore::new()));
        let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
        let master = SigningKey::from_bytes(&[7u8; 32]);

        // Device A escrows the warm seed + its own catalogue record,
        // sealed to (A, paper) — B is not yet a recipient.
        let a = IdentitySecretsVault::new(
            master.clone(),
            store.clone(),
            registry.clone(),
            vec![rec_a.clone(), rec_paper.clone()],
            vec![id_a.clone()],
        );
        a.publish(&[3u8; 32]).await.unwrap();
        a.upsert_device("laptop", &device_keys(10)).await.unwrap();

        let b = IdentitySecretsVault::new(
            master.clone(),
            store.clone(),
            registry.clone(),
            vec![],
            vec![id_b],
        );
        assert!(
            b.read_devices().await.is_err(),
            "B must not read the vault before the re-wrap"
        );

        // Re-wrap to the FULL set (A + B + paper) — writers ⊆ readers.
        crate::admission::rewrap_special_vaults(
            &master,
            registry.as_ref(),
            store.clone(),
            std::slice::from_ref(&id_a),
            &[rec_a, rec_b, rec_paper],
        )
        .await
        .unwrap();

        // B now reads everything; contents survived the re-seal.
        assert_eq!(b.read().await.unwrap(), Some([3u8; 32]));
        let catalogue = b.read_devices().await.unwrap();
        assert_eq!(catalogue.get("laptop"), Some(&device_keys(10)));
    }
}
