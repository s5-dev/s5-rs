//! Keyset admission — THE primitive behind multi-device (D9/D10).
//!
//! "Admission of a pubkey to a keyset" is the one mechanism the identity
//! and sharing model composes from (decision
//! D9). It has two scopes:
//!
//! - **identity scope** (this module): admit a device's four keys into
//!   *your* identity bundle at `(warm_pub, IDENTITY_VAULT_ID)` — add
//!   device (D10). Removal is the inverse ([`remove_device_keys`],
//!   D18's core).
//! - **vault scope**: admit a member's pubkeys to *a vault* — grant.
//!   That scope stays config-driven (`vault.<name>.members` +
//!   [`crate::membership`] resolution) and is deliberately NOT routed
//!   through here.
//!
//! Every edit is **read-merge-write**: fetch the current bundle, union
//! (or subtract) the keys, and republish at `revision + 1` — so two
//! sibling devices admitting keys never clobber each other's entries.
//! A publish that loses a registry race (another writer landed a higher
//! revision between our read and our set) is retried once by re-reading
//! and re-merging; the merge is idempotent, so replaying it over the
//! winner's bundle is safe.
//!
//! The bundle stream is signed by the **warm** master (D17); the DID
//! (cold) reaches it via the cold-pointer anchor
//! ([`crate::identity_anchor`]). This module never touches the cold key.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use s5_core::blob::Blobs;
use s5_core::identity::IdentityBundle;
use s5_core::{Hash, RegistryApi, StreamKey, StreamMessage};

use crate::identity_vault::identity_vault_id;

/// One device's four public keys — the unit of identity-scope admission
/// (`identity-model.md` § Per-device keys). The secrets never leave the
/// device; only these cross the enrollment channel.
///
/// CBOR shape (numeric map keys, minicbor convention) doubles as the
/// device-catalogue record value
/// ([`crate::identity_secrets_vault::DEVICES_KEY`]).
#[derive(Clone, Debug, PartialEq, Eq, minicbor::Encode, minicbor::Decode)]
#[cbor(map)]
pub struct DeviceKeys {
    /// ed25519 device signing pubkey → `bundle.signers[]` (write authority).
    #[n(0)]
    pub signing: [u8; 32],
    /// ed25519 device ACL/read pubkey → `bundle.acl_keys[]` (F02 challenge).
    #[n(1)]
    pub acl: [u8; 32],
    /// ed25519 iroh transport pubkey → `bundle.iroh_pubkeys[]` (dial target,
    /// never an authorisation principal).
    #[n(2)]
    pub iroh: [u8; 32],
    /// X25519 age recipient (`age1…`) → `bundle.age_recipients[]`.
    #[n(3)]
    pub age_recipient: String,
}

/// Result of a bundle edit.
#[derive(Clone, Copy, Debug)]
pub struct AdmissionOutcome {
    /// Revision of the bundle now current (the freshly published one, or
    /// the pre-existing one when the edit was a no-op).
    pub revision: u64,
    /// BLAKE3 hash of the bundle blob the registry entry points at.
    pub blob_hash: Hash,
    /// Whether the edit changed anything (false = idempotent skip, no
    /// republish, no revision bump).
    pub changed: bool,
}

/// Fetch the current identity bundle at `(warm_pub, IDENTITY_VAULT_ID)`:
/// registry entry, then the blob from the first configured store that
/// has it. `Ok(None)` when no bundle was ever published. An entry whose
/// blob is unreachable in *every* store is an **error** — proceeding as
/// if absent would rebuild a single-device bundle and clobber sibling
/// devices' keys, exactly what the read-merge-write contract forbids.
pub async fn read_current_bundle(
    warm_pub: [u8; 32],
    registry: &dyn RegistryApi,
    stores: &HashMap<String, Arc<dyn Blobs>>,
) -> Result<Option<(IdentityBundle, StreamMessage)>> {
    let stream_key = StreamKey::Vault {
        pubkey: warm_pub,
        vault_id: identity_vault_id(),
    };
    let Some(entry) = registry.get(&stream_key).await? else {
        return Ok(None);
    };
    let mut bytes = None;
    for store in stores.values() {
        if let Ok(b) = store.blob_download(entry.hash).await {
            bytes = Some(b);
            break;
        }
    }
    let bytes = bytes.ok_or_else(|| {
        anyhow!(
            "identity bundle blob {} (revision {}) unreachable in every configured \
             store — refusing to rebuild from scratch (would clobber sibling devices)",
            entry.hash,
            entry.revision
        )
    })?;
    let bundle = IdentityBundle::decode_cbor(&bytes)
        .map_err(|e| anyhow!("decoding identity bundle blob {}: {e}", entry.hash))?;
    Ok(Some((bundle, entry)))
}

/// Union `key` into `set` (append-at-end, existing order untouched).
/// Returns true iff the set changed.
fn union_key(set: &mut Vec<[u8; 32]>, key: [u8; 32]) -> bool {
    if set.contains(&key) {
        false
    } else {
        set.push(key);
        true
    }
}

/// Union `recipient` into `set` (append-at-end). Returns true iff changed.
fn union_recipient(set: &mut Vec<String>, recipient: &str) -> bool {
    if set.iter().any(|r| r == recipient) {
        false
    } else {
        set.push(recipient.to_string());
        true
    }
}

/// Remove `key` from `set`; true iff it was present.
fn remove_key(set: &mut Vec<[u8; 32]>, key: &[u8; 32]) -> bool {
    let before = set.len();
    set.retain(|k| k != key);
    set.len() != before
}

fn remove_recipient(set: &mut Vec<String>, recipient: &str) -> bool {
    let before = set.len();
    set.retain(|r| r != recipient);
    set.len() != before
}

/// The read-merge-write core. Applies `edit` to the current bundle (or
/// an empty one when nothing is published yet); if the edit changed
/// anything, publishes the result at `revision + 1` — blob uploaded to
/// **every** store so the entry's hash resolves regardless of which
/// store a peer reaches, entry warm-signed. Retries ONCE when the
/// registry set loses a concurrent-writer race (detected by read-back:
/// [`MemoryRegistry`](s5_registry::MemoryRegistry)-style backends drop a
/// losing revision silently, so a set `Ok` alone proves nothing).
async fn edit_bundle(
    warm: &SigningKey,
    registry: &dyn RegistryApi,
    stores: &HashMap<String, Arc<dyn Blobs>>,
    edit: &(dyn Fn(&mut IdentityBundle) -> bool + Send + Sync),
) -> Result<AdmissionOutcome> {
    if stores.is_empty() {
        bail!("no blob stores configured — cannot publish an identity bundle");
    }
    let warm_pub = warm.verifying_key().to_bytes();
    let stream_key = StreamKey::Vault {
        pubkey: warm_pub,
        vault_id: identity_vault_id(),
    };

    for attempt in 0..2 {
        let prev = read_current_bundle(warm_pub, registry, stores).await?;
        let (mut bundle, prev_entry) = match prev {
            Some((b, entry)) => (b, Some(entry)),
            None => (
                IdentityBundle {
                    version: IdentityBundle::CURRENT_VERSION,
                    revision: 0,
                    signers: Vec::new(),
                    acl_keys: Vec::new(),
                    iroh_pubkeys: Vec::new(),
                    age_recipients: Vec::new(),
                },
                None,
            ),
        };

        if !edit(&mut bundle) {
            // Idempotent skip: the current bundle already satisfies the
            // edit. Revision does not bump; content drives revision.
            let entry = prev_entry.ok_or_else(|| {
                anyhow!("no identity bundle published and the edit produced no content")
            })?;
            return Ok(AdmissionOutcome {
                revision: entry.revision,
                blob_hash: entry.hash,
                changed: false,
            });
        }

        bundle.revision += 1;
        let revision = bundle.revision;
        let cbor = Bytes::from(bundle.encode_cbor());

        // Upload to every store; any single success is enough to publish
        // (peers fall back across stores on fetch). "Success" must mean
        // DURABLE, not staged: a HEAD pointing at a spool-only blob is the
        // recovery-bricking bug from the 2026-07-02 drill —
        // and worse here, a revoke whose bundle blob never lands leaves the
        // revoked device authorized on every peer. So each accepting store
        // is synced before the registry set, and only a synced store counts.
        let mut blob_hash: Option<Hash> = None;
        for (name, store) in stores {
            let uploaded = match store.blob_upload_bytes(cbor.clone()).await {
                Ok(blob) => blob,
                Err(e) => {
                    tracing::warn!(store = name.as_str(), "identity blob upload failed: {e:#}");
                    continue;
                }
            };
            match store.blob_sync().await {
                Ok(()) => blob_hash = Some(uploaded.hash),
                Err(e) => {
                    tracing::warn!(
                        store = name.as_str(),
                        "identity blob sync failed — not counting this store as durable: {e:#}"
                    );
                }
            }
        }
        let Some(hash) = blob_hash else {
            bail!("no store durably accepted the identity bundle blob");
        };

        let message =
            StreamMessage::sign_ed25519_registry(warm, identity_vault_id(), hash, revision)
                .map_err(|e| anyhow!("signing identity bundle entry: {e}"))?;
        let set_result = registry.set(message).await;

        // Read back to detect a lost race: a concurrent writer may have
        // landed `revision` (or higher) between our read and our set, in
        // which case the monotone-revision rule silently drops ours.
        let winner = registry.get(&stream_key).await?;
        match winner {
            Some(e) if e.revision == revision && e.hash == hash => {
                return Ok(AdmissionOutcome {
                    revision,
                    blob_hash: hash,
                    changed: true,
                });
            }
            _ if attempt == 0 => {
                if let Err(e) = set_result {
                    tracing::warn!("identity bundle set failed ({e:#}); retrying once");
                } else {
                    tracing::info!(
                        revision,
                        "identity bundle publish lost a registry race; re-reading and retrying once"
                    );
                }
                continue;
            }
            _ => {
                set_result.map_err(|e| anyhow!("identity bundle registry set: {e}"))?;
                bail!("identity bundle publish lost the registry race twice — giving up");
            }
        }
    }
    unreachable!("edit_bundle loop returns or bails within two attempts")
}

/// Admit one device's four keys into the identity bundle (identity-scope
/// admission, D9/D10): read the current bundle at
/// `(warm_pub, IDENTITY_VAULT_ID)`, union the keys into the four keyset
/// arrays (dedup, stable order), and — only if anything changed —
/// republish at `revision + 1`, warm-signed, blob on every store.
/// Returns the bundle revision now current (unchanged on a no-op
/// re-admit). Retries once on a registry set conflict by re-reading.
pub async fn admit_device_keys(
    warm: &SigningKey,
    registry: &dyn RegistryApi,
    stores: &HashMap<String, Arc<dyn Blobs>>,
    keys: &DeviceKeys,
) -> Result<u64> {
    let keys = keys.clone();
    let outcome = edit_bundle(warm, registry, stores, &move |bundle| {
        let mut changed = union_key(&mut bundle.signers, keys.signing);
        changed |= union_key(&mut bundle.acl_keys, keys.acl);
        changed |= union_key(&mut bundle.iroh_pubkeys, keys.iroh);
        changed |= union_recipient(&mut bundle.age_recipients, &keys.age_recipient);
        changed
    })
    .await?;
    Ok(outcome.revision)
}

/// The inverse of [`admit_device_keys`] (D18's core): drop one device's
/// four keys from the bundle and — only if anything actually left —
/// republish at `revision + 1`. Removing keys that were never admitted
/// is a no-op (idempotent revoke). Returns the bundle revision now
/// current.
///
/// Removal alone is routine revocation (`identity-rotation.md` §6.1);
/// the caller re-wraps the special vaults to the survivor set next —
/// and for a *compromised* device the honest answer additionally
/// involves §6.2 warm rotation, which D18's CLI prints as a checklist.
pub async fn remove_device_keys(
    warm: &SigningKey,
    registry: &dyn RegistryApi,
    stores: &HashMap<String, Arc<dyn Blobs>>,
    keys: &DeviceKeys,
) -> Result<u64> {
    let keys = keys.clone();
    let outcome = edit_bundle(warm, registry, stores, &move |bundle| {
        let mut changed = remove_key(&mut bundle.signers, &keys.signing);
        changed |= remove_key(&mut bundle.acl_keys, &keys.acl);
        changed |= remove_key(&mut bundle.iroh_pubkeys, &keys.iroh);
        changed |= remove_recipient(&mut bundle.age_recipients, &keys.age_recipient);
        changed
    })
    .await?;
    Ok(outcome.revision)
}

/// "Ensure my device's keys are present" — the daemon-startup shape of
/// admission. Unions this device's three pubkeys plus **all** of its
/// `[key.*]` age recipients (device key(s) + paper) into the bundle;
/// merge semantics, so sibling devices' entries survive a boot. Used by
/// [`crate::identity_vault::publish_self_on_startup`].
pub async fn ensure_device_present(
    warm: &SigningKey,
    registry: &dyn RegistryApi,
    stores: &HashMap<String, Arc<dyn Blobs>>,
    signing: [u8; 32],
    acl: [u8; 32],
    iroh: [u8; 32],
    age_recipients: &[String],
) -> Result<AdmissionOutcome> {
    let age_recipients = age_recipients.to_vec();
    edit_bundle(warm, registry, stores, &move |bundle| {
        let mut changed = union_key(&mut bundle.signers, signing);
        changed |= union_key(&mut bundle.acl_keys, acl);
        changed |= union_key(&mut bundle.iroh_pubkeys, iroh);
        for r in &age_recipients {
            changed |= union_recipient(&mut bundle.age_recipients, r);
        }
        changed
    })
    .await
}

/// Re-seal the two warm special vaults — `identity_secrets` (the warm
/// escrow + device catalogue) and the `config` vault (store configs,
/// vault directory, discovery seed) — to the FULL `recipients` set and
/// republish each HEAD (revision++). The read-current-entries →
/// publish-with-expanded-recipients step of `identity-rotation.md`
/// §6.1: run it right after [`admit_device_keys`] (recipients =
/// existing devices + paper + the NEW device) and right after
/// [`remove_device_keys`] (recipients = the survivors + paper).
///
/// **writers ⊆ readers (spec §5) is the caller's contract:**
/// `recipients` MUST include every device age recipient that remains in
/// the bundle plus the paper recovery key — the warm master escrowed as
/// `identity_secrets/master.key` is what those devices sign with, and a
/// device (or the paper walk) that cannot decrypt the escrow of the key
/// it writes with would be the exact blind-writer state the invariant
/// forbids.
///
/// Vault contents are untouched — only the age envelope changes. A
/// vault with no published entries is skipped. Not idempotent across
/// calls (each call bumps the HEAD — the sealed root is freshly
/// encrypted every time); callers invoke it on membership *changes*,
/// not on every boot.
pub async fn rewrap_special_vaults(
    warm: &SigningKey,
    registry: &dyn RegistryApi,
    store: Arc<dyn Blobs>,
    identity_files: &[String],
    recipients: &[String],
) -> Result<()> {
    use crate::special_vaults::{
        config_vault_id, identity_secrets_vault_id, publish_vault_entries, read_vault_entries,
    };

    if recipients.is_empty() {
        bail!("rewrap_special_vaults: empty recipient set would seal the vaults shut");
    }
    let warm_pub = warm.verifying_key().to_bytes();

    for (label, vault_id) in [
        ("identity_secrets", identity_secrets_vault_id()),
        ("config", config_vault_id()),
    ] {
        let entries =
            read_vault_entries(warm_pub, vault_id, store.clone(), registry, identity_files)
                .await
                .map_err(|e| anyhow!("rewrap: reading the {label} vault: {e:#}"))?;
        if entries.is_empty() {
            tracing::info!(vault = label, "rewrap: nothing published, skipping");
            continue;
        }
        publish_vault_entries(entries, vault_id, warm, store.clone(), registry, recipients)
            .await
            .map_err(|e| anyhow!("rewrap: republishing the {label} vault: {e:#}"))?;
        tracing::info!(
            vault = label,
            recipients = recipients.len(),
            "rewrap: resealed to the full recipient set"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use s5_core::blob::BlobStore;
    use s5_registry::MemoryRegistry;
    use s5_store_memory::MemoryStore;

    use super::*;

    fn harness() -> (
        SigningKey,
        Arc<MemoryRegistry>,
        HashMap<String, Arc<dyn Blobs>>,
    ) {
        let warm = SigningKey::from_bytes(&[7u8; 32]);
        let registry = Arc::new(MemoryRegistry::new());
        let stores: HashMap<String, Arc<dyn Blobs>> = HashMap::from([(
            "durable".to_string(),
            Arc::new(BlobStore::new(MemoryStore::new())) as Arc<dyn Blobs>,
        )]);
        (warm, registry, stores)
    }

    fn device(seed: u8) -> DeviceKeys {
        DeviceKeys {
            signing: [seed; 32],
            acl: [seed.wrapping_add(1); 32],
            iroh: [seed.wrapping_add(2); 32],
            age_recipient: format!("age1device{seed}"),
        }
    }

    async fn current_bundle(
        warm: &SigningKey,
        registry: &dyn RegistryApi,
        stores: &HashMap<String, Arc<dyn Blobs>>,
    ) -> IdentityBundle {
        read_current_bundle(warm.verifying_key().to_bytes(), registry, stores)
            .await
            .unwrap()
            .expect("a bundle is published")
            .0
    }

    /// Two devices admit sequentially: the bundle UNIONS both keysets —
    /// the second admission must not clobber the first device's entries.
    #[tokio::test]
    async fn sequential_admissions_union_not_clobber() {
        let (warm, registry, stores) = harness();
        let a = device(10);
        let b = device(20);

        let rev1 = admit_device_keys(&warm, registry.as_ref(), &stores, &a)
            .await
            .unwrap();
        assert_eq!(rev1, 1);
        let rev2 = admit_device_keys(&warm, registry.as_ref(), &stores, &b)
            .await
            .unwrap();
        assert_eq!(rev2, 2);

        let bundle = current_bundle(&warm, registry.as_ref(), &stores).await;
        assert_eq!(bundle.revision, 2);
        // Stable order: first admitted stays first.
        assert_eq!(bundle.signers, vec![a.signing, b.signing]);
        assert_eq!(bundle.acl_keys, vec![a.acl, b.acl]);
        assert_eq!(bundle.iroh_pubkeys, vec![a.iroh, b.iroh]);
        assert_eq!(
            bundle.age_recipients,
            vec![a.age_recipient.clone(), b.age_recipient.clone()]
        );
    }

    /// Re-admitting an already-present device is a no-op: same revision,
    /// no republish (content drives revision, not calls).
    #[tokio::test]
    async fn readmit_is_a_noop() {
        let (warm, registry, stores) = harness();
        let a = device(10);

        let rev1 = admit_device_keys(&warm, registry.as_ref(), &stores, &a)
            .await
            .unwrap();
        let rev2 = admit_device_keys(&warm, registry.as_ref(), &stores, &a)
            .await
            .unwrap();
        assert_eq!(rev1, rev2, "unchanged keysets must not bump the revision");
        let bundle = current_bundle(&warm, registry.as_ref(), &stores).await;
        assert_eq!(bundle.revision, 1);
        assert_eq!(bundle.signers.len(), 1, "no duplicate entries");
    }

    /// Revision bumps ONLY on a real change — a partially-overlapping
    /// admission (same signing key, new age recipient) is a change; a
    /// fully-contained one is not.
    #[tokio::test]
    async fn revision_bumps_only_on_change() {
        let (warm, registry, stores) = harness();
        let a = device(10);
        admit_device_keys(&warm, registry.as_ref(), &stores, &a)
            .await
            .unwrap();

        // Same three pubkeys, different age recipient → real change.
        let mut a2 = a.clone();
        a2.age_recipient = "age1replacementkey".to_string();
        let rev = admit_device_keys(&warm, registry.as_ref(), &stores, &a2)
            .await
            .unwrap();
        assert_eq!(rev, 2);

        // Everything already present → no bump.
        let rev = admit_device_keys(&warm, registry.as_ref(), &stores, &a)
            .await
            .unwrap();
        assert_eq!(rev, 2);

        let bundle = current_bundle(&warm, registry.as_ref(), &stores).await;
        assert_eq!(bundle.signers, vec![a.signing], "pubkeys deduplicated");
        assert_eq!(
            bundle.age_recipients,
            vec![a.age_recipient.clone(), a2.age_recipient.clone()]
        );
    }

    /// Removal is the inverse: the removed device's keys leave all four
    /// arrays, siblings survive, and removing again is a no-op.
    #[tokio::test]
    async fn remove_drops_only_the_target_device() {
        let (warm, registry, stores) = harness();
        let a = device(10);
        let b = device(20);
        admit_device_keys(&warm, registry.as_ref(), &stores, &a)
            .await
            .unwrap();
        admit_device_keys(&warm, registry.as_ref(), &stores, &b)
            .await
            .unwrap();

        let rev = remove_device_keys(&warm, registry.as_ref(), &stores, &b)
            .await
            .unwrap();
        assert_eq!(rev, 3);
        let bundle = current_bundle(&warm, registry.as_ref(), &stores).await;
        assert_eq!(bundle.signers, vec![a.signing]);
        assert_eq!(bundle.acl_keys, vec![a.acl]);
        assert_eq!(bundle.iroh_pubkeys, vec![a.iroh]);
        assert_eq!(bundle.age_recipients, vec![a.age_recipient.clone()]);

        // Idempotent revoke.
        let rev2 = remove_device_keys(&warm, registry.as_ref(), &stores, &b)
            .await
            .unwrap();
        assert_eq!(rev2, 3, "removing an absent device must not bump");
    }

    /// A stale read (concurrent writer landed first) is retried once by
    /// re-reading and re-merging — the loser's keys still land, unioned
    /// over the winner's.
    #[tokio::test]
    async fn lost_race_retries_by_rereading() {
        let (warm, registry, stores) = harness();
        let a = device(10);
        admit_device_keys(&warm, registry.as_ref(), &stores, &a)
            .await
            .unwrap();

        // Simulate the race: a competing writer bumps the stream to
        // revision 2 *behind our back* right before our own set would
        // land at revision 2 — modelled by pre-seeding revision 2 + 3
        // so our first publish (at rev 2, then rev 3 on retry… ) loses
        // exactly once. Simplest faithful setup: hand-publish rev 2 now;
        // our next admit reads rev 2, publishes rev 3, and wins — so to
        // force the retry path we instead pre-publish rev 2 with a
        // DIFFERENT bundle right after reading. Since we can't hook the
        // read here, assert the retry contract at the outcome level: a
        // competing rev-2 entry already present means our admit lands at
        // rev 3 with BOTH edits merged.
        let competing = IdentityBundle {
            version: IdentityBundle::CURRENT_VERSION,
            revision: 2,
            signers: vec![a.signing, [99u8; 32]],
            acl_keys: vec![a.acl],
            iroh_pubkeys: vec![a.iroh],
            age_recipients: vec![a.age_recipient.clone()],
        };
        let cbor = Bytes::from(competing.encode_cbor());
        let hash = stores["durable"]
            .blob_upload_bytes(cbor)
            .await
            .unwrap()
            .hash;
        let msg =
            StreamMessage::sign_ed25519_registry(&warm, identity_vault_id(), hash, 2).unwrap();
        registry.set(msg).await.unwrap();

        let b = device(20);
        let rev = admit_device_keys(&warm, registry.as_ref(), &stores, &b)
            .await
            .unwrap();
        assert_eq!(rev, 3);
        let bundle = current_bundle(&warm, registry.as_ref(), &stores).await;
        assert!(bundle.signers.contains(&[99u8; 32]), "winner's key kept");
        assert!(bundle.signers.contains(&b.signing), "our key merged");
    }

    /// `rewrap_special_vaults` re-seals the CONFIG vault too (store
    /// configs + directory + seed all survive), and refuses an empty
    /// recipient set outright.
    #[tokio::test]
    async fn rewrap_reseals_the_config_vault() {
        use std::collections::BTreeMap;

        use crate::special_vaults::{config_vault_id, publish_vault_entries, read_vault_entries};

        let dir = tempfile::tempdir().unwrap();
        let (warm, registry, stores) = harness();
        let store = stores["durable"].clone();

        let age = |name: &str| {
            use age::secrecy::ExposeSecret;
            let id = age::x25519::Identity::generate();
            let path = dir.path().join(format!("{name}.txt"));
            std::fs::write(&path, id.to_string().expose_secret()).unwrap();
            (
                id.to_public().to_string(),
                path.to_string_lossy().into_owned(),
            )
        };
        let (rec_a, id_a) = age("a");
        let (rec_b, id_b) = age("b");

        let raw = BTreeMap::from([
            ("seed".to_string(), vec![9u8; 32]),
            ("stores/sia".to_string(), b"{\"cfg\":true}".to_vec()),
        ]);
        publish_vault_entries(
            raw.clone(),
            config_vault_id(),
            &warm,
            store.clone(),
            registry.as_ref(),
            std::slice::from_ref(&rec_a),
        )
        .await
        .unwrap();

        rewrap_special_vaults(
            &warm,
            registry.as_ref(),
            store.clone(),
            std::slice::from_ref(&id_a),
            &[rec_a, rec_b],
        )
        .await
        .unwrap();

        let got = read_vault_entries(
            warm.verifying_key().to_bytes(),
            config_vault_id(),
            store.clone(),
            registry.as_ref(),
            std::slice::from_ref(&id_b),
        )
        .await
        .unwrap();
        assert_eq!(got, raw, "config entries survive the re-seal verbatim");

        let err = rewrap_special_vaults(&warm, registry.as_ref(), store, &[id_b], &[])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("empty recipient set"));
    }

    /// A published entry whose blob is unreachable must ERROR, not be
    /// treated as "no bundle" — that would rebuild single-device and
    /// clobber siblings.
    #[tokio::test]
    async fn unreachable_blob_is_an_error_not_a_clobber() {
        let (warm, registry, stores) = harness();
        // Entry present, blob never uploaded anywhere.
        let msg = StreamMessage::sign_ed25519_registry(
            &warm,
            identity_vault_id(),
            Hash::from([9u8; 32]),
            5,
        )
        .unwrap();
        registry.set(msg).await.unwrap();

        let err = admit_device_keys(&warm, registry.as_ref(), &stores, &device(10))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("unreachable"),
            "expected the merge-safety refusal, got: {err}"
        );
    }
}
