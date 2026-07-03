//! `vup device …` — this identity's own devices (D10/D16 noun-scope).
//!
//! - `invite [--label <name>]` — inviter side; asks the running daemon
//!   to mint a one-time `vupd-…` code and blocks until the new device
//!   redeems it (the daemon then admits its keys, writes the catalogue
//!   entry, and re-wraps the special vaults).
//! - `join <code>` — the NEW device; daemon-less like `onboard` /
//!   `recover` (it *creates* this machine's config). Generates the four
//!   device keys, dials the inviter over `s5/enroll/0`, then runs the
//!   same bootstrap tail paper recovery runs — minus the phrase.
//! - `ls` — the device catalogue (labels are UI-only, never
//!   authorization).
//! - `revoke @<label>` — D18: the daemon resolves the label via the
//!   catalogue, drops the device's four keys from the bundle, removes
//!   the catalogue entry, and re-wraps the special vaults to the
//!   survivors; the CLI then prints the honest compromised-case
//!   checklist (removal alone is not sufficient for compromise).

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use s5_node_api::{DeviceInviteEvent, S5NodeClient};

use super::device_bootstrap::{
    FinishArgs, durable_handles, finish_device_bootstrap, generate_device_key_files, scaffold,
};
use super::onboard::store_choice_from_synced;

/// `vup device invite [--label <name>]`.
pub async fn run_invite(client: &S5NodeClient, label: Option<String>) -> Result<()> {
    // Label: flag, else a prompt with an empty default (Enter / --yes →
    // auto-name from the joiner's signing pubkey, daemon-side).
    let label = match label {
        Some(l) => Some(validate_label(l)?),
        None => {
            let input = crate::interact::input_with_default(
                "Label for the new device (empty = auto)",
                String::new(),
            )?;
            let trimmed = input.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(validate_label(trimmed)?)
            }
        }
    };

    let mut rx = client.device_invite(label).await?;
    let token = match rx.recv().await {
        Ok(Some(DeviceInviteEvent::Minted { token })) => token,
        Ok(Some(DeviceInviteEvent::Failed { error })) => bail!("device invite: {error}"),
        Ok(Some(other)) => bail!("device invite: unexpected first event {other:?}"),
        Ok(None) => bail!("device invite: daemon closed stream before minting"),
        Err(e) => return Err(anyhow!("device invite: stream error: {e}")),
    };
    println!();
    println!("On the new device, run:");
    println!();
    println!("    vup device join {token}");
    println!();
    println!("Waiting for the new device to join (code valid ~30 min)…");
    match rx.recv().await {
        Ok(Some(DeviceInviteEvent::Admitted { label, signing_hex })) => {
            println!(
                "✓ Device enrolled as '{label}' (signing key {}…)",
                &signing_hex[..8]
            );
            println!("  Its keys are in the identity bundle; the special vaults are");
            println!("  re-wrapped to include it. See `vup device ls`.");
            Ok(())
        }
        Ok(Some(DeviceInviteEvent::Failed { error })) => bail!("device invite: {error}"),
        Ok(Some(other)) => bail!("device invite: unexpected event {other:?}"),
        Ok(None) => bail!("device invite: stream closed before the device joined"),
        Err(e) => Err(anyhow!("device invite: stream error: {e}")),
    }
}

/// `vup device ls`.
pub async fn run_ls(client: &S5NodeClient) -> Result<()> {
    let resp = client.list_devices().await?;
    if resp.devices.is_empty() {
        println!("No devices in the catalogue yet.");
        println!("(Enroll one with `vup device invite` → `vup device join <code>`.)");
        return Ok(());
    }
    println!("Devices of this identity (labels are informational only):");
    for d in &resp.devices {
        println!(
            "  {label:<20} signing {sig}…  iroh {iroh}…  {age}",
            label = d.label,
            sig = &d.signing_hex[..8.min(d.signing_hex.len())],
            iroh = &d.iroh_hex[..8.min(d.iroh_hex.len())],
            age = d.age_recipient,
        );
    }
    Ok(())
}

/// `vup device revoke @<label>` (D18). The daemon performs the routine
/// `identity-rotation.md` §6.1 removal; this prints what happened and —
/// honestly — what removal does NOT cover for a compromised device.
pub async fn run_revoke(client: &S5NodeClient, id: &str) -> Result<()> {
    let label = id.strip_prefix('@').unwrap_or(id);
    if label.is_empty() {
        bail!("revoke target must be a device label (e.g. `@old-phone`), got '{id}'");
    }
    let resp = client.revoke_device(label).await?;
    println!(
        "revoked device '@{label}' (signing {}…).",
        &resp.signing_hex[..8.min(resp.signing_hex.len())]
    );
    println!();
    println!("Routine removal is done:");
    println!(
        "  - its four keys are out of the identity bundle (revision {}):",
        resp.bundle_revision
    );
    println!("    honest nodes stop serving it blobs and reject its registry writes,");
    println!("  - its catalogue entry is removed,");
    println!("  - identity_secrets + config are re-wrapped to the surviving");
    println!("    devices + paper, so it cannot open any NEW vault roots.");
    println!();
    println!("If the device was merely lost, wiped, or retired: you are done.");
    println!();
    println!("If the device may be COMPROMISED, removal alone is NOT enough — it");
    println!("still holds secrets that no removal can take back:");
    println!("  1. The WARM master key (from the identity_secrets escrow it could");
    println!("     read). Rotate the warm master from your paper phrase. The");
    println!("     ceremony is not built into vup yet — follow");
    println!("     docs/reference/identity-rotation.md §6.2 manually.");
    println!("  2. The store credentials from the stores/config vault (e.g. the");
    println!("     indexd AppKey). Credentials outlive key revocation — no key");
    println!("     rotation touches them. Rotate the app keys at each storage");
    println!("     provider and update the config.");
    println!();
    println!("(Content the device already fetched stays readable — content keys");
    println!(" are not rotated; the accepted restic-comparable model, see");
    println!(" docs/reference/acl-and-revocation.md.)");
    Ok(())
}

/// `vup device join <code>` — daemon-less; creates this machine's
/// config, exactly like `vup recover` minus the phrase (D10).
pub async fn run_join(config_path: &Path, code: &str) -> Result<()> {
    if config_path.exists() {
        bail!(
            "Config already exists at {}.\n\
             `vup device join` sets up a FRESH device — remove the config first \
             if you really want to re-enroll this machine.",
            config_path.display()
        );
    }
    let token = s5_node::enroll::EnrollToken::decode(code)?;
    let dirs = scaffold(config_path)?;

    // -- This device's four random keys (mnemonic-derivation.md §6) ----------
    // Main age key + node secret (shared helper), plus the device keyset
    // written where the daemon will look for it (sibling of the node
    // secret), so the keys we enroll are exactly the keys the daemon boots
    // with.
    let key_files = generate_device_key_files(&dirs.keys_dir)?;
    let keyset_path = dirs.keys_dir.join("device_keyset.cbor.age");
    let key_map = BTreeMap::from([(
        "main".to_string(),
        s5_node_api::config::NodeConfigKey {
            public_key: key_files.main_public.clone(),
            identity_file: Some(key_files.identity_path.to_string_lossy().into_owned()),
        },
    )]);
    let keyset =
        s5_node::device_keyset::load_or_generate_device_keyset(&keyset_path, &key_map, None)
            .context("generating the device keyset")?;
    let device_keys = s5_node::admission::DeviceKeys {
        signing: keyset.device_signing_key().verifying_key().to_bytes(),
        acl: keyset.device_acl_key().verifying_key().to_bytes(),
        iroh: ed25519_dalek::SigningKey::from_bytes(&keyset.iroh)
            .verifying_key()
            .to_bytes(),
        age_recipient: key_files.main_public.clone(),
    };

    // -- Redeem the code: the inviter admits us + re-wraps, then hands back
    // its (self-certifying) anchor entry and the bootstrap-store grant.
    println!("Contacting the inviting device…");
    let accept = s5_node::enroll::join_as_new_device(&keyset, &token, &device_keys)
        .await
        .context("redeeming the enroll code")?;
    let did = s5_core::Did::from_pubkey(s5_core::identity::DidMasterPubkey::new(token.did_pubkey));
    println!("✓ Enrolled into {did}");

    // Persist the verified anchor entry for the daemon (public data; the
    // cold signature carries the authority).
    let anchor_entry_path = dirs.keys_dir.join("identity_anchor.entry");
    fs::write(&anchor_entry_path, accept.anchor_entry.serialize())
        .with_context(|| format!("writing anchor entry: {}", anchor_entry_path.display()))?;

    // -- Open the durable store from the grant --------------------------------
    let store_choice = store_choice_from_synced(&accept.grant.store, &dirs.store_path)?;
    fs::create_dir_all(store_choice.local_path())?;
    let created = s5_node::create_raw_store(store_choice.to_node_config_store(), &HashMap::new())
        .await
        .context("opening the bootstrap store from the enroll grant")?;
    let (blob_store, registry) = durable_handles(created)?;
    // Seed the anchor into the durable registry (idempotent — the daemon
    // republishes it at startup anyway).
    let _ = registry.set(accept.anchor_entry.clone()).await;

    // -- Read the warm seed from identity_secrets with OUR OWN age key --------
    // (the inviter re-wrapped the escrow to include us before acking; this
    // is the same read path recovery runs with the paper key).
    let reader_files = vec![key_files.identity_path.to_string_lossy().into_owned()];
    let secrets = s5_node::special_vaults::read_vault_entries(
        accept.warm_pub,
        s5_node::special_vaults::identity_secrets_vault_id(),
        blob_store.clone(),
        registry.as_ref(),
        &reader_files,
    )
    .await
    .context("reading the identity_secrets vault with this device's key")?;
    let warm_seed: [u8; 32] = secrets
        .get(s5_node::identity_secrets_vault::MASTER_KEY)
        .and_then(|v| v.as_slice().try_into().ok())
        .ok_or_else(|| {
            anyhow!(
                "no warm-master escrow found in the identity_secrets vault — \
                 the inviter's re-wrap did not reach the durable store"
            )
        })?;
    // Sanity: the escrowed seed must be the warm key the anchor names.
    let warm_check = ed25519_dalek::SigningKey::from_bytes(&warm_seed)
        .verifying_key()
        .to_bytes();
    if warm_check != accept.warm_pub {
        bail!("escrowed warm seed does not match the anchor's warm pubkey — refusing");
    }

    let recovery_public = accept.grant.recovery_recipient.clone().ok_or_else(|| {
        anyhow!(
            "the inviter advertised no paper recovery key ([key.recovery]) — \
             re-onboard the identity with a recovery phrase before adding devices"
        )
    })?;

    // -- Shared tail: bootstrap walk + vault roots + config.toml -------------
    finish_device_bootstrap(FinishArgs {
        config_path,
        dirs: &dirs,
        key_files: &key_files,
        anchor_entry_path: &anchor_entry_path,
        store_choice: &store_choice,
        blob_store,
        registry,
        warm_seed,
        // Our own key opens the config vault (re-wrapped for us); vault
        // roots published BEFORE our enrollment may defer to the next snap.
        reader_identity_files: reader_files,
        recovery_public,
        did,
        headline: "Device enrolled",
    })
    .await
}

/// Same label rules as vault names — catalogue labels are UI strings,
/// keep them shell- and display-safe.
fn validate_label(label: String) -> Result<String> {
    s5_node::validate_share_label(&label).map_err(|reason| anyhow!("invalid label: {reason}"))?;
    Ok(label)
}
