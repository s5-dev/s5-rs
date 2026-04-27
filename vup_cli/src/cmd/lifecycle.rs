//! Top-level vault lifecycle verbs: `ls`, `new`, `drop`.
//!
//! These do their work via JSON Patches against the daemon's config
//! (preserving the rule that the CLI is an RPC frontend — the daemon
//! holds the live config and applies the mutations).

use anyhow::{Result, bail};
use s5_node_api::S5NodeClient;

use crate::sigil::RESERVED_VAULT_NAMES;

/// `vup ls` — list configured vaults, one per line.
pub async fn run_ls(client: &S5NodeClient) -> Result<()> {
    let config_resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&config_resp.config_json)?;

    let vaults = config
        .get("vault")
        .and_then(|v| v.as_object())
        .filter(|o| !o.is_empty());

    let Some(obj) = vaults else {
        println!("(no vaults configured — run `vup new +<name>` to create one)");
        return Ok(());
    };

    for (name, vault) in obj {
        let recipients = vault
            .get("recipients")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        let stores = vault
            .get("blob_stores")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        let publishes = vault
            .get("meta_targets")
            .and_then(|v| v.as_array())
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        let publish_marker = if publishes { "*" } else { " " };
        println!(
            "{} +{name}  recipients={recipients}  blob_stores={stores}",
            publish_marker
        );
    }
    if obj.values().any(|v| {
        v.get("meta_targets")
            .and_then(|m| m.as_array())
            .map(|a| !a.is_empty())
            .unwrap_or(false)
    }) {
        println!();
        println!("(* = publishes via meta_targets)");
    }

    Ok(())
}

/// `vup new +<vault>` — create a vault with sane defaults.
///
/// Defaults: `key = "main"`, `recipients = ["main"]`, empty
/// sources/blob_stores/meta_targets. The user fills the rest in via
/// `vup config` or `vup +<vault> add`.
pub async fn run_new(client: &S5NodeClient, vault: &str) -> Result<()> {
    if RESERVED_VAULT_NAMES.contains(&vault) || vault == "all" {
        bail!("'{vault}' is a reserved name; pick another");
    }

    let config_resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&config_resp.config_json)?;
    if config.get("vault").and_then(|v| v.get(vault)).is_some() {
        bail!("vault '{vault}' already exists");
    }

    // Default key — error helpfully if the user has no [key.main] yet
    // (which is what `vup onboard` normally creates).
    let default_key = config
        .get("key")
        .and_then(|k| k.as_object())
        .and_then(|m| {
            if m.contains_key("main") {
                Some("main")
            } else {
                m.keys().next().map(String::as_str)
            }
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no [key.*] entries in config; run `vup onboard` first or add a key manually"
            )
        })?;

    let mut recipients = vec![default_key.to_string()];
    if config.get("key").and_then(|k| k.get("recovery")).is_some() {
        recipients.push("recovery".to_string());
    }

    // Vault root_path defaults under the user's data dir; the daemon
    // creates it on first snap.
    let dirs = directories::ProjectDirs::from("pro", "s5", "s5")
        .ok_or_else(|| anyhow::anyhow!("could not determine application data directory"))?;
    let vault_root = dirs
        .data_dir()
        .join("vaults")
        .join(vault)
        .display()
        .to_string();

    let patch = serde_json::json!([{
        "op": "add",
        "path": format!("/vault/{vault}"),
        "value": {
            "root_path": vault_root,
            "key": default_key,
            "recipients": recipients,
            "sources": [],
            "blob_stores": [],
            "meta_targets": [],
        }
    }]);

    let resp = client.patch_config(patch).await?;
    if resp.ok {
        println!("+{vault} created. Next:");
        println!("  vup +{vault} add <path>          attach paths");
        println!("  vup config --json                review the full config");
    } else {
        bail!("config update failed: {}", resp.message);
    }
    Ok(())
}

/// `vup drop +<vault>` — remove a vault config entry. The on-disk
/// vault data (root_path/blob stores) is **not** touched — only the
/// config knob is dropped, so re-adding the vault later picks up the
/// existing on-disk state.
pub async fn run_drop(client: &S5NodeClient, vault: &str) -> Result<()> {
    let config_resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&config_resp.config_json)?;
    if config.get("vault").and_then(|v| v.get(vault)).is_none() {
        bail!("vault '{vault}' not found in config");
    }

    let patch = serde_json::json!([{
        "op": "remove",
        "path": format!("/vault/{vault}"),
    }]);

    let resp = client.patch_config(patch).await?;
    if resp.ok {
        println!("+{vault} dropped from config (on-disk data not touched).");
    } else {
        bail!("config update failed: {}", resp.message);
    }
    Ok(())
}
