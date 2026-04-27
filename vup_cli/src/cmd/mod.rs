//! `vup` command surface.
//!
//! - `onboard` is the bootstrap wizard (the only verb besides `_daemon`
//!   that runs without a daemon connection — it creates the config the
//!   daemon needs to start).
//! - `lifecycle` holds top-level vault lifecycle verbs (`ls`, `new`, `drop`).
//! - `vault` holds vault-scoped verbs (`snap`, `history`, `restore`,
//!   `add`, `info`) — everything reachable via `vup +<vault> <verb>`.
//! - `tasks` holds the RPC-client helpers, including the shared
//!   `poll_until_done` loop that vault verbs submitting long-running
//!   tasks call into.
//! - `stubs` holds the verbs that need infrastructure not yet built
//!   (`mount`, `share`/`export`/`join`, `grant`/`pair`/`kick`/`who`,
//!   `peers`/`unpair`, `store …`, and `snap --watch`).
//!
//! Utility verbs (`status`, `config`, `shutdown`) live directly in this
//! module.

pub mod lifecycle;
pub mod onboard;
pub mod stubs;
pub mod tasks;
pub mod vault;

use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::Subcommand;
use s5_node_api::S5NodeClient;

// ---------------------------------------------------------------------------
// Vault-scoped action enum (dispatched after `+vault` rewrite)
// ---------------------------------------------------------------------------

/// Verbs that operate on a single vault. Each is reachable via
/// `vup +<vault> <verb>` (which the sigil module rewrites to
/// `vup vault <name> <verb>`).
///
/// First-letter aliases (`s`, `i`, `a`, …) mirror the canonical name.
#[derive(Subcommand, Debug)]
pub enum VaultAction {
    /// Attach paths to the vault for snap to pick up.
    #[command(alias = "a")]
    Add {
        /// One or more paths.
        paths: Vec<PathBuf>,
    },
    /// Show vault details.
    #[command(alias = "i")]
    Info,
    /// Take a snapshot and publish it to the vault's meta_targets.
    #[command(alias = "s")]
    Snap {
        /// Continuously watch the vault's sources and snap on change.
        #[arg(long)]
        watch: bool,
    },
    /// List snapshots in this vault.
    #[command(alias = "h")]
    History,
    /// Restore from a snapshot (defaults to the latest).
    #[command(alias = "r")]
    Restore {
        /// Snapshot id to restore. Omit for the latest snapshot.
        #[arg(long)]
        snap: Option<String>,
    },
    /// FUSE-mount this vault read-only at a local path.
    #[command(alias = "m")]
    Mount {
        /// Mount point.
        path: PathBuf,
    },
    /// Grant a peer ongoing read or write access to the vault.
    #[command(alias = "g")]
    Grant {
        /// Identity to grant (e.g. `@alice`).
        id: String,
        /// Read access (default).
        #[arg(long, short)]
        read: bool,
        /// Write access — interactive confirm required on a TTY.
        #[arg(long, short)]
        write: bool,
    },
    /// Pair an own-device for bidirectional sync.
    #[command(alias = "p")]
    Pair {
        /// Identity to pair with (e.g. `@laptop`).
        id: String,
    },
    /// Generate an anonymous frozen-snapshot share URL.
    #[command(alias = "e")]
    Export {
        /// Re-encrypt only this subtree under fresh keys.
        #[arg(long)]
        path: Option<String>,
    },
    /// List vault members and their capabilities.
    #[command(alias = "w")]
    Who,
    /// Revoke a member from future snapshots.
    #[command(alias = "k")]
    Kick {
        /// Identity to kick (e.g. `@alice`).
        id: String,
    },
}

// ---------------------------------------------------------------------------
// Store sub-namespace
// ---------------------------------------------------------------------------

/// Verbs under `vup store …`. Stores are configured rarely and referenced
/// by name in vault `--target` flags — they never appear as the subject of
/// an interactive command.
#[derive(Subcommand, Debug)]
pub enum StoreCmd {
    /// Add a new store.
    Add {
        /// Store name.
        name: String,
    },
    /// List configured stores.
    Ls,
    /// Show store details.
    Info {
        /// Store name.
        name: String,
    },
    /// Remove a store.
    Rm {
        /// Store name.
        name: String,
    },
    /// Authorise an identity to push to this store.
    Allow {
        /// Store name.
        name: String,
        /// Identity (e.g. `@alice`).
        id: String,
    },
    /// Revoke an identity's push access to this store.
    Disallow {
        /// Store name.
        name: String,
        /// Identity (e.g. `@alice`).
        id: String,
    },
}

// ---------------------------------------------------------------------------
// Utility verbs (kept working through the grammar rewrite)
// ---------------------------------------------------------------------------

/// Shut down the running node.
pub async fn run_shutdown(client: &S5NodeClient) -> Result<()> {
    client.shutdown().await?;
    println!("Service stopped.");
    Ok(())
}

/// `vup status` — show node status summary.
pub async fn run_status(client: &S5NodeClient) -> Result<()> {
    let resp = client.get_status().await?;

    println!("S5 Node Status");
    println!("  Endpoint:     {}", resp.endpoint_id);
    println!("  Stores:       {}", resp.store_count);
    println!("  Vaults:       {}", resp.vault_count);
    println!("  Sources:      {}", resp.source_count);
    println!("  Active tasks: {}", resp.running_tasks);

    // Also show configured sources from config
    let config_resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&config_resp.config_json)?;
    if let Some(obj) = config
        .get("source")
        .and_then(|s| s.as_object())
        .filter(|o| !o.is_empty())
    {
        println!("\nSources:");
        for (name, source) in obj {
            if let Some(paths) = source.get("paths").and_then(|p| p.as_array()) {
                let path_strs: Vec<&str> = paths.iter().filter_map(|p| p.as_str()).collect();
                println!("  {}: {}", name, path_strs.join(", "));
            }
        }
    }

    if let Some(obj) = config
        .get("vault")
        .and_then(|v| v.as_object())
        .filter(|o| !o.is_empty())
    {
        println!("\nVaults:");
        for (name, vault) in obj {
            let stores = vault
                .get("blob_stores")
                .and_then(|s| s.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            println!("  {}: stores=[{}]", name, stores);
        }
    }

    Ok(())
}

/// `vup config` — JSON read/patch + interactive wizard.
pub async fn run_config(
    client: &S5NodeClient,
    json: bool,
    patch: Option<String>,
    patch_file: Option<PathBuf>,
) -> Result<()> {
    // --patch: apply and exit
    if let Some(patch_str) = patch {
        let patch_val: serde_json::Value = serde_json::from_str(&patch_str)
            .map_err(|e| anyhow::anyhow!("invalid JSON patch: {}", e))?;
        let resp = client.patch_config(patch_val).await?;
        if resp.ok {
            println!("Config updated.");
            if let (true, Some(config_json)) = (json, &resp.config_json) {
                let config: serde_json::Value = serde_json::from_str(config_json)?;
                println!("{}", serde_json::to_string_pretty(&config)?);
            }
        } else {
            bail!("patch failed: {}", resp.message);
        }
        return Ok(());
    }

    // --patch-file: apply and exit
    if let Some(path) = patch_file {
        let content = tokio::fs::read_to_string(&path).await?;
        let patch_val: serde_json::Value = serde_json::from_str(&content)
            .map_err(|e| anyhow::anyhow!("invalid JSON in {}: {}", path.display(), e))?;
        let resp = client.patch_config(patch_val).await?;
        if resp.ok {
            println!("Config updated from {}.", path.display());
            if let (true, Some(config_json)) = (json, &resp.config_json) {
                let config: serde_json::Value = serde_json::from_str(config_json)?;
                println!("{}", serde_json::to_string_pretty(&config)?);
            }
        } else {
            bail!("patch failed: {}", resp.message);
        }
        return Ok(());
    }

    // --json: dump config and exit
    if json {
        let resp = client.get_config().await?;
        let config: serde_json::Value = serde_json::from_str(&resp.config_json)?;
        println!("{}", serde_json::to_string_pretty(&config)?);
        return Ok(());
    }

    // Default: interactive wizard
    run_config_wizard(client).await
}

/// Interactive configuration wizard.
async fn run_config_wizard(client: &S5NodeClient) -> Result<()> {
    use dialoguer::{Confirm, Select};

    loop {
        let config_resp = client.get_config().await?;
        let config: serde_json::Value = serde_json::from_str(&config_resp.config_json)?;

        // Show current state summary
        print_config_summary(&config);
        println!();

        let choices = &[
            "Generate recovery key",
            "Show current config as JSON",
            "Done",
        ];
        let selection = Select::new()
            .with_prompt("What would you like to configure?")
            .items(choices)
            .default(0)
            .interact()?;

        match selection {
            0 => wizard_recovery_key(client, &config).await?,
            1 => {
                println!("{}", serde_json::to_string_pretty(&config)?);
                println!();
            }
            2 => break,
            _ => unreachable!(),
        }

        if !Confirm::new()
            .with_prompt("Configure something else?")
            .default(false)
            .interact()?
        {
            break;
        }
    }

    Ok(())
}

/// Print a compact config summary for the wizard header.
fn print_config_summary(config: &serde_json::Value) {
    println!("Current Configuration");
    println!("─────────────────────");

    if let Some(keys) = config.get("key").and_then(|v| v.as_object()) {
        if !keys.is_empty() {
            let names: Vec<&String> = keys.keys().collect();
            println!(
                "  Keys:    {}",
                names
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        } else {
            println!("  Keys:    (none)");
        }
    } else {
        println!("  Keys:    (none)");
    }

    if let Some(stores) = config.get("store").and_then(|v| v.as_object()) {
        if !stores.is_empty() {
            for (name, store) in stores {
                let stype = store.get("type").and_then(|v| v.as_str()).unwrap_or("?");
                println!("  Store:   {} ({})", name, stype);
            }
        } else {
            println!("  Stores:  (none)");
        }
    } else {
        println!("  Stores:  (none)");
    }

    if let Some(sources) = config
        .get("source")
        .and_then(|v| v.as_object())
        .filter(|o| !o.is_empty())
    {
        for (name, source) in sources {
            let count = source
                .get("paths")
                .and_then(|p| p.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            println!("  Source:  {} ({} paths)", name, count);
        }
    }

    if let Some(vaults) = config
        .get("vault")
        .and_then(|v| v.as_object())
        .filter(|o| !o.is_empty())
    {
        for (name, vault) in vaults {
            let key = vault.get("key").and_then(|v| v.as_str()).unwrap_or("?");
            println!("  Vault:   {} (key={})", name, key);
        }
    }
}

/// Wizard: generate a recovery key phrase and add it to config.
async fn wizard_recovery_key(client: &S5NodeClient, config: &serde_json::Value) -> Result<()> {
    use dialoguer::Confirm;

    let has_recovery = config.get("key").and_then(|k| k.get("recovery")).is_some();

    if has_recovery {
        println!("\n⚠  A recovery key is already configured.");
        if !Confirm::new()
            .with_prompt("Generate a NEW recovery key? (the old one will be replaced)")
            .default(false)
            .interact()?
        {
            return Ok(());
        }
    }

    println!();
    println!("Generating a new age recovery key...");
    println!("This key can restore access to your encrypted vaults.");
    println!();

    let (pubkey, secret_key) = crate::recovery::generate_recovery_key();

    println!("┌─────────────────────────────────────────────────────────────────────────┐");
    println!("│  WRITE DOWN THIS KEY — it is your only way to recover your data         │");
    println!("│  if you lose this device.                                               │");
    println!("│                                                                         │");
    println!("│  {:<69}  │", &secret_key);
    println!("│                                                                         │");
    println!("│  Store this key OFFLINE in a safe place.                                │");
    println!("│  Anyone with this key can decrypt your backups.                         │");
    println!("│                                                                         │");
    println!("│  Tip: Bech32 encoding avoids ambiguous characters (0/O, l/1)            │");
    println!("│  and includes a checksum to catch typos.                                │");
    println!("└─────────────────────────────────────────────────────────────────────────┘");
    println!();
    println!("Derived age public key: {}", pubkey);
    println!();

    if !Confirm::new()
        .with_prompt("I have written down the recovery key")
        .default(false)
        .interact()?
    {
        println!("Aborted — no changes made.");
        return Ok(());
    }

    let ops = vec![serde_json::json!({
        "op": if has_recovery { "replace" } else { "add" },
        "path": "/key/recovery",
        "value": {
            "public_key": pubkey,
        }
    })];

    let resp = client.patch_config(serde_json::Value::Array(ops)).await?;
    if resp.ok {
        println!("Recovery key added to config.");
    } else {
        bail!("failed to save recovery key: {}", resp.message);
    }

    Ok(())
}
