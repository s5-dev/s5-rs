//! `vup` command surface.
//!
//! - `onboard` is the bootstrap wizard (the only verb besides `_daemon`
//!   that runs without a daemon connection — it creates the config the
//!   daemon needs to start).
//! - `lifecycle` holds the vault list + lifecycle verbs (`list`,
//!   `vault create`, `vault drop`, `vault rename`).
//! - `vault` holds the vault-scoped verb bodies (`restore`, `history`,
//!   `mount`, `info`) plus the `backup` source-mapping helpers that the
//!   top-level D20 data verbs dispatch into.
//! - `tasks` holds the RPC-client helpers, including the shared
//!   `poll_until_done` loop that vault verbs submitting long-running
//!   tasks call into.
//! - `store` / `store_config` hold the storage-backend namespace
//!   (`store add/ls/info/rm`) and the shared backend
//!   collectors `onboard`/`recover` also use.
//! - `stubs` holds the real `join` / `friend pair` / `grant` bodies that
//!   grew there before earning their own module.
//! - `membership` holds `who` / `revoke` / `friend list` /
//!   `friend forget` (config read/patch over the daemon).
//!
//! Utility verbs (`status`, `config`, `shutdown`) live directly in this
//! module.

pub mod automate;
pub mod backup;
pub mod copy;
pub mod device;
pub mod device_bootstrap;
pub mod doctor;
pub mod lifecycle;
pub mod membership;
pub mod onboard;
pub mod recover;
pub mod service;
pub mod share;
pub mod store;
pub mod store_config;
pub mod stubs;
pub mod tasks;
pub mod vault;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Subcommand;
use s5_node_api::S5NodeClient;

// ---------------------------------------------------------------------------
// Vault management namespace (`vup vault …`)
// ---------------------------------------------------------------------------

/// Infrequent, wizard-friendly vault management. Frequent *data* verbs
/// (`backup`, `restore`, `list`, `history`, `mount`, `share`) are
/// top-level and take D20 refs; lifecycle lives here (D20 §B2).
#[derive(Subcommand, Debug)]
pub enum VaultCmd {
    /// Create a new vault.
    Create {
        /// Vault name (e.g. `docs`).
        name: String,
    },
    /// Delete a vault config (does not destroy stored data).
    Drop {
        /// Vault name.
        name: String,
    },
    /// Rename a vault config entry (on-disk data untouched).
    Rename {
        /// Current vault name.
        old: String,
        /// New vault name.
        new: String,
    },
}

// ---------------------------------------------------------------------------
// Friend sub-namespace (D16: other identities, each with their own DID)
// ---------------------------------------------------------------------------

/// Verbs under `vup friend …` — OTHER identities this one has paired
/// with. The D9 noun-scope counterpart of `vup device` (a device shares
/// your identity; a friend has their own DID). Named `friend`, not
/// `peer`: it matches the `[friend.*]` config table by construction,
/// and `peer` stays reserved for the transport layer (iroh peers) —
/// D16. The old top-level `pair`/`peers`/`unpair` remain as hidden
/// aliases through the beta.
#[derive(Subcommand, Debug)]
pub enum FriendCmd {
    /// Pair with another identity. Without arguments: ask the running
    /// daemon to mint a one-time pair token, print it, and block until
    /// the friend redeems it. With a token argument: redeem the
    /// friend's token. In both cases the CLI then interactively
    /// prompts for a petname and saves the friend as `[friend.<name>]`.
    Pair {
        /// Token from the friend's side. Omit to mint a new token and
        /// wait for redemption.
        token: Option<String>,
    },
    /// List paired friends (`[friend.*]`).
    #[command(alias = "ls")]
    List,
    /// Forget a paired friend. Refused while they are still a member
    /// of any vault — `vup revoke <vault>: @<name>` first, so their
    /// access is explicitly revoked rather than silently orphaned.
    Forget {
        /// Friend to forget (e.g. `@alice`).
        id: String,
    },
}

// ---------------------------------------------------------------------------
// Device sub-namespace (D10/D16: your identity's own devices)
// ---------------------------------------------------------------------------

/// Verbs under `vup device …` — enrollment and inventory of THIS
/// identity's devices. Distinct from `vup friend …` (other
/// identities): a device shares your identity; a friend has their own.
#[derive(Subcommand, Debug)]
pub enum DeviceCmd {
    /// Mint a one-time enrollment code for a new device and wait for
    /// it to join. The daemon admits the joiner's keys to the identity
    /// bundle and re-wraps the special vaults for it.
    Invite {
        /// Catalogue label for the new device (informational only).
        /// Omitted: prompted, empty = auto-named.
        #[arg(long)]
        label: Option<String>,
    },
    /// Enroll THIS machine into an existing identity using a code from
    /// `vup device invite` on an already-set-up device. Creates this
    /// machine's config — run it instead of `vup onboard`.
    Join {
        /// `vupd-…` code from the inviting device.
        code: String,
    },
    /// List this identity's enrolled devices (from the catalogue;
    /// labels are informational, never authorization).
    #[command(alias = "ls")]
    List,
    /// Revoke an enrolled device by catalogue label: its keys leave
    /// the identity bundle and the special vaults are re-wrapped to
    /// the surviving devices (+ paper). Prints the compromised-case
    /// checklist — removal alone is NOT sufficient for a compromised
    /// device (D18).
    Revoke {
        /// Device to revoke, by catalogue label (e.g. `@old-phone`;
        /// see `vup device ls`).
        id: String,
    },
}

// ---------------------------------------------------------------------------
// Store sub-namespace
// ---------------------------------------------------------------------------

/// Which physical backend a `store add` provisions. Value order matches
/// the D20 help (`sia|s3|local`); each renders lower-case on the CLI.
#[derive(Copy, Clone, Debug, clap::ValueEnum)]
pub enum StoreBackend {
    /// Sia (decentralized) via an indexd service — one-time OAuth.
    #[value(name = "sia")]
    Sia,
    /// S3-compatible object storage.
    #[value(name = "s3")]
    S3,
    /// A local directory on this machine.
    #[value(name = "local")]
    Local,
}

/// Verbs under `vup store …`. Stores are configured rarely and referenced
/// by name in vault `data_store`/`meta_store` config — they never appear as
/// the subject of an interactive command.
#[derive(Subcommand, Debug)]
pub enum StoreCmd {
    /// Add a new store. Flags supply the backend config non-interactively;
    /// anything omitted is prompted (a TTY is required, or exit 3).
    Add {
        /// Backend: `sia`, `s3`, or `local`.
        backend: StoreBackend,
        /// Store name (referenced by `vault.<>.data_store`).
        name: String,
        /// Local store directory (`local`; default under the data dir).
        #[arg(long)]
        path: Option<PathBuf>,
        /// S3 endpoint URL (`s3`).
        #[arg(long)]
        endpoint: Option<String>,
        /// S3 bucket name (`s3`).
        #[arg(long)]
        bucket: Option<String>,
        /// S3 access key id (`s3`).
        #[arg(long)]
        access_key: Option<String>,
        /// S3 secret access key (`s3`).
        #[arg(long)]
        secret_key: Option<String>,
        /// S3 region (`s3`; default `us-east-1`).
        #[arg(long)]
        region: Option<String>,
        /// Indexer URL (`sia`; default `https://sia.storage`).
        #[arg(long)]
        indexer_url: Option<String>,
    },
    /// List configured stores.
    #[command(alias = "list")]
    Ls,
    /// Show store details (backend config, vaults using it).
    Info {
        /// Store name.
        name: String,
    },
    /// Remove a store (refused while a vault still references it).
    Rm {
        /// Store name.
        name: String,
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
    // Daily-rotated: files are `node.log.YYYY-MM-DD` inside this directory.
    let log_dir = dirs::cache_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join("s5")
        .join("logs");
    println!(
        "  Logs:         {} (node.log.<date>, 7 kept)",
        log_dir.display()
    );

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
        for (name, _vault) in obj {
            let store = vault::resolve_data_store(&config, name)
                .unwrap_or_else(|_| "(unresolved!)".to_string());
            println!("  {name}: store={store}");
        }
    }

    // Durability gauges + next scheduled runs (GetHealth). Best-effort: an
    // older daemon without the RPC just omits the section.
    if let Ok(health) = client.get_health().await {
        let staged: Vec<_> = health
            .stores
            .iter()
            .filter_map(|s| s.staging.as_ref().map(|g| (s.name.as_str(), g)))
            .collect();
        if !staged.is_empty() {
            println!("\nDurability:");
            for (name, g) in staged {
                let flushed = doctor::format_age(g.since_last_flush_secs);
                if g.staged_bytes > 0 {
                    println!(
                        "  {}: {} staged, NOT yet durable (last flush {} ago{})",
                        name,
                        humansize::format_size(g.staged_bytes, humansize::BINARY),
                        flushed,
                        if g.inflight { ", upload in flight" } else { "" },
                    );
                } else {
                    println!("  {name}: durable (staging drained, last flush {flushed} ago)");
                }
            }
        }
        if !health.schedules.is_empty() {
            println!("\nScheduled backups:");
            for run in &health.schedules {
                println!(
                    "  {}: every {}",
                    run.vault,
                    doctor::format_age(run.interval_secs)
                );
            }
        }
    }

    Ok(())
}

/// `vup config [VAULT]` — JSON read/patch + interactive wizard.
///
/// With a `VAULT` argument (and no `--json`/`--patch`) it shows that
/// vault's config block; the full interactive per-vault editor lands in a
/// later stage. `--json`/`--patch`/`--patch-file` stay the escape hatch.
pub async fn run_config(
    client: &S5NodeClient,
    vault: Option<String>,
    json: bool,
    patch: Option<String>,
    patch_file: Option<PathBuf>,
) -> Result<()> {
    // VAULT arg (read-only view for now).
    if let Some(v) = vault
        && patch.is_none()
        && patch_file.is_none()
    {
        let name = crate::refs::strip_plus(v.trim_end_matches(':'));
        if json {
            let resp = client.get_config().await?;
            let config: serde_json::Value = serde_json::from_str(&resp.config_json)?;
            let block = config
                .get("vault")
                .and_then(|m| m.get(&name))
                .ok_or_else(|| anyhow::anyhow!("vault '{name}:' not found in config"))?;
            println!("{}", serde_json::to_string_pretty(block)?);
            return Ok(());
        }
        println!("(interactive per-vault editing lands in a later step; showing details)\n");
        return vault::run_info(client, &name).await;
    }

    // --patch: apply and exit
    if let Some(patch_str) = patch {
        let patch_val: serde_json::Value = serde_json::from_str(&patch_str)
            .map_err(|e| anyhow::anyhow!("invalid JSON patch: {}", e))?;
        let config_json = client.patch_config(patch_val).await?;
        println!("Config updated.");
        if json {
            let config: serde_json::Value = serde_json::from_str(&config_json)?;
            println!("{}", serde_json::to_string_pretty(&config)?);
        }
        return Ok(());
    }

    // --patch-file: apply and exit
    if let Some(path) = patch_file {
        let content = tokio::fs::read_to_string(&path).await?;
        let patch_val: serde_json::Value = serde_json::from_str(&content)
            .map_err(|e| anyhow::anyhow!("invalid JSON in {}: {}", path.display(), e))?;
        let config_json = client.patch_config(patch_val).await?;
        println!("Config updated from {}.", path.display());
        if json {
            let config: serde_json::Value = serde_json::from_str(&config_json)?;
            println!("{}", serde_json::to_string_pretty(&config)?);
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
    use crate::interact;

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
        let selection = interact::select("What would you like to configure?", choices, 0)?;

        match selection {
            0 => wizard_recovery_key(client, &config).await?,
            1 => {
                println!("{}", serde_json::to_string_pretty(&config)?);
                println!();
            }
            2 => break,
            _ => unreachable!(),
        }

        if !interact::confirm("Configure something else?", false)? {
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
    use crate::interact;

    let has_recovery = config.get("key").and_then(|k| k.get("recovery")).is_some();

    if has_recovery {
        println!("\n⚠  A recovery key is already configured.");
        if !interact::confirm(
            "Generate a NEW recovery key? (the old one will be replaced)",
            false,
        )? {
            return Ok(());
        }
    }

    println!();
    println!("Generating a new recovery phrase...");
    println!("These 12 words can restore access to your encrypted vaults.");
    println!();

    let (mnemonic, pubkey) =
        crate::recovery::generate_recovery_phrase().context("generating recovery phrase")?;

    println!("========================================================================");
    println!();
    println!("  RECOVERY PHRASE — write these 12 words down, in order, and keep them");
    println!("  somewhere safe and offline. This is the ONLY way to recover your data");
    println!("  if you lose this device. Anyone with these words can decrypt it.");
    println!();
    println!("  {}", mnemonic);
    println!();
    println!("========================================================================");
    println!();
    println!("Derived recovery age public key: {}", pubkey);
    println!();

    if !interact::confirm("I have written down the recovery key", false)? {
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

    client.patch_config(serde_json::Value::Array(ops)).await?;
    println!("Recovery key added to config.");
    Ok(())
}
