pub mod init;
pub mod tasks;

use std::path::PathBuf;

use anyhow::{bail, Result};
use s5_node_api::S5NodeClient;

use crate::node::ensure_node_running;

/// Shut down the running node.
pub async fn run_shutdown(config_path: &std::path::Path) -> Result<()> {
    let client = ensure_node_running(config_path).await?;
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
    if let Some(sources) = config.get("source") {
        if let Some(obj) = sources.as_object() {
            if !obj.is_empty() {
                println!("\nSources:");
                for (name, source) in obj {
                    if let Some(paths) = source.get("paths").and_then(|p| p.as_array()) {
                        let path_strs: Vec<&str> =
                            paths.iter().filter_map(|p| p.as_str()).collect();
                        println!("  {}: {}", name, path_strs.join(", "));
                    }
                }
            }
        }
    }

    if let Some(vaults) = config.get("vault") {
        if let Some(obj) = vaults.as_object() {
            if !obj.is_empty() {
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
        }
    }

    Ok(())
}

/// `vup add <paths> --source <name>` — add paths to a source via JSON Patch.
pub async fn run_add(client: &S5NodeClient, source: &str, paths: &[PathBuf]) -> Result<()> {
    if paths.is_empty() {
        bail!("no paths specified");
    }

    // Canonicalize paths
    let mut abs_paths = Vec::new();
    for p in paths {
        let abs = std::fs::canonicalize(p)
            .map_err(|e| anyhow::anyhow!("cannot resolve path '{}': {}", p.display(), e))?;
        abs_paths.push(abs.to_string_lossy().to_string());
    }

    // Check if the source already exists
    let config_resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&config_resp.config_json)?;
    let source_exists = config
        .get("source")
        .and_then(|s| s.get(source))
        .is_some();

    let patch = if source_exists {
        // Append paths to existing source
        let mut ops: Vec<serde_json::Value> = Vec::new();
        for path in &abs_paths {
            ops.push(serde_json::json!({
                "op": "add",
                "path": format!("/source/{}/paths/-", source),
                "value": path,
            }));
        }
        serde_json::Value::Array(ops)
    } else {
        // Create new source with these paths
        serde_json::json!([
            {
                "op": "add",
                "path": format!("/source/{}", source),
                "value": {
                    "paths": abs_paths,
                    "exclude": [],
                    "ignore": false,
                }
            }
        ])
    };

    let resp = client.patch_config(patch).await?;
    if resp.ok {
        if source_exists {
            println!(
                "Added {} path(s) to source '{}'.",
                abs_paths.len(),
                source
            );
        } else {
            println!(
                "Created source '{}' with {} path(s).",
                source,
                abs_paths.len()
            );
        }
        for p in &abs_paths {
            println!("  + {}", p);
        }
    } else {
        bail!("failed to update config: {}", resp.message);
    }

    Ok(())
}

/// `vup snapshots [vault]` — list vault snapshots.
pub async fn run_snapshots(client: &S5NodeClient, vault: Option<String>) -> Result<()> {
    let resp = client.list_snapshots(vault.clone()).await?;

    if resp.snapshots.is_empty() {
        match vault {
            Some(v) => println!("No snapshots found for vault '{}'.", v),
            None => println!("No snapshots found."),
        }
        return Ok(());
    }

    println!(
        "{:<12} {:<16} {:<10} {:<12} {}",
        "VAULT", "HASH", "FILES", "SIZE", "DATE"
    );
    for snap in &resp.snapshots {
        let hash_short = if snap.hash.len() > 12 {
            &snap.hash[..12]
        } else {
            &snap.hash
        };
        let files = snap
            .file_count
            .map(|n| n.to_string())
            .unwrap_or_else(|| "-".into());
        let size = snap
            .total_bytes
            .map(|b| humansize::format_size(b, humansize::BINARY))
            .unwrap_or_else(|| "-".into());
        let date = &snap.timestamp;
        println!(
            "{:<12} {:<16} {:<10} {:<12} {}",
            snap.vault, hash_short, files, size, date
        );
    }

    Ok(())
}

/// `vup config` — interactive configuration wizard or JSON patch.
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
            if json {
                if let Some(ref config_json) = resp.config_json {
                    let config: serde_json::Value = serde_json::from_str(config_json)?;
                    println!("{}", serde_json::to_string_pretty(&config)?);
                }
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
            if json {
                if let Some(ref config_json) = resp.config_json {
                    let config: serde_json::Value = serde_json::from_str(config_json)?;
                    println!("{}", serde_json::to_string_pretty(&config)?);
                }
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

    // Keys
    if let Some(keys) = config.get("key").and_then(|v| v.as_object()) {
        if !keys.is_empty() {
            let names: Vec<&String> = keys.keys().collect();
            println!("  Keys:    {}", names.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", "));
        } else {
            println!("  Keys:    (none)");
        }
    } else {
        println!("  Keys:    (none)");
    }

    // Stores
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

    // Sources
    if let Some(sources) = config.get("source").and_then(|v| v.as_object()) {
        if !sources.is_empty() {
            for (name, source) in sources {
                let count = source
                    .get("paths")
                    .and_then(|p| p.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0);
                println!("  Source:  {} ({} paths)", name, count);
            }
        }
    }

    // Vaults
    if let Some(vaults) = config.get("vault").and_then(|v| v.as_object()) {
        if !vaults.is_empty() {
            for (name, vault) in vaults {
                let key = vault.get("key").and_then(|v| v.as_str()).unwrap_or("?");
                println!("  Vault:   {} (key={})", name, key);
            }
        }
    }
}

/// Wizard: generate a recovery key phrase and add it to config.
async fn wizard_recovery_key(
    client: &S5NodeClient,
    config: &serde_json::Value,
) -> Result<()> {
    use dialoguer::Confirm;

    // Check if recovery key already exists
    let has_recovery = config
        .get("key")
        .and_then(|k| k.get("recovery"))
        .is_some();

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
    println!("│  {}  │", format!("{:<69}", &secret_key));
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

    // Build the patch to add/replace the recovery key
    let mut ops = vec![serde_json::json!({
        "op": if has_recovery { "replace" } else { "add" },
        "path": "/key/recovery",
        "value": {
            "public_key": pubkey,
        }
    })];

    // Also add "recovery" to each vault's backup tasks' keys list if not already present
    if let Some(tasks) = config.get("task").and_then(|v| v.as_object()) {
        for (name, task) in tasks {
            let task_type = task.get("type").and_then(|v| v.as_str());
            if matches!(task_type, Some("backup") | Some("publish")) {
                if let Some(keys) = task.get("keys").and_then(|k| k.as_array()) {
                    let already_has = keys.iter().any(|k| k.as_str() == Some("recovery"));
                    if !already_has {
                        ops.push(serde_json::json!({
                            "op": "add",
                            "path": format!("/task/{}/keys/-", name),
                            "value": "recovery"
                        }));
                    }
                }
            }
        }
    }

    let resp = client.patch_config(serde_json::Value::Array(ops)).await?;
    if resp.ok {
        println!("Recovery key added to config.");

        // Check if it was added to any tasks
        if let Some(ref new_config_json) = resp.config_json {
            let new_config: serde_json::Value = serde_json::from_str(new_config_json)?;
            if let Some(tasks) = new_config.get("task").and_then(|v| v.as_object()) {
                for (name, task) in tasks {
                    if let Some(keys) = task.get("keys").and_then(|k| k.as_array()) {
                        if keys.iter().any(|k| k.as_str() == Some("recovery")) {
                            println!("  + added to task '{}'", name);
                        }
                    }
                }
            }
        }
    } else {
        bail!("failed to save recovery key: {}", resp.message);
    }

    Ok(())
}
