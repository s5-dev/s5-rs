//! Vault-scoped verbs (everything reachable via `vup +<vault> <verb>`).
//!
//! These read the vault's `recipients`, `sources`, `blob_stores`, and
//! `meta_targets` from config and translate the `vup`-grammar request
//! into the appropriate `TaskSpec` RPC for the daemon's task executor
//! to run. Most are thin shells over the helpers in `cmd::tasks`.

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use s5_node_api::S5NodeClient;
use s5_node_api::config::TaskSpec;

/// `vup +<vault> snap [--watch]` — take a snapshot and publish it.
///
/// Reads `vault.sources` (must currently have exactly one entry; multi-source
/// vaults are a future enhancement), `vault.blob_stores[0]` for content,
/// and `vault.recipients` for snapshot encryption recipients. `--watch` is
/// not yet wired (continuous mode lives in a follow-up).
pub async fn run_snap(client: &S5NodeClient, vault: &str, watch: bool) -> Result<()> {
    if watch {
        bail!("--watch is not yet implemented; run `vup +{vault} snap` once for now");
    }

    let cfg = fetch_vault_config(client, vault).await?;
    let sources = string_array(&cfg, "sources");
    let recipients = string_array(&cfg, "recipients");
    let blob_stores = string_array(&cfg, "blob_stores");

    let source = match sources.as_slice() {
        [] => bail!(
            "vault '{vault}' has no sources configured — run `vup +{vault} add <path>` first \
             (or edit `vault.{vault}.sources` directly with `vup config`)"
        ),
        [s] => s.clone(),
        many => bail!(
            "vault '{vault}' has {} sources configured ({}); multi-source snap not yet supported",
            many.len(),
            many.join(", "),
        ),
    };

    let blob_store = blob_stores.first().cloned().ok_or_else(|| {
        anyhow!("vault '{vault}' has no blob_stores configured — set `vault.{vault}.blob_stores`")
    })?;

    if recipients.is_empty() {
        bail!(
            "vault '{vault}' has no recipients configured — without recipients, the encrypted \
             snapshot has no readers. Set `vault.{vault}.recipients` to at least one [key.*] entry."
        );
    }

    let spec = TaskSpec::Backup {
        vault: vault.to_string(),
        source,
        blob_store,
        keys: recipients,
        target_path: None,
    };

    let resp = client.run_task(spec).await?;
    println!("snap on +{vault} started (task id={})", resp.task_id);
    crate::cmd::tasks::poll_until_done(client, resp.task_id).await
}

/// `vup +<vault> history` — list snapshots for this vault.
pub async fn run_history(client: &S5NodeClient, vault: &str) -> Result<()> {
    let resp = client.list_snapshots(Some(vault.to_string())).await?;

    if resp.snapshots.is_empty() {
        println!("No snapshots in +{vault} yet.");
        return Ok(());
    }

    println!("{:<16} {:<10} {:<12} DATE", "HASH", "FILES", "SIZE");
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
        println!(
            "{:<16} {:<10} {:<12} {}",
            hash_short, files, size, snap.timestamp
        );
    }

    Ok(())
}

/// `vup +<vault> restore [--snap <id>]` — restore the latest snapshot
/// (or a named one) to a temporary directory printed on completion.
///
/// `--snap` selection is parsed but not yet honoured — the underlying
/// task always restores the current snapshot. Snap-id selection lands
/// alongside the `#snap` sigil parser.
pub async fn run_restore(
    client: &S5NodeClient,
    vault: &str,
    snap: Option<&str>,
    target_path: &std::path::Path,
) -> Result<()> {
    if snap.is_some() {
        eprintln!("note: --snap selection not yet implemented; restoring current snapshot");
    }
    crate::cmd::tasks::run_restore_task(
        client,
        vault,
        &target_path.display().to_string(),
        None, // blob_store override — defaults to vault's blob_stores chain
    )
    .await
}

/// `vup +<vault> add <paths>...` — add paths to the vault's source.
///
/// If the vault has no source configured yet, creates one named after
/// the vault and registers it in `vault.<name>.sources`. If exactly one
/// source is already configured, appends to it. Multi-source vaults are
/// rejected — pick the source explicitly with `vup config`.
pub async fn run_add(client: &S5NodeClient, vault: &str, paths: &[PathBuf]) -> Result<()> {
    if paths.is_empty() {
        bail!("no paths specified");
    }

    let mut abs_paths = Vec::new();
    for p in paths {
        let abs = std::fs::canonicalize(p)
            .map_err(|e| anyhow!("cannot resolve path '{}': {}", p.display(), e))?;
        abs_paths.push(abs.to_string_lossy().to_string());
    }

    let cfg = fetch_vault_config(client, vault).await?;
    let sources = string_array(&cfg, "sources");

    let source_name = match sources.as_slice() {
        [] => vault.to_string(),
        [s] => s.clone(),
        many => bail!(
            "vault '{vault}' has multiple sources ({}); use `vup config` to add to a specific one",
            many.join(", "),
        ),
    };

    // Re-fetch the full config to check whether the source already exists.
    let config_resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&config_resp.config_json)?;
    let source_exists = config
        .get("source")
        .and_then(|s| s.get(&source_name))
        .is_some();

    let mut ops: Vec<serde_json::Value> = Vec::new();

    if source_exists {
        for path in &abs_paths {
            ops.push(serde_json::json!({
                "op": "add",
                "path": format!("/source/{}/paths/-", source_name),
                "value": path,
            }));
        }
    } else {
        ops.push(serde_json::json!({
            "op": "add",
            "path": format!("/source/{}", source_name),
            "value": {
                "paths": abs_paths,
                "exclude": [],
            },
        }));
    }

    if sources.is_empty() {
        ops.push(serde_json::json!({
            "op": "add",
            "path": format!("/vault/{}/sources/-", vault),
            "value": source_name,
        }));
    }

    let resp = client.patch_config(serde_json::Value::Array(ops)).await?;
    if resp.ok {
        println!(
            "+{vault}: added {} path(s) to source '{source_name}'",
            abs_paths.len()
        );
        for p in &abs_paths {
            println!("  + {p}");
        }
    } else {
        bail!("config update failed: {}", resp.message);
    }

    Ok(())
}

/// `vup +<vault> export [--path <p>]` — produce a frozen anonymous
/// share URL for the current snapshot. The daemon re-encrypts the
/// current Transparent Node with a fresh ephemeral age recipient
/// added, uploads the new blob, and returns the URL with the
/// recipient secret in the fragment.
///
/// `--path` is reserved for sub-tree exports and currently rejected
/// by the daemon; passing it surfaces a clear error rather than
/// silently producing a whole-vault URL.
pub async fn run_export(client: &S5NodeClient, vault: &str, path: Option<&str>) -> Result<()> {
    let share = client
        .export_vault(vault.to_string(), path.map(String::from))
        .await?;
    let hash_short = &share.blob_hash_hex[..share.blob_hash_hex.len().min(12)];
    println!(
        "Frozen export of +{vault}{} ready (blob {hash_short}…).",
        path.map(|p| format!(":{p}")).unwrap_or_default()
    );
    println!();
    println!("  Share this URL:");
    println!("    {}", share.url);
    println!();
    println!("  Recipient gets only this snapshot. No future updates,");
    println!("  no individual revocation; the URL is the capability.");
    Ok(())
}

/// `vup +<vault> mount [--rw] <path>` — mount the vault at a local
/// path. The mount runs on the daemon (it owns `s5_fuse` and the
/// vault stores); this CLI verb just dispatches the lifecycle and
/// blocks on Ctrl-C to send the unmount RPC back.
///
/// Read-only by default. With `--rw`, writes accumulate in the
/// daemon's in-memory overlay and a debounced flush + publish loop
/// (`--debounce-ms`, default 2 s) folds bursts into fresh snapshots.
///
/// `path` is canonicalised to an absolute path before sending so the
/// daemon resolves the mount-point against the same absolute layout
/// the user typed, regardless of working directory drift between the
/// two processes.
pub async fn run_mount(
    client: &S5NodeClient,
    vault: &str,
    mountpoint: &std::path::Path,
    rw: bool,
    debounce_ms: u64,
) -> Result<()> {
    // Canonicalise so the daemon mounts at exactly the path the user
    // typed, even if the daemon's CWD differs. fs::canonicalize fails
    // if the path doesn't exist; that gives us the same up-front
    // "create the mount point first" hint the daemon-side preflight
    // would surface, without the round-trip.
    let mountpoint = std::fs::canonicalize(mountpoint).with_context(|| {
        format!(
            "resolving mount point '{}' (it must exist on disk)",
            mountpoint.display()
        )
    })?;

    let resp = client
        .mount_vault(vault.to_string(), mountpoint.clone(), rw, debounce_ms)
        .await?;

    let mode = if rw {
        format!("read-write (debounce {debounce_ms}ms)")
    } else {
        "read-only".to_string()
    };
    println!(
        "Mounted +{vault} {mode} at {} (mount id={}). Press Ctrl-C to unmount.",
        mountpoint.display(),
        resp.mount_id,
    );

    // Block until the user hits Ctrl-C, then send the unmount RPC.
    // The daemon-side `MountHandle` drop is what performs the actual
    // `umount(2)`; awaiting `unmount_vault` ensures it's complete by
    // the time we return (so the user's shell sees the mount gone).
    tokio::signal::ctrl_c()
        .await
        .context("waiting for Ctrl-C")?;

    eprintln!("\nUnmounting +{vault}…");
    client.unmount_vault(resp.mount_id).await?;
    println!("+{vault} unmounted.");
    Ok(())
}

/// `vup +<vault> info` — pretty-print the vault's config block.
pub async fn run_info(client: &S5NodeClient, vault: &str) -> Result<()> {
    let cfg = fetch_vault_config(client, vault).await?;

    println!("+{vault}");
    print_optional_string(&cfg, "root_path", "  root_path:    ");
    print_optional_string(&cfg, "key", "  key:          ");
    print_optional_array(&cfg, "recipients", "  recipients:   ");
    print_optional_array(&cfg, "sources", "  sources:      ");
    print_optional_array(&cfg, "blob_stores", "  blob_stores:  ");
    print_optional_array(&cfg, "meta_targets", "  meta_targets: ");
    if let Some(true) = cfg.get("plaintext_tree").and_then(|v| v.as_bool()) {
        println!("  plaintext_tree: true (tree nodes stored unencrypted)");
    }
    print_optional_string(&cfg, "preset", "  preset:       ");

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn fetch_vault_config(client: &S5NodeClient, vault: &str) -> Result<serde_json::Value> {
    let resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&resp.config_json)?;
    config
        .get("vault")
        .and_then(|v| v.get(vault))
        .cloned()
        .ok_or_else(|| anyhow!("vault '{vault}' not found in config"))
}

fn string_array(cfg: &serde_json::Value, key: &str) -> Vec<String> {
    cfg.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn print_optional_string(cfg: &serde_json::Value, key: &str, prefix: &str) {
    if let Some(s) = cfg.get(key).and_then(|v| v.as_str()) {
        println!("{prefix}{s}");
    }
}

fn print_optional_array(cfg: &serde_json::Value, key: &str, prefix: &str) {
    let v = string_array(cfg, key);
    if !v.is_empty() {
        println!("{prefix}{}", v.join(", "));
    }
}
