//! Vault-scoped verb bodies (backup mapping, restore, mount, export,
//! history, info) plus the shared source-mapping + backup-spec helpers.
//!
//! These read the vault's `recipients`, `sources`, and resolved
//! `data_store`/`meta_store` from config and translate a D20-grammar
//! request into the appropriate `TaskSpec` RPC for the daemon's task
//! executor. Most are thin shells over the helpers in `cmd::tasks`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use s5_node_api::S5NodeClient;
use s5_node_api::config::TaskSpec;

use crate::refs::VaultRef;

// ---------------------------------------------------------------------------
// backup (fidelity-in) — spec + run + source mapping
// ---------------------------------------------------------------------------

/// Build the `TaskSpec::Backup` a `backup` dispatches for `vault`.
///
/// Reads `vault.sources` (must currently have exactly one entry; a source
/// legitimately holds N roots), the resolved data store, and
/// `vault.recipients` for snapshot encryption recipients. `target_path`
/// scopes the snapshot to a subtree inside the vault (D20 `vault:path`).
pub(crate) async fn build_backup_spec(
    client: &S5NodeClient,
    vault: &str,
    target_path: Option<&str>,
) -> Result<TaskSpec> {
    let resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&resp.config_json)?;
    let cfg = config
        .get("vault")
        .and_then(|v| v.get(vault))
        .cloned()
        .ok_or_else(|| anyhow!("vault '{vault}:' not found in config"))?;
    let sources = string_array(&cfg, "sources");
    let recipients = string_array(&cfg, "recipients");

    let source = match sources.as_slice() {
        [] => {
            bail!("vault '{vault}:' has no source mapping — run `vup backup <path> {vault}:` first")
        }
        [s] => s.clone(),
        many => bail!(
            "vault '{vault}:' has {} sources configured ({}); multi-source backup not yet supported",
            many.len(),
            many.join(", "),
        ),
    };

    let blob_store = resolve_data_store(&config, vault)?;

    if recipients.is_empty() {
        bail!(
            "vault '{vault}:' has no recipients configured — without recipients, the encrypted \
             snapshot has no readers. Set `vault.{vault}.recipients` to at least one [key.*] entry."
        );
    }

    Ok(TaskSpec::Backup {
        vault: vault.to_string(),
        source,
        blob_store,
        keys: recipients,
        target_path: target_path.map(String::from),
        changed_paths: None,
    })
}

/// Run the vault's persisted backup mapping once and poll to completion.
/// Echoes the resolved vault on the first output line (D20).
pub(crate) async fn run_backup_mapped(
    client: &S5NodeClient,
    vault: &str,
    target_path: Option<&str>,
) -> Result<()> {
    let spec = build_backup_spec(client, vault, target_path).await?;
    let resp = client.run_task(spec).await?;
    println!("{vault}: backup started (task id={})", resp.task_id);
    crate::cmd::tasks::poll_until_done(client, resp.task_id).await
}

/// Persist a source→vault mapping: canonicalize `paths`, create or extend
/// the vault's `[source.*]`, and register it in `vault.<name>.sources`.
/// This is the side effect that `backup SRC vault:` produces (and that
/// `automate` later promotes). The vault must already exist.
///
/// If exactly one source is already configured, appends to it; a vault
/// with multiple sources is rejected (edit with `vup config`).
pub(crate) async fn persist_source_paths(
    client: &S5NodeClient,
    vault: &str,
    paths: &[PathBuf],
) -> Result<()> {
    if paths.is_empty() {
        bail!("no source paths specified");
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
            "vault '{vault}:' has multiple sources ({}); edit a specific one with `vup config`",
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

    client.patch_config(serde_json::Value::Array(ops)).await?;
    println!(
        "{vault}: mapped {} path(s) → source '{source_name}'",
        abs_paths.len()
    );
    for p in &abs_paths {
        println!("  + {p}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// restore (fidelity-out)
// ---------------------------------------------------------------------------

/// `vup restore vault:[path][#snap] TARGET [--force]` — rebuild the exact
/// recorded filesystem into `target`.
///
/// TARGET is a required positional (no default, no surprise overwrite). A
/// non-empty target is refused unless `--force` is passed (drill §7). `#snap`
/// selects a past published snapshot; `vault:path` restores only that subtree,
/// re-rooted so its contents land directly under TARGET.
pub async fn run_restore(
    client: &S5NodeClient,
    vref: &VaultRef,
    target: &Path,
    force: bool,
) -> Result<()> {
    if target.exists() {
        let non_empty = std::fs::read_dir(target)
            .map(|mut it| it.next().is_some())
            .unwrap_or(false);
        if non_empty && !force {
            bail!(
                "target '{}' is not empty — pass --force to restore into it anyway",
                target.display()
            );
        }
    }
    std::fs::create_dir_all(target)
        .with_context(|| format!("creating restore target '{}'", target.display()))?;

    // Echo the resolved vault + selectors on line 1 (D20).
    let scope = match (&vref.path, &vref.snap) {
        (Some(p), Some(s)) => format!("{}:{p}#{s}", vref.name),
        (Some(p), None) => format!("{}:{p}", vref.name),
        (None, Some(s)) => format!("{}:#{s}", vref.name),
        (None, None) => format!("{}:", vref.name),
    };
    println!("{scope}: restoring → {}", target.display());
    crate::cmd::tasks::run_restore_task(
        client,
        &vref.name,
        &target.display().to_string(),
        None, // blob_store override — defaults to the vault's read chain (D1)
        vref.snap.as_deref(),
        vref.path.as_deref(),
    )
    .await
}

// ---------------------------------------------------------------------------
// history
// ---------------------------------------------------------------------------

/// `vup history [vault:]` — list snapshots for a vault.
pub async fn run_history(client: &S5NodeClient, vault: &str) -> Result<()> {
    let resp = client.list_snapshots(Some(vault.to_string())).await?;

    if resp.snapshots.is_empty() {
        println!("{vault}: no snapshots yet.");
        return Ok(());
    }

    println!("{vault}:");
    println!("  {:<16} {:<10} {:<12} DATE", "HASH", "FILES", "SIZE");
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
            "  {:<16} {:<10} {:<12} {}",
            hash_short, files, size, snap.timestamp
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// share (frozen export) — used by cmd::share for the whole-vault case
// ---------------------------------------------------------------------------

/// Produce a frozen anonymous share URL for the vault's current snapshot.
/// The daemon re-encrypts the current Transparent Node with a fresh
/// ephemeral age recipient added, uploads the new blob, and returns the
/// URL with the recipient secret in the fragment.
pub async fn run_export(client: &S5NodeClient, vault: &str, path: Option<&str>) -> Result<()> {
    let share = client
        .export_vault(vault.to_string(), path.map(String::from))
        .await?;
    let hash_short = &share.blob_hash_hex[..share.blob_hash_hex.len().min(12)];
    println!(
        "{vault}: frozen share{} ready (blob {hash_short}…).",
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

// ---------------------------------------------------------------------------
// mount
// ---------------------------------------------------------------------------

/// `vup mount vault: DIR [--rw]` — mount the vault at a local path. The
/// mount runs on the daemon; this CLI verb drives the lifecycle and
/// unmounts on Ctrl-C.
///
/// Read-only by default. With `--rw`, writes accumulate in the daemon's
/// in-memory overlay and a debounced flush + publish loop folds bursts
/// into fresh snapshots.
pub async fn run_mount(
    client: &S5NodeClient,
    vault: &str,
    mountpoint: &Path,
    rw: bool,
    debounce_ms: u64,
) -> Result<()> {
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
        "{vault}: mounted {mode} at {} (mount id={}). Press Ctrl-C to unmount.",
        mountpoint.display(),
        resp.mount_id,
    );

    tokio::signal::ctrl_c()
        .await
        .context("waiting for Ctrl-C")?;

    eprintln!("\nUnmounting {vault}…");
    client.unmount_vault(resp.mount_id).await?;
    println!("{vault}: unmounted.");
    Ok(())
}

// ---------------------------------------------------------------------------
// info
// ---------------------------------------------------------------------------

/// Pretty-print a vault's config block (`vup config <vault>:`; hidden `info` alias).
pub async fn run_info(client: &S5NodeClient, vault: &str) -> Result<()> {
    let cfg = fetch_vault_config(client, vault).await?;

    println!("{vault}:");
    print_optional_string(&cfg, "root_path", "  root_path:    ");
    print_optional_string(&cfg, "key", "  key:          ");
    print_optional_array(&cfg, "recipients", "  recipients:   ");
    print_optional_array(&cfg, "sources", "  sources:      ");
    print_optional_string(&cfg, "data_store", "  data_store:   ");
    print_optional_string(&cfg, "meta_store", "  meta_store:   ");
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
        .ok_or_else(|| anyhow!("vault '{vault}:' not found in config"))
}

/// Resolve the vault's primary data store from the full config JSON (D1):
/// `vault.<name>.data_store`, else the top-level `default_store`, else the
/// sole `[store.*]` entry. Mirrors `S5NodeConfig::vault_data_store`.
pub(crate) fn resolve_data_store(config: &serde_json::Value, vault: &str) -> Result<String> {
    if let Some(s) = config
        .get("vault")
        .and_then(|v| v.get(vault))
        .and_then(|v| v.get("data_store"))
        .and_then(|v| v.as_str())
    {
        return Ok(s.to_string());
    }
    if let Some(s) = config.get("default_store").and_then(|v| v.as_str()) {
        return Ok(s.to_string());
    }
    if let Some(stores) = config.get("store").and_then(|v| v.as_object())
        && stores.len() == 1
    {
        return Ok(stores.keys().next().expect("len checked").clone());
    }
    bail!(
        "vault '{vault}:' resolves no data store — set `vault.{vault}.data_store` \
         or the top-level `default_store`"
    )
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
