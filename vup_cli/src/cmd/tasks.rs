//! Task-based CLI commands that use the s5_node_api RPC.
//!
//! Each command receives an already-connected `S5NodeClient` from `ensure_node_running()`.

use std::time::Duration;

use anyhow::{bail, Result};
use indicatif::{ProgressBar, ProgressStyle};
use s5_node_api::config::TaskSpec;
use s5_node_api::{S5NodeClient, TaskProgress, TaskState};

fn format_bytes(bytes: u64) -> String {
    humansize::format_size(bytes, humansize::BINARY)
}

/// `vup run-task <name>` — run a named task from node config and poll progress.
pub async fn run_task_by_name(client: &S5NodeClient, name: &str) -> Result<()> {
    let resp = client.run_task_by_name(name).await?;
    println!("Task {} started (id={})", name, resp.task_id);
    poll_until_done(client, resp.task_id).await
}

/// `vup ingest` — run an inline ingest task.
pub async fn run_ingest(
    client: &S5NodeClient,
    vault: &str,
    source: &str,
    blob_store: &str,
) -> Result<()> {
    let spec = TaskSpec::Ingest {
        vault: vault.to_string(),
        source: source.to_string(),
        blob_store: blob_store.to_string(),
        target_path: None,
    };
    let resp = client.run_task(spec).await?;
    println!("Ingest started (id={})", resp.task_id);
    poll_until_done(client, resp.task_id).await
}

/// `vup backup` — run a backup with smart defaults from config.
///
/// All parameters are optional. When omitted, they're resolved from config:
/// - vault: the sole vault, or "default"
/// - source: the sole source, or "default"
/// - blob_store: first store from the vault's `blob_stores`
/// - keys: vault's `key` + "recovery" if configured
pub async fn run_backup(
    client: &S5NodeClient,
    vault_override: Option<&str>,
    source_override: Option<&str>,
    blob_store_override: Option<&str>,
    key_overrides: &[String],
) -> Result<()> {
    let config_resp = client.get_config().await?;
    let config = &config_resp.config;

    // Resolve vault
    let vault_name = match vault_override {
        Some(v) => v.to_string(),
        None => resolve_single_or_default(config, "vault", "default")?,
    };

    // Get vault config for downstream defaults
    let vault_cfg = config
        .get("vault")
        .and_then(|v| v.get(&vault_name))
        .ok_or_else(|| anyhow::anyhow!("vault '{}' not found in config", vault_name))?;

    // Resolve source
    let source_name = match source_override {
        Some(s) => s.to_string(),
        None => resolve_single_or_default(config, "source", "default")?,
    };

    // Resolve blob store
    let blob_store = match blob_store_override {
        Some(b) => b.to_string(),
        None => {
            vault_cfg
                .get("blob_stores")
                .and_then(|s| s.as_array())
                .and_then(|a| a.first())
                .and_then(|v| v.as_str())
                .map(String::from)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "vault '{}' has no blob_stores configured — use --blob-store",
                        vault_name
                    )
                })?
        }
    };

    // Resolve keys
    let keys = if !key_overrides.is_empty() {
        key_overrides.to_vec()
    } else {
        let mut keys = vec![];
        // Always include the vault's own key
        if let Some(k) = vault_cfg.get("key").and_then(|v| v.as_str()) {
            keys.push(k.to_string());
        }
        // Include "recovery" if configured
        if config
            .get("key")
            .and_then(|k| k.get("recovery"))
            .is_some()
        {
            keys.push("recovery".to_string());
        }
        if keys.is_empty() {
            bail!(
                "no encryption keys found — configure [key.*] in your config\n\
                 or use --key <name>"
            );
        }
        keys
    };

    let spec = TaskSpec::Backup {
        vault: vault_name.clone(),
        source: source_name,
        blob_store,
        keys,
        target_path: None,
    };
    let resp = client.run_task(spec).await?;
    println!("Backup started (id={})", resp.task_id);
    poll_until_done(client, resp.task_id).await
}

/// Resolve a config section: if there's exactly one entry use it, otherwise
/// try `fallback` name, otherwise error with a helpful message.
fn resolve_single_or_default(
    config: &serde_json::Value,
    section: &str,
    fallback: &str,
) -> Result<String> {
    let obj = config
        .get(section)
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow::anyhow!("no [{}.*] configured", section))?;

    if obj.is_empty() {
        bail!("no [{}.*] configured", section);
    }
    if obj.len() == 1 {
        return Ok(obj.keys().next().unwrap().clone());
    }
    if obj.contains_key(fallback) {
        return Ok(fallback.to_string());
    }
    bail!(
        "multiple {}s configured ({}). Use --{} to pick one.",
        section,
        obj.keys().cloned().collect::<Vec<_>>().join(", "),
        section,
    );
}

/// `vup restore` — restore a vault to a target directory.
pub async fn run_restore_task(
    client: &S5NodeClient,
    vault: &str,
    target_path: &str,
    blob_store: Option<&str>,
) -> Result<()> {
    let spec = TaskSpec::Restore {
        vault: vault.to_string(),
        target_path: target_path.to_string(),
        blob_store: blob_store.map(String::from),
    };
    let resp = client.run_task(spec).await?;
    println!("Restore started (id={})", resp.task_id);
    poll_until_done(client, resp.task_id).await
}

/// `vup remote-restore` — disaster recovery from paper age key.
pub async fn run_remote_restore_task(
    client: &S5NodeClient,
    age_secret_key: &str,
    vault: &str,
    blob_store: &str,
    target_path: &str,
) -> Result<()> {
    let spec = TaskSpec::RemoteRestore {
        vault: vault.to_string(),
        age_secret_key: age_secret_key.to_string(),
        blob_store: blob_store.to_string(),
        target_path: target_path.to_string(),
    };
    let resp = client.run_task(spec).await?;
    println!("Remote restore started (id={})", resp.task_id);
    poll_until_done(client, resp.task_id).await
}

/// `vup task-status <id>` — show task status.
pub async fn task_status(client: &S5NodeClient, task_id: u64) -> Result<()> {
    let resp = client.get_task_status(task_id).await?;
    print_status(resp.task_id, &resp.state, resp.progress.as_ref());
    Ok(())
}

/// `vup tasks` — list all tasks.
pub async fn list_tasks(client: &S5NodeClient) -> Result<()> {
    let resp = client.list_tasks().await?;
    if resp.tasks.is_empty() {
        println!("No tasks.");
        return Ok(());
    }
    for t in &resp.tasks {
        print_status(t.task_id, &t.state, t.progress.as_ref());
        println!();
    }
    Ok(())
}

/// `vup cancel <id>` — cancel a running task.
pub async fn cancel_task(client: &S5NodeClient, task_id: u64) -> Result<()> {
    let resp = client.cancel_task(task_id).await?;
    if resp.ok {
        println!("Task {} cancelled.", task_id);
    } else {
        println!("Task {}: {}", task_id, resp.message);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Poll a task until it finishes, printing progress updates.
async fn poll_until_done(client: &S5NodeClient, task_id: u64) -> Result<()> {
    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(120));
    pb.set_style(
        ProgressStyle::with_template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    pb.set_message("starting…");

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let resp = client.get_task_status(task_id).await?;

        match &resp.state {
            TaskState::Pending | TaskState::Running => {
                if let Some(ref p) = resp.progress {
                    pb.set_message(format_progress(p));
                }
            }
            TaskState::Completed => {
                if let Some(ref p) = resp.progress {
                    pb.set_message(format_progress(p));
                }
                pb.finish_with_message(format!("✓ task {} completed", task_id));
                return Ok(());
            }
            TaskState::Failed { error } => {
                pb.finish_with_message(format!("✗ task {} failed", task_id));
                bail!("Task {} failed: {}", task_id, error);
            }
            TaskState::Cancelled => {
                pb.finish_with_message(format!("⊘ task {} cancelled", task_id));
                return Ok(());
            }
        }
    }
}

fn format_progress(progress: &TaskProgress) -> String {
    match progress {
        TaskProgress::Ingest {
            files_scanned,
            files_changed,
            files_skipped,
            bytes_uploaded,
        } => {
            format!(
                "scanned: {} | changed: {} | skipped: {} | uploaded: {}",
                files_scanned,
                files_changed,
                files_skipped,
                format_bytes(*bytes_uploaded),
            )
        }
        TaskProgress::Restore {
            files_restored,
            bytes_restored,
        } => {
            format!(
                "files: {} | written: {}",
                files_restored,
                format_bytes(*bytes_restored),
            )
        }
    }
}

fn print_status(task_id: u64, state: &TaskState, progress: Option<&TaskProgress>) {
    let state_str = match state {
        TaskState::Pending => "pending",
        TaskState::Running => "running",
        TaskState::Completed => "completed",
        TaskState::Failed { .. } => "failed",
        TaskState::Cancelled => "cancelled",
    };
    print!("Task {}: {}", task_id, state_str);
    if let TaskState::Failed { error } = state {
        print!(" — {}", error);
    }
    println!();
    if let Some(p) = progress {
        match p {
            TaskProgress::Ingest {
                files_scanned,
                files_changed,
                files_skipped,
                bytes_uploaded,
            } => {
                println!(
                    "  scanned: {} | changed: {} | skipped: {} | uploaded: {}",
                    files_scanned,
                    files_changed,
                    files_skipped,
                    format_bytes(*bytes_uploaded),
                );
            }
            TaskProgress::Restore {
                files_restored,
                bytes_restored,
            } => {
                println!(
                    "  files: {} | written: {}",
                    files_restored,
                    format_bytes(*bytes_restored),
                );
            }
        }
    }
}
