//! Task-based CLI commands that use the s5_node_api RPC.
//!
//! Each command receives an already-connected `S5NodeClient` from
//! `ensure_node_running()`.
//!
//! Some helpers here (`run_task_by_name`, `run_ingest`, `run_backup`,
//! `resolve_single_or_default`) currently
//! have no caller — they're legacy verb wrappers tied to the old config
//! shape (e.g. `run_backup` derives keys from `vault.<name>.key` rather
//! than `vault.<name>.recipients`). The new vocabulary verbs in
//! `cmd::vault` build their `TaskSpec`s inline against the new schema.
//! These wrappers are kept as the natural starting point for future
//! sub-verbs (`vup tasks run <name>`, recovery URL consumers) that will
//! revive them once they are needed.

use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use indicatif::HumanDuration;
use s5_node_api::config::TaskSpec;
use s5_node_api::{S5NodeClient, TaskProgressMap, TaskState};
use tokio_util::sync::CancellationToken;

use crate::progress::{format_one_line, new_progress_bar, update_progress_bar};

/// `vup run-task <name>` — run a named task from node config and poll progress.
#[allow(dead_code)] // retained as the basis for a future `vup tasks run <name>` sub-verb
pub async fn run_task_by_name(client: &S5NodeClient, name: &str) -> Result<()> {
    let resp = client.run_task_by_name(name).await?;
    println!("Task {} started (id={})", name, resp.task_id);
    poll_until_done(client, resp.task_id).await
}

/// `vup ingest` — run an inline ingest task.
#[allow(dead_code)] // ingest is bundled into `vup backup`; kept for direct-task scenarios
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
/// - blob_store: the vault's resolved data store (D1)
/// - keys: vault's `key` + "recovery" if configured
///
/// This reads the **legacy** vault shape (`vault.<name>.key` as a single
/// key string + a parallel `recovery` lookup). The new vocabulary verb
/// `vup backup … <vault>:` reads `vault.<name>.recipients` directly and
/// builds the `TaskSpec::Backup` inline in `cmd::vault::run_snap`.
#[allow(dead_code)] // legacy resolver — superseded by cmd::vault::run_snap
pub async fn run_backup(
    client: &S5NodeClient,
    vault_override: Option<&str>,
    source_override: Option<&str>,
    blob_store_override: Option<&str>,
    key_overrides: &[String],
) -> Result<()> {
    let config_resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&config_resp.config_json)?;

    // Resolve vault
    let vault_name = match vault_override {
        Some(v) => v.to_string(),
        None => resolve_single_or_default(&config, "vault", "default")?,
    };

    // Get vault config for downstream defaults
    let vault_cfg = config
        .get("vault")
        .and_then(|v| v.get(&vault_name))
        .ok_or_else(|| anyhow::anyhow!("vault '{}' not found in config", vault_name))?;

    // Resolve source
    let source_name = match source_override {
        Some(s) => s.to_string(),
        None => resolve_single_or_default(&config, "source", "default")?,
    };

    // Resolve blob store
    let blob_store = match blob_store_override {
        Some(b) => b.to_string(),
        None => super::vault::resolve_data_store(&config, &vault_name)?,
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
        if config.get("key").and_then(|k| k.get("recovery")).is_some() {
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
        changed_paths: None,
    };
    let resp = client.run_task(spec).await?;
    println!("Backup started (id={})", resp.task_id);
    poll_until_done(client, resp.task_id).await
}

/// Resolve a config section: if there's exactly one entry use it, otherwise
/// try `fallback` name, otherwise error with a helpful message.
#[allow(dead_code)] // helper for run_backup (currently legacy)
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
///
/// `snapshot` (D20 `#snap`) selects a past published snapshot; `subtree`
/// (D20 `vault:path`) restores only that path, re-rooted at the target. Both
/// default to the whole current snapshot when `None`.
#[allow(clippy::too_many_arguments)]
pub async fn run_restore_task(
    client: &S5NodeClient,
    vault: &str,
    target_path: &str,
    blob_store: Option<&str>,
    snapshot: Option<&str>,
    subtree: Option<&str>,
) -> Result<()> {
    let spec = TaskSpec::Restore {
        vault: vault.to_string(),
        target_path: target_path.to_string(),
        blob_store: blob_store.map(String::from),
        snapshot: snapshot.map(String::from),
        subtree: subtree.map(String::from),
    };
    let resp = client.run_task(spec).await?;
    println!("Restore started (id={})", resp.task_id);
    poll_until_done(client, resp.task_id).await
}

/// `vup tasks <id>` — show task status.
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
    match client.cancel_task(task_id).await {
        Ok(()) => println!("Task {} cancelled.", task_id),
        Err(e) => println!("Task {}: {:#}", task_id, e),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Poll a task until it finishes, printing progress updates.
/// Uses a streaming RPC to receive status updates as they happen,
/// avoiding tight polling loops.
pub async fn poll_until_done(client: &S5NodeClient, task_id: u64) -> Result<()> {
    let started = Instant::now();

    // Create a spinner for initial state
    let pb = new_progress_bar();
    pb.set_message("starting…");

    // Create a cancellation token to signal Ctrl+C
    let cancel_token = CancellationToken::new();

    // Spawn a task to listen for Ctrl+C and cancel the task on the daemon
    let sig_client = client.clone();
    let sig_task_id = task_id;
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                tracing::debug!(task_id = sig_task_id, "Ctrl+C received, cancelling task");
                cancel_clone.cancel();
                if let Err(e) = sig_client.cancel_task(sig_task_id).await {
                    tracing::warn!(task_id = sig_task_id, error = %e, "failed to cancel task");
                }
            }
            Err(e) => {
                tracing::warn!("failed to listen for ctrl+c: {}", e);
            }
        }
    });

    // Open a streaming RPC to get status updates as they happen.
    let mut rx = client.watch_task_status(task_id).await?;

    loop {
        tokio::select! {
            // Check if Ctrl+C was pressed
            _ = cancel_token.cancelled() => {
                pb.finish_with_message(format!("⊘ task {} cancelled (waiting for daemon to save state...)", task_id));
                tokio::time::sleep(Duration::from_secs(2)).await;
                return Ok(());
            }
            // Receive next status update from the server stream
            msg = rx.recv() => {
                let resp = match msg {
                    Ok(Some(resp)) => resp,
                    Ok(None) => {
                        // Stream ended normally (server closed)
                        pb.finish_with_message(format!("⚠ task {} stream ended", task_id));
                        return Ok(());
                    }
                    Err(e) => {
                        pb.finish_with_message(format!("⚠ task {} stream error: {}", task_id, e));
                        return Ok(());
                    }
                };

                match &resp.state {
                    TaskState::Pending | TaskState::Running => {
                        if let Some(ref progress) = resp.progress {
                            update_progress_bar(&pb, progress);
                        }
                    }
                    TaskState::Completed => {
                        let elapsed = HumanDuration(started.elapsed());
                        let summary = resp.progress.as_ref()
                            .map(format_one_line)
                            .unwrap_or_default();
                        if summary.is_empty() {
                            pb.finish_with_message(format!("✓ done ({elapsed})"));
                        } else {
                            pb.finish_with_message(format!("✓ {summary} ({elapsed})"));
                        }
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
    }
}

fn print_status(task_id: u64, state: &TaskState, progress: Option<&TaskProgressMap>) {
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
        println!("  {}", format_one_line(p));
    }
}
