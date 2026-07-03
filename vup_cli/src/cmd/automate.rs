//! `vup automate` — keep a backup running on its own (Stage 7 / D20 §B2).
//!
//! An *automation* is a `[task.*]` entry with a non-`Manual` trigger. The
//! daemon's automation engine reconciles them into live loops; every verb here
//! is a plain `patch_config` op on `/task/*` (add / pause / resume / remove), so
//! changes take effect the moment the daemon reconciles — no dedicated RPC.
//!
//! - `automate` (bare) → a context-aware wizard: offer to promote your most
//!   recent one-shot backup, else show the automation table.
//! - `automate add <vault>: --watch | --every 1h` → persist a `Watch`/`Every`
//!   automation of the vault's mapped source.
//! - `automate list | show | pause | resume | rm` → manage them.

use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use clap::Subcommand;
use s5_node_api::S5NodeClient;

use crate::interact;
use crate::refs;

#[derive(Subcommand, Debug)]
pub enum AutomateCmd {
    /// Add an automation for a vault (exactly one of `--watch` / `--every`).
    Add {
        /// Target `vault:[path]` (defaults to your most recent backup).
        target: Option<String>,
        /// Automation name (defaults to one derived from the vault + trigger).
        #[arg(long)]
        name: Option<String>,
        /// Snap whenever the mapped source changes (filesystem watch).
        #[arg(long)]
        watch: bool,
        /// Snap on a fixed cadence, e.g. `1h`, `30m`, `15s`.
        #[arg(long, value_name = "DURATION")]
        every: Option<String>,
    },
    /// List configured automations + their live status.
    #[command(alias = "ls")]
    List,
    /// Show one automation in detail.
    Show {
        /// Automation name.
        name: String,
    },
    /// Pause an automation (stays configured, stops running).
    Pause {
        /// Automation name.
        name: String,
    },
    /// Resume a paused automation.
    Resume {
        /// Automation name.
        name: String,
    },
    /// Remove an automation.
    Rm {
        /// Automation name.
        name: String,
    },
}

/// Local view of the chosen trigger, resolved from the CLI flags.
enum Trigger {
    Watch,
    Every(u64),
}

impl Trigger {
    fn wire(&self) -> &'static str {
        match self {
            Trigger::Watch => "watch",
            Trigger::Every(_) => "every",
        }
    }
    fn describe(&self) -> String {
        match self {
            Trigger::Watch => "on every change".to_string(),
            Trigger::Every(secs) => format!("every {}", short_duration(*secs)),
        }
    }
}

/// `vup automate [SUBCOMMAND]`.
pub async fn run_automate(client: &S5NodeClient, cmd: Option<AutomateCmd>) -> Result<()> {
    match cmd {
        None => run_wizard(client).await,
        Some(AutomateCmd::Add {
            target,
            name,
            watch,
            every,
        }) => run_add(client, target, name, watch, every).await,
        Some(AutomateCmd::List) => run_list(client).await,
        Some(AutomateCmd::Show { name }) => run_show(client, &name).await,
        Some(AutomateCmd::Pause { name }) => set_paused(client, &name, true).await,
        Some(AutomateCmd::Resume { name }) => set_paused(client, &name, false).await,
        Some(AutomateCmd::Rm { name }) => run_rm(client, &name).await,
    }
}

// ---------------------------------------------------------------------------
// add
// ---------------------------------------------------------------------------

async fn run_add(
    client: &S5NodeClient,
    target: Option<String>,
    name: Option<String>,
    watch: bool,
    every: Option<String>,
) -> Result<()> {
    let trigger = match (watch, &every) {
        (true, None) => Trigger::Watch,
        (false, Some(s)) => Trigger::Every(parse_every(s)?),
        (false, None) => bail!(
            "choose a trigger: `--watch` (snap on change) or `--every <duration>` (e.g. `--every 1h`)"
        ),
        (true, Some(_)) => bail!("`--watch` and `--every` are mutually exclusive"),
    };

    let config = fetch_config(client).await?;

    // Resolve the target vault (+ optional subtree path).
    let (vault, path) = match target {
        Some(t) => {
            let vref = refs::parse_ref(&t)
                .and_then(|r| r.require_vault())
                .map_err(|e| anyhow!(e))?;
            (vref.name, vref.path)
        }
        None => {
            let status = client.get_status().await?;
            let lb = status.last_backup.ok_or_else(|| {
                anyhow!(
                    "no target given and no recent backup to promote — name a vault \
                     (`vup automate add <vault>: --watch`) or run `vup backup <path> <vault>:` first"
                )
            })?;
            (lb.vault, None)
        }
    };

    let vcfg = config
        .get("vault")
        .and_then(|v| v.get(&vault))
        .ok_or_else(|| anyhow!("vault '{vault}:' not found in config"))?;

    let source = match string_array(vcfg, "sources").as_slice() {
        [s] => s.clone(),
        [] => bail!(
            "vault '{vault}:' has no source mapping to automate — run `vup backup <path> {vault}:` first"
        ),
        many => bail!(
            "vault '{vault}:' has multiple sources ({}); automate a single-source vault \
             (edit with `vup config`)",
            many.join(", ")
        ),
    };
    let recipients = string_array(vcfg, "recipients");
    if recipients.is_empty() {
        bail!(
            "vault '{vault}:' has no recipients configured — without readers the snapshot is \
             pointless. Set `vault.{vault}.recipients`."
        );
    }
    let blob_store = crate::cmd::vault::resolve_data_store(&config, &vault)?;

    let task_name = name.unwrap_or_else(|| default_name(&vault, &trigger));
    if config.get("task").and_then(|t| t.get(&task_name)).is_some() {
        bail!(
            "an automation named '{task_name}' already exists — pass `--name`, or \
             `vup automate rm {task_name}` first"
        );
    }

    let mut value = serde_json::json!({
        "type": "backup",
        "vault": vault,
        "source": source,
        "blob_store": blob_store,
        "keys": recipients,
        "trigger": trigger.wire(),
    });
    if let Trigger::Every(secs) = &trigger {
        value["interval_secs"] = serde_json::json!(secs);
    }
    if let Some(p) = &path {
        value["target_path"] = serde_json::json!(p);
    }

    let ops = serde_json::json!([{
        "op": "add",
        "path": format!("/task/{task_name}"),
        "value": value,
    }]);
    client.patch_config(ops).await?;

    println!(
        "{vault}: automation '{task_name}' added — backing up {} {}.",
        source,
        trigger.describe()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// list / show
// ---------------------------------------------------------------------------

async fn run_list(client: &S5NodeClient) -> Result<()> {
    let config = fetch_config(client).await?;
    let automations = automation_entries(&config);

    if automations.is_empty() {
        println!("No automations configured. Add one with `vup automate add <vault>: --watch`.");
        return Ok(());
    }

    let live = client
        .get_status()
        .await
        .map(|s| s.automations)
        .unwrap_or_default();

    println!("{:<22} {:<14} {:<12} STATUS", "NAME", "VAULT", "TRIGGER");
    for (name, task) in &automations {
        let vault = task_vault_display(task);
        let paused = task
            .get("paused")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let status = if paused {
            "paused".to_string()
        } else {
            match live.iter().find(|a| &a.name == name) {
                Some(a) if a.alive => "running".to_string(),
                Some(a) => format!("down (restarts {})", a.restarts),
                None => "not running".to_string(),
            }
        };
        println!(
            "{:<22} {:<14} {:<12} {}",
            name,
            vault,
            trigger_display(task),
            status
        );
    }
    Ok(())
}

async fn run_show(client: &S5NodeClient, name: &str) -> Result<()> {
    let config = fetch_config(client).await?;
    let task = config
        .get("task")
        .and_then(|m| m.get(name))
        .ok_or_else(|| anyhow!("automation '{name}' not found in config"))?;
    if !is_automation(task) {
        bail!("task '{name}' is not an automation (trigger = manual)");
    }

    println!("{name}:");
    if let Some(v) = task.get("vault").and_then(|v| v.as_str()) {
        println!("  vault:        {v}:");
    }
    if let Some(s) = task.get("source").and_then(|v| v.as_str()) {
        println!("  source:       {s}");
    }
    // Copy automations (`share … --live`) carry src/dst instead of vault/source.
    if let Some(src) = task.get("src_vault").and_then(|v| v.as_str()) {
        let src_path = task.get("src_path").and_then(|v| v.as_str());
        match src_path {
            Some(p) => println!("  copies from:  {src}:{p}"),
            None => println!("  copies from:  {src}:"),
        }
    }
    if let Some(dst) = task.get("dst_vault").and_then(|v| v.as_str()) {
        let dst_path = task.get("dst_path").and_then(|v| v.as_str());
        match dst_path {
            Some(p) => println!("  copies to:    {dst}:{p}"),
            None => println!("  copies to:    {dst}:"),
        }
    }
    println!("  trigger:      {}", trigger_display(task));
    let paused = task
        .get("paused")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    println!("  paused:       {paused}");
    let recipients = string_array(task, "keys");
    if !recipients.is_empty() {
        println!("  recipients:   {}", recipients.join(", "));
    }

    if let Ok(status) = client.get_status().await {
        match status.automations.iter().find(|a| a.name == name) {
            Some(a) => {
                println!(
                    "  status:       {}",
                    if a.alive { "running" } else { "down" }
                );
                if a.restarts > 0 {
                    println!("  restarts:     {}", a.restarts);
                }
                if let Some(err) = &a.last_error {
                    println!("  last error:   {err}");
                }
                if let Some(secs) = a.last_ok_unix {
                    println!("  last backup:  {secs} (unix seconds)");
                }
            }
            None if !paused => println!("  status:       not running"),
            None => {}
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// pause / resume / rm
// ---------------------------------------------------------------------------

async fn set_paused(client: &S5NodeClient, name: &str, paused: bool) -> Result<()> {
    let config = fetch_config(client).await?;
    let task = config
        .get("task")
        .and_then(|m| m.get(name))
        .ok_or_else(|| anyhow!("automation '{name}' not found in config"))?;
    if !is_automation(task) {
        bail!("task '{name}' is not an automation (trigger = manual)");
    }

    let ops = serde_json::json!([{
        "op": "replace",
        "path": format!("/task/{name}/paused"),
        "value": paused,
    }]);
    client.patch_config(ops).await?;
    println!("{name}: {}.", if paused { "paused" } else { "resumed" });
    Ok(())
}

async fn run_rm(client: &S5NodeClient, name: &str) -> Result<()> {
    let config = fetch_config(client).await?;
    if config.get("task").and_then(|m| m.get(name)).is_none() {
        bail!("automation '{name}' not found in config");
    }
    if !interact::confirm(&format!("Remove automation '{name}'?"), true)? {
        println!("Aborted — nothing removed.");
        return Ok(());
    }
    let ops = serde_json::json!([{
        "op": "remove",
        "path": format!("/task/{name}"),
    }]);
    client.patch_config(ops).await?;
    println!("{name}: removed.");
    Ok(())
}

// ---------------------------------------------------------------------------
// wizard (bare `automate`)
// ---------------------------------------------------------------------------

async fn run_wizard(client: &S5NodeClient) -> Result<()> {
    let status = client.get_status().await?;
    let config = fetch_config(client).await?;

    // Offer to promote the most recent one-shot backup, if nothing already
    // automates that vault.
    if let Some(lb) = &status.last_backup {
        let covered = automation_entries(&config)
            .iter()
            .any(|(_, t)| t.get("vault").and_then(|v| v.as_str()) == Some(lb.vault.as_str()));
        if !covered {
            println!(
                "You recently backed up '{}:'. Keep doing that automatically?",
                lb.vault
            );
            let choices = &[
                "On every change (watch)",
                "On a schedule (every…)",
                "No thanks",
            ];
            match interact::select("How should it run?", choices, 0)? {
                0 => {
                    return run_add(client, Some(format!("{}:", lb.vault)), None, true, None).await;
                }
                1 => {
                    let every =
                        interact::input_with_default("Interval (e.g. 1h, 30m)", "1h".to_string())?;
                    return run_add(
                        client,
                        Some(format!("{}:", lb.vault)),
                        None,
                        false,
                        Some(every),
                    )
                    .await;
                }
                _ => {}
            }
            println!();
        }
    }

    // Otherwise show the table.
    run_list(client).await
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

async fn fetch_config(client: &S5NodeClient) -> Result<serde_json::Value> {
    let resp = client.get_config().await?;
    Ok(serde_json::from_str(&resp.config_json)?)
}

/// `(name, task-json)` for every `[task.*]` entry with a non-`Manual` trigger,
/// name-sorted (config is a JSON object; sort for stable output).
fn automation_entries(config: &serde_json::Value) -> Vec<(String, serde_json::Value)> {
    let mut out: Vec<(String, serde_json::Value)> = config
        .get("task")
        .and_then(|t| t.as_object())
        .map(|o| {
            o.iter()
                .filter(|(_, t)| is_automation(t))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
        .unwrap_or_default();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// A task is an automation iff its trigger is present and not `manual`.
fn is_automation(task: &serde_json::Value) -> bool {
    !matches!(
        task.get("trigger").and_then(|v| v.as_str()),
        None | Some("manual")
    )
}

/// The vault an automation targets, for the `list` VAULT column. Backup-shaped
/// tasks carry a `vault`; a `Copy` automation (what `share … --live` writes)
/// has no `vault` — describe it as `src→dst` so it isn't a bare `?`.
fn task_vault_display(task: &serde_json::Value) -> String {
    if let Some(v) = task.get("vault").and_then(|v| v.as_str()) {
        return v.to_string();
    }
    match (
        task.get("src_vault").and_then(|v| v.as_str()),
        task.get("dst_vault").and_then(|v| v.as_str()),
    ) {
        (Some(src), Some(dst)) => format!("{src}→{dst}"),
        (Some(src), None) => src.to_string(),
        (None, Some(dst)) => dst.to_string(),
        (None, None) => "?".to_string(),
    }
}

/// Render a task's trigger for display (`watch` / `every 1h`).
fn trigger_display(task: &serde_json::Value) -> String {
    match task.get("trigger").and_then(|v| v.as_str()) {
        Some("every") => task
            .get("interval_secs")
            .and_then(|v| v.as_u64())
            .map(|s| format!("every {}", short_duration(s)))
            .unwrap_or_else(|| "every".to_string()),
        Some(other) => other.to_string(),
        None => "manual".to_string(),
    }
}

fn default_name(vault: &str, trigger: &Trigger) -> String {
    match trigger {
        Trigger::Watch => format!("{vault}-watch"),
        Trigger::Every(secs) => format!("{vault}-{}", short_duration(*secs)),
    }
}

/// `--every 1h` → seconds. Rejects zero / unparseable durations.
fn parse_every(s: &str) -> Result<u64> {
    let d = humantime::parse_duration(s)
        .map_err(|e| anyhow!("invalid `--every` duration '{s}': {e}"))?;
    let secs = d.as_secs();
    if secs == 0 {
        bail!("`--every` must be at least one second");
    }
    Ok(secs)
}

/// Compact cadence string (`3600` → `1h`), spaces stripped so it is safe in a
/// task name.
fn short_duration(secs: u64) -> String {
    humantime::format_duration(Duration::from_secs(secs))
        .to_string()
        .replace(' ', "")
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
