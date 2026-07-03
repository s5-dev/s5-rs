//! Top-level `list` + the `vault create|drop|rename` namespace.
//!
//! All do their work via JSON Patches against the daemon's config
//! (the CLI is an RPC frontend — the daemon holds the live config and
//! applies the mutations).

use anyhow::{Result, bail};
use s5_node_api::S5NodeClient;

use crate::refs::{self, validate_user_vault_name};

/// `vup list` (no ref) — vaults + stores overview. `_system` vaults are
/// hidden unless `--all`.
pub async fn run_list(client: &S5NodeClient, all: bool) -> Result<()> {
    let config_resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&config_resp.config_json)?;

    // -- Vaults --
    let vaults = config.get("vault").and_then(|v| v.as_object());
    let shown: Vec<(&String, &serde_json::Value)> = vaults
        .map(|o| {
            o.iter()
                .filter(|(name, _)| all || !refs::is_system_vault(name))
                .collect()
        })
        .unwrap_or_default();

    if shown.is_empty() {
        println!("(no vaults — run `vup backup <path> <name>:` to create one)");
    } else {
        println!("Vaults:");
        for (name, vault) in &shown {
            let recipients = vault
                .get("recipients")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            let store = super::vault::resolve_data_store(&config, name)
                .unwrap_or_else(|_| "(unresolved!)".to_string());
            println!("  {name}:  recipients={recipients}  store={store}");
        }
    }

    // -- Stores --
    if let Some(stores) = config
        .get("store")
        .and_then(|v| v.as_object())
        .filter(|o| !o.is_empty())
    {
        println!("\nStores:");
        for (name, store) in stores {
            let stype = store.get("type").and_then(|v| v.as_str()).unwrap_or("?");
            println!("  {name} ({stype})");
        }
    }

    Ok(())
}

/// `vup list vault:[path][#snap]` — a vault's contents as an indented tree.
///
/// `path` scopes to a subtree; `snap` lists a past snapshot. The daemon
/// returns a flat `(path, is_dir, size)` listing in prolly-tree key order; we
/// sort it dirs-before-files at each level and render it with indentation.
pub async fn run_list_tree(
    client: &S5NodeClient,
    vault: &str,
    path: Option<String>,
    snap: Option<String>,
) -> Result<()> {
    // Echo the resolved subject on line 1 (D20).
    let subject = match (&path, &snap) {
        (Some(p), Some(s)) => format!("{vault}:{p}#{s}"),
        (Some(p), None) => format!("{vault}:{p}"),
        (None, Some(s)) => format!("{vault}:#{s}"),
        (None, None) => format!("{vault}:"),
    };

    let resp = client.list_tree(vault, snap, path, None).await?;

    if resp.entries.is_empty() {
        println!("{subject}  (empty)");
        return Ok(());
    }

    println!("{subject}");

    // Sort dirs-before-files within each directory level while keeping every
    // parent ahead of its children: compare the paths component-by-component,
    // and at the entry's own last component rank directories (0) before files
    // (1). Intermediate components are always directories, so they rank 0.
    let mut entries = resp.entries;
    entries.sort_by(|a, b| tree_sort_key(&a.path, a.is_dir).cmp(&tree_sort_key(&b.path, b.is_dir)));

    let mut files = 0u64;
    let mut dirs = 0u64;
    let mut total = 0u64;
    for e in &entries {
        let depth = e.path.split('/').count();
        let indent = "  ".repeat(depth.saturating_sub(1));
        let name = e.path.rsplit('/').next().unwrap_or(&e.path);
        if e.is_dir {
            dirs += 1;
            println!("  {indent}{name}/");
        } else {
            files += 1;
            total += e.size;
            let size = humansize::format_size(e.size, humansize::BINARY);
            println!("  {indent}{name}  ({size})");
        }
    }

    println!(
        "\n{files} file(s), {dirs} dir(s), {} total",
        humansize::format_size(total, humansize::BINARY)
    );
    Ok(())
}

/// Build the tree sort key for a path: `[(0,c0), (0,c1), …, (rank,last)]`
/// where `rank` is 0 for a directory entry and 1 for a file. Lexicographic
/// comparison of these keys yields parents-before-children with
/// dirs-before-files at each level.
fn tree_sort_key(path: &str, is_dir: bool) -> Vec<(u8, &str)> {
    let comps: Vec<&str> = path.split('/').collect();
    let last = comps.len().saturating_sub(1);
    comps
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let rank = if i == last && !is_dir { 1 } else { 0 };
            (rank, *c)
        })
        .collect()
}

/// `vup vault create <name>` — create a vault with sane defaults.
///
/// Defaults: `key = "main"` (+ `recovery` recipient if configured), empty
/// sources; stores resolve to the node default (D1). The rest is set via
/// `vup config` or a `backup` mapping.
pub async fn run_vault_create(client: &S5NodeClient, name: &str) -> Result<()> {
    create_vault(client, name).await?;
    println!("{name}: created. Next:");
    println!("  vup backup <path> {name}:     start backing paths up");
    println!("  vup config --json             review the full config");
    Ok(())
}

/// Ensure a vault exists, offering to create it on a TTY. Used by
/// `backup <path> <name>:` for the zero-ceremony first backup (D20 §B1).
pub(crate) async fn ensure_vault(client: &S5NodeClient, name: &str) -> Result<()> {
    let config_resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&config_resp.config_json)?;
    if config.get("vault").and_then(|v| v.get(name)).is_some() {
        return Ok(());
    }
    if !crate::interact::confirm(&format!("Vault '{name}:' does not exist. Create it?"), true)? {
        bail!("aborted — vault '{name}:' not created");
    }
    create_vault(client, name).await
}

/// Core create: validate, refuse duplicates, patch `/vault/<name>` with
/// defaults. Shared by `vault create`, `ensure_vault`, and the `share`
/// subtree composition (which mints a share-vault).
pub(crate) async fn create_vault(client: &S5NodeClient, name: &str) -> Result<()> {
    validate_user_vault_name(name).map_err(|e| anyhow::anyhow!(e))?;

    let config_resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&config_resp.config_json)?;
    if config.get("vault").and_then(|v| v.get(name)).is_some() {
        bail!("vault '{name}:' already exists");
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

    let dirs = directories::ProjectDirs::from("pro", "s5", "s5")
        .ok_or_else(|| anyhow::anyhow!("could not determine application data directory"))?;
    let vault_root = dirs
        .data_dir()
        .join("vaults")
        .join(name)
        .display()
        .to_string();

    let patch = serde_json::json!([{
        "op": "add",
        "path": format!("/vault/{name}"),
        "value": {
            "root_path": vault_root,
            "key": default_key,
            "recipients": recipients,
            "sources": [],
        }
    }]);

    client.patch_config(patch).await?;
    Ok(())
}

/// `vup vault drop <name>` — remove a vault config entry. The on-disk
/// vault data (root_path/blob stores) is NOT touched — only the config
/// knob is dropped, so re-adding the vault later picks up existing state.
pub async fn run_vault_drop(client: &S5NodeClient, name: &str) -> Result<()> {
    let config_resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&config_resp.config_json)?;
    if config.get("vault").and_then(|v| v.get(name)).is_none() {
        bail!("vault '{name}:' not found in config");
    }

    let patch = serde_json::json!([{
        "op": "remove",
        "path": format!("/vault/{name}"),
    }]);

    client.patch_config(patch).await?;
    println!("{name}: dropped from config (on-disk data not touched).");
    Ok(())
}

/// `vup vault rename <old> <new>` — rename a vault config entry via an
/// atomic RFC-6902 `move`. On-disk data (root_path) is untouched, so the
/// renamed vault keeps reading its existing state.
pub async fn run_vault_rename(client: &S5NodeClient, old: &str, new: &str) -> Result<()> {
    validate_user_vault_name(new).map_err(|e| anyhow::anyhow!(e))?;

    let config_resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&config_resp.config_json)?;
    if config.get("vault").and_then(|v| v.get(old)).is_none() {
        bail!("vault '{old}:' not found in config");
    }
    if config.get("vault").and_then(|v| v.get(new)).is_some() {
        bail!("vault '{new}:' already exists");
    }

    let patch = serde_json::json!([{
        "op": "move",
        "from": format!("/vault/{old}"),
        "path": format!("/vault/{new}"),
    }]);

    client.patch_config(patch).await?;
    println!("{old}: → {new}:");
    Ok(())
}
