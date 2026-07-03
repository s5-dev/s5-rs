//! `vup backup [SRC…] vault:[path]` — fidelity-in snapshot (D20/D21).
//!
//! The trio's fidelity-in verb: capture perms/mtimes/symlinks into an
//! incremental, published snapshot. Supersedes the old `snap` + `add`
//! pair (both gone). Semantics (`docs/reference/cli-redesign-d20.md` §B1):
//!
//! - With SRC paths + a `vault:` ref → persist a source→vault mapping (the
//!   artifact `automate` later promotes) and run one backup.
//! - `backup vault:` with no SRC → re-run that vault's persisted mapping.
//! - Bare `backup` → re-run every vault's persisted mapping (all vaults).
//! - SRC given but no vault ref → error (no auto-created vaults).

use anyhow::{Result, anyhow, bail};
use s5_node_api::S5NodeClient;

use crate::refs;

/// `vup backup [SRC…] vault:[path]`.
pub async fn run_backup(client: &S5NodeClient, args: &[String]) -> Result<()> {
    let (srcs, dest) = refs::split_backup_args(args).map_err(|e| anyhow!(e))?;

    if !srcs.is_empty() {
        let Some(dest) = dest else {
            bail!(
                "backup needs a destination vault — e.g. `vup backup {} docs:` \
                 (no auto-created vaults)",
                srcs[0].display()
            );
        };
        // Zero-ceremony first backup: offer to create the vault if missing
        // (TTY), then persist the mapping and run once.
        crate::cmd::lifecycle::ensure_vault(client, &dest.name).await?;
        crate::cmd::vault::persist_source_paths(client, &dest.name, &srcs).await?;
        return crate::cmd::vault::run_backup_mapped(client, &dest.name, dest.path.as_deref())
            .await;
    }

    // No SRC: re-run persisted mapping(s).
    match dest {
        Some(dest) => backup_persisted(client, Some(&dest.name)).await,
        None => backup_persisted(client, None).await,
    }
}

/// Re-run persisted source→vault mappings. `vault = Some` scopes to one
/// vault (and errors if it has no mapping yet); `None` runs every vault
/// that has a mapping, one echo line each.
async fn backup_persisted(client: &S5NodeClient, vault: Option<&str>) -> Result<()> {
    let resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&resp.config_json)?;

    let vaults: Vec<String> = match vault {
        Some(v) => vec![v.to_string()],
        None => config
            .get("vault")
            .and_then(|v| v.as_object())
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default(),
    };

    if vaults.is_empty() {
        println!("No vaults configured. Run `vup backup <path> <name>:` to create the first one.");
        return Ok(());
    }

    let mut ran = 0usize;
    for v in &vaults {
        let has_source = config
            .get("vault")
            .and_then(|m| m.get(v))
            .and_then(|vc| vc.get("sources"))
            .and_then(|s| s.as_array())
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        if !has_source {
            if vault.is_some() {
                bail!(
                    "vault '{v}:' has no backup mapping yet — run `vup backup <path> {v}:` first"
                );
            }
            continue;
        }
        crate::cmd::vault::run_backup_mapped(client, v, None).await?;
        ran += 1;
    }

    if ran == 0 && vault.is_none() {
        println!("No vaults have a backup mapping yet. Run `vup backup <path> <name>:`.");
    }
    Ok(())
}
