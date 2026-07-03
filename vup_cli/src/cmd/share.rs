//! `vup share REF` — sharing (D21: sharing composes from vault + copy +
//! automation; there is no bespoke subtree-share crypto path).
//!
//! Two shapes:
//!
//! * **Whole vault** (`share docs:`) — the frozen anonymous `export`: the
//!   recipient gets exactly the current snapshot, no future updates, no
//!   individual revocation. Unchanged since Stage 1.
//! * **Subtree** (`share docs:Photos`) — the D21 composition sugar,
//!   orchestrated entirely from existing RPCs (it is *not* a new mechanism):
//!     1. mint a **share-vault** (`vault create`, reusing
//!        [`lifecycle::create_vault`]),
//!     2. **`copy`** the subtree into it (shallow by default, `--deep` to
//!        re-encrypt — [`copy::run_copy`]),
//!     3. optionally **automate** an `Every` copy so the share stays live,
//!     4. **export** the share-vault → a share URL.
//!
//!   The share-vault is the unit that carries the shared keys/membership, so
//!   "revoke" = re-key THAT vault (never your source). The sugar bottoms out
//!   in vault + copy + automation by construction.

use anyhow::{Context, Result, anyhow, bail};
use s5_node_api::S5NodeClient;

use crate::interact;
use crate::refs::{VaultRef, validate_user_vault_name};

/// Options parsed from the `share` verb (the composition flags only bite on a
/// subtree share).
#[derive(Debug, Default)]
pub struct ShareOpts {
    /// `--deep`: deep-copy the subtree (re-encrypt under the share-vault's own
    /// keys → true future-revocability) instead of the shallow default.
    pub deep: bool,
    /// `--name`: explicit share-vault name (else derived from the ref).
    pub name: Option<String>,
    /// `--live`: keep the share updated with an `Every` copy automation.
    pub live: bool,
    /// `--every DURATION`: cadence for `--live` (default `1h`).
    pub every: Option<String>,
}

/// `vup share vault:[path][#snap]`.
pub async fn run_share(client: &S5NodeClient, vref: &VaultRef, opts: ShareOpts) -> Result<()> {
    match &vref.path {
        // -- Whole-vault frozen export (the original `export`) --------------
        None => {
            if let Some(snap) = &vref.snap {
                bail!(
                    "a whole-vault share is a frozen export of the CURRENT snapshot — it can't \
                     pin a past one (`{}:#{snap}`). Share a subtree at a snapshot instead \
                     (`vup share {}:<path>#{snap}`), or export now (`vup share {}:`).",
                    vref.name,
                    vref.name,
                    vref.name
                );
            }
            if opts.deep || opts.name.is_some() || opts.live || opts.every.is_some() {
                bail!(
                    "--deep / --name / --live / --every apply to a SUBTREE share \
                     (`vup share {}:<path>`), which composes a share-vault + copy + automation. \
                     A whole-vault share is just a frozen export.",
                    vref.name
                );
            }
            crate::cmd::vault::run_export(client, &vref.name, None).await
        }
        // -- Subtree share = the D21 composition sugar ---------------------
        Some(path) => share_subtree(client, vref, path, opts).await,
    }
}

/// Build (or refresh) a subtree share: share-vault ← copy ← (optional)
/// automation ← export.
async fn share_subtree(
    client: &S5NodeClient,
    vref: &VaultRef,
    path: &str,
    opts: ShareOpts,
) -> Result<()> {
    let src_scope = match &vref.snap {
        Some(s) => format!("{}:{path}#{s}", vref.name),
        None => format!("{}:{path}", vref.name),
    };

    print_honesty_warnings();

    // -- 1. Resolve + mint the share-vault -----------------------------------
    let default_name = derive_share_vault_name(&vref.name, path);
    let share_name = match &opts.name {
        Some(n) => crate::refs::strip_plus(n.trim_end_matches(':')),
        None => interact::input_with_default("Share-vault name", default_name)?,
    };
    validate_user_vault_name(&share_name).map_err(|e| anyhow!(e))?;
    if share_name == vref.name {
        bail!(
            "the share-vault must be a DIFFERENT vault from the source '{}:' — a share composes a \
             separate vault so revoking it never touches your source. Pick another `--name`.",
            vref.name
        );
    }

    let already_exists = {
        let resp = client.get_config().await?;
        let config: serde_json::Value = serde_json::from_str(&resp.config_json)?;
        config
            .get("vault")
            .and_then(|v| v.get(&share_name))
            .is_some()
    };
    if already_exists {
        println!("{share_name}: reusing existing share-vault.");
    } else {
        crate::cmd::lifecycle::create_vault(client, &share_name).await?;
        println!("{share_name}: created share-vault.");
    }

    // -- 2. Copy the subtree into the share-vault ----------------------------
    // This reuses the `copy` verb wholesale (the D21 composition operator): it
    // prints the resolved copy, computes the reader-set diff, and — on a shallow
    // copy that widens who can decrypt — prompts (or honours `--yes`) before
    // proceeding. Aborting there aborts the whole share.
    let src_ref = VaultRef {
        name: vref.name.clone(),
        path: Some(path.to_string()),
        snap: vref.snap.clone(),
    };
    let dst_ref = VaultRef {
        name: share_name.clone(),
        path: None,
        snap: None,
    };
    println!();
    crate::cmd::copy::run_copy(client, &src_ref, &dst_ref, opts.deep).await?;

    // -- 3. Optionally keep it live with an `Every` copy automation ----------
    // A pinned snapshot can't be "kept updated", so the live offer is skipped
    // (and refused if explicitly asked for) when the source ref carries `#snap`.
    let pinned = vref.snap.is_some();
    let want_live = if pinned {
        if opts.live {
            bail!(
                "can't keep a pinned-snapshot share updated (`{src_scope}` names one snapshot). \
                 Drop the `#snap` for a live share, or omit `--live` for a frozen one."
            );
        }
        false
    } else if opts.live || opts.every.is_some() {
        true
    } else {
        interact::confirm("Keep the share updated as the source changes?", false)?
    };

    if want_live {
        automate_live_copy(client, vref, path, &share_name, &opts).await?;
    }

    // -- 4. Export the share-vault → a share URL -----------------------------
    println!();
    crate::cmd::vault::run_export(client, &share_name, None).await?;

    if !want_live {
        println!();
        println!(
            "  This share is frozen at the copy above. Re-run `vup share {src_scope}` to refresh \
             it, or add `--live` to keep it updated automatically."
        );
    }
    Ok(())
}

/// Persist an `Every` copy automation so the share-vault re-mirrors the source
/// subtree on a cadence (the D21 "live share link"). Pure `patch_config` on
/// `/task/*` — no dedicated RPC, exactly like `automate add`.
///
/// A `Watch` trigger only applies to a `Backup` (the daemon watches source
/// *paths*; a copy has none), so a live subtree share is a scheduled copy. The
/// automation carries `confirm_widen = true`: the human just reviewed and
/// approved this exact disclosure interactively, so the unattended re-runs
/// repeat it without re-prompting.
async fn automate_live_copy(
    client: &S5NodeClient,
    vref: &VaultRef,
    path: &str,
    share_name: &str,
    opts: &ShareOpts,
) -> Result<()> {
    let interval_secs = parse_every(opts.every.as_deref().unwrap_or("1h"))?;

    let resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&resp.config_json)?;
    let blob_store = crate::cmd::vault::resolve_data_store(&config, share_name)?;
    let keys = vault_recipients(&config, share_name)?;
    if keys.is_empty() {
        bail!("share-vault '{share_name}:' has no recipients — cannot automate a copy into it");
    }

    let task_name = format!("{share_name}-sync");
    if config.get("task").and_then(|t| t.get(&task_name)).is_some() {
        println!("{task_name}: live-copy automation already configured (leaving it in place).");
        return Ok(());
    }

    let value = serde_json::json!({
        "type": "copy",
        "src_vault": vref.name,
        "src_path": path,
        "dst_vault": share_name,
        "blob_store": blob_store,
        "keys": keys,
        "deep": opts.deep,
        // The interactive copy above already gated any reader-set widening.
        "confirm_widen": true,
        "trigger": "every",
        "interval_secs": interval_secs,
    });
    let ops = serde_json::json!([{
        "op": "add",
        "path": format!("/task/{task_name}"),
        "value": value,
    }]);
    client.patch_config(ops).await?;
    println!(
        "{task_name}: live-copy automation added — re-copying {}:{path} into {share_name}: every {}.",
        vref.name,
        humantime::format_duration(std::time::Duration::from_secs(interval_secs))
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Honesty warnings (D21) — printed verbatim before a
// subtree share composes.
// ---------------------------------------------------------------------------

fn print_honesty_warnings() {
    println!(
        "Sharing a subtree composes a share-vault + copy + automation. Three honesty facts (D21):"
    );
    println!();
    println!(
        "  • writer ⊆ reader — there is NO write-only capability. A blind multi-writer\n\
         \x20   dropbox is N+1 vaults (each submitter owns one and grants you read), never a\n\
         \x20   single shared folder."
    );
    println!(
        "  • A shallow copy across a trust boundary WIDENS who can decrypt: every reader of\n\
         \x20   the share-vault can decrypt the copied data. You'll confirm (or pass --yes) if\n\
         \x20   the share-vault's recipient set differs from the source's."
    );
    println!(
        "  • \"Revoke\" re-keys the share-vault's METADATA (killing discovery + future\n\
         \x20   additions) — it does NOT re-key blobs already disclosed. Use `share --deep` for\n\
         \x20   true future-revocability."
    );
    println!();
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Derive a default share-vault name from `src:path`, e.g. `docs` + `Photos` →
/// `docs-photos`, `docs` + `Photos/2024` → `docs-photos-2024`. Lower-cased,
/// non-`[a-z0-9._-]` runs collapsed to a single `-`. Always prefixed by the
/// source vault name, so it starts with a valid leading char.
fn derive_share_vault_name(src_vault: &str, path: &str) -> String {
    let mut out = String::from(src_vault);
    let mut dash = true; // already ends on the vault name; suppress a leading '-'
    for c in path.chars() {
        let lc = c.to_ascii_lowercase();
        if lc.is_ascii_lowercase() || lc.is_ascii_digit() || lc == '.' || lc == '_' {
            if dash {
                out.push('-');
                dash = false;
            }
            out.push(lc);
        } else {
            // '/', spaces, uppercase-mapped punctuation, etc. → a single dash.
            dash = true;
        }
    }
    // Never leave a trailing dash; cap length for sanity.
    let trimmed = out.trim_end_matches('-');
    trimmed.chars().take(48).collect::<String>()
}

/// A vault's configured recipient key names (`vault.<name>.recipients`).
fn vault_recipients(config: &serde_json::Value, vault: &str) -> Result<Vec<String>> {
    let cfg = config
        .get("vault")
        .and_then(|v| v.get(vault))
        .ok_or_else(|| anyhow!("vault '{vault}:' not found in config"))?;
    Ok(cfg
        .get("recipients")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default())
}

/// `--every 1h` → seconds. Rejects zero / unparseable durations.
fn parse_every(s: &str) -> Result<u64> {
    let d = humantime::parse_duration(s)
        .with_context(|| format!("invalid `--every` duration '{s}'"))?;
    let secs = d.as_secs();
    if secs == 0 {
        bail!("`--every` must be at least one second");
    }
    Ok(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_default_share_names() {
        assert_eq!(derive_share_vault_name("docs", "Photos"), "docs-photos");
        assert_eq!(
            derive_share_vault_name("docs", "Photos/2024"),
            "docs-photos-2024"
        );
        assert_eq!(
            derive_share_vault_name("work", "Reports/Q3 final"),
            "work-reports-q3-final"
        );
        // Trailing slash / punctuation never yields a trailing or doubled dash.
        assert_eq!(derive_share_vault_name("docs", "Photos/"), "docs-photos");
        // The result is always a valid user vault name.
        assert!(validate_user_vault_name(&derive_share_vault_name("docs", "Photos")).is_ok());
        assert!(
            validate_user_vault_name(&derive_share_vault_name("work", "Reports/Q3 final")).is_ok()
        );
    }

    #[test]
    fn parse_every_rejects_zero_and_garbage() {
        assert_eq!(parse_every("1h").unwrap(), 3600);
        assert_eq!(parse_every("30m").unwrap(), 1800);
        assert!(parse_every("0s").is_err());
        assert!(parse_every("soon").is_err());
    }
}
