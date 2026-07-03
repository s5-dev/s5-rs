//! `vup copy SRC DST [--deep]` — copy contents between any refs.
//!
//! Three directions, routed by [`run`]:
//!
//! * **vault → vault** — the D21 sharing primitive. A **shallow** copy
//!   (default) re-homes the source's leaf ciphertext into the destination vault
//!   and inlines each leaf's per-blob key; the source master data key is never
//!   shared. Because that discloses the source data to whoever can read the
//!   destination, it computes the reader-set difference and — when the
//!   destination has readers the source lacks — refuses to widen silently
//!   (prints the delta, asks for confirmation, or honours global `--yes`).
//!   `--deep` re-encrypts under the destination's own keys, so nothing widens.
//! * **vault → local** — get files/versions out (`copy docs:report.md#3 ./`):
//!   reuses the restore task, so the contents land as plain files with their
//!   RECORDED metadata (perms/mtimes/symlinks). The D20 spec's "fresh metadata
//!   like cp" is a deferred nuance.
//! * **local → vault** — `backup` without a persisted mapping: ingest the paths
//!   into the vault (optionally under a subpath), tracked by nothing.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use s5_node_api::S5NodeClient;
use s5_node_api::config::TaskSpec;

use crate::refs::{Ref, VaultRef};

/// Route `copy SRC DST` by the kinds of the two refs.
pub async fn run(client: &S5NodeClient, src: Ref, dst: Ref, deep: bool) -> Result<()> {
    match (src, dst) {
        (
            Ref::Vault { name, path, snap },
            Ref::Vault {
                name: dn,
                path: dp,
                snap: ds,
            },
        ) => {
            if ds.is_some() {
                bail!("cannot copy INTO a snapshot (`{dn}:#…`) — a snapshot is immutable");
            }
            let s = VaultRef { name, path, snap };
            let d = VaultRef {
                name: dn,
                path: dp,
                snap: ds,
            };
            run_copy(client, &s, &d, deep).await
        }
        (Ref::Vault { name, path, snap }, Ref::Local(dir)) => {
            let s = VaultRef { name, path, snap };
            copy_out(client, &s, &dir, deep).await
        }
        (Ref::Local(p), Ref::Vault { name, path, snap }) => {
            if snap.is_some() {
                bail!("cannot copy INTO a snapshot (`{name}:#…`) — a snapshot is immutable");
            }
            let d = VaultRef { name, path, snap };
            copy_in(client, &[p], &d, deep).await
        }
        (Ref::Local(_), Ref::Local(_)) => {
            bail!("both SRC and DST are local paths — use `cp` for local-to-local copies")
        }
        (_, Ref::Hash(_)) => bail!("cannot copy INTO an immutable `#snapshot`"),
        (Ref::Hash(h), _) => bail!(
            "copying FROM a bare `#{h}` isn't wired yet — reach it through its vault, \
             e.g. `vup copy <vault>:<path>#<snap> …`"
        ),
    }
}

/// vault → local: write the source contents (at an optional subtree/snapshot)
/// out as plain files, `cp`-style. Reuses the restore task; no metadata-fidelity
/// promise and no non-empty-target guard (unlike `restore`).
async fn copy_out(client: &S5NodeClient, src: &VaultRef, target: &Path, deep: bool) -> Result<()> {
    if deep {
        bail!(
            "--deep applies only to vault→vault copies; copying OUT always writes plaintext files"
        );
    }
    std::fs::create_dir_all(target).with_context(|| format!("creating '{}'", target.display()))?;
    let src_scope = scope(&src.name, src.path.as_deref(), src.snap.as_deref());
    println!("{src_scope} → {} (contents out)", target.display());
    crate::cmd::tasks::run_restore_task(
        client,
        &src.name,
        &target.display().to_string(),
        None,
        src.snap.as_deref(),
        src.path.as_deref(),
    )
    .await
}

/// local → vault: ingest the paths into the vault (optionally under a subpath),
/// with NO persisted source mapping — `backup` minus the tracking. Implemented
/// by staging a transient `[source.*]` (never registered in `vault.sources`),
/// running one Backup, then removing it.
async fn copy_in(
    client: &S5NodeClient,
    srcs: &[PathBuf],
    dst: &VaultRef,
    deep: bool,
) -> Result<()> {
    if deep {
        bail!("--deep applies only to vault→vault copies");
    }

    let mut abs = Vec::new();
    for p in srcs {
        let a = std::fs::canonicalize(p)
            .map_err(|e| anyhow!("cannot resolve source path '{}': {e}", p.display()))?;
        abs.push(a.to_string_lossy().to_string());
    }

    let resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&resp.config_json)?;
    let recipients = vault_recipients(&config, &dst.name)?;
    if recipients.is_empty() {
        bail!(
            "vault '{}:' has no recipients configured — nothing could read the ingested data.",
            dst.name
        );
    }
    let blob_store = crate::cmd::vault::resolve_data_store(&config, &dst.name)?;

    // Transient source — refuse to clobber a real one on the astronomically
    // unlikely name clash rather than silently overwrite it.
    let tmp_source = format!("copy-in-{}", dst.name);
    if config
        .get("source")
        .and_then(|s| s.get(&tmp_source))
        .is_some()
    {
        bail!(
            "a source named '{tmp_source}' already exists — remove it (`vup config`) and retry, \
             or use `vup backup` for a tracked ingest."
        );
    }

    let add = serde_json::json!([{
        "op": "add",
        "path": format!("/source/{tmp_source}"),
        "value": { "paths": abs, "exclude": [] },
    }]);
    client.patch_config(add).await?;

    let dst_scope = scope(&dst.name, dst.path.as_deref(), None);
    println!(
        "{} → {dst_scope} (contents in, untracked)",
        srcs.iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Run the ingest; the transient source must outlive the task, so remove it
    // only after the task settles (success or failure).
    let spec = TaskSpec::Backup {
        vault: dst.name.clone(),
        source: tmp_source.clone(),
        blob_store,
        keys: recipients,
        target_path: dst.path.clone(),
        changed_paths: None,
    };
    let run_res = async {
        let r = client.run_task(spec).await?;
        println!("{}: copy started (task id={})", dst.name, r.task_id);
        crate::cmd::tasks::poll_until_done(client, r.task_id).await
    }
    .await;

    let rm = serde_json::json!([{ "op": "remove", "path": format!("/source/{tmp_source}") }]);
    if let Err(e) = client.patch_config(rm).await {
        eprintln!("warning: could not remove transient source '{tmp_source}': {e}");
    }

    run_res
}

/// `vup copy SRC DST [--deep]`.
pub async fn run_copy(
    client: &S5NodeClient,
    src: &VaultRef,
    dst: &VaultRef,
    deep: bool,
) -> Result<()> {
    if dst.snap.is_some() {
        bail!(
            "cannot copy INTO a snapshot (`{}:#…`) — a snapshot is immutable; drop the `#…`",
            dst.name
        );
    }

    let resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&resp.config_json)?;

    // Destination recipients (key names) — published-to + the reader set that
    // shallow-inlined keys become visible to. Also the vault's data store.
    let dst_recipients = vault_recipients(&config, &dst.name)?;
    if dst_recipients.is_empty() {
        bail!(
            "vault '{}:' has no recipients configured — a copy with no readers is pointless. \
             Set `vault.{}.recipients` to at least one [key.*] entry.",
            dst.name,
            dst.name
        );
    }
    let blob_store = crate::cmd::vault::resolve_data_store(&config, &dst.name)?;
    let src_recipients = vault_recipients(&config, &src.name)?;

    // Echo the resolved copy on line 1 (D20).
    let src_scope = scope(&src.name, src.path.as_deref(), src.snap.as_deref());
    let dst_scope = scope(&dst.name, dst.path.as_deref(), None);
    let mode = if deep { "deep" } else { "shallow" };
    println!("{src_scope} → {dst_scope} ({mode} copy)");

    // Reader-set diff (shallow only — a deep copy re-keys so it can't widen).
    let mut confirm_widen = deep;
    if !deep {
        let src_set = to_pubkeys(&config, &src_recipients);
        let dst_set = to_pubkeys(&config, &dst_recipients);
        let widened: Vec<String> = dst_set.difference(&src_set).cloned().collect();
        if !widened.is_empty() {
            println!();
            println!(
                "  Shallow copy WIDENS who can read the source data: '{}:' has {} reader(s) that",
                dst.name,
                widened.len()
            );
            println!(
                "  '{}:' does not. The inlined per-blob keys let every '{}:' reader decrypt the",
                src.name, dst.name
            );
            println!(
                "  copied data. (Use `--deep` to re-encrypt under the destination's keys instead.)"
            );
            println!();
            confirm_widen = crate::interact::confirm("Proceed and widen the reader set?", false)?;
            if !confirm_widen {
                bail!("aborted — reader set not widened");
            }
        }
    }

    let spec = TaskSpec::Copy {
        src_vault: src.name.clone(),
        src_path: src.path.clone(),
        src_snap: src.snap.clone(),
        dst_vault: dst.name.clone(),
        dst_path: dst.path.clone(),
        blob_store,
        keys: dst_recipients,
        deep,
        confirm_widen,
    };

    let resp = client.run_task(spec).await?;
    println!("{}: copy started (task id={})", dst.name, resp.task_id);
    crate::cmd::tasks::poll_until_done(client, resp.task_id).await
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn scope(name: &str, path: Option<&str>, snap: Option<&str>) -> String {
    match (path, snap) {
        (Some(p), Some(s)) => format!("{name}:{p}#{s}"),
        (Some(p), None) => format!("{name}:{p}"),
        (None, Some(s)) => format!("{name}:#{s}"),
        (None, None) => format!("{name}:"),
    }
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

/// Map `[key.*]` names to their age recipient public keys (set — order and
/// duplicates don't matter for the reader-set comparison).
fn to_pubkeys(config: &serde_json::Value, key_names: &[String]) -> BTreeSet<String> {
    key_names
        .iter()
        .filter_map(|n| {
            config
                .get("key")
                .and_then(|k| k.get(n))
                .and_then(|k| k.get("public_key"))
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .collect()
}
