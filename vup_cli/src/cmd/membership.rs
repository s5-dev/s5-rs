//! Membership verbs: `who`, `revoke`, `friend list`, `friend forget`
//! (D16 vocabulary; `kick`/`peers`/`unpair` live on as hidden aliases).
//!
//! All four are pure config read/patch operations against the daemon (the
//! same model as `list`/`vault create`/`vault drop`): the daemon holds the live config, and a
//! `patch_config` that touches `friend` or `vault.<>.members` triggers a
//! membership rebuild (`notify_membership_refresh`), so a `revoke`/`forget`
//! takes effect on the ACL/signer sets going forward — decision D3: removal
//! from the vault drops the identity from `acl_keys[]` (honest nodes stop
//! serving it blobs) and `signers[]` (its future registry writes are no longer
//! accepted). Content keys are NOT rotated; already-fetched ciphertext stays
//! readable — the restic-comparable threat model, documented in
//! `docs/reference/acl-and-revocation.md`.

use anyhow::{Result, anyhow, bail};
use s5_node_api::S5NodeClient;

/// Read the full node config as JSON.
async fn get_config_json(client: &S5NodeClient) -> Result<serde_json::Value> {
    let resp = client.get_config().await?;
    Ok(serde_json::from_str(&resp.config_json)?)
}

/// `vup who <vault>:` — list the vault's members and their DIDs.
pub async fn run_who(client: &S5NodeClient, vault: &str) -> Result<()> {
    let config = get_config_json(client).await?;
    let vault_obj = config
        .get("vault")
        .and_then(|v| v.get(vault))
        .ok_or_else(|| anyhow!("vault '{vault}' not found (see `vup list`)"))?;

    let members = vault_obj
        .get("members")
        .and_then(|m| m.as_array())
        .cloned()
        .unwrap_or_default();

    if members.is_empty() {
        println!("{vault}: has no members — it's local-only.");
        println!("(pair a friend with `vup friend pair`, then `vup grant {vault}: @<name>`)");
        return Ok(());
    }

    let friends = config.get("friend").and_then(|f| f.as_object());
    let writers: Vec<&str> = vault_obj
        .get("writers")
        .and_then(|w| w.as_array())
        .map(|a| a.iter().filter_map(|s| s.as_str()).collect())
        .unwrap_or_default();
    let cap = |name: &str| {
        if name == "self" || writers.contains(&name) {
            "rw"
        } else {
            "ro"
        }
    };
    println!("{vault}: members:");
    for m in &members {
        let Some(name) = m.as_str() else { continue };
        if name == "self" {
            println!("  @self  [rw]  (this identity)");
            continue;
        }
        let did = friends
            .and_then(|f| f.get(name))
            .and_then(|e| e.get("id"))
            .and_then(|d| d.as_str())
            .unwrap_or("(unpaired — no [friend.*] entry)");
        println!("  @{name}  [{}]  {did}", cap(name));
    }
    Ok(())
}

/// `vup revoke <vault>: @<name>` — revoke a member's access (`kick` is
/// the hidden legacy alias).
///
/// Removes the petname from `vault.<vault>.members`. On the resulting
/// membership rebuild the member leaves the vault's authorised iroh/ACL/signer
/// sets, so honest nodes stop serving it blobs and reject its future registry
/// writes (D3). Historical entries it signed while authorised remain valid
/// (they were accepted at write time); content keys are not rotated.
pub async fn run_revoke(client: &S5NodeClient, vault: &str, id: &str) -> Result<()> {
    let name = id.strip_prefix('@').unwrap_or(id);
    if name.is_empty() {
        bail!("revoke target must be a member petname (e.g. `@alice`), got '{id}'");
    }

    let config = get_config_json(client).await?;
    let members: Vec<String> = config
        .get("vault")
        .and_then(|v| v.get(vault))
        .ok_or_else(|| anyhow!("vault '{vault}' not found (see `vup list`)"))?
        .get("members")
        .and_then(|m| m.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    if !members.iter().any(|m| m == name) {
        bail!("@{name} is not a member of {vault}: (see `vup who {vault}:`)");
    }

    // Replace members with the filtered list (index-free — no races with a
    // concurrent grant reordering the list). If the member was also a writer,
    // filter that list too — a revoke removes read AND write (D11).
    let writers: Vec<String> = config
        .get("vault")
        .and_then(|v| v.get(vault))
        .and_then(|vc| vc.get("writers"))
        .and_then(|w| w.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let remaining_members: Vec<&String> = members.iter().filter(|m| m.as_str() != name).collect();
    let mut ops = vec![serde_json::json!({
        "op": "replace",
        "path": format!("/vault/{vault}/members"),
        "value": remaining_members,
    })];
    if writers.iter().any(|w| w == name) {
        let remaining_writers: Vec<&String> =
            writers.iter().filter(|w| w.as_str() != name).collect();
        ops.push(serde_json::json!({
            "op": "replace",
            "path": format!("/vault/{vault}/writers"),
            "value": remaining_writers,
        }));
    }
    client.patch_config(serde_json::Value::Array(ops)).await?;
    println!("revoked @{name} from {vault}:");
    println!("  Future snapshots exclude them; honest nodes stop serving them blobs.");
    println!("  (Note: data they already fetched stays readable — keys are not rotated.)");
    Ok(())
}

/// `vup friend ls` — list paired identities (`[friend.*]`). `peers` is
/// the hidden legacy alias.
pub async fn run_friend_ls(client: &S5NodeClient) -> Result<()> {
    let config = get_config_json(client).await?;
    let friends = config.get("friend").and_then(|f| f.as_object());

    match friends.filter(|f| !f.is_empty()) {
        None => {
            println!("No paired friends yet — run `vup friend pair` to add one.");
        }
        Some(map) => {
            println!("Paired friends:");
            for (name, entry) in map {
                let did = entry
                    .get("id")
                    .and_then(|d| d.as_str())
                    .unwrap_or("(no DID?)");
                println!("  @{name}  {did}");
            }
        }
    }
    Ok(())
}

/// `vup friend forget @<name>` — forget a paired identity. `unpair` is
/// the hidden legacy alias.
///
/// Removes `[friend.<name>]`. Rejects if the friend is still a member of
/// any vault (revoke them first, so their access is explicitly revoked
/// rather than silently orphaned).
pub async fn run_friend_forget(client: &S5NodeClient, id: &str) -> Result<()> {
    let name = id.strip_prefix('@').unwrap_or(id);
    if name.is_empty() {
        bail!("forget target must be a petname (e.g. `@alice`), got '{id}'");
    }

    let config = get_config_json(client).await?;
    let friends = config.get("friend").and_then(|f| f.as_object());
    if friends.map(|f| !f.contains_key(name)).unwrap_or(true) {
        bail!("@{name} is not a paired friend (see `vup friend ls`)");
    }

    // Refuse if still a member somewhere — avoid a dangling members entry.
    let mut still_member_of = Vec::new();
    if let Some(vaults) = config.get("vault").and_then(|v| v.as_object()) {
        for (vname, vcfg) in vaults {
            let is_member = vcfg
                .get("members")
                .and_then(|m| m.as_array())
                .map(|a| a.iter().any(|s| s.as_str() == Some(name)))
                .unwrap_or(false);
            if is_member {
                still_member_of.push(vname.clone());
            }
        }
    }
    if !still_member_of.is_empty() {
        bail!(
            "@{name} is still a member of: {}. Revoke them first \
             (`vup revoke <vault>: @{name}`), then forget.",
            still_member_of.join(", ")
        );
    }

    let patch = serde_json::json!([{
        "op": "remove",
        "path": format!("/friend/{name}"),
    }]);
    client.patch_config(patch).await?;
    println!("forgot @{name}.");
    Ok(())
}
