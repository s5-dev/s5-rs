//! `vup store …` — the storage-backend namespace.
//!
//! Stores are configured rarely and referenced by name from
//! `vault.<>.data_store` / `meta_store` / the node `default_store`. Every
//! verb here is a config read or an RFC-6902 `patch_config` against the
//! live daemon (the CLI is a thin RPC frontend); `add` additionally opens
//! the chosen backend directly to validate connectivity before it patches,
//! exactly as `onboard` does.
//!
//! `add` reuses the backend collectors + validation in
//! [`super::store_config`] so a store stood up here is byte-for-byte the
//! shape `onboard`/`recover`/`device join` produce.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use s5_node_api::S5NodeClient;

use super::store_config::{
    S3Config, StoreChoice, ask_local_store, ask_s3_store, build_and_test_s3, register_sia,
};
use super::{StoreBackend, StoreCmd};

/// Dispatch `vup store <sub>`.
pub async fn run_store(client: &S5NodeClient, cmd: StoreCmd) -> Result<()> {
    match cmd {
        StoreCmd::Add {
            backend,
            name,
            path,
            endpoint,
            bucket,
            access_key,
            secret_key,
            region,
            indexer_url,
        } => {
            run_add(
                client,
                backend,
                &name,
                AddFlags {
                    path,
                    endpoint,
                    bucket,
                    access_key,
                    secret_key,
                    region,
                    indexer_url,
                },
            )
            .await
        }
        StoreCmd::Ls => run_ls(client).await,
        StoreCmd::Info { name } => run_info(client, &name).await,
        StoreCmd::Rm { name } => run_rm(client, &name).await,
        // TODO(friend-hosted storage): the push-ACL CLI (`store allow/disallow`)
        // was removed 2026-07-03 because `[store.*].allow` is unenforced; re-add
        // it TOGETHER WITH enforcement when friend-hosted blob serving consumes
        // the ACL (see `NodeConfigStore::allow` in s5_node/src/config.rs).
    }
}

/// Backend config supplied on the command line; anything `None` falls back
/// to an interactive prompt (or a default).
struct AddFlags {
    path: Option<std::path::PathBuf>,
    endpoint: Option<String>,
    bucket: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    region: Option<String>,
    indexer_url: Option<String>,
}

/// `vup store add <backend> <name> [flags]` — provision a new `[store.<name>]`.
async fn run_add(
    client: &S5NodeClient,
    backend: StoreBackend,
    name: &str,
    flags: AddFlags,
) -> Result<()> {
    validate_store_name(name)?;

    let config = get_config(client).await?;
    if config.get("store").and_then(|s| s.get(name)).is_some() {
        bail!("store '{name}' already exists — `vup store info {name}` to inspect it");
    }

    let default_local = default_local_store_path(name)?;
    let choice = collect_choice(backend, &default_local, flags).await?;

    // Validate connectivity the way `onboard` does: the S3/Sia collectors
    // already probed the backend live with a `list` / OAuth; here we just
    // create the backing local directory (the store dir for `local`, the
    // index/cache dir for `s3`/`sia`) so the daemon can open it — exactly
    // what `onboard` does before writing the config.
    let local_dir = choice.local_path();
    std::fs::create_dir_all(local_dir)
        .with_context(|| format!("creating store dir {}", local_dir.display()))?;

    let is_sia = matches!(choice, StoreChoice::Sia { .. });
    let ops = build_add_ops(name, &choice)?;
    client.patch_config(Value::Array(ops)).await?;

    println!("store '{name}' added.");
    if is_sia {
        // A durable Sia store is exactly what unblocks recovery, so point the
        // node default + identity bootstrap at it (mirrors `onboard`).
        println!("  set as the node default_store and identity bootstrap_store.");
    }
    println!("  use it:  vup vault create <v> then set vault.<v>.data_store = \"{name}\"");
    Ok(())
}

/// Collect a fully-specified [`StoreChoice`] from flags, prompting only for
/// what the flags leave out.
async fn collect_choice(
    backend: StoreBackend,
    default_local: &std::path::Path,
    flags: AddFlags,
) -> Result<StoreChoice> {
    match backend {
        StoreBackend::Local => match flags.path {
            Some(path) => Ok(StoreChoice::Local { path }),
            None => ask_local_store(default_local),
        },
        StoreBackend::S3 => {
            // All four required S3 fields present → non-interactive; else
            // fall back to the wizard (which prompts + validates).
            match (
                flags.endpoint,
                flags.bucket,
                flags.access_key,
                flags.secret_key,
            ) {
                (Some(endpoint), Some(bucket_name), Some(access_key), Some(secret_key)) => {
                    build_and_test_s3(
                        default_local,
                        S3Config {
                            endpoint,
                            bucket_name,
                            access_key,
                            secret_key,
                            region: flags.region.unwrap_or_else(|| "us-east-1".to_string()),
                        },
                    )
                    .await
                }
                _ => ask_s3_store(default_local).await,
            }
        }
        StoreBackend::Sia => {
            // The indexd AppKey derives from the storage seed, which only the
            // recovery phrase yields — the daemon never holds it. Ask for the
            // phrase (hidden), derive the seed, then run the one-time OAuth.
            let phrase = crate::interact::password("Recovery phrase (12 words)")
                .context("reading recovery phrase")?;
            let root_master = s5_node::mnemonic::root_master(phrase.trim())
                .context("deriving the storage seed from the phrase — check the words")?;
            let stores_seed = s5_node::mnemonic::storage_root_seed(&root_master);
            let indexer_url = match flags.indexer_url {
                Some(u) => u,
                None => crate::interact::input_with_default(
                    "Indexer URL",
                    "https://sia.storage".to_string(),
                )?,
            };
            register_sia(default_local, &stores_seed, indexer_url).await
        }
    }
}

/// The RFC-6902 ops `store add` applies: always `add /store/<name>`; for a
/// Sia store also point the node default + identity bootstrap at it (a
/// durable store is what makes recovery possible — same effect `onboard`'s
/// `build_config` bakes in). RFC-6902 `add` replaces an existing scalar, so
/// these are safe whether or not the fields were already set.
fn build_add_ops(name: &str, choice: &StoreChoice) -> Result<Vec<Value>> {
    let store_val = serde_json::to_value(choice.to_node_config_store())
        .context("serialising the store config")?;
    let mut ops = vec![json!({
        "op": "add",
        "path": format!("/store/{name}"),
        "value": store_val,
    })];
    if matches!(choice, StoreChoice::Sia { .. }) {
        ops.push(json!({ "op": "add", "path": "/default_store", "value": name }));
        ops.push(json!({ "op": "add", "path": "/identity/bootstrap_store", "value": name }));
    }
    Ok(ops)
}

/// `vup store ls` — one line per `[store.*]`, marking the node default.
async fn run_ls(client: &S5NodeClient) -> Result<()> {
    let config = get_config(client).await?;
    let default = config.get("default_store").and_then(|v| v.as_str());

    let stores = config.get("store").and_then(|v| v.as_object());
    match stores.filter(|o| !o.is_empty()) {
        None => {
            println!("(no stores configured — `vup store add local <name>` to create one)");
        }
        Some(stores) => {
            println!("Stores:");
            for (name, store) in stores {
                let loc = store_locator(store);
                let flag = if Some(name.as_str()) == default {
                    "  [default]"
                } else {
                    ""
                };
                println!("  {name} ({loc}){flag}");
            }
            // If exactly one store and no explicit default, D1 makes it the
            // implicit default — say so.
            if default.is_none() && stores.len() == 1 {
                println!("\n(the sole store is the implicit default_store)");
            }
        }
    }
    Ok(())
}

/// `vup store info <name>` — backend config and who uses it.
async fn run_info(client: &S5NodeClient, name: &str) -> Result<()> {
    let config = get_config(client).await?;
    let store = config
        .get("store")
        .and_then(|s| s.get(name))
        .ok_or_else(|| anyhow::anyhow!("no such store '{name}'"))?;

    println!("store '{name}'");
    let stype = store.get("type").and_then(|v| v.as_str()).unwrap_or("?");
    println!("  backend:  {stype}");
    for field in [
        "base_path",
        "endpoint",
        "bucket_name",
        "indexer_url",
        "cache_path",
        "path",
    ] {
        if let Some(v) = store.get(field).and_then(|v| v.as_str()) {
            println!("  {field}:  {v}");
        }
    }
    if store.get("outboard").and_then(|v| v.as_bool()) == Some(true) {
        println!("  outboard: true");
    }
    if let Some(n) = store.get("read_cache_bytes").and_then(|v| v.as_u64()) {
        println!("  read_cache_bytes: {n}");
    }

    let default = config.get("default_store").and_then(|v| v.as_str());
    let is_default = Some(name) == default
        || (default.is_none()
            && config
                .get("store")
                .and_then(|v| v.as_object())
                .map(|o| o.len())
                == Some(1));
    if is_default {
        println!("  default:  yes (node default_store)");
    }
    if config
        .get("identity")
        .and_then(|i| i.get("bootstrap_store"))
        .and_then(|v| v.as_str())
        == Some(name)
    {
        println!("  bootstrap: yes (identity.bootstrap_store)");
    }

    let users = vaults_using(&config, name);
    if users.is_empty() {
        println!("  used by:  (no vaults reference it directly)");
    } else {
        println!("  used by:  {}", users.join(", "));
    }
    Ok(())
}

/// `vup store rm <name>` — drop `[store.<name>]`; refuse while anything
/// still references it (a bare `remove` would otherwise leave the config
/// failing validation on the daemon).
async fn run_rm(client: &S5NodeClient, name: &str) -> Result<()> {
    let config = get_config(client).await?;
    if config.get("store").and_then(|s| s.get(name)).is_none() {
        bail!("no such store '{name}'");
    }
    let refs = store_references(&config, name);
    if !refs.is_empty() {
        bail!(
            "store '{name}' is still referenced by {} — repoint or remove those first",
            refs.join(", ")
        );
    }
    let patch = json!([{ "op": "remove", "path": format!("/store/{name}") }]);
    client.patch_config(patch).await?;
    println!("store '{name}' removed (stored blobs on disk are not touched).");
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn get_config(client: &S5NodeClient) -> Result<Value> {
    let resp = client.get_config().await?;
    Ok(serde_json::from_str(&resp.config_json)?)
}

/// A short human locator for a store, backend-dependent.
fn store_locator(store: &Value) -> String {
    let stype = store.get("type").and_then(|v| v.as_str()).unwrap_or("?");
    let detail = match stype {
        "local" => store.get("base_path").and_then(|v| v.as_str()),
        "s3" => store.get("bucket_name").and_then(|v| v.as_str()),
        "indexd" => store.get("indexer_url").and_then(|v| v.as_str()),
        "fjall" | "local_links" => store.get("path").and_then(|v| v.as_str()),
        _ => None,
    };
    match detail {
        Some(d) => format!("{stype}: {d}"),
        None => stype.to_string(),
    }
}

/// Vaults that name this store in `data_store` / `meta_store`.
fn vaults_using(config: &Value, name: &str) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(vaults) = config.get("vault").and_then(|v| v.as_object()) {
        for (vname, v) in vaults {
            let data = v.get("data_store").and_then(|s| s.as_str());
            let meta = v.get("meta_store").and_then(|s| s.as_str());
            if data == Some(name) || meta == Some(name) {
                out.push(format!("{vname}:"));
            }
        }
    }
    out
}

/// Everything that would break if `name` were removed: vault data/meta
/// stores, the node default, and the identity bootstrap store.
fn store_references(config: &Value, name: &str) -> Vec<String> {
    let mut refs = vaults_using(config, name);
    if config.get("default_store").and_then(|v| v.as_str()) == Some(name) {
        refs.push("default_store".to_string());
    }
    if config
        .get("identity")
        .and_then(|i| i.get("bootstrap_store"))
        .and_then(|v| v.as_str())
        == Some(name)
    {
        refs.push("identity.bootstrap_store".to_string());
    }
    if let Some(stores) = config.get("store").and_then(|v| v.as_object()) {
        for (sname, s) in stores {
            for tier in ["hot", "cold"] {
                if s.get(tier).and_then(|v| v.as_str()) == Some(name) {
                    refs.push(format!("store.{sname}.{tier}"));
                }
            }
        }
    }
    refs
}

/// A store name is a JSON-pointer segment and a TOML table key — keep it to
/// simple identifier characters so neither needs escaping.
fn validate_store_name(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("store name must not be empty");
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        bail!("store name '{name}' may only contain letters, digits, '.', '_', '-'");
    }
    Ok(())
}

/// Default local-store directory for a named store: `<data>/stores/<name>`.
fn default_local_store_path(name: &str) -> Result<std::path::PathBuf> {
    let dirs = directories::ProjectDirs::from("pro", "s5", "s5")
        .ok_or_else(|| anyhow::anyhow!("could not determine application data directory"))?;
    Ok(dirs.data_dir().join("stores").join(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::onboard::build_config;
    use std::path::{Path, PathBuf};

    /// A helper that produces an onboarded local config which is known to
    /// parse (proven by `onboard`'s own test), to use as a realistic
    /// baseline the `store add` patch is applied on top of.
    fn baseline_local_config() -> s5_node::config::S5NodeConfig {
        let choice = StoreChoice::Local {
            path: PathBuf::from("/data/store"),
        };
        let toml = build_config(
            choice.local_path(),
            Path::new("/data/registry"),
            Path::new("/keys/main.txt"),
            "age1main",
            "age1paper",
            Path::new("/keys/node.key"),
            Path::new("/keys/master.key"),
            Path::new("/keys/identity_anchor.entry"),
            &choice,
            &[("backup".to_string(), PathBuf::from("/data/vaults/backup"))],
        );
        toml::from_str(&toml).expect("baseline config must parse")
    }

    /// `store add local` (with a `--path` flag) must produce a patch that,
    /// applied to a live config, yields a valid `[store.<name>]` local entry
    /// the daemon accepts.
    #[test]
    fn store_add_local_yields_store_entry() {
        let choice = StoreChoice::Local {
            path: PathBuf::from("/data/cold"),
        };
        let ops = build_add_ops("cold", &choice).expect("build ops");

        // A non-Sia add touches only /store/<name> — no default/bootstrap hijack.
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0]["op"], "add");
        assert_eq!(ops[0]["path"], "/store/cold");
        assert_eq!(ops[0]["value"]["type"], "local");
        assert_eq!(ops[0]["value"]["base_path"], "/data/cold");

        // Apply it to a real, parseable baseline and confirm the entry lands
        // and the resulting config still validates.
        let base = baseline_local_config();
        let mut json = serde_json::to_value(&base).unwrap();
        let patch: json_patch::Patch =
            serde_json::from_value(Value::Array(ops)).expect("valid patch");
        json_patch::patch(&mut json, &patch).expect("patch applies");
        let merged: s5_node::config::S5NodeConfig = serde_json::from_value(json).expect("parses");

        assert!(merged.store.contains_key("cold"));
        match &merged.store["cold"].backend {
            s5_node::config::NodeConfigStoreBackend::Local(cfg) => {
                assert_eq!(cfg.base_path, "/data/cold");
            }
            other => panic!("expected Local backend, got {other:?}"),
        }
        assert!(merged.store["cold"].allow.is_empty());
        assert!(
            merged.validate().is_empty(),
            "merged config must validate: {:?}",
            merged.validate()
        );
    }

    /// A Sia `store add` additionally repoints the node default + identity
    /// bootstrap store at the new durable store (recovery unblock).
    #[test]
    fn store_add_sia_sets_default_and_bootstrap() {
        let choice = StoreChoice::Sia {
            cache_path: PathBuf::from("/data/indexd-cache"),
            indexer_url: "https://sia.storage".to_string(),
            app_key: [9u8; 32],
        };
        let ops = build_add_ops("sia2", &choice).expect("build ops");
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[0]["path"], "/store/sia2");
        assert_eq!(ops[0]["value"]["type"], "indexd");
        assert!(
            ops.iter()
                .any(|o| o["path"] == "/default_store" && o["value"] == "sia2")
        );
        assert!(
            ops.iter()
                .any(|o| o["path"] == "/identity/bootstrap_store" && o["value"] == "sia2")
        );
    }

    #[test]
    fn store_name_validation() {
        assert!(validate_store_name("cold").is_ok());
        assert!(validate_store_name("cold-2.eu_1").is_ok());
        assert!(validate_store_name("").is_err());
        assert!(validate_store_name("bad/name").is_err());
        assert!(validate_store_name("bad name").is_err());
    }

    #[test]
    fn rm_refused_while_referenced() {
        let base = baseline_local_config();
        let json = serde_json::to_value(&base).unwrap();
        // onboard's baseline points default_store + the backup vault at "local".
        let refs = store_references(&json, "local");
        assert!(
            !refs.is_empty(),
            "the onboard store is referenced: {refs:?}"
        );
        assert!(refs.iter().any(|r| r == "default_store"));
    }
}
