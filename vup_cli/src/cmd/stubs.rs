//! Stubs for verbs that depend on infrastructure not yet built.
//!
//! These bodies print a `not yet implemented` line and exit 0. As each
//! piece of infrastructure lands, the corresponding stub gets replaced
//! by a real implementation in its own module.
//!
//! All non-bootstrap verbs receive an `&S5NodeClient` even when the stub
//! doesn't use it — this matches the production model where the CLI is
//! a thin RPC frontend over the running daemon.
//!
//! Currently stubbed:
//! - `join` — needs the share-link consumer (URL parser + import)
//! - `grant`, `pair`, `kick`, `who`, `peers`, `unpair` — need the
//!   identity layer (DID resolver + recipient bundle)
//! - `store add/ls/info/rm/allow/disallow` — store sub-namespace polish

use anyhow::Result;
use s5_node_api::S5NodeClient;

use super::StoreCmd;

// ---------------------------------------------------------------------------
// Top-level
// ---------------------------------------------------------------------------

pub async fn run_join(_client: &S5NodeClient, url: &str) -> Result<()> {
    todo_stub(&format!("join {url}"));
    Ok(())
}

pub async fn run_peers(_client: &S5NodeClient) -> Result<()> {
    todo_stub("peers");
    Ok(())
}

pub async fn run_unpair(_client: &S5NodeClient, id: &str) -> Result<()> {
    todo_stub(&format!("unpair {id}"));
    Ok(())
}

pub async fn run_store(_client: &S5NodeClient, cmd: StoreCmd) -> Result<()> {
    todo_stub(&format!("store {cmd:?}").to_lowercase());
    Ok(())
}

// ---------------------------------------------------------------------------
// Vault-scoped
// ---------------------------------------------------------------------------

pub async fn run_grant(
    _client: &S5NodeClient,
    vault: &str,
    id: &str,
    read: bool,
    write: bool,
) -> Result<()> {
    let mode = match (read, write) {
        (true, false) => " --read",
        (false, true) => " --write",
        (true, true) => " --read --write",
        _ => "",
    };
    todo_stub(&format!("+{vault} grant {id}{mode}"));
    Ok(())
}

pub async fn run_pair(_client: &S5NodeClient, vault: &str, id: &str) -> Result<()> {
    todo_stub(&format!("+{vault} pair {id}"));
    Ok(())
}

pub async fn run_who(_client: &S5NodeClient, vault: &str) -> Result<()> {
    todo_stub(&format!("+{vault} who"));
    Ok(())
}

pub async fn run_kick(_client: &S5NodeClient, vault: &str, id: &str) -> Result<()> {
    todo_stub(&format!("+{vault} kick {id}"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn todo_stub(invocation: &str) {
    eprintln!("vup: '{invocation}' — not yet implemented");
}
