//! Small verb bodies that grew here before earning their own module.
//!
//! All non-bootstrap verbs receive an `&S5NodeClient` — this matches the
//! production model where the CLI is a thin RPC frontend over the running
//! daemon.
//!
//! `join`, `friend pair` (top-level) and `grant` (vault-scoped) are real
//! below; the rest of the membership verbs live in [`super::membership`],
//! and the store namespace is now real in [`super::store`].

use anyhow::{Result, anyhow, bail};
use s5_node_api::{PairEvent, S5NodeClient};

// ---------------------------------------------------------------------------
// Top-level
// ---------------------------------------------------------------------------

/// `vup join <url>` — consume a frozen `s5://export/…` share link. The
/// daemon fetches the blob from a configured store, decrypts with the
/// fragment secret, and materialises a read-only vault; then
/// `vup restore <vault>: <dir>` pulls the files.
pub async fn run_join(client: &S5NodeClient, url: &str) -> Result<()> {
    let label = client.join_export(url).await?;
    println!("✓ Joined frozen share as {label}: (read-only).");
    println!("  Restore its files:  vup restore {label}: <target-dir>");
    Ok(())
}

// ---------------------------------------------------------------------------
// Vault-scoped
// ---------------------------------------------------------------------------

/// `vup grant <vault>: @<petname> [--read|--write]` — give an
/// already-paired friend access to a vault. The friend's DID must already
/// be recorded via `vup friend pair`.
///
/// Capability = keyset membership (D11): `--read` (the default) adds the
/// peer to `members` (read ACL + decryption recipient); `--write` also adds
/// it to `writers` (its registry writes are accepted). A later
/// `grant --write` promotes a read-only member.
pub async fn run_grant(
    client: &S5NodeClient,
    vault: &str,
    id: &str,
    read: bool,
    write: bool,
) -> Result<()> {
    let petname = id.strip_prefix('@').unwrap_or(id);
    if petname.is_empty() {
        bail!("grant target must be a friend petname (e.g. `@alice`), got '{id}'");
    }
    if read && write {
        bail!("pass at most one of --read / --write (default is --read)");
    }
    client.grant_vault(vault, petname, write).await?;
    let cap = if write { "read+write" } else { "read-only" };
    println!("granted @{petname} {cap} access to {vault}:");
    Ok(())
}

/// `vup friend pair` (top-level `pair` is the hidden legacy alias).
/// Either side runs this verb; `token` distinguishes sender (None —
/// mint, print, wait) from receiver (Some — redeem). Once the
/// redemption fires, both sides interactively prompt for a petname
/// and persist the friend's DID under `[friend.<petname>]`.
pub async fn run_pair_top_level(client: &S5NodeClient, token: Option<String>) -> Result<()> {
    let peer_did = match token {
        None => {
            // Sender side: open a pair stream. First event is the
            // freshly-minted token; second is the redemption result.
            let mut rx = client.pair().await?;
            let token = match rx.recv().await {
                Ok(Some(PairEvent::Minted { token })) => token,
                Ok(Some(PairEvent::Failed { error })) => bail!("pair: {error}"),
                Ok(Some(other)) => bail!("pair: unexpected first event {other:?}"),
                Ok(None) => bail!("pair: daemon closed stream before minting"),
                Err(e) => return Err(anyhow!("pair: stream error: {e}")),
            };
            println!();
            println!("Share this token with the friend (paste into `vup friend pair`):");
            println!();
            println!("    {token}");
            println!();
            println!("Waiting for the friend to redeem...");
            match rx.recv().await {
                Ok(Some(PairEvent::Redeemed { peer_did })) => {
                    println!("✓ Redeemed by {peer_did}");
                    peer_did
                }
                Ok(Some(PairEvent::Failed { error })) => bail!("pair: {error}"),
                Ok(Some(other)) => bail!("pair: unexpected event {other:?}"),
                Ok(None) => bail!("pair: stream closed before redemption"),
                Err(e) => return Err(anyhow!("pair: stream error: {e}")),
            }
        }
        Some(token_str) => {
            // Receiver side: dial the sender's endpoint, present
            // the secret, learn their DID.
            let did = client.redeem_pair(token_str).await?;
            println!("✓ Got friend's DID: {did}");
            did
        }
    };

    let petname: String = crate::interact::input_required("Save friend as @")?;
    let petname = petname.trim().to_string();
    if petname.is_empty() {
        bail!("petname must not be empty");
    }
    client.add_friend(&petname, &peer_did).await?;
    println!("Friend @{petname} paired ({peer_did}).");
    println!();
    println!("Next: grant @{petname} access to a vault, e.g. `vup grant <vault>: @{petname}`");
    Ok(())
}
