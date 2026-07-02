//! App registration against an indexd service.
//!
//! Two entry points, both filesystem-free — persisting the returned
//! AppKey is the caller's concern (in S5 it goes into the node's
//! age-encrypted `stores` vault — the warm, availability tier):
//!
//! 1. [`register`] — drives the OAuth-style enrollment (the upstream
//!    `Builder::request_connection` / `wait_for_approval` / `register`
//!    chain) and returns the 32-byte AppKey export to persist.
//! 2. [`connect`] — checks whether a previously-exported AppKey is
//!    still recognised by the indexer.
//!
//! The registration mnemonic is derived from a **caller-supplied 32-byte
//! secret** via [`derive_indexd_mnemonic`]. In S5 that secret is the node's
//! **managed storage secret** — `blake3::derive_key("s5/storage/indexd/v1",
//! stores_seed ‖ label)` (`docs/reference/mnemonic-derivation.md` § Layer C)
//! — **not** the cold identity master, which only *anchors* the vaults.
//! Per-account / per-indexer scoping lives in that secret (distinct labels →
//! distinct secrets → distinct AppKeys), so this layer folds in nothing
//! else. The mnemonic stays the one derivation input the indexer never sees,
//! keeping the AppKey — and the object data key it unwraps — underivable by
//! the indexer (see the module docs in `lib.rs`).

use anyhow::{Result, anyhow};
use sia_storage::{AppKey, AppMetadata, Builder, Hash256};
use zeroize::Zeroize;

/// Domain tag for deriving the indexd registration mnemonic from the
/// caller-supplied storage secret. Bumping the `/v1` suffix re-keys the
/// AppKey and orphans existing data — treat it as frozen once any non-mock
/// write has happened.
const S5_INDEXD_MNEMONIC_DOMAIN: &str = "s5/indexd-app-mnemonic/v1";

/// Derive the BIP-39 mnemonic fed to `Builder::register` from a 32-byte
/// storage secret.
///
/// The AppKey is `derive_app_key(mnemonic, app_id, user_secret)`. `app_id`
/// is a public constant and `user_secret` is held by the indexer, so the
/// mnemonic is the **only** derivation input the indexer never sees. In S5,
/// `secret` is the node's **managed storage secret**
/// (`blake3::derive_key("s5/storage/indexd/v1", stores_seed ‖ label)`,
/// `mnemonic-derivation.md` § Layer C), which keeps the AppKey — and the
/// object `data_key` it unwraps — underivable by the indexer, while staying
/// re-derivable from the paper mnemonic for recovery.
///
/// Per-account / per-indexer scoping is already baked into `secret` by that
/// upstream derivation (distinct labels → distinct secrets), so this
/// function folds in nothing else. Deterministic: the same `secret` always
/// yields the same mnemonic, so re-registration (re-OAuth returns the same
/// `user_secret`) re-derives the same AppKey.
pub fn derive_indexd_mnemonic(secret: &[u8; 32]) -> String {
    let mut derived = blake3::derive_key(S5_INDEXD_MNEMONIC_DOMAIN, secret);
    let mut entropy = [0u8; 16];
    entropy.copy_from_slice(&derived[..16]);
    derived.zeroize();
    // Reuse sia_core's own encoder so the phrase round-trips exactly back to
    // `entropy` through `Seed::new` inside `Builder::register`.
    let phrase = sia_core::seed::Seed::from_seed(entropy).to_string();
    entropy.zeroize();
    phrase
}

/// Identifier the indexer ties to S5 registrations. The `id` is the
/// load-bearing field — it's the AppID derived from
/// [`crate::S5_INDEXD_APP_ID_PREIMAGE`] and salts the AppKey HKDF, so
/// once any non-mock write happens it must not change. The other fields
/// (`name`, `description`, `service_url`, …) are presentational; they
/// appear in the OAuth approval dialog and can be edited safely.
pub fn app_metadata() -> AppMetadata {
    AppMetadata {
        id: Hash256::from(crate::app_id_bytes()),
        name: "S5",
        description: "Content-addressed personal backup, sync, and archive built on Sia.",
        service_url: "https://s5.pro",
        logo_url: None,
        callback_url: None,
    }
}

/// Is a previously-exported `app_key` still recognised by the indexer
/// at `indexer_url`? Network-only; no filesystem.
pub async fn connect(
    indexer_url: &str,
    app_key: [u8; 32],
    metadata: Option<AppMetadata>,
) -> Result<bool> {
    let builder = Builder::new(indexer_url, metadata.unwrap_or_else(app_metadata))
        .map_err(|e| anyhow!("Builder::new({indexer_url}): {e:?}"))?;
    let key = AppKey::import(app_key);
    Ok(builder
        .connected(&key)
        .await
        .map_err(|e| anyhow!("Builder::connected: {e:?}"))?
        .is_some())
}

/// Run one interactive OAuth registration round against `indexer_url`
/// and return the resulting 32-byte AppKey export.
///
/// Network + callback only — **no filesystem**. `on_response_url` is
/// invoked once with the URL the user must visit to authorise the
/// application; the function then blocks on `wait_for_approval`. After
/// approval, the mnemonic derived from `secret` via
/// [`derive_indexd_mnemonic`] is fed to `Builder::register`, keeping the
/// AppKey underivable by the indexer (end-to-end confidentiality).
///
/// `secret` is the node's 32-byte managed storage secret (already scoped
/// per indexer/account via `stores_seed ‖ label`); it must never be
/// transmitted to the indexer. The same `secret` reproduces the same AppKey
/// across devices and re-registrations.
///
/// The caller owns the returned bytes and is responsible for persisting
/// them (e.g. in the node's age-encrypted `stores` vault — the warm,
/// availability-tier vault that holds storage credentials).
pub async fn register<URL>(
    indexer_url: &str,
    secret: &[u8; 32],
    metadata: Option<AppMetadata>,
    on_response_url: URL,
) -> Result<[u8; 32]>
where
    URL: FnOnce(&str),
{
    let builder = Builder::new(indexer_url, metadata.unwrap_or_else(app_metadata))
        .map_err(|e| anyhow!("Builder::new({indexer_url}): {e:?}"))?;

    let pending = builder
        .request_connection()
        .await
        .map_err(|e| anyhow!("Builder::request_connection: {e:?}"))?;
    on_response_url(pending.response_url());

    let approved = pending
        .wait_for_approval()
        .await
        .map_err(|e| anyhow!("Builder::wait_for_approval: {e:?}"))?;

    let mut mnemonic = derive_indexd_mnemonic(secret);
    let sdk = approved
        .register(&mnemonic)
        .await
        .map_err(|e| anyhow!("Builder::register: {e:?}"))?;
    mnemonic.zeroize();

    Ok(sdk.app_key().export())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexd_mnemonic_is_deterministic_and_secret() {
        let seed_a = [7u8; 32];
        let seed_b = [8u8; 32];

        let a1 = derive_indexd_mnemonic(&seed_a);
        let a2 = derive_indexd_mnemonic(&seed_a);
        assert_eq!(a1, a2, "same secret must yield the same mnemonic");
        assert_ne!(
            a1,
            derive_indexd_mnemonic(&seed_b),
            "a different secret must yield a different mnemonic"
        );

        // A valid 12-word BIP-39 phrase that round-trips through sia_core.
        sia_core::seed::Seed::new(&a1).expect("derived phrase must be a valid BIP-39 mnemonic");
        assert_eq!(a1.split_whitespace().count(), 12);

        // The derived phrase must never be the trivial all-zeros BIP-39 seed:
        // it is publicly known, so anyone could reconstruct the AppKey from it.
        let zero_phrase = "abandon abandon abandon abandon abandon abandon \
                           abandon abandon abandon abandon abandon about";
        assert_ne!(
            a1, zero_phrase,
            "derived phrase must not be the all-zeros seed"
        );
    }
}
