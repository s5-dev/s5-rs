//! App-registration flow + at-rest AppKey storage.
//!
//! Two pieces:
//!
//! 1. **`register_via_browser`** — the OAuth-style enrollment dance
//!    (matches the upstream `Builder::request_connection` /
//!    `wait_for_approval` / `register` chain, plus an
//!    `on_response_url` hook so the caller can display the link
//!    and/or shell out to a browser).
//! 2. **`AppKeyVault`** — a thin file-backed AppKey store. When
//!    given an `AgeRecipient`, it writes the 32-byte AppKey export
//!    age-encrypted (matching the pattern used by s5_node's identity
//!    master key — `dev/s5/s5_node/src/identity_vault.rs:90-181`).
//!    Without recipients, it falls back to a plaintext file with
//!    `0600` permissions and emits a warning.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use sia_storage::{AppKey, AppMetadata, Builder, Sdk};
use zeroize::Zeroize;

/// Mnemonic input to `Builder::register`. Static across all S5
/// installations on purpose: per-user differentiation is supplied by
/// indexd's per-user `user_secret` (returned by the OAuth approval
/// step), so `derive_app_key(this_mnemonic, app_id, user_secret)`
/// already produces a unique AppKey per user. Recoverability is the
/// same as the spec's "re-OAuth fetches the same user_secret"
/// recovery path (§8): the AppKey is determined by indexd's
/// per-account state, not by anything we have to remember.
///
/// The chosen value is the canonical BIP-39 12-word zero-entropy
/// phrase ([0u8; 16] + valid checksum byte). Picking a constant
/// keeps the implementation deterministic without dragging in a
/// BIP-39 wordlist of our own.
pub const S5_INDEXD_MNEMONIC: &str =
    "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";

const AGE_V1_HEADER: &[u8] = b"age-encryption.org/v1";

/// Identifier the indexer ties to S5 registrations. The `id` is the
/// load-bearing field — it's the AppID derived from
/// [`crate::S5_INDEXD_APP_ID_PREIMAGE`] and salts the AppKey HKDF, so
/// once any non-mock write happens it must not change. The other
/// fields (`name`, `description`, `service_url`, …) are
/// presentational; they appear in the OAuth approval dialog and can
/// be edited safely.
pub fn app_metadata() -> AppMetadata {
    AppMetadata {
        id: sia_storage::Hash256::from(crate::app_id_bytes()),
        name: "S5",
        description: "Content-addressed personal backup, sync, and archive built on Sia.",
        service_url: "https://s5.pro",
        logo_url: None,
        callback_url: None,
    }
}

/// One age recipient (public key) the AppKey file should be encrypted
/// to. Multi-recipient lists are accepted so a node and a recovery
/// agent can both decrypt.
#[derive(Clone, Debug)]
pub struct AgeRecipient(pub String);

/// File-backed AppKey store using the same age-encrypted pattern that
/// `s5_node` uses for its master signing key. Each call serializes
/// the 32-byte AppKey export plus a 1-byte version prefix.
#[derive(Debug, Clone)]
pub struct AppKeyVault {
    path: PathBuf,
    recipients: Vec<AgeRecipient>,
    identity_files: Vec<PathBuf>,
}

impl AppKeyVault {
    /// Construct a vault rooted at `path`.
    ///
    /// `recipients` — age public keys the AppKey is encrypted to.
    /// Empty means plaintext storage (insecure, warned at write).
    ///
    /// `identity_files` — age secret-key files used to decrypt on
    /// read. Tried in order; first one that opens wins.
    pub fn new(
        path: impl Into<PathBuf>,
        recipients: Vec<AgeRecipient>,
        identity_files: Vec<PathBuf>,
    ) -> Self {
        Self {
            path: path.into(),
            recipients,
            identity_files,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Does the vault file already exist on disk?
    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    /// Read + decrypt the stored AppKey. `Ok(None)` if the file does
    /// not exist; error if the file exists but cannot be decoded.
    pub fn load(&self) -> Result<Option<AppKey>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let raw = std::fs::read(&self.path)
            .with_context(|| format!("reading appkey vault {}", self.path.display()))?;

        let mut plaintext: Vec<u8> = if raw.starts_with(AGE_V1_HEADER) {
            if self.identity_files.is_empty() {
                return Err(anyhow!(
                    "appkey vault {} is age-encrypted but no identity_files were configured",
                    self.path.display()
                ));
            }
            age_decrypt(&raw, &self.identity_files)?
        } else {
            raw
        };

        let bytes: [u8; 32] = plaintext.as_slice().try_into().map_err(|_| {
            anyhow!(
                "appkey vault {}: wrong size after decrypt",
                self.path.display()
            )
        })?;
        plaintext.zeroize();
        Ok(Some(AppKey::import(bytes)))
    }

    /// Encrypt + write the AppKey. Overwrites any existing file. On
    /// Unix, sets `0600` permissions to limit accidental exposure.
    pub fn store(&self, key: &AppKey) -> Result<()> {
        let mut export = key.export();
        let bytes_to_write: Vec<u8> = if self.recipients.is_empty() {
            tracing::warn!(
                path = %self.path.display(),
                "appkey vault has no age recipients configured — writing AppKey as plaintext. \
                 Configure an age public key to enable at-rest encryption."
            );
            export.to_vec()
        } else {
            age_encrypt(&export, &self.recipients)?
        };
        export.zeroize();

        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating parent dir {}", parent.display()))?;
        }
        std::fs::write(&self.path, &bytes_to_write)
            .with_context(|| format!("writing appkey vault {}", self.path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(())
    }

    /// Remove the stored AppKey, if any. Idempotent.
    pub fn clear(&self) -> Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(anyhow!(
                "removing appkey vault {}: {e}",
                self.path.display()
            )),
        }
    }
}

/// Acquire an SDK against `indexer_url`, reusing a cached AppKey if
/// present and otherwise driving the browser-OAuth enrollment.
///
/// `on_response_url` is invoked once with the URL the user must visit
/// to authorise the application. The function then blocks on
/// `wait_for_approval`. After approval, the static
/// [`S5_INDEXD_MNEMONIC`] is fed to `Builder::register` — per-user
/// differentiation in the derived AppKey comes from indexd's
/// `user_secret`, not the mnemonic input.
///
/// On success the AppKey is written to `vault`.
pub async fn connect_or_register<URL>(
    indexer_url: &str,
    vault: &AppKeyVault,
    on_response_url: URL,
) -> Result<Sdk>
where
    URL: FnOnce(&str),
{
    let builder = Builder::new(indexer_url, app_metadata())
        .map_err(|e| anyhow!("Builder::new({indexer_url}): {e:?}"))?;

    if let Some(key) = vault.load()? {
        if let Some(sdk) = builder
            .connected(&key)
            .await
            .map_err(|e| anyhow!("Builder::connected: {e:?}"))?
        {
            return Ok(sdk);
        }
        tracing::warn!(
            "cached AppKey at {} no longer recognised by indexer; re-registering",
            vault.path().display()
        );
    }

    let pending = builder
        .request_connection()
        .await
        .map_err(|e| anyhow!("Builder::request_connection: {e:?}"))?;
    on_response_url(pending.response_url());

    let approved = pending
        .wait_for_approval()
        .await
        .map_err(|e| anyhow!("Builder::wait_for_approval: {e:?}"))?;

    let sdk = approved
        .register(S5_INDEXD_MNEMONIC)
        .await
        .map_err(|e| anyhow!("Builder::register: {e:?}"))?;

    vault.store(sdk.app_key())?;
    Ok(sdk)
}

// --- age helpers, inlined from s5_node's pattern --------------------

fn age_encrypt(plaintext: &[u8], recipients: &[AgeRecipient]) -> Result<Vec<u8>> {
    if recipients.is_empty() {
        return Err(anyhow!("no age recipients"));
    }
    let parsed: Vec<age::x25519::Recipient> = recipients
        .iter()
        .map(|r| {
            r.0.parse::<age::x25519::Recipient>()
                .map_err(|e| anyhow!("invalid age recipient '{}': {}", r.0, e))
        })
        .collect::<Result<Vec<_>>>()?;
    let encryptor =
        age::Encryptor::with_recipients(parsed.iter().map(|r| r as &dyn age::Recipient))
            .map_err(|e| anyhow!("age encryptor: {e}"))?;
    let mut ciphertext = Vec::new();
    let mut writer = encryptor
        .wrap_output(&mut ciphertext)
        .map_err(|e| anyhow!("age wrap_output: {e}"))?;
    writer
        .write_all(plaintext)
        .context("write age ciphertext")?;
    writer.finish().context("finish age encryption")?;
    Ok(ciphertext)
}

fn age_decrypt(ciphertext: &[u8], identity_files: &[PathBuf]) -> Result<Vec<u8>> {
    for path in identity_files {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("reading identity file {}", path.display()))?;
        let id_file = age::IdentityFile::from_buffer(std::io::BufReader::new(content.as_bytes()))
            .with_context(|| format!("parsing identity file {}", path.display()))?;
        let identities = id_file
            .into_identities()
            .map_err(|e| anyhow!("loading identities from {}: {e}", path.display()))?;
        let decryptor =
            age::Decryptor::new(ciphertext).map_err(|e| anyhow!("age decryptor: {e}"))?;
        let identity_refs: Vec<&dyn age::Identity> =
            identities.iter().map(|i| i.as_ref()).collect();
        if let Ok(mut reader) = decryptor.decrypt(identity_refs.into_iter()) {
            let mut plaintext = Vec::new();
            reader
                .read_to_end(&mut plaintext)
                .context("read age plaintext")?;
            return Ok(plaintext);
        }
    }
    Err(anyhow!(
        "none of {} identity file(s) could decrypt the appkey vault",
        identity_files.len()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vault_roundtrip_plaintext() {
        let tmp = tempdir_path();
        let vault = AppKeyVault::new(tmp.join("appkey.bin"), Vec::new(), Vec::new());

        let key = AppKey::import([7u8; 32]);
        vault.store(&key).unwrap();
        let loaded = vault.load().unwrap().expect("must be present");
        assert_eq!(loaded.export(), [7u8; 32]);

        vault.clear().unwrap();
        assert!(vault.load().unwrap().is_none());
    }

    #[test]
    fn vault_roundtrip_age_encrypted() {
        let tmp = tempdir_path();
        let identity = age::x25519::Identity::generate();
        let recipient = identity.to_public().to_string();
        let identity_file = tmp.join("id.txt");
        std::fs::write(&identity_file, identity.to_string().expose_secret()).unwrap();

        let vault = AppKeyVault::new(
            tmp.join("appkey.age"),
            vec![AgeRecipient(recipient)],
            vec![identity_file],
        );

        let key = AppKey::import([13u8; 32]);
        vault.store(&key).unwrap();
        // The file should not be plaintext.
        let raw = std::fs::read(vault.path()).unwrap();
        assert!(raw.starts_with(AGE_V1_HEADER), "should be age-wrapped");
        let loaded = vault.load().unwrap().expect("must decrypt");
        assert_eq!(loaded.export(), [13u8; 32]);
    }

    fn tempdir_path() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "s5-indexd-auth-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// `age::x25519::Identity::to_string` returns a
    /// `secrecy::SecretString`; pull the inner string for test
    /// I/O. Naming the trait inline keeps it within the test scope.
    trait ExposeSecret {
        fn expose_secret(&self) -> String;
    }
    impl ExposeSecret for age::secrecy::SecretString {
        fn expose_secret(&self) -> String {
            age::secrecy::ExposeSecret::<str>::expose_secret(self).to_string()
        }
    }
}
