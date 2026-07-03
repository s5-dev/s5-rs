//! Share-link consumer — the `vup join` side of `docs/reference/share-links.md`.
//!
//! Parses an `export` URL, fetches the frozen encrypted Transparent Node from
//! a configured store by its content hash, decrypts it with the ephemeral
//! secret carried in the URL fragment, and materialises a read-only local
//! vault root the ordinary restore machinery can read.
//!
//! The producer side is [`crate::export`]; this closes the share loop.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use s5_core::Hash;
use s5_core::blob::Blobs;
use s5_fs_v2::node::Node;

/// A parsed `s5://export/<label>?m=<hex(hash)>#<age-secret>` URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportUrl {
    /// Suggested local vault nickname (validated `[a-z0-9_-]{1,64}`).
    pub label: String,
    /// Content hash of the frozen encrypted Transparent Node blob.
    pub blob_hash: Hash,
    /// The ephemeral age secret that decrypts the blob (from the fragment).
    pub secret: String,
}

impl ExportUrl {
    /// Parse and validate. Rejects unknown intents, malformed hashes, and
    /// missing/blank secrets — a consumer must refuse anything it can't safely
    /// consume (`share-links.md` § intent handling).
    pub fn parse(url: &str) -> Result<Self> {
        let rest = url
            .strip_prefix("s5://export/")
            .ok_or_else(|| anyhow!("not an s5 export URL (expected `s5://export/…`)"))?;

        // Split off the fragment (the secret) first — it may contain no `?`/`&`.
        let (before_frag, secret) = rest
            .split_once('#')
            .ok_or_else(|| anyhow!("export URL has no `#<secret>` fragment"))?;
        if secret.trim().is_empty() {
            bail!("export URL fragment (the decryption secret) is empty");
        }

        let (label, query) = before_frag
            .split_once('?')
            .ok_or_else(|| anyhow!("export URL has no `?m=<hash>` query"))?;

        crate::validate_share_label(label)
            .map_err(|e| anyhow!("export URL vault label '{label}': {e}"))?;

        let m = query
            .split('&')
            .find_map(|kv| kv.strip_prefix("m="))
            .ok_or_else(|| anyhow!("export URL query has no `m=<hash>` parameter"))?;
        let hash_bytes = hex::decode(m).context("export URL `m` is not valid hex")?;
        let hash_arr: [u8; 32] = hash_bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("export URL `m` is not a 32-byte hash"))?;

        Ok(Self {
            label: label.to_string(),
            blob_hash: Hash::from(hash_arr),
            secret: secret.to_string(),
        })
    }
}

/// Fetch the frozen export blob from whichever configured store holds it
/// (content-addressed → any store with the bytes works — the "pre-configured
/// shared store" model of `share-links.md`), decrypt it with the ephemeral
/// secret, and write a read-only local vault root under `root_dir`,
/// re-encrypted to `recipients` (the consumer's own keys).
///
/// Returns the parsed URL (so the caller can wire up `[vault.<label>]`).
pub async fn join_export(
    url: &str,
    stores: &HashMap<String, Arc<dyn Blobs>>,
    recipients: &[String],
    root_dir: &std::path::Path,
) -> Result<ExportUrl> {
    let parsed = ExportUrl::parse(url)?;

    // Fetch by hash from any configured store (BlobsRead verifies the bytes
    // against the hash, so a mis-serving store is rejected).
    let mut encrypted = None;
    for store in stores.values() {
        if let Ok(bytes) = store.blob_download(parsed.blob_hash).await {
            encrypted = Some(bytes);
            break;
        }
    }
    let encrypted = encrypted.ok_or_else(|| {
        anyhow!(
            "export blob {} not found in any configured store — the recipient \
             needs access to the store the producer uploaded it to (a shared \
             indexer, or the producer online)",
            parsed.blob_hash
        )
    })?;

    // Decrypt with the ephemeral secret from the fragment.
    let identity: age::x25519::Identity = parsed
        .secret
        .parse()
        .map_err(|e| anyhow!("export URL secret is not a valid age identity: {e}"))?;
    let cbor = age_decrypt_with_identity(&encrypted, &identity)
        .context("decrypting the frozen export (wrong or corrupt secret?)")?;

    // Validate it's a Transparent Node, then re-wrap to the consumer's keys.
    let node = Node::from_bytes(&cbor)
        .map_err(|e| anyhow!("export blob did not decode as a Transparent Node: {e}"))?;
    let path = crate::tasks::vault_persist::vault_root_path(&root_dir.to_string_lossy());
    crate::tasks::vault_persist::save_node(&path, &node, recipients)
        .context("writing the joined vault's local root")?;

    Ok(parsed)
}

/// Age-decrypt `ciphertext` with a single in-memory x25519 identity.
fn age_decrypt_with_identity(
    ciphertext: &[u8],
    identity: &age::x25519::Identity,
) -> Result<Vec<u8>> {
    use std::io::Read;
    let decryptor = age::Decryptor::new(ciphertext).context("parsing age header")?;
    let mut reader = decryptor
        .decrypt(std::iter::once(identity as &dyn age::Identity))
        .context("age decrypt (secret does not match)")?;
    let mut out = Vec::new();
    reader
        .read_to_end(&mut out)
        .context("reading age plaintext")?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_well_formed_export_url() {
        let hash = Hash::from([0xABu8; 32]);
        let url = format!(
            "s5://export/music?m={}#AGE-SECRET-KEY-1EXAMPLE",
            hex::encode(hash.as_bytes())
        );
        let parsed = ExportUrl::parse(&url).unwrap();
        assert_eq!(parsed.label, "music");
        assert_eq!(parsed.blob_hash, hash);
        assert_eq!(parsed.secret, "AGE-SECRET-KEY-1EXAMPLE");
    }

    #[test]
    fn rejects_malformed_urls() {
        // wrong intent
        assert!(ExportUrl::parse("s5://recover/x?m=ab#s").is_err());
        // no fragment
        assert!(
            ExportUrl::parse(
                "s5://export/x?m=0000000000000000000000000000000000000000000000000000000000000000"
            )
            .is_err()
        );
        // empty secret
        assert!(
            ExportUrl::parse(
                "s5://export/x?m=0000000000000000000000000000000000000000000000000000000000000000#"
            )
            .is_err()
        );
        // no m=
        assert!(ExportUrl::parse("s5://export/x?y=1#s").is_err());
        // bad hex
        assert!(ExportUrl::parse("s5://export/x?m=zz#s").is_err());
        // bad label
        assert!(ExportUrl::parse("s5://export/BAD NAME?m=ab#s").is_err());
    }
}
