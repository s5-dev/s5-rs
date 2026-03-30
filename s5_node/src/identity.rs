use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;

use base64::Engine;
use iroh::SecretKey;
use s5_node_api::config::NodeConfigKey;

/// Load the node's secret key from config.
///
/// The `config_dir` should be the directory containing the config file,
/// used to resolve relative `secret_key_file` paths.
///
/// When `identity.encrypted_with` names a key from `keys`, the secret key
/// file is age-decrypted using that key's `identity_file` before parsing.
pub fn load_secret_key(
    identity: &crate::config::NodeConfigIdentity,
    config_dir: Option<&Path>,
    keys: &BTreeMap<String, NodeConfigKey>,
) -> Option<SecretKey> {
    // Prefer inline key over file
    if let Some(s) = &identity.secret_key
        && let Some(sk) = parse_secret_key_string(s)
    {
        return Some(sk);
    }
    if let Some(path_str) = &identity.secret_key_file {
        // Resolve relative paths against config directory
        let path = Path::new(path_str);
        let resolved = if path.is_relative() {
            if let Some(dir) = config_dir {
                dir.join(path)
            } else {
                path.to_path_buf()
            }
        } else {
            path.to_path_buf()
        };

        if let Ok(raw_bytes) = std::fs::read(&resolved) {
            let bytes = if let Some(key_name) = &identity.encrypted_with {
                match age_decrypt_with_key(&raw_bytes, key_name, keys, config_dir) {
                    Ok(decrypted) => decrypted,
                    Err(e) => {
                        tracing::warn!(
                            key = key_name,
                            "failed to age-decrypt node identity: {e}"
                        );
                        return None;
                    }
                }
            } else {
                raw_bytes
            };

            if let Ok(s) = std::str::from_utf8(&bytes)
                && let Some(sk) = parse_secret_key_string(s.trim())
            {
                return Some(sk);
            }
            if let Some(sk) = parse_secret_key_bytes(&bytes) {
                return Some(sk);
            }
        }
    }
    None
}

/// Age-decrypt `ciphertext` using the identity file of the named key.
fn age_decrypt_with_key(
    ciphertext: &[u8],
    key_name: &str,
    keys: &BTreeMap<String, NodeConfigKey>,
    config_dir: Option<&Path>,
) -> anyhow::Result<Vec<u8>> {
    let key_cfg = keys
        .get(key_name)
        .ok_or_else(|| anyhow::anyhow!("encrypted_with references unknown key '{key_name}'"))?;

    let id_path_str = key_cfg
        .identity_file
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("key '{key_name}' has no identity_file for decryption"))?;

    // Resolve relative paths
    let id_path = Path::new(id_path_str);
    let resolved = if id_path.is_relative() {
        config_dir
            .map(|d| d.join(id_path))
            .unwrap_or_else(|| id_path.to_path_buf())
    } else {
        id_path.to_path_buf()
    };

    let file_content = std::fs::read_to_string(&resolved)
        .map_err(|e| anyhow::anyhow!("reading identity file '{}': {e}", resolved.display()))?;

    let identity_file =
        age::IdentityFile::from_buffer(std::io::BufReader::new(file_content.as_bytes()))
            .map_err(|e| anyhow::anyhow!("parsing identity file '{}': {e}", resolved.display()))?;

    let identities = identity_file
        .into_identities()
        .map_err(|e| anyhow::anyhow!("loading identities: {e}"))?;

    let decryptor = age::Decryptor::new(ciphertext)
        .map_err(|e| anyhow::anyhow!("age decryptor: {e}"))?;

    let id_refs: Vec<&dyn age::Identity> = identities.iter().map(|i| i.as_ref()).collect();

    let mut reader = decryptor
        .decrypt(id_refs.into_iter())
        .map_err(|e| anyhow::anyhow!("age decrypt failed: {e}"))?;

    let mut plaintext = vec![];
    reader.read_to_end(&mut plaintext)?;
    Ok(plaintext)
}

pub fn parse_secret_key_string(s: &str) -> Option<SecretKey> {
    let s = s.trim();
    if let Ok(bytes) = hex::decode(s)
        && let Some(sk) = parse_secret_key_bytes(&bytes)
    {
        return Some(sk);
    }
    if let Ok(bytes) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s)
        && let Some(sk) = parse_secret_key_bytes(&bytes)
    {
        return Some(sk);
    }
    None
}

pub fn parse_secret_key_bytes(bytes: &[u8]) -> Option<SecretKey> {
    if bytes.len() == 32 {
        let mut arr = [0u8; 32];
        arr.copy_from_slice(bytes);
        return Some(SecretKey::from_bytes(&arr));
    }
    None
}
