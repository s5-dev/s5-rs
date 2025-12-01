use base64::Engine;
use iroh::SecretKey;
use std::path::Path;

/// Load the node's secret key from config.
///
/// The `config_dir` should be the directory containing the config file,
/// used to resolve relative `secret_key_file` paths.
pub fn load_secret_key(
    identity: &crate::config::NodeConfigIdentity,
    config_dir: Option<&Path>,
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

        if let Ok(bytes) = std::fs::read(&resolved) {
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
