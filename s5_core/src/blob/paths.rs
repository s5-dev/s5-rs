use crate::{Hash, store::StoreFeatures};
use base64::Engine;

pub fn path_for_hash(hash: Hash, features: &StoreFeatures) -> String {
    let hash_str = if features.case_sensitive {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash)
    } else {
        let mut output = Vec::with_capacity(base32_fs::encoded_len(hash.as_bytes().len()));
        base32_fs::encode(hash.as_bytes(), &mut output);
        String::from_utf8(output).unwrap()
    };

    if features.recommended_max_dir_size < 10000 {
        if features.case_sensitive {
            format!("{}/{}/{}", &hash_str[0..2], &hash_str[2..4], &hash_str[4..],)
        } else {
            format!(
                "{}/{}/{}/{}",
                &hash_str[0..2],
                &hash_str[2..4],
                &hash_str[4..6],
                &hash_str[6..]
            )
        }
    } else {
        hash_str
    }
}

pub fn blob_path_for_hash(hash: Hash, features: &StoreFeatures) -> String {
    format!("blob3/{}", path_for_hash(hash, features))
}

pub fn obao6_path_for_hash(hash: Hash, features: &StoreFeatures) -> String {
    format!("obao6/{}", path_for_hash(hash, features))
}

pub fn hash_from_blob_path(
    path: &str,
    features: &StoreFeatures,
) -> Result<Option<Hash>, std::io::Error> {
    const PREFIX: &str = "blob3/";
    if !path.starts_with(PREFIX) {
        return Ok(None);
    }
    let rest = &path[PREFIX.len()..];
    let encoded: String = rest.chars().filter(|&c| c != '/').collect();
    if encoded.is_empty() {
        return Ok(None);
    }

    let bytes = if features.case_sensitive {
        // Base64url (no padding) variant used for case-sensitive stores.
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded.as_bytes())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
    } else {
        // Base32 variant used for case-insensitive file systems. We
        // validate the string first to avoid panics in `decode`.
        if !base32_fs::is_valid(encoded.as_bytes()) {
            return Ok(None);
        }
        let len = match base32_fs::decoded_len(encoded.len()) {
            Some(len) => len,
            None => return Ok(None),
        };
        let mut out = Vec::with_capacity(len);
        let _ = base32_fs::decode(encoded.as_bytes(), &mut out);
        out
    };

    if bytes.len() != 32 {
        return Ok(None);
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(Some(Hash::from_bytes(arr)))
}
