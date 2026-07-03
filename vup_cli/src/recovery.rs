//! Recovery key generation, rooted in the paper mnemonic.
//!
//! [`generate_recovery_phrase`] mints a 12-word BIP-39 phrase and derives the
//! age recovery recipient from it (via [`s5_node::mnemonic`]). The phrase is the
//! user's offline recovery token — the only thing they write down; the age
//! public key it derives is what lands in the node config as `[key.recovery]`.
//! Because the recipient is *derived*, recovery needs only the words, not a
//! separately-stored key. Full disaster recovery is `vup recover`, which derives
//! the identity from the phrase and resolves every vault via the config vault
//! ([`s5_node::bootstrap`]).

use anyhow::Result;

/// Generate a fresh recovery **phrase** and the age recipient derived from it.
///
/// Returns `(mnemonic, recovery_public)`:
/// - `mnemonic` — the 12-word phrase, shown once and written on paper.
/// - `recovery_public` — `"age1…"`, stored in `[key.recovery]`.
pub fn generate_recovery_phrase() -> Result<(String, String)> {
    let mnemonic = s5_node::mnemonic::generate_mnemonic()?;
    let root_master = s5_node::mnemonic::root_master(&mnemonic)?;
    let recovery_public = s5_node::mnemonic::paper_age_identity(&root_master)?
        .to_public()
        .to_string();
    Ok((mnemonic, recovery_public))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phrase_is_12_words_and_derives_an_age_recipient() {
        let (mnemonic, pubkey) = generate_recovery_phrase().unwrap();
        assert_eq!(mnemonic.split_whitespace().count(), 12);
        assert!(pubkey.starts_with("age1"), "got: {pubkey}");
    }

    #[test]
    fn the_phrase_re_derives_the_same_recipient() {
        // The load-bearing property: typing the words back reproduces the
        // recipient the config was set up with.
        let (mnemonic, pubkey) = generate_recovery_phrase().unwrap();
        let root_master = s5_node::mnemonic::root_master(&mnemonic).unwrap();
        let re = s5_node::mnemonic::paper_age_identity(&root_master)
            .unwrap()
            .to_public()
            .to_string();
        assert_eq!(re, pubkey);
    }
}
