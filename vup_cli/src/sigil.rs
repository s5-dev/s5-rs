//! Sigil parsing for the `vup` CLI grammar.
//!
//! The grammar uses sigil-prefixed tokens to denote references:
//!
//! - `+vault` — a configured vault name (load-bearing, wired here)
//! - `@identity` — a paired peer/identity (handled by individual verb args)
//! - `#snapshot` — a snapshot id (handled by individual verb args)
//! - `:namespace` — a path namespace (handled by individual verb args)
//!
//! The only sigil this module wires for routing is `+vault`. Positional
//! placement is required:
//!
//! - **Vault-scoped verbs:** `+vault` is the first non-flag arg
//!   (`vup +music snap`).
//! - **Top-level verbs taking a vault:** `+vault` appears in the verb's
//!   positional slot (`vup new +music`); the value parser strips the `+`.
//!
//! "Subject-first or verb-first" flexibility (`vup snap +music`) is not
//! supported — it can be added later without breaking the canonical form.
//!
//! `++` is reserved for future tag syntax and left untouched.

/// If the first positional (non-flag) arg starts with a single `+`,
/// rewrites argv so clap dispatches to the hidden `vault` subcommand
/// with the bare name as the next arg. Walks past known global flags
/// (`--config <path>`, `-v`/`-q`/`--verbose`/`--quiet [<n>]`) so a
/// user can write either:
///   - `vup +music snap`
///   - `vup --config /path +music snap`
///
/// New global flags need updating `find_first_positional` below.
///
/// - `["vup", "+music", "snap"]` → `["vup", "vault", "music", "snap"]`
/// - `["vup", "--config", "/p", "+all", "s"]` → `["vup", "--config", "/p", "vault", "all", "s"]`
/// - `["vup", "snap"]` → unchanged
/// - `["vup", "new", "+music"]` → unchanged (`+music` is a value to `new`)
/// - `["vup", "++tag", "x"]` → unchanged (`++` reserved)
pub fn rewrite_vault_prefix(argv: &mut Vec<String>) {
    let Some(idx) = find_first_positional(argv) else {
        return;
    };
    let arg = &argv[idx];
    if !arg.starts_with('+') || arg.starts_with("++") {
        return;
    }
    let name = arg[1..].to_string();
    argv[idx] = "vault".to_string();
    argv.insert(idx + 1, name);
}

/// Index of the first positional argument in argv (skipping argv[0] and any
/// known global flags). Returns `None` if argv has no positional args.
///
/// Knowledge baked in:
/// - `--config <path>` consumes the next arg as a value
/// - `--config=<path>` consumes itself
/// - `-v`/`-q`/`--verbose`/`--quiet` are bool flags (no value)
/// - any other `--xxx=yyy` is self-contained
/// - any other `--xxx` (or `-x`) is treated as a bool flag (no value)
fn find_first_positional(argv: &[String]) -> Option<usize> {
    let mut i = 1;
    while i < argv.len() {
        let arg = &argv[i];
        if arg == "--config" {
            i += 2; // skip flag + value
            continue;
        }
        if arg.starts_with('-') {
            i += 1; // bool flag or self-contained --xxx=yyy
            continue;
        }
        return Some(i);
    }
    None
}

/// clap value parser for `+vault` arguments to top-level verbs (`new`, `drop`).
///
/// Accepts both `+music` and `music`; strips the leading `+` if present;
/// enforces the character/length rules. Reserved-name policy (`default`,
/// `all`, `paper`, etc.) is verb-specific and checked by the verb handler.
pub fn parse_vault_ref(s: &str) -> Result<String, String> {
    let name = s.strip_prefix('+').unwrap_or(s);
    validate_vault_name_chars(name)?;
    Ok(name.to_string())
}

/// Character + length rules for vault names per `cli-workflows.md` §
/// Vault name validation. Does **not** check reserved names — that's
/// the calling verb's responsibility (e.g., `new` rejects `default`/`all`,
/// `+all` is a valid wildcard for vault-scoped commands).
pub fn validate_vault_name_chars(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("vault name is empty".into());
    }
    if name.len() > 64 {
        return Err(format!("vault name '{name}' is longer than 64 chars"));
    }
    let first = name.chars().next().unwrap();
    if first.is_ascii_digit() {
        return Err(format!("vault name '{name}' cannot start with a digit"));
    }
    if first == '_' {
        return Err(format!(
            "vault name '{name}' cannot start with '_' (reserved for internal use)"
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
    {
        return Err(format!(
            "vault name '{name}' contains characters outside [a-z0-9_-]"
        ));
    }
    Ok(())
}

/// Reserved vault names that verbs creating/modifying a vault must reject.
/// `all` is **not** in this list — it's a valid wildcard for vault-scoped
/// dispatch. Verbs that don't support wildcards (`new`, `drop`) check for
/// `all` separately.
#[allow(dead_code)] // consumed by the upcoming `new`/`drop` verb implementations
pub const RESERVED_VAULT_NAMES: &[&str] = &["default", "none", "self", "me", "paper", "recovery"];

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn rewrite_subject_first_inserts_vault_subcommand() {
        let mut a = argv(&["vup", "+music", "snap"]);
        rewrite_vault_prefix(&mut a);
        assert_eq!(a, argv(&["vup", "vault", "music", "snap"]));
    }

    #[test]
    fn rewrite_all_wildcard_routes_through_vault() {
        let mut a = argv(&["vup", "+all", "s"]);
        rewrite_vault_prefix(&mut a);
        assert_eq!(a, argv(&["vup", "vault", "all", "s"]));
    }

    #[test]
    fn rewrite_top_level_verb_untouched() {
        let mut a = argv(&["vup", "ls"]);
        rewrite_vault_prefix(&mut a);
        assert_eq!(a, argv(&["vup", "ls"]));
    }

    #[test]
    fn rewrite_new_with_vault_arg_untouched() {
        let mut a = argv(&["vup", "new", "+music"]);
        rewrite_vault_prefix(&mut a);
        assert_eq!(a, argv(&["vup", "new", "+music"]));
    }

    #[test]
    fn rewrite_double_plus_untouched() {
        let mut a = argv(&["vup", "++tag", "x"]);
        rewrite_vault_prefix(&mut a);
        assert_eq!(a, argv(&["vup", "++tag", "x"]));
    }

    #[test]
    fn rewrite_program_only_untouched() {
        let mut a = argv(&["vup"]);
        rewrite_vault_prefix(&mut a);
        assert_eq!(a, argv(&["vup"]));
    }

    #[test]
    fn rewrite_with_global_config_flag() {
        let mut a = argv(&["vup", "--config", "/etc/vup.toml", "+music", "snap"]);
        rewrite_vault_prefix(&mut a);
        assert_eq!(
            a,
            argv(&["vup", "--config", "/etc/vup.toml", "vault", "music", "snap"])
        );
    }

    #[test]
    fn rewrite_with_verbosity_flag() {
        let mut a = argv(&["vup", "-v", "+music", "snap"]);
        rewrite_vault_prefix(&mut a);
        assert_eq!(a, argv(&["vup", "-v", "vault", "music", "snap"]));
    }

    #[test]
    fn rewrite_with_combined_globals() {
        let mut a = argv(&["vup", "--config", "/p", "--verbose", "+music", "s"]);
        rewrite_vault_prefix(&mut a);
        assert_eq!(
            a,
            argv(&["vup", "--config", "/p", "--verbose", "vault", "music", "s"])
        );
    }

    #[test]
    fn parse_vault_ref_accepts_both_forms() {
        assert_eq!(parse_vault_ref("music").unwrap(), "music");
        assert_eq!(parse_vault_ref("+music").unwrap(), "music");
    }

    #[test]
    fn validate_chars_rejects_invalid() {
        assert!(validate_vault_name_chars("").is_err());
        assert!(validate_vault_name_chars("Music").is_err()); // uppercase
        assert!(validate_vault_name_chars("1music").is_err()); // starts with digit
        assert!(validate_vault_name_chars("_internal").is_err()); // _ prefix
        assert!(validate_vault_name_chars("with space").is_err());
        assert!(validate_vault_name_chars("with/slash").is_err());
        assert!(validate_vault_name_chars(&"a".repeat(65)).is_err());
    }

    #[test]
    fn validate_chars_accepts_valid() {
        assert!(validate_vault_name_chars("a").is_ok());
        assert!(validate_vault_name_chars("music").is_ok());
        assert!(validate_vault_name_chars("my-vault-2").is_ok());
        assert!(validate_vault_name_chars("snake_case").is_ok());
        assert!(validate_vault_name_chars(&"a".repeat(64)).is_ok());
        // 'all' passes char rules — wildcard handling is verb-level
        assert!(validate_vault_name_chars("all").is_ok());
    }
}
