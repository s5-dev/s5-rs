//! D20 reference grammar for the `vup` CLI.
//!
//! Supersedes the old `+vault` sigil (decision D20). A
//! **reference** is one of:
//!
//! ```text
//! ref := vault ':' [path] ['#' snap]   — vault-scoped (the colon marks a vault)
//!      | '#' fullhash                  — vault-free immutable snapshot (read-only)
//!      | path                          — a local filesystem path, ALWAYS literal
//! ```
//!
//! Examples:
//! - `docs:`                    whole vault, live head
//! - `docs:Photos/2024`         a path inside the vault, live head
//! - `docs:#3`                  the whole vault at snapshot 3
//! - `docs:report.md#2026-06-01` a path at a named/dated snapshot
//! - `#<blake3>`                a self-certifying snapshot root, no vault context
//! - `~/Music`, `./x`, `/etc`   local paths, always
//!
//! Rules:
//! - **The colon is the only vault marker.** A bare token is a local path
//!   and may legally contain `#`, so `#` suffixes are parsed ONLY inside
//!   vault refs and in the bare-hash form — local paths are never split
//!   on `#`. (History on a local file is addressed through its vault:
//!   `docs:report.md#3`.)
//! - Tokens starting with `./`, `../`, `/`, `~`, or matching a Windows
//!   drive (`X:\`, `X:/`) are paths, always. A single letter before a
//!   colon is reserved for drive-letter disambiguation and is treated as
//!   a local path — a local name containing `:` is reachable as
//!   `./weird:file` (rclone's rule).
//! - User vault names: `[a-z0-9][a-z0-9._-]*`, length ≥ 2. System vaults
//!   start with `_` (`_config:`, `_identity:`); creating a user vault
//!   named `_*` is refused.

use std::path::PathBuf;

/// A parsed D20 reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ref {
    /// A vault-scoped reference: `vault:[path][#snap]`. `name` may be a
    /// system vault (leading `_`). `path` is `None` for the whole vault;
    /// `snap` is `None` for the live head.
    Vault {
        name: String,
        path: Option<String>,
        snap: Option<String>,
    },
    /// A bare, vault-free immutable snapshot: `#<fullhash>` (read-only).
    Hash(String),
    /// A literal local filesystem path.
    Local(PathBuf),
}

impl Ref {
    /// Require this ref to be a vault ref, returning its parts. Used by
    /// verbs whose subject must be a vault (`grant`, `history`, …).
    pub fn require_vault(self) -> Result<VaultRef, String> {
        match self {
            Ref::Vault { name, path, snap } => Ok(VaultRef { name, path, snap }),
            Ref::Hash(_) => {
                Err("expected a vault reference (e.g. `docs:`), got a bare `#snapshot`".into())
            }
            Ref::Local(p) => Err(format!(
                "expected a vault reference (e.g. `docs:`), got the local path '{}' \
                 (a vault ref needs a trailing colon)",
                p.display()
            )),
        }
    }
}

/// The parts of a vault reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultRef {
    pub name: String,
    pub path: Option<String>,
    pub snap: Option<String>,
}

/// Parse a single token into a [`Ref`].
pub fn parse_ref(s: &str) -> Result<Ref, String> {
    if s.is_empty() {
        return Err("empty reference".into());
    }

    // Bare immutable snapshot: `#<hash>`.
    if let Some(hash) = s.strip_prefix('#') {
        if hash.is_empty() {
            return Err("'#' must be followed by a snapshot hash".into());
        }
        return Ok(Ref::Hash(hash.to_string()));
    }

    // Explicit local paths (incl. Windows drives) are always literal.
    if is_local_path(s) {
        return Ok(Ref::Local(PathBuf::from(s)));
    }

    // The colon is the only vault marker.
    if let Some(colon) = s.find(':') {
        let name = &s[..colon];
        let rest = &s[colon + 1..];

        // A single letter before the colon is the drive-letter space —
        // treat as a local path (use `./` for a real colon-containing name).
        if name.chars().count() == 1 {
            return Ok(Ref::Local(PathBuf::from(s)));
        }

        validate_vault_ref_name(name)?;

        let (path, snap) = match rest.find('#') {
            Some(h) => (non_empty(&rest[..h]), non_empty(&rest[h + 1..])),
            None => (non_empty(rest), None),
        };
        return Ok(Ref::Vault {
            name: name.to_string(),
            path,
            snap,
        });
    }

    // A bare token with no colon is a local path — never split on '#'.
    Ok(Ref::Local(PathBuf::from(s)))
}

/// Split a variadic `backup [SRC…] vault:[path]` invocation into the
/// source paths and the single destination vault ref. The destination is
/// the (exactly one) argument that parses as a vault ref; every other
/// argument is a source path. Unambiguous by D20: only vault refs carry a
/// colon, and a local path that contains one must be written `./x:y`.
pub fn split_backup_args(args: &[String]) -> Result<(Vec<PathBuf>, Option<VaultRef>), String> {
    let mut srcs = Vec::new();
    let mut dest: Option<VaultRef> = None;
    for a in args {
        match parse_ref(a)? {
            Ref::Vault { name, path, snap } => {
                if dest.is_some() {
                    return Err(format!(
                        "more than one vault reference given ('{}' and another); \
                         backup takes exactly one destination vault",
                        name
                    ));
                }
                dest = Some(VaultRef { name, path, snap });
            }
            Ref::Local(p) => srcs.push(p),
            Ref::Hash(h) => {
                return Err(format!(
                    "'#{h}' is a snapshot, not a backup source or destination"
                ));
            }
        }
    }
    Ok((srcs, dest))
}

/// clap value parser: accept only a syntactically-valid vault reference,
/// returning the original string. Rejects locals and bare `#hash` at parse
/// time (pre-daemon) so a typo can't spawn/hit the daemon. Existence is
/// still checked later, against the live config.
pub fn vault_ref_arg(s: &str) -> Result<String, String> {
    parse_ref(s).and_then(Ref::require_vault)?;
    Ok(s.to_string())
}

/// True when `name` is a system vault (leading `_`), e.g. `_config`.
pub fn is_system_vault(name: &str) -> bool {
    name.starts_with('_')
}

/// Validate a vault name that appears in a *reference* — accepts both
/// user vaults and `_`-prefixed system vaults. Does NOT enforce the
/// reserved-name or no-leading-`_` rules (those gate vault *creation*;
/// see [`validate_user_vault_name`]).
pub fn validate_vault_ref_name(name: &str) -> Result<(), String> {
    let count = name.chars().count();
    if count < 2 {
        return Err(format!(
            "vault name '{name}' is too short (min 2 chars; single letters are reserved \
             for drive letters — use `./{name}:…` for a local path)"
        ));
    }
    if count > 64 {
        return Err(format!("vault name '{name}' is longer than 64 chars"));
    }
    let first = name.chars().next().unwrap();
    if !(first.is_ascii_lowercase() || first.is_ascii_digit() || first == '_') {
        return Err(format!(
            "vault name '{name}' must start with a lowercase letter, digit, or '_' (system)"
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_' || c == '-')
    {
        return Err(format!(
            "vault name '{name}' contains characters outside [a-z0-9._-]"
        ));
    }
    Ok(())
}

/// Validate a vault name for *creation* — user vaults only: the reference
/// rules, plus no leading `_` (reserved for system vaults) and not a
/// reserved word.
pub fn validate_user_vault_name(name: &str) -> Result<(), String> {
    validate_vault_ref_name(name)?;
    if name.starts_with('_') {
        return Err(format!(
            "vault name '{name}' cannot start with '_' (reserved for system vaults)"
        ));
    }
    if RESERVED_VAULT_NAMES.contains(&name) {
        return Err(format!("'{name}' is a reserved name; pick another"));
    }
    Ok(())
}

/// Reserved vault names: never valid as a user vault, even though they
/// pass the character rules. (`all` was once a multi-vault wildcard; the
/// wildcard is gone but the name stays reserved.)
pub const RESERVED_VAULT_NAMES: &[&str] =
    &["default", "none", "self", "me", "paper", "recovery", "all"];

// ---------------------------------------------------------------------------
// Legacy `+vault` compatibility (hidden through the beta)
// ---------------------------------------------------------------------------

/// Rewrite the legacy subject-first `+vault <verb> …` invocation into the
/// D20 verb-first `<verb> vault: …` form, so old muscle memory keeps
/// working through the beta (D16/D20 alias precedent). Only fires when the
/// first positional arg starts with a single `+` (not `++`, reserved for
/// future tag syntax). Walks past known global flags first.
///
/// - `["vup", "+music", "backup"]` → `["vup", "backup", "music:"]`
/// - `["vup", "+work", "restore", "./out"]` → `["vup", "restore", "work:", "./out"]`
/// - `["vup", "--config", "/p", "+m", "who"]` → `["vup", "--config", "/p", "who", "m:"]`
/// - `["vup", "backup", "music:"]` → unchanged (already verb-first)
/// - `["vup", "new", "+music"]` → unchanged (`+music` is a value, not subject)
pub fn rewrite_legacy_plus(argv: &mut [String]) {
    let Some(idx) = find_first_positional(argv) else {
        return;
    };
    let arg = &argv[idx];
    if !arg.starts_with('+') || arg.starts_with("++") {
        return;
    }
    let name = arg[1..].to_string();
    if name.is_empty() {
        return;
    }
    if idx + 1 < argv.len() {
        // `+vault verb …` → `verb vault: …`: swap the two tokens, appending
        // the colon so the name becomes a D20 vault ref.
        argv[idx] = argv[idx + 1].clone();
        argv[idx + 1] = format!("{name}:");
    } else {
        // Bare `+vault` with no verb — best-effort: treat as `list vault:`.
        argv[idx] = "list".to_string();
        // Safe: idx is the last element; extend is not possible on a slice,
        // so callers that want the bare form handled must pass a Vec. Here we
        // only rewrite in place; the bare case degrades to `list` with no ref.
    }
}

/// Index of the first positional argument (skipping argv[0] and known
/// global flags: `--config <path>`, `-v`/`-q`/`--verbose`/`--quiet`, and
/// any other `--flag`/`-x`). Mirrors the old sigil router.
fn find_first_positional(argv: &[String]) -> Option<usize> {
    let mut i = 1;
    while i < argv.len() {
        let arg = &argv[i];
        if arg == "--config" {
            i += 2; // flag + value
            continue;
        }
        if arg.starts_with('-') {
            i += 1; // bool flag or self-contained --flag=value
            continue;
        }
        return Some(i);
    }
    None
}

/// Strip a leading `+` from a vault value passed to the hidden `new`/`drop`
/// aliases (`vup new +music` and `vup new music` both work).
pub fn strip_plus(s: &str) -> String {
    s.strip_prefix('+').unwrap_or(s).to_string()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// True when `s` is unambiguously a local path: a `.`/`..`/`/`/`~` prefix,
/// or a Windows drive (`X:\`, `X:/`).
fn is_local_path(s: &str) -> bool {
    s == "."
        || s == ".."
        || s.starts_with("./")
        || s.starts_with("../")
        || s.starts_with(".\\")
        || s.starts_with("..\\")
        || s.starts_with('/')
        || s.starts_with('~')
        || is_windows_drive(s)
}

/// True when `s` starts with a Windows drive designator `X:\` or `X:/`.
fn is_windows_drive(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() >= 3 && b[0].is_ascii_alphabetic() && b[1] == b':' && (b[2] == b'\\' || b[2] == b'/')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vault(name: &str, path: Option<&str>, snap: Option<&str>) -> Ref {
        Ref::Vault {
            name: name.to_string(),
            path: path.map(String::from),
            snap: snap.map(String::from),
        }
    }

    #[test]
    fn whole_vault() {
        assert_eq!(parse_ref("docs:").unwrap(), vault("docs", None, None));
    }

    #[test]
    fn vault_with_path() {
        assert_eq!(
            parse_ref("docs:Photos/2024").unwrap(),
            vault("docs", Some("Photos/2024"), None)
        );
    }

    #[test]
    fn vault_at_snapshot() {
        assert_eq!(
            parse_ref("docs:#3").unwrap(),
            vault("docs", None, Some("3"))
        );
    }

    #[test]
    fn vault_path_at_snapshot() {
        assert_eq!(
            parse_ref("docs:report.md#2026-06-01").unwrap(),
            vault("docs", Some("report.md"), Some("2026-06-01"))
        );
    }

    #[test]
    fn bare_hash_is_immutable_snapshot() {
        assert_eq!(parse_ref("#b3a9f2").unwrap(), Ref::Hash("b3a9f2".into()));
    }

    #[test]
    fn bare_hash_needs_a_hash() {
        assert!(parse_ref("#").is_err());
    }

    #[test]
    fn system_vault_ref_parses() {
        assert_eq!(parse_ref("_config:").unwrap(), vault("_config", None, None));
        assert!(is_system_vault("_config"));
        assert!(!is_system_vault("docs"));
    }

    #[test]
    fn local_paths_are_literal() {
        assert_eq!(parse_ref("~/Music").unwrap(), Ref::Local("~/Music".into()));
        assert_eq!(parse_ref("./x").unwrap(), Ref::Local("./x".into()));
        assert_eq!(
            parse_ref("/etc/hosts").unwrap(),
            Ref::Local("/etc/hosts".into())
        );
        assert_eq!(parse_ref("../up").unwrap(), Ref::Local("../up".into()));
        assert_eq!(parse_ref(".").unwrap(), Ref::Local(".".into()));
    }

    #[test]
    fn bare_token_is_a_local_path_not_a_vault() {
        // The colon is the ONLY vault marker — `docs` (no colon) is a path.
        assert_eq!(parse_ref("docs").unwrap(), Ref::Local("docs".into()));
    }

    #[test]
    fn local_path_with_hash_is_not_split() {
        // A bare token may contain '#'; it is never split.
        assert_eq!(
            parse_ref("report#3.md").unwrap(),
            Ref::Local("report#3.md".into())
        );
    }

    #[test]
    fn windows_drive_is_local() {
        assert_eq!(
            parse_ref("C:\\Users").unwrap(),
            Ref::Local("C:\\Users".into())
        );
        assert_eq!(parse_ref("D:/data").unwrap(), Ref::Local("D:/data".into()));
    }

    #[test]
    fn single_letter_before_colon_is_local() {
        // Single letters are the drive-letter space, not vault names.
        assert_eq!(parse_ref("c:foo").unwrap(), Ref::Local("c:foo".into()));
    }

    #[test]
    fn uppercase_vault_name_rejected() {
        assert!(parse_ref("Music:").is_err());
    }

    #[test]
    fn require_vault_rejects_locals_and_hashes() {
        assert!(parse_ref("./x").unwrap().require_vault().is_err());
        assert!(parse_ref("#abc").unwrap().require_vault().is_err());
        assert!(parse_ref("docs:").unwrap().require_vault().is_ok());
    }

    #[test]
    fn split_backup_finds_the_single_vault_ref() {
        let args = vec!["./a".to_string(), "/b".to_string(), "docs:".to_string()];
        let (srcs, dest) = split_backup_args(&args).unwrap();
        assert_eq!(srcs, vec![PathBuf::from("./a"), PathBuf::from("/b")]);
        assert_eq!(dest.unwrap().name, "docs");
    }

    #[test]
    fn split_backup_no_vault_ref() {
        let args = vec!["./a".to_string()];
        let (srcs, dest) = split_backup_args(&args).unwrap();
        assert_eq!(srcs.len(), 1);
        assert!(dest.is_none());
    }

    #[test]
    fn split_backup_two_vault_refs_is_error() {
        let args = vec!["a:".to_string(), "docs:".to_string()];
        // note: `a:` is single-letter → local, so use two real vault names
        let args2 = vec!["aa:".to_string(), "docs:".to_string()];
        let _ = args;
        assert!(split_backup_args(&args2).is_err());
    }

    fn argv(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn legacy_plus_subject_first_rewrites() {
        let mut a = argv(&["vup", "+music", "backup"]);
        rewrite_legacy_plus(&mut a);
        assert_eq!(a, argv(&["vup", "backup", "music:"]));
    }

    #[test]
    fn legacy_plus_carries_trailing_args() {
        let mut a = argv(&["vup", "+work", "restore", "./out"]);
        rewrite_legacy_plus(&mut a);
        assert_eq!(a, argv(&["vup", "restore", "work:", "./out"]));
    }

    #[test]
    fn legacy_plus_skips_globals() {
        let mut a = argv(&["vup", "--config", "/p", "+m", "who"]);
        rewrite_legacy_plus(&mut a);
        assert_eq!(a, argv(&["vup", "--config", "/p", "who", "m:"]));
    }

    #[test]
    fn legacy_plus_leaves_verb_first_untouched() {
        let mut a = argv(&["vup", "new", "+music"]);
        rewrite_legacy_plus(&mut a);
        assert_eq!(a, argv(&["vup", "new", "+music"]));

        let mut b = argv(&["vup", "backup", "music:"]);
        rewrite_legacy_plus(&mut b);
        assert_eq!(b, argv(&["vup", "backup", "music:"]));
    }

    #[test]
    fn legacy_double_plus_untouched() {
        let mut a = argv(&["vup", "++tag", "x"]);
        rewrite_legacy_plus(&mut a);
        assert_eq!(a, argv(&["vup", "++tag", "x"]));
    }

    #[test]
    fn strip_plus_accepts_both() {
        assert_eq!(strip_plus("+music"), "music");
        assert_eq!(strip_plus("music"), "music");
    }

    #[test]
    fn user_vault_name_rules() {
        assert!(validate_user_vault_name("docs").is_ok());
        assert!(validate_user_vault_name("my-vault-2").is_ok());
        assert!(validate_user_vault_name("feeds.atproto").is_ok());
        assert!(validate_user_vault_name("a").is_err()); // too short
        assert!(validate_user_vault_name("_config").is_err()); // system
        assert!(validate_user_vault_name("Music").is_err()); // uppercase
        assert!(validate_user_vault_name("all").is_err()); // reserved
        assert!(validate_user_vault_name("1music").is_ok()); // digit first is allowed by D20
    }
}
