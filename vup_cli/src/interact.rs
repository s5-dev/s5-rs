//! Centralized prompt gate implementing the CLI scripting contract
//! (`cli-workflows.md` § Exit codes):
//!
//! - Prompts are shown only when stdin AND stdout are TTYs.
//! - `--yes` answers every confirmation and accepts every default
//!   without prompting.
//! - A prompt that is needed but cannot be shown fails with
//!   [`NonInteractive`], which `main` maps to **exit code 3**.
//!
//! Every interactive question in the CLI must go through these helpers —
//! never call `dialoguer` directly from a verb.

use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;

static ASSUME_YES: AtomicBool = AtomicBool::new(false);

/// Record the global `--yes` flag (called once from `main` after parsing).
pub fn set_assume_yes(yes: bool) {
    ASSUME_YES.store(yes, Ordering::Relaxed);
}

fn assume_yes() -> bool {
    ASSUME_YES.load(Ordering::Relaxed)
}

fn is_tty() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

/// A prompt was required but the session is non-interactive and `--yes`
/// couldn't answer it. `main` exits 3 on this error.
#[derive(Debug)]
pub struct NonInteractive(pub String);

impl std::fmt::Display for NonInteractive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for NonInteractive {}

fn refuse(prompt: &str) -> anyhow::Error {
    anyhow::Error::new(NonInteractive(format!(
        "'{prompt}' needs an interactive terminal (pass --yes to accept defaults, \
         or run from a TTY)"
    )))
}

/// Yes/no confirmation. `--yes` answers `true` without prompting.
pub fn confirm(prompt: &str, default: bool) -> Result<bool> {
    if assume_yes() {
        return Ok(true);
    }
    if !is_tty() {
        return Err(refuse(prompt));
    }
    Ok(dialoguer::Confirm::new()
        .with_prompt(prompt)
        .default(default)
        .interact()?)
}

/// Free-text input with a default; the prompt shows the default and Enter
/// accepts it. `--yes` takes the default without prompting.
pub fn input_with_default(prompt: &str, default: String) -> Result<String> {
    if assume_yes() {
        return Ok(default);
    }
    if !is_tty() {
        return Err(refuse(prompt));
    }
    Ok(dialoguer::Input::new()
        .with_prompt(format!("{prompt} (Enter for default)"))
        .default(default)
        .interact_text()?)
}

/// Free-text input with no sane default — prompted even under `--yes`;
/// any non-TTY run fails.
pub fn input_required(prompt: &str) -> Result<String> {
    if !is_tty() {
        return Err(refuse(prompt));
    }
    Ok(dialoguer::Input::new()
        .with_prompt(prompt)
        .interact_text()?)
}

/// Hidden input (recovery phrases, secrets). Same rules as [`input_required`].
pub fn password(prompt: &str) -> Result<String> {
    if !is_tty() {
        return Err(refuse(prompt));
    }
    Ok(dialoguer::Password::new().with_prompt(prompt).interact()?)
}

/// Menu selection returning the chosen index. `--yes` takes the default.
pub fn select(prompt: &str, items: &[&str], default: usize) -> Result<usize> {
    if assume_yes() {
        return Ok(default);
    }
    if !is_tty() {
        return Err(refuse(prompt));
    }
    Ok(dialoguer::Select::new()
        .with_prompt(prompt)
        .items(items)
        .default(default)
        .interact()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The test harness runs without a TTY, so this covers both legs in one
    /// test (the ASSUME_YES static is process-global — don't split this).
    #[test]
    fn yes_answers_and_non_tty_refuses() {
        set_assume_yes(true);
        assert!(confirm("proceed?", false).unwrap());
        assert_eq!(
            input_with_default("path", "/tmp/x".into()).unwrap(),
            "/tmp/x"
        );
        assert_eq!(select("pick", &["a", "b"], 1).unwrap(), 1);

        set_assume_yes(false);
        let err = confirm("proceed?", false).unwrap_err();
        assert!(err.downcast_ref::<NonInteractive>().is_some());
        let err = input_with_default("path", "/tmp/x".into()).unwrap_err();
        assert!(err.downcast_ref::<NonInteractive>().is_some());
        // Required inputs refuse on non-TTY even with --yes.
        set_assume_yes(true);
        let err = input_required("Endpoint URL").unwrap_err();
        assert!(err.downcast_ref::<NonInteractive>().is_some());
        let err = password("Recovery phrase").unwrap_err();
        assert!(err.downcast_ref::<NonInteractive>().is_some());
        set_assume_yes(false);
    }
}
