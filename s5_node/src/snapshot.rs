//! Snapshot utilities — duration parsing and legacy snapshot-cycle stub.

use std::time::Duration;

use anyhow::{Context, Result, anyhow};

use crate::S5Node;

// ---------------------------------------------------------------------------
// Node integration — spawns ingest workers + snapshot timer from config
// ---------------------------------------------------------------------------

/// Legacy stub — ingest and snapshot publishing are now driven by the
/// vault/task model via the task executor and RunTask RPC.
pub async fn spawn_snapshot_cycles(_node: &S5Node) {
    tracing::debug!(
        "spawn_snapshot_cycles: no-op (fs config removed, use vault/task model instead)"
    );
}

// ---------------------------------------------------------------------------
// Duration parsing
// ---------------------------------------------------------------------------

/// Parses a human-friendly duration string like "10s", "60s", "5m", "1h".
///
/// Supported suffixes: `s` (seconds), `m` (minutes), `h` (hours).
/// Plain numbers without suffix are treated as seconds.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Err(anyhow!("empty duration string"));
    }

    let (num_str, multiplier) = if let Some(n) = s.strip_suffix('s') {
        (n, 1u64)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3600)
    } else {
        (s, 1)
    };

    let num: u64 = num_str
        .trim()
        .parse()
        .with_context(|| format!("invalid duration number: {num_str:?}"))?;

    Ok(Duration::from_secs(num * multiplier))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("60s").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("0s").unwrap(), Duration::from_secs(0));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
    }

    #[test]
    fn test_parse_duration_bare_number() {
        assert_eq!(parse_duration("30").unwrap(), Duration::from_secs(30));
    }

    #[test]
    fn test_parse_duration_whitespace() {
        assert_eq!(parse_duration("  10s  ").unwrap(), Duration::from_secs(10));
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("10x").is_err());
    }
}
