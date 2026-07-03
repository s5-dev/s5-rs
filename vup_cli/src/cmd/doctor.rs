//! `vup doctor` — health walk.
//!
//! One line per signal so a glance answers "is my backup actually safe?":
//! daemon reachable; each `[store.*]` reachable/UNREACHABLE; staging
//! drained? + last-flush age; the OS service installed/active; iroh peers
//! (formerly `vup debug peers`). Unreachable stores and undrained staging are
//! flagged `WARN` so they stand out — everything else is a terse `ok`.
//!
//! The per-store + staging signals come from the daemon's `GetHealth` RPC
//! (`s5_node::health::gather_health`); the service row reuses
//! `service::service_health`; peers reuse `DebugPeers`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use s5_node_api::S5NodeClient;

use crate::cmd::service::service_health;

/// `vup doctor` — walk the daemon's health signals.
pub async fn run_doctor(client: &S5NodeClient) -> Result<()> {
    println!("vup doctor");
    println!("─────────");

    let status = match client.get_status().await {
        Ok(s) => {
            println!("  daemon:   reachable");
            println!("  endpoint: {}", s.endpoint_id);
            s
        }
        Err(e) => {
            println!("  daemon:   UNREACHABLE ({e})");
            return Ok(());
        }
    };

    // ── Stores + staging (GetHealth) ─────────────────────────────────
    println!();
    match client.get_health().await {
        Ok(health) => {
            if health.stores.is_empty() {
                println!("  stores:   none configured");
            }
            for st in &health.stores {
                if st.reachable {
                    println!("  store {:<10} reachable", format!("{}:", st.name));
                } else {
                    let why = st.error.as_deref().unwrap_or("no response");
                    println!(
                        "  store {:<10} UNREACHABLE — WARN ({why})",
                        format!("{}:", st.name)
                    );
                }
                // Staging line only for backends that buffer writes.
                if let Some(g) = &st.staging {
                    let flushed = format_age(g.since_last_flush_secs);
                    if g.staged_bytes > 0 {
                        println!(
                            "            staging: {} NOT durable — WARN (last flush {} ago{})",
                            humansize::format_size(g.staged_bytes, humansize::BINARY),
                            flushed,
                            if g.inflight { ", upload in flight" } else { "" },
                        );
                    } else {
                        println!("            staging: drained (last flush {flushed} ago)");
                    }
                }
            }

            // ── Scheduled backups ────────────────────────────────────
            if !health.schedules.is_empty() {
                println!();
                println!("  scheduled backups:");
                for run in &health.schedules {
                    println!(
                        "    {:<12} every {}",
                        format!("{}:", run.vault),
                        format_age(run.interval_secs),
                    );
                }
            }
        }
        Err(e) => {
            println!("  stores:   (health unavailable: {e})");
        }
    }

    // ── Config summary (counts) ──────────────────────────────────────
    println!();
    println!(
        "  configured: {} store(s), {} vault(s), {} source(s), {} task(s) active",
        status.store_count, status.vault_count, status.source_count, status.running_tasks,
    );

    // ── OS service ───────────────────────────────────────────────────
    let svc = service_health();
    if svc.installed {
        if svc.active {
            println!("  service:  installed + active ({})", svc.detail);
        } else {
            println!("  service:  installed but INACTIVE — WARN ({})", svc.detail);
        }
    } else {
        println!("  service:  not a persistent service ({})", svc.detail);
    }

    // ── Peers ────────────────────────────────────────────────────────
    println!();
    print_peers(client).await;
    Ok(())
}

/// The iroh peer-observation table (formerly `vup debug peers`).
async fn print_peers(client: &S5NodeClient) {
    let resp = match client.debug_peers().await {
        Ok(r) => r,
        Err(e) => {
            println!("  peers:   (unavailable: {e})");
            return;
        }
    };
    if resp.peers.is_empty() {
        println!("  peers:   none observed yet");
        return;
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    println!("  peers:   {} observed", resp.peers.len());
    for peer in &resp.peers {
        println!("    @{}…", &peer.pubkey_hex[..peer.pubkey_hex.len().min(8)]);
        for alpn in &peer.alpns {
            let last_seen = format_age(now.saturating_sub(alpn.last_seen_unix));
            let direction = if alpn.last_was_incoming {
                "incoming"
            } else {
                "outgoing"
            };
            println!(
                "      {:<20}  hs={:<5}  last={} ago  {}",
                alpn.alpn, alpn.handshakes, last_seen, direction,
            );
        }
    }
}

pub(crate) fn format_age(secs: u64) -> String {
    let s = Duration::from_secs(secs).as_secs();
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m{}s", s / 60, s % 60)
    } else if s < 86400 {
        format!("{}h{}m", s / 3600, (s % 3600) / 60)
    } else {
        format!("{}d{}h", s / 86400, (s % 86400) / 3600)
    }
}
