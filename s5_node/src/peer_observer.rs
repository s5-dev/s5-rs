//! Aggregated per-peer connection observer.
//!
//! Implements `iroh::endpoint::EndpointHooks` purely as a side-channel
//! sink — `after_handshake` always returns Accept, the membership ACL
//! hook (chained alongside via `Endpoint::builder().hooks(..).hooks(..)`)
//! does the actual policy. We just record what we saw: which peer
//! pubkey, which ALPN, when, which side.
//!
//! Powers `vup doctor`'s peer table (the old `debug peers`). Modelled after iroh's `remote-info.rs`
//! example (the `RemoteMap` pattern). Today only counts +
//! per-(peer, alpn) timestamps; future passes can add live
//! `ConnectionInfo`/`paths()` watchers for direct-vs-relay reporting,
//! `ConnectionStats` for RTT, etc. — see TODOs at the bottom.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use dashmap::DashMap;
use iroh::endpoint::{AfterHandshakeOutcome, Connection, EndpointHooks};
use serde::{Deserialize, Serialize};

/// Per-(peer, alpn) summary kept in the `RemoteMap`. Cheap to clone
/// for the RPC return path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlpnStats {
    /// Total handshake-completes seen on this ALPN since daemon start.
    pub handshakes: u64,
    /// First time we saw this peer on this ALPN (epoch seconds).
    pub first_seen_unix: u64,
    /// Most-recent time we saw this peer on this ALPN (epoch seconds).
    pub last_seen_unix: u64,
    /// Whether the latest connection was incoming (`true`) or
    /// outgoing (`false`).
    pub last_was_incoming: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerStats {
    /// Map keyed by ALPN bytes (CBOR-friendly when serialised — bytes
    /// are sometimes not UTF-8). Each entry is a stats snapshot.
    pub by_alpn: HashMap<Vec<u8>, AlpnStats>,
}

/// Daemon-wide observer. Implements `EndpointHooks` so it slots into
/// `Endpoint::builder().hooks(..)` chain. Lookup-by-pubkey is
/// lock-free thanks to `DashMap`.
#[derive(Debug, Clone, Default)]
pub struct PeerObserver {
    inner: Arc<DashMap<[u8; 32], PeerStats>>,
}

impl PeerObserver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of every peer this observer has seen. Allocates; for
    /// the `vup doctor` RPC path, fine — the map is tiny.
    pub fn snapshot(&self) -> Vec<(/* pubkey */ [u8; 32], PeerStats)> {
        self.inner
            .iter()
            .map(|e| (*e.key(), e.value().clone()))
            .collect()
    }
}

impl EndpointHooks for PeerObserver {
    async fn after_handshake(&self, conn: &Connection) -> AfterHandshakeOutcome {
        let pubkey = *conn.remote_id().as_bytes();
        let alpn = conn.alpn().to_vec();
        let incoming = matches!(conn.side(), iroh::endpoint::Side::Server);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        self.inner
            .entry(pubkey)
            .or_default()
            .by_alpn
            .entry(alpn)
            .and_modify(|s| {
                s.handshakes = s.handshakes.saturating_add(1);
                s.last_seen_unix = now;
                s.last_was_incoming = incoming;
            })
            .or_insert_with(|| AlpnStats {
                handshakes: 1,
                first_seen_unix: now,
                last_seen_unix: now,
                last_was_incoming: incoming,
            });

        // Observation only — the membership hook installed alongside
        // is what actually decides accept/reject.
        AfterHandshakeOutcome::Accept
    }
}

// TODO(audit:iroh-metrics): wire iroh's `iroh_metrics::MetricsGroup`
// derive into a daemon-wide metrics struct (peer connections
// up/down, blobs fetched, registry events received, pair attempts,
// reject-by-ACL count, etc.) and expose via Prometheus exporter.
// Surface via `vup status --metrics` or a `/metrics` endpoint
// alongside the iroh router. Reference:
// https://docs.rs/iroh-metrics

// TODO(audit:0.5-rtt-reads): server-side 0.5-RTT for idempotent RPCs
// — Get / Subscribe / Query. iroh's `Connecting::into_0rtt`
// (client-side opt-in) lets the responder start writing before the
// client handshake completes; saves one round-trip on every fresh-
// peer dial. Strictly read-only operations only — never Set, never
// publish. Reference: protocols/using-quic.md § "Server-side 0.5-RTT".

// TODO(audit:dht-discovery): enable BitTorrent-mainline DHT
// discovery as a parallel backend to n0's DNS, so pkarr resolution
// survives `dns.iroh.link` going away. iroh ships the resolver but
// it's off by default. Hooks into the existing `presets::N0`
// builder — adds a second discovery service rather than replacing
// the DNS one. Sovereignty hedge for the broader iroh-services
// independence TODO.

// TODO(audit:stream-stop): the membership subscriber's reconnect
// loop today drops the entire QUIC connection on any registry recv
// error and re-handshakes both ALPNs. `RecvStream::stop(code)`
// would let us kill the stale subscribe stream and reopen a fresh
// one on the same connection — cheaper, no handshake. Most useful
// when the peer's RegistryServer briefly stalls but the underlying
// QUIC connection is still healthy.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_is_empty_initially() {
        let obs = PeerObserver::new();
        assert!(obs.snapshot().is_empty());
    }
}
