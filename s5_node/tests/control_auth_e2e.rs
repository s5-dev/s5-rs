//! Control-plane auth gate E2E (the F03 fix).
//!
//! The daemon's control RPC is reachable only through a loopback-bound
//! endpoint behind [`ControlAuthGate`]: one preamble bi-stream carrying
//! `CONTROL_AUTH_MAGIC ‖ token` must succeed before the inner protocol
//! handler sees the connection. These tests drive the gate over real
//! iroh connections (loopback, `presets::Minimal`) with a trivial echo
//! handler inside, proving:
//!
//! 1. the right token reaches the inner handler and RPC streams work;
//! 2. a wrong token never reaches the inner handler;
//! 3. garbage instead of the preamble is rejected the same way.

use anyhow::Result;
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use s5_node::s5_server::ControlAuthGate;
use s5_node_api::{ALPN, CONTROL_AUTH_MAGIC, CONTROL_TOKEN_LEN};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Inner handler standing in for `S5NodeServer`: echoes one 4-byte
/// message per bi-stream and counts how many connections got through
/// the gate.
#[derive(Debug, Clone)]
struct Echo {
    reached: Arc<AtomicUsize>,
}

impl ProtocolHandler for Echo {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        self.reached.fetch_add(1, Ordering::SeqCst);
        let (mut send, mut recv) = conn.accept_bi().await?;
        let mut buf = [0u8; 4];
        if recv.read_exact(&mut buf).await.is_ok() {
            let _ = send.write_all(&buf).await;
            let _ = send.finish();
        }
        conn.closed().await;
        Ok(())
    }
}

struct Gateway {
    addr: iroh::EndpointAddr,
    reached: Arc<AtomicUsize>,
    _router: Router,
    _endpoint: iroh::Endpoint,
}

/// Spin up a loopback endpoint serving the control ALPN behind the gate,
/// mirroring `ControlPlane::spawn` (Minimal preset, loopback-only bind).
async fn spawn_gateway(token: [u8; CONTROL_TOKEN_LEN]) -> Result<Gateway> {
    let reached = Arc::new(AtomicUsize::new(0));
    let echo = Echo {
        reached: reached.clone(),
    };
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")?
        .bind()
        .await?;
    let router = Router::builder(endpoint.clone())
        .accept(ALPN, ControlAuthGate::new(echo, token))
        .spawn();
    Ok(Gateway {
        addr: endpoint.addr(),
        reached,
        _router: router,
        _endpoint: endpoint,
    })
}

/// Dial the gateway and run the preamble with `presented` as the token
/// bytes; on ack, exercise one echo round-trip. Returns Ok(()) only for
/// the full authenticated path.
async fn dial(gateway: &Gateway, preamble: &[u8]) -> Result<()> {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .bind()
        .await?;
    let conn = endpoint.connect(gateway.addr.clone(), ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(preamble).await?;
    send.finish()?;
    let mut ok = [0u8; 1];
    recv.read_exact(&mut ok).await?;
    anyhow::ensure!(ok[0] == 0x01, "gate did not ack");

    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(b"ping").await?;
    send.finish()?;
    let mut back = [0u8; 4];
    recv.read_exact(&mut back).await?;
    anyhow::ensure!(&back == b"ping", "echo mismatch");
    Ok(())
}

fn preamble(token: &[u8]) -> Vec<u8> {
    let mut p = CONTROL_AUTH_MAGIC.to_vec();
    p.extend_from_slice(token);
    p
}

#[tokio::test]
async fn right_token_reaches_inner_handler() -> Result<()> {
    let token = [0x42u8; CONTROL_TOKEN_LEN];
    let gateway = spawn_gateway(token).await?;
    dial(&gateway, &preamble(&token)).await?;
    assert_eq!(gateway.reached.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn wrong_token_never_reaches_inner_handler() -> Result<()> {
    let token = [0x42u8; CONTROL_TOKEN_LEN];
    let gateway = spawn_gateway(token).await?;
    let wrong = [0x43u8; CONTROL_TOKEN_LEN];
    assert!(dial(&gateway, &preamble(&wrong)).await.is_err());
    assert_eq!(gateway.reached.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn garbage_preamble_is_rejected() -> Result<()> {
    let token = [0x42u8; CONTROL_TOKEN_LEN];
    let gateway = spawn_gateway(token).await?;
    // Right length, wrong magic AND wrong token.
    let garbage = vec![0xffu8; CONTROL_AUTH_MAGIC.len() + CONTROL_TOKEN_LEN];
    assert!(dial(&gateway, &garbage).await.is_err());
    assert_eq!(gateway.reached.load(Ordering::SeqCst), 0);
    Ok(())
}
