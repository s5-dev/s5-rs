//! S5 Node API — RPC protocol, config types, and client for communicating
//! with an S5 node.
//!
//! This crate defines the wire protocol and shared configuration types.
//! It is intentionally lightweight so that any frontend (CLI, desktop,
//! mobile, web) can depend on it without pulling in node internals.

/// ALPN identifier for the S5 node RPC protocol.
///
/// Served ONLY on the daemon's dedicated loopback-bound control endpoint
/// (never on the public endpoint), behind the lock-file cookie preamble —
/// see [`CONTROL_AUTH_MAGIC`] and the `connect` module.
pub const ALPN: &[u8] = b"s5/node/0";

/// Magic prefix of the control-plane auth preamble. Before any RPC, the
/// client opens one bi-stream and sends `CONTROL_AUTH_MAGIC ‖ token`
/// (raw bytes); the daemon answers a single `0x01` on success and closes
/// the connection on anything else. The token is the per-run random
/// secret from the 0600 service lock file, so possession proves the
/// caller can read the daemon owner's files — that file is the access
/// control boundary.
pub const CONTROL_AUTH_MAGIC: &[u8; 8] = b"s5ctrl/1";

/// Length in raw bytes of the control-plane auth token (hex in the lock
/// file, so twice this many characters there).
pub const CONTROL_TOKEN_LEN: usize = 32;

/// Build version string: `<cargo_version>+<git_hash>` or `<cargo_version>+<git_hash>-dirty`.
///
/// Used to detect CLI ↔ daemon version mismatches. When the CLI reads a
/// lock file whose version differs from its own, it shuts down the stale
/// daemon and spawns a fresh one.
pub const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "+", env!("S5_GIT_VERSION"));

mod client;
pub mod config;
mod rpc;

pub use client::S5NodeClient;
pub use rpc::*;

#[cfg(not(target_arch = "wasm32"))]
pub mod connect;
