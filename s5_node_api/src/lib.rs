//! S5 Node API — RPC protocol, config types, and client for communicating
//! with an S5 node.
//!
//! This crate defines the wire protocol and shared configuration types.
//! It is intentionally lightweight so that any frontend (CLI, desktop,
//! mobile, web) can depend on it without pulling in node internals.

/// ALPN identifier for the S5 node RPC protocol.
pub const ALPN: &[u8] = b"s5/node/0";

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
