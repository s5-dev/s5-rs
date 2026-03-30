//! S5 Node API — RPC protocol, config types, and client for communicating
//! with an S5 node.
//!
//! This crate defines the wire protocol and shared configuration types.
//! It is intentionally lightweight so that any frontend (CLI, desktop,
//! mobile, web) can depend on it without pulling in node internals.

/// ALPN identifier for the S5 node RPC protocol.
pub const ALPN: &[u8] = b"s5/node/0";

pub mod config;
mod rpc;
mod client;

pub use rpc::*;
pub use client::S5NodeClient;

#[cfg(not(target_arch = "wasm32"))]
pub mod connect;
