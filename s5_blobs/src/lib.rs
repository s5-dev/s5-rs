//! iroh-based blob transport client and server for S5.
//!
//! This crate provides the network protocol, client, and server
//! for exchanging content-addressed blobs over iroh:
//!
//! - [`Client`]: a high-level RPC client that implements
//!   [`s5_core::BlobsRead`] and [`s5_core::BlobsWrite`] (with `server` feature).
//! - [`BlobsServer`]: a server-side handler that exposes named
//!   blob stores over an iroh [`iroh::Endpoint`]. (requires `server` feature)
//! - [`RemoteBlobStore`]: a remote implementation of
//!   [`s5_core::Store`] backed by a [`Client`], suitable for
//!   use with [`s5_core::BlobStore`] as a generic remote
//!   storage backend.
//! - [`MultiFetcher`]: fetches blobs from multiple sources with fallback.
//!
//! These building blocks can be composed to run a blob-serving
//! node and to connect remote applications or S5 nodes to it.
//!
//! ## Features
//!
//! - `server` (default): Enables server-side functionality including `BlobsServer`
//!   and the `BlobsRead`/`BlobsWrite` trait implementations on `Client`.
//!   Requires tokio. Not WASM-compatible.
//!
//! For WASM/browser usage, disable default features to get `Client`, `RemoteBlobStore`,
//! `MultiFetcher`, and RPC types.

pub mod rpc;
pub use crate::rpc::ALPN;

#[cfg(feature = "server")]
mod config;
#[cfg(feature = "server")]
pub use config::PeerConfigBlobs;

mod client;
pub use client::Client;

#[cfg(feature = "server")]
mod net_protocol;
#[cfg(feature = "server")]
pub use net_protocol::BlobsServer;

mod store_remote;
pub use store_remote::RemoteBlobStore;

mod multi_fetcher;
pub use multi_fetcher::{BlobSource, FetchError, FetchResult, MultiFetcher};
