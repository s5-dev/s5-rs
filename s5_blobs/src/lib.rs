//! iroh-based blob transport client and server for S5.
//!
//! This crate provides the network protocol, client, and server
//! for exchanging content-addressed blobs over iroh:
//!
//! - [`Client`]: a high-level RPC client that implements
//!   [`s5_core::BlobsRead`] and [`s5_core::BlobsWrite`].
//! - [`BlobsServer`]: a server-side handler that exposes named
//!   blob stores over an iroh [`iroh::Endpoint`].
//! - [`RemoteBlobStore`]: a remote implementation of
//!   [`s5_core::Store`] backed by a [`Client`], suitable for
//!   use with [`s5_core::BlobStore`] as a generic remote
//!   storage backend.
//!
//! These building blocks can be composed to run a blob-serving
//! node and to connect remote applications or S5 nodes to it.

pub mod rpc;
pub use crate::rpc::ALPN;

mod config;
pub use config::PeerConfigBlobs;

mod client;
pub use client::Client;

mod net_protocol;
pub use net_protocol::BlobsServer;

mod store_remote;
pub use store_remote::RemoteBlobStore;
