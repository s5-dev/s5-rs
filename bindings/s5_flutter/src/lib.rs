//! S5 Flutter bindings via flutter_rust_bridge.
//!
//! This crate provides a clean API for S5 operations that flutter_rust_bridge
//! will use to generate Dart bindings automatically.
//!
//! ## Features
//!
//! - Generate and validate BIP39 seed phrases
//! - Derive cryptographic keys for E2EE storage
//! - Connect to remote S5 nodes via iroh relay
//! - Upload/download encrypted files
//! - Manage encrypted filesystem directories
//!
//! ## Usage
//!
//! ```dart
//! // Generate seed phrase
//! final seedPhrase = generateSeedPhrase();
//!
//! // Connect to remote node
//! final client = await S5Client.connect(
//!   seedPhrase: seedPhrase,
//!   remoteNodeId: 'node-id...',
//! );
//!
//! // Create directory and upload file
//! await client.createDirectory(path: 'documents');
//! await client.uploadFile(
//!   path: 'documents',
//!   filename: 'hello.txt',
//!   content: utf8.encode('Hello, S5!'),
//!   mediaType: 'text/plain',
//! );
//! ```

mod api;
mod frb_generated; /* AUTO INJECTED BY flutter_rust_bridge. */

pub use api::*;
