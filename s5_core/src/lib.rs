//! Core S5 protocol types and traits.
//!
//! This crate defines the shared types and traits used by all S5 crates.
//!
//! ## Protocol types (wire-stable)
//!
//! The following modules define on-the-wire formats that are intended to be
//! stable for the 1.0 protocol:
//!
//! - Content hashes (`hash::Hash`)
//! - Blob identifiers (`blob::identifier::BlobId`)
//! - Blob locations and related types (`blob::location::BlobLocation`, etc.)
//! - Stream and registry message types (`stream::types::StreamKey`,
//!   `stream::types::StreamMessage`, `stream::types::MessageType`, etc.)
//!
//! These types are used directly in network protocols and persistent
//! metadata; changes to them are considered protocol changes.
//!
//! ## Convenience APIs (non-wire)
//!
//! In addition, this crate exposes higher-level helpers that build on the
//! protocol types but are not themselves part of the on-the-wire format:
//!
//! - Storage abstractions (`Store`, `StoreFeatures`) and the `BlobStore` facade
//! - Registry abstractions (`RegistryApi`); implementations in `s5_registry`
//!   (MemoryRegistry, TeeRegistry, MultiRegistry) and `s5_registry_redb` (RedbRegistry)
//! - Pinning abstractions (`Pins`, `PinContext`, `RegistryPinner`)
//! - CBOR utilities (`cbor::Value`)
//!
//! These are provided for ergonomics and may evolve more freely in future
//! major versions, or even move to separate crates, without affecting the
//! core wire protocol.
//!
//! ## Platform support
//!
//! This crate supports both native and WASM targets. Some functionality
//! (filesystem operations, blocking thread pool) is only available on
//! native targets and is automatically excluded on WASM.

pub mod bao;
pub mod blob;
pub mod cbor;
pub mod hash;
pub mod pins;
// Store trait is available on all platforms
pub mod store;
pub mod stream;

// Test utilities (behind feature flag)
#[cfg(feature = "testutil")]
pub mod testutil;

// --- Core Public Surface ---

// Blob identifiers & locations (always available - protocol types)
pub use blob::identifier::BlobId;
pub use blob::location::BlobLocation;

// Hash type (always available - protocol type)
pub use hash::Hash;

// Stream & Registry protocol types (always available - protocol types)
pub use stream::types::{MessageType, PublicKeyEd25519};
pub use stream::{RegistryApi, StreamKey, StreamMessage};

// BlobStore and traits (available on all platforms)
pub use blob::store::BlobStore;
pub use blob::{BlobsRead, BlobsWrite};

// Storage traits (available on all platforms)
pub use store::{Store, StoreFeatures, StoreResult};

// --- Native-only exports ---

// Note: RedbRegistry has been moved to s5_registry_redb

// Pinning traits and types (WASM-compatible)
pub use pins::{PinContext, Pins};
// RegistryPinner implementation (native only - uses spawn_blocking internally via registry)
#[cfg(not(target_arch = "wasm32"))]
pub use pins::registry_pinner::RegistryPinner;
