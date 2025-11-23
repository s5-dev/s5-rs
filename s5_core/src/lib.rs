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
//! - Registry abstractions (`RegistryApi`) and the local `RedbRegistry`
//!   implementation
//! - Pinning abstractions (`Pins`, `PinContext`, `RegistryPinner`)
//! - CBOR utilities (`cbor::Value`)
//!
//! These are provided for ergonomics and may evolve more freely in future
//! major versions, or even move to separate crates, without affecting the
//! core wire protocol.

pub mod bao;
pub mod blob;
pub mod cbor;
pub mod hash;
pub mod pins;
pub mod store;
pub mod stream;

// --- Core Public Surface ---

// Blob identifiers & locations
pub use blob::identifier::BlobId;
pub use blob::location::BlobLocation;
pub use blob::store::BlobStore;
// Blob read/write traits
pub use blob::{BlobsRead, BlobsWrite};

// Hash type
pub use hash::Hash;

// Storage traits
pub use store::{Store, StoreFeatures, StoreResult};

// Stream & Registry protocol
pub use stream::registry::RedbRegistry;
pub use stream::types::{MessageType, PublicKeyEd25519};
pub use stream::{RegistryApi, StreamKey, StreamMessage};

// Pinning
pub use pins::registry_pinner::RegistryPinner;
pub use pins::{PinContext, Pins};
