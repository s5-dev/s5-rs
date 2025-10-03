//! Defines the public API traits for interacting with S5 Streams and the Registry.
//!
//! These traits provide a high-level, asynchronous interface for core S5
//! functionalities, abstracting away the underlying network and storage details.
//! They are intended to be used by higher-level components like the S5 File System (`FS5`)
//! or other applications built on the S5 network.
//!
//! The APIs are designed around the unified `StreamMessage` data structure.

use crate::stream::{StreamKey, StreamMessage};

use async_trait::async_trait;
use futures::Stream;
use std::error::Error as StdError;
use std::ops::RangeBounds;

use anyhow::Result;

// pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An error originating from the underlying network transport.
    #[error("network error: {0}")]
    Network(Box<dyn StdError + Send + Sync>),

    /// The provided message was invalid or failed validation.
    #[error("invalid message: {0}")]
    InvalidMessage(String),

    /// An error occurred in the underlying storage layer.
    #[error("storage error: {0}")]
    Storage(Box<dyn StdError + Send + Sync>),

    /// A generic, unexpected error.
    #[error("an unexpected error occurred: {0}")]
    Other(String),
}

/// Interface for the S5 Registry, a mutable key-value store.
///
/// The Registry only stores the single "best" entry for a given key, determined
/// by the highest revision number, with the payload hash as a tie-breaker.
#[async_trait]
pub trait RegistryApi {
    /// Retrieves the latest entry for a given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The `StreamKey` identifying the registry entry.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(StreamMessage))` if an entry is found.
    /// * `Ok(None)` if no entry exists for the key.
    /// * `Err(self::Error)` if an error occurs during retrieval.
    async fn get(&self, key: &StreamKey) -> Result<Option<StreamMessage>>;

    /// Publishes a new version of a registry entry.
    ///
    /// The caller is responsible for constructing a valid `StreamMessage`,
    /// including signing it if required by the `StreamKey`. The underlying
    /// implementation will broadcast this message to the network. Nodes will
    /// only accept it if it is "better" than their currently stored version.
    ///
    /// # Arguments
    ///
    /// * `message` - The `StreamMessage` to publish. Must have `type_id` = `MessageType::Registry`.
    ///
    /// # Returns
    ///
    /// * `Ok(())` on successful broadcast.
    /// * `Err(self::Error)` if the message is invalid or a network error occurs.
    async fn set(&self, message: StreamMessage) -> Result<()>;
}