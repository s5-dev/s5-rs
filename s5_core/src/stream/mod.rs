//! Defines the public API traits for interacting with S5 Streams and the Registry.
//!
//! These traits provide a high-level, asynchronous interface for core S5
//! functionalities, abstracting away the underlying network and storage details.
//! They are intended to be used by higher-level components like the S5 File System (`FS5`)
//! or other applications built on the S5 network.
//!
//! The APIs are designed around the unified `StreamMessage` data structure.

// Protocol types (always available)
pub mod types;

pub use types::{StreamKey, StreamMessage};

use anyhow::Result;
use async_trait::async_trait;

/// Interface for the S5 Registry, a mutable key-value store.
///
/// # Semantics
///
/// Implementations should only store an entry if it is "better" than the current one,
/// as determined by [`StreamMessage::should_store`]. This means:
/// - Higher revision number wins
/// - On tie, larger payload hash (lexicographic) wins
///
/// **Important:** Not all implementations enforce this. `RedbRegistry` (in `s5_registry_redb`)
/// correctly uses `should_store`, but simpler implementations like `MemoryRegistry` may
/// overwrite blindly. In multi-writer scenarios, this can cause regressions to older revisions.
///
/// # Example
///
/// ```ignore
/// use s5_registry_redb::RedbRegistry;
/// use s5_core::{StreamKey, RegistryApi, StreamMessage, MessageType, Hash};
///
/// async fn demo() -> anyhow::Result<()> {
///     let registry = RedbRegistry::open("/tmp/s5-registry")?;
///     let key = StreamKey::Local([0u8; 32]);
///
///     // Read current value
///     let _current = registry.get(&key).await?;
///
///     let msg = StreamMessage::new(
///         MessageType::Registry,
///         key,
///         1,
///         Hash::EMPTY,
///         Box::new([]),
///         None,
///     )?;
///     registry.set(msg).await?;
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait RegistryApi: std::fmt::Debug + Send + Sync {
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
    /// * `Err(anyhow::Error)` if an error occurs during retrieval.
    async fn get(&self, key: &StreamKey) -> Result<Option<StreamMessage>>;

    /// Publishes a new version of a registry entry.
    ///
    /// The caller is responsible for constructing a valid `StreamMessage`,
    /// including signing it if required by the `StreamKey`.
    ///
    /// Implementations are free to decide how the message is propagated:
    /// some may only persist locally, while others may additionally broadcast
    /// to the network. Implementations that respect `should_store` semantics
    /// (like `RedbRegistry`) will only keep the entry that is "better" than
    /// their currently stored version.
    ///
    /// # Arguments
    ///
    /// * `message` - The `StreamMessage` to publish. Must have `type_id` = `MessageType::Registry`.
    ///
    /// # Returns
    ///
    /// * `Ok(())` on successful handling by the implementation.
    /// * `Err(anyhow::Error)` if the message is invalid or persistence/propagation fails.
    async fn set(&self, message: StreamMessage) -> Result<()>;

    /// Deletes the entry for the given key, if it exists.
    ///
    /// Implementations SHOULD treat this as a local operation; it is primarily
    /// intended for housekeeping of local-only metadata such as pin sets.
    async fn delete(&self, key: &StreamKey) -> Result<()>;
}

#[async_trait]
impl<T: RegistryApi + ?Sized + Send + Sync> RegistryApi for std::sync::Arc<T> {
    async fn get(&self, key: &StreamKey) -> Result<Option<StreamMessage>> {
        (**self).get(key).await
    }

    async fn set(&self, message: StreamMessage) -> Result<()> {
        (**self).set(message).await
    }

    async fn delete(&self, key: &StreamKey) -> Result<()> {
        (**self).delete(key).await
    }
}

#[async_trait]
impl<T: RegistryApi + ?Sized + Send + Sync> RegistryApi for Box<T> {
    async fn get(&self, key: &StreamKey) -> Result<Option<StreamMessage>> {
        (**self).get(key).await
    }

    async fn set(&self, message: StreamMessage) -> Result<()> {
        (**self).set(message).await
    }

    async fn delete(&self, key: &StreamKey) -> Result<()> {
        (**self).delete(key).await
    }
}
