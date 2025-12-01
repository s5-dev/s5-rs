use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use s5_core::{RegistryApi, Store, StreamKey, StreamMessage};

/// A registry implementation backed by a generic `Store`.
///
/// Registry entries are stored as individual files in the store,
/// keyed by the hex representation of the stream key. Respects
/// `should_store` semantics to avoid regressing to older revisions.
#[derive(Debug, Clone)]
pub struct StoreRegistry {
    store: Arc<dyn Store>,
    prefix: String,
}

impl StoreRegistry {
    pub fn new(store: Arc<dyn Store>, prefix: Option<String>) -> Self {
        Self {
            store,
            prefix: prefix.unwrap_or_else(|| "registry".to_string()),
        }
    }

    fn key_path(&self, key: &StreamKey) -> String {
        let (key_type, key_bytes) = key.to_bytes();

        let mut full_key = Vec::with_capacity(33);
        full_key.push(key_type);
        full_key.extend_from_slice(key_bytes);

        let hex_key = hex::encode(full_key);
        format!("{}/{}", self.prefix, hex_key)
    }
}

#[async_trait]
impl RegistryApi for StoreRegistry {
    async fn get(&self, key: &StreamKey) -> Result<Option<StreamMessage>> {
        let path = self.key_path(key);

        match self.store.open_read_bytes(&path, 0, None).await {
            Ok(bytes) => {
                let message = StreamMessage::deserialize(bytes)?;
                Ok(Some(message))
            }
            Err(_) => {
                // Assume error means not found for now.
                // Ideally we'd check for NotFound specifically, but StoreResult is generic.
                Ok(None)
            }
        }
    }

    async fn set(&self, message: StreamMessage) -> Result<()> {
        let path = self.key_path(&message.key);

        // Check if we should store this message (respects revision ordering)
        let existing = self.get(&message.key).await?;
        if !message.should_store(existing.as_ref()) {
            return Ok(());
        }

        let bytes = message.serialize();
        self.store.put_bytes(&path, bytes).await?;
        Ok(())
    }

    async fn delete(&self, key: &StreamKey) -> Result<()> {
        let path = self.key_path(key);
        self.store.delete(&path).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use s5_core::{Hash, MessageType, StreamKey, StreamMessage};
    use s5_store_memory::MemoryStore;

    #[tokio::test]
    async fn test_store_registry() -> Result<()> {
        let store = Arc::new(MemoryStore::new());
        let registry = StoreRegistry::new(store, None);

        let key = StreamKey::Local([1u8; 32]);
        let msg = StreamMessage::new(
            MessageType::Registry,
            key,
            1,
            Hash::from([2u8; 32]),
            Box::new([]),
            Some(Bytes::from("hello")),
        )?;

        registry.set(msg.clone()).await?;

        let fetched = registry.get(&key).await?;
        assert_eq!(fetched, Some(msg));

        registry.delete(&key).await?;
        let fetched = registry.get(&key).await?;
        assert_eq!(fetched, None);

        Ok(())
    }
}
