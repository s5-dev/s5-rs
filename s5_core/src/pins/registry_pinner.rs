use super::{PinContext, Pins};
use crate::stream::RegistryApi;
use crate::stream::types::MessageType;
use crate::{StreamKey, StreamMessage};
use anyhow::{Result, anyhow};
use bytes::Bytes;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

/// `Pins` implementation backed by an S5 registry.
///
/// `RegistryPinner` stores pin information as CBOR-encoded `PinContext`
/// sets in the registry, keyed by a `StreamKey::Blake3HashPin` derived
/// from the blob `Hash`.
#[derive(Clone, Debug)]
pub struct RegistryPinner<R> {
    registry: Arc<R>,
    /// Ensures that `get -> modify -> set` is atomic for this instance.
    /// Sharded by hash to improve concurrency.
    write_locks: [Arc<Mutex<()>>; 64],
}

#[async_trait::async_trait]
impl<R: RegistryApi + Send + Sync + std::fmt::Debug + 'static> Pins for RegistryPinner<R> {
    async fn pin_hash(&self, hash: crate::Hash, context: PinContext) -> Result<()> {
        // 1. Acquire lock to serialize metadata updates
        let lock = self.lock_for_hash(hash);
        let _guard = lock.lock().await;

        let key = self.hash_to_key(hash);

        // 2. Get existing message (or None)
        let (mut pinners, revision) = self.get_internal(&key).await?;

        // 3. Modify
        pinners.insert(context);

        // 4. Save back to Registry
        self.save_internal(key, pinners, revision + 1).await
    }

    async fn unpin_hash(&self, hash: crate::Hash, context: PinContext) -> Result<bool> {
        let lock = self.lock_for_hash(hash);
        let _guard = lock.lock().await;

        let key = self.hash_to_key(hash);
        let (mut pinners, revision) = self.get_internal(&key).await?;

        if !pinners.remove(&context) {
            return Ok(pinners.is_empty());
        }

        let is_empty = pinners.is_empty();
        self.save_internal(key, pinners, revision + 1).await?;
        Ok(is_empty)
    }

    async fn unpin_hash_all(&self, hash: crate::Hash) -> Result<()> {
        let lock = self.lock_for_hash(hash);
        let _guard = lock.lock().await;

        let key = self.hash_to_key(hash);
        let (pinners, revision) = self.get_internal(&key).await?;

        if pinners.is_empty() {
            return Ok(());
        }

        self.save_internal(key, HashSet::new(), revision + 1).await
    }

    async fn get_pinners(&self, hash: crate::Hash) -> Result<HashSet<PinContext>> {
        let key = self.hash_to_key(hash);
        let (pinners, _) = self.get_internal(&key).await?;
        Ok(pinners)
    }

    async fn is_pinned(&self, hash: crate::Hash, context: PinContext) -> Result<bool> {
        let key = self.hash_to_key(hash);
        let (pinners, _) = self.get_internal(&key).await?;
        Ok(pinners.contains(&context))
    }
}

impl<R: RegistryApi + Send + Sync + 'static> RegistryPinner<R> {
    pub fn new(registry: R) -> Self {
        let mut locks = Vec::with_capacity(64);
        for _ in 0..64 {
            locks.push(Arc::new(Mutex::new(())));
        }
        Self {
            registry: Arc::new(registry),
            write_locks: locks.try_into().unwrap(),
        }
    }

    /// Returns a clone of the underlying registry as a trait object.
    ///
    /// This is useful when a caller needs both a `RegistryApi`
    /// handle and a `Pins` implementation over the same backing
    /// database.
    pub fn registry_arc(&self) -> Arc<dyn RegistryApi + Send + Sync> {
        self.registry.clone() as Arc<dyn RegistryApi + Send + Sync>
    }

    /// Removes a user. Returns `true` if the blob is now orphaned (0 pinners).
    pub async fn unpin(&self, hash: [u8; 32], user_id: PinContext) -> Result<bool> {
        let hash_obj = crate::Hash::from(hash);
        let lock = self.lock_for_hash(hash_obj);
        let _guard = lock.lock().await;

        let key = self.hash_to_key(hash_obj);
        let (mut pinners, revision) = self.get_internal(&key).await?;

        // If user wasn't in the list, we don't change anything.
        if !pinners.remove(&user_id) {
            return Ok(pinners.is_empty());
        }

        let is_empty = pinners.is_empty();

        self.save_internal(key, pinners, revision + 1).await?;

        Ok(is_empty)
    }

    /// Read-only view of pinners (does not require write lock).
    pub async fn get_pinners(&self, hash: [u8; 32]) -> Result<HashSet<PinContext>> {
        let key = self.hash_to_key(hash.into());
        let (pinners, _) = self.get_internal(&key).await?;
        Ok(pinners)
    }

    // --- Helpers ---

    fn lock_for_hash(&self, hash: crate::Hash) -> Arc<Mutex<()>> {
        let first_byte = hash.as_bytes()[0] as usize;
        // Map 0..256 to 0..64
        let index = first_byte % 64;
        self.write_locks[index].clone()
    }

    fn hash_to_key(&self, hash: crate::Hash) -> StreamKey {
        StreamKey::Blake3HashPin(hash.into())
    }

    /// Helper to decode the HashSet from the registry
    async fn get_internal(&self, key: &StreamKey) -> Result<(HashSet<PinContext>, u64)> {
        if let Some(msg) = self.registry.get(key).await? {
            let pinners: HashSet<PinContext> = if let Some(data) = &msg.data {
                let vec: Vec<PinContext> =
                    minicbor::decode(data).map_err(|e| anyhow!("CBOR decode failed: {}", e))?;
                vec.into_iter().collect()
            } else {
                HashSet::new()
            };

            Ok((pinners, msg.revision))
        } else {
            Ok((HashSet::new(), 0))
        }
    }

    /// Helper to encode and save
    async fn save_internal(
        &self,
        key: StreamKey,
        pinners: HashSet<PinContext>,
        new_revision: u64,
    ) -> Result<()> {
        // If there are no pinners left, delete the registry entry entirely.
        // Pin metadata is intended to be local-only housekeeping, so we
        // prefer to drop the row instead of keeping an empty value.
        if pinners.is_empty() {
            return self.registry.delete(&key).await;
        }

        let mut pinners_vec: Vec<PinContext> = pinners.into_iter().collect();
        pinners_vec.sort(); // Sort for deterministic output
        let data_vec = minicbor::to_vec(&pinners_vec)?;
        let data_bytes: Bytes = data_vec.into();
        let hash = crate::Hash::new(&data_bytes);

        // Construct the StreamMessage.
        let message = StreamMessage::new(
            MessageType::Registry,
            key,
            new_revision,
            hash,
            Box::new([]), // No signature for Local key
            Some(data_bytes),
        )?;

        self.registry.set(message).await
    }
}
