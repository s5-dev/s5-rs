//! RedbRegistry - A local registry implementation backed by redb.

use bytes::Bytes;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use s5_core::stream::RegistryApi;
use s5_core::{StreamKey, StreamMessage};
use std::{path::Path, sync::Arc};

/// Registry table — key is `StreamKey::storage_key()` (variable length:
/// 33 bytes for `Local`/`Blake3HashPin`, 49 bytes for `Vault`).
const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("registry");

/// Simple local `RegistryApi` implementation backed by a Redb database.
#[derive(Clone)]
pub struct RedbRegistry {
    db: Arc<Database>,
}

impl RedbRegistry {
    pub fn open<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let db = Database::create(path.join("registry.redb"))?;

        // Ensure the primary `registry` table exists before returning.
        // This avoids runtime errors when the first access is a read
        // (e.g. via `RegistryPinner` during FS5 autosave) on a fresh DB.
        {
            let write_txn = db.begin_write()?;
            {
                // `open_table` on a write transaction creates the table
                // if it does not already exist.
                let _ = write_txn.open_table(TABLE)?;
            }
            write_txn.commit()?;
        }

        Ok(Self { db: Arc::new(db) })
    }
}

impl std::fmt::Debug for RedbRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbRegistry").finish()
    }
}

#[async_trait::async_trait]
impl RegistryApi for RedbRegistry {
    async fn get(&self, key: &StreamKey) -> anyhow::Result<Option<StreamMessage>> {
        let db = self.db.clone();
        let key = *key;

        let storage_key = key.storage_key();

        tokio::task::spawn_blocking(move || -> anyhow::Result<Option<StreamMessage>> {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(TABLE)?;

            let maybe_message = table
                .get(storage_key.as_slice())?
                .map(|guard| StreamMessage::deserialize(Bytes::copy_from_slice(guard.value())))
                .transpose()?;

            Ok(maybe_message)
        })
        .await
        .map_err(|e| anyhow::anyhow!("redb read task failed: {}", e))?
    }

    async fn set(&self, message: StreamMessage) -> anyhow::Result<()> {
        let db = self.db.clone();
        let storage_key = message.key.storage_key();

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(TABLE)?;

                // Get the current message from the DB to pass to `should_store`.
                let existing_message = table
                    .get(storage_key.as_slice())?
                    .map(|guard| StreamMessage::deserialize(Bytes::copy_from_slice(guard.value())))
                    .transpose()?;

                // Check if the new message should be stored.
                if message.should_store(existing_message.as_ref()) {
                    table.insert(storage_key.as_slice(), message.serialize().as_ref())?;
                }
            }
            write_txn.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("redb write task failed: {}", e))?
    }

    async fn delete(&self, key: &StreamKey) -> anyhow::Result<()> {
        let db = self.db.clone();
        let storage_key = key.storage_key();

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(TABLE)?;
                table.remove(storage_key.as_slice())?;
            }
            write_txn.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("redb delete task failed: {}", e))?
    }
}
