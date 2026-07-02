//! The local cache layer — the only client state an [`IndexdStore`] keeps.
//!
//! [`SealedObjectCache`] is a thin typed facade over the single backing
//! `C: Store`, owning the on-disk layout. Two kinds of entry:
//!
//! - `p/<path>` -> the object's [`SealedObject`], serialized. This is the whole
//!   index entry: the object id (`SealedObject::id()`) and size derive from it.
//! - `s/cursor` -> the persisted enumeration checkpoint (a serialized
//!   [`EnumCursor`]). Outside the `p/` prefix, so it never appears in
//!   [`SealedObjectCache::list_paths`].
//!
//! The indexer is the source of truth; this is only a cache. Drop it and
//! [`IndexdStore::reconstruct_from_indexer`](crate::IndexdStore::reconstruct_from_indexer)
//! rebuilds it by enumeration, so persistence is the caller's choice of `C`
//! (`MemoryStore` for ephemeral, a durable `Store` to survive restarts).
//!
//! [`IndexdStore`]: crate::IndexdStore

use anyhow::anyhow;
use bytes::Bytes;
use futures::stream::{Stream, StreamExt};
use s5_core::store::{Store, StoreFeatures, StoreResult};
use sia_storage::SealedObject;

use crate::backend::EnumCursor;

/// Key prefix for path entries: `p/<path>` -> sealed object. Caller paths live
/// under this prefix; the cursor (`s/cursor`) never collides with it.
const INDEX_PREFIX: &str = "p/";

/// Cache key for a caller path's sealed-object entry.
fn index_key(path: &str) -> String {
    format!("{INDEX_PREFIX}{path}")
}

/// Cache key holding the persisted enumeration checkpoint (a serialized
/// [`EnumCursor`]). Outside the `p/` prefix, so it never surfaces in
/// [`SealedObjectCache::list_paths`].
const SYNC_CURSOR_KEY: &str = "s/cursor";

/// Cache key marking that a full enumeration has completed **to the end** at
/// least once — i.e. the cache is authoritative for negative lookups. Distinct
/// from [`SYNC_CURSOR_KEY`], which advances *per page*: an interrupted first
/// reconstruct leaves a cursor but no such marker, so it is not mistaken for a
/// complete cache. Outside the `p/` prefix.
const RECONSTRUCTED_KEY: &str = "s/reconstructed";

/// Serialize an [`EnumCursor`]: 8-byte LE `after_unix_nanos` followed by the
/// 32-byte object id (40 bytes total).
fn encode_cursor(cursor: &EnumCursor) -> Bytes {
    let mut buf = Vec::with_capacity(40);
    buf.extend_from_slice(&cursor.after_unix_nanos.to_le_bytes());
    buf.extend_from_slice(&cursor.object_id);
    Bytes::from(buf)
}

/// Inverse of [`encode_cursor`]; `None` if the bytes aren't a 40-byte cursor.
fn decode_cursor(bytes: &[u8]) -> Option<EnumCursor> {
    if bytes.len() != 40 {
        return None;
    }
    Some(EnumCursor {
        after_unix_nanos: i64::from_le_bytes(bytes[0..8].try_into().ok()?),
        object_id: bytes[8..40].try_into().ok()?,
    })
}

/// Typed facade over the single backing `C: Store` — the local cache. Holds the
/// `p/<path>` -> sealed-object map and the `s/cursor` checkpoint; see the module
/// docs for the layout.
#[derive(Debug, Clone)]
pub(crate) struct SealedObjectCache<C: Store> {
    inner: C,
}

impl<C: Store> SealedObjectCache<C> {
    pub(crate) fn new(inner: C) -> Self {
        Self { inner }
    }

    /// Load the cached [`SealedObject`] for `path`. Errors if `path` isn't
    /// locally indexed: the indexer can't resolve a path (it's sealed ciphertext
    /// there), so the caller reconstructs from the indexer first.
    pub(crate) async fn load(&self, path: &str) -> StoreResult<SealedObject> {
        let bytes = self
            .inner
            .open_read_bytes(&index_key(path), 0, None)
            .await?;
        serde_json::from_slice(&bytes).map_err(|e| anyhow!("decoding SealedObject for {path}: {e}"))
    }

    /// Store `path` -> `sealed` — one atomic write, the whole index entry.
    pub(crate) async fn store(&self, path: &str, sealed: &SealedObject) -> StoreResult<()> {
        let bytes = serde_json::to_vec(sealed)
            .map_err(|e| anyhow!("serializing SealedObject for {path}: {e}"))?;
        self.inner
            .put_bytes(&index_key(path), Bytes::from(bytes))
            .await
    }

    /// Whether `path` is locally indexed.
    pub(crate) async fn exists(&self, path: &str) -> StoreResult<bool> {
        self.inner.exists(&index_key(path)).await
    }

    /// Drop `path`'s entry. Cache-only — the object is reclaimed separately.
    pub(crate) async fn remove(&self, path: &str) -> StoreResult<()> {
        self.inner.delete(&index_key(path)).await
    }

    /// Stream the caller paths currently indexed (the `p/` keys, prefix stripped
    /// — so the `s/cursor` checkpoint never surfaces).
    pub(crate) async fn list_paths(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>>
    {
        let inner = self.inner.list().await?;
        let stripped = inner.filter_map(|item| async move {
            match item {
                Ok(key) => key.strip_prefix(INDEX_PREFIX).map(|p| Ok(p.to_string())),
                Err(e) => Some(Err(e)),
            }
        });
        Ok(Box::new(Box::pin(stripped)))
    }

    /// Load the persisted enumeration checkpoint, if any.
    pub(crate) async fn load_cursor(&self) -> Option<EnumCursor> {
        let bytes = self
            .inner
            .open_read_bytes(SYNC_CURSOR_KEY, 0, None)
            .await
            .ok()?;
        decode_cursor(&bytes)
    }

    /// Persist the enumeration checkpoint so the next sync resumes past it.
    pub(crate) async fn store_cursor(&self, cursor: &EnumCursor) -> StoreResult<()> {
        self.inner
            .put_bytes(SYNC_CURSOR_KEY, encode_cursor(cursor))
            .await
    }

    /// Whether a full enumeration has completed at least once — i.e. the cache
    /// mirrors the indexer up to some cursor, so a miss is authoritative. `false`
    /// on a cold cache AND on one whose first reconstruct was interrupted
    /// mid-enumeration (a per-page [`SYNC_CURSOR_KEY`] alone does NOT imply it).
    pub(crate) async fn is_reconstructed(&self) -> bool {
        self.inner.exists(RECONSTRUCTED_KEY).await.unwrap_or(false)
    }

    /// Record that a full enumeration reached the end. The caller must set this
    /// ONLY after [`run_events`] returns (not mid-pass), so an interrupted
    /// reconstruct never looks complete.
    ///
    /// [`run_events`]: crate::IndexdStore::run_events
    pub(crate) async fn mark_reconstructed(&self) -> StoreResult<()> {
        self.inner
            .put_bytes(RECONSTRUCTED_KEY, Bytes::from_static(&[1u8]))
            .await
    }

    /// Inherit the backing store's feature set — `IndexdStore` adds no FS semantics.
    pub(crate) fn features(&self) -> StoreFeatures {
        self.inner.features()
    }

    /// Flush the backing store.
    pub(crate) async fn sync(&self) -> StoreResult<()> {
        self.inner.sync().await
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_cursor, encode_cursor};
    use crate::backend::EnumCursor;

    #[test]
    fn cursor_round_trips_and_rejects_bad_length() {
        // Negative nanos exercises the signed round-trip (pre-1970 / clock skew).
        let c = EnumCursor {
            after_unix_nanos: -123_456_789,
            object_id: [7u8; 32],
        };
        let bytes = encode_cursor(&c);
        assert_eq!(bytes.len(), 40);
        assert_eq!(decode_cursor(&bytes), Some(c));

        for bad in [0usize, 39, 41] {
            assert_eq!(decode_cursor(&vec![0u8; bad]), None);
        }
    }

    #[tokio::test]
    async fn cursor_alone_does_not_imply_reconstructed() {
        // The A2 invariant: `run_events` checkpoints the cursor PER PAGE, so an
        // interrupted first reconstruct leaves a cursor but no reconstructed
        // marker. `IndexdStore::open` gates on the marker (not the cursor), so a
        // partial cache is never mistaken for a complete one — which would answer
        // real, un-enumerated keys with a false "not found".
        use super::SealedObjectCache;
        use s5_store_memory::MemoryStore;

        let cache = SealedObjectCache::new(MemoryStore::new());
        assert!(
            !cache.is_reconstructed().await,
            "fresh cache: not reconstructed"
        );

        cache
            .store_cursor(&EnumCursor {
                after_unix_nanos: 1,
                object_id: [3u8; 32],
            })
            .await
            .unwrap();
        assert!(
            !cache.is_reconstructed().await,
            "a per-page cursor must NOT look reconstructed"
        );

        cache.mark_reconstructed().await.unwrap();
        assert!(cache.is_reconstructed().await, "marked after a full pass");
    }
}
