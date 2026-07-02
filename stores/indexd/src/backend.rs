//! Sia operations behind a single concrete backend.
//!
//! [`SiaBackend`] wraps a live `sia_storage::Sdk` and is the one place that
//! touches Sia's `Object` / `SealedObject` wire types and the AppKey. The rest
//! of the crate works in `SealedObject`s and store paths; this module turns
//! those into uploads, downloads, shares, deletes, and indexer enumeration.
//!
//! ## Why upload seals the store path into the object metadata
//!
//! Each uploaded object's sealed `metadata` field carries a tiny recovery
//! record — the ASCII magic `b"S5"`, a `u8` path length, then the object's
//! **store path** (and, for a registry HEAD, a trailing value). The indexer
//! stores that field end-to-end-encrypted, which makes it the source of truth:
//! a cold client rebuilds its entire local index by enumerating
//! ([`SiaBackend::object_events`]) and reading the path back out —
//! **self-describing**, for *any* key, including non-content-addressed registry
//! entries. The blob *size* is the slab-length sum, recovered on enumeration —
//! never stored here. The path is known before upload, so nothing is teed off
//! the stream and `put_stream` stays truly streaming.

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use sia_storage::{
    AppKey, AppMetadata, Builder, DownloadOptions, Hash256, Object, ObjectsCursor, Sdk,
    SealedObject, UploadOptions,
};
use tokio::io::AsyncRead;

use crate::UploadOptionsBuilder;

/// Boxed async reader used for both upload input and download output.
pub type ByteReader = Box<dyn AsyncRead + Send + Unpin + 'static>;

/// ASCII magic prefixing every S5 recovery record in an object's sealed
/// metadata: `b"S5"` (`0x53 0x35`). Self-identifies S5 objects on the shared,
/// multi-tenant indexer — a hex dump reads `53 35 …` = "S5…", and the recovery
/// scan can robustly skip foreign objects (a 1-byte tag risks a 1/256
/// misclassification; a 2-byte magic makes that negligible). Consistent with the
/// pack body's `S5.pro`-prefixed magic (`s5_store_packing::manifest::MAGIC`),
/// which self-identifies pack objects at rest for the same reason.
pub const RECORD_MAGIC: [u8; 2] = *b"S5";

/// The metadata limit the indexer enforces. A pointer record (path ~107 B +
/// value ~200 B + 3 B framing) sits well under it; `put_pointer` rejects values
/// that would overflow rather than silently truncating.
pub const METADATA_LIMIT: usize = 1024;

/// A decoded recovery record from an object's sealed metadata: either a blob
/// (data-bearing object, no trailing value) or a pointer (metadata-bearing — a
/// registry HEAD whose value rides here, updated in place via
/// `update_object_metadata` so a high-churn HEAD costs ~ms per update, not a
/// fresh erasure-coded slab). Both carry the **store path**, which is what makes
/// reconstruction self-describing for *any* key. The two are distinguished by
/// the presence of trailing value bytes — there is no type tag; the path prefix
/// (`blob3/` vs `registry/`) is the human-readable discriminant.
pub enum RecoveryRecord {
    Blob { path: String },
    Pointer { path: String, value: Vec<u8> },
}

impl RecoveryRecord {
    /// The store path the object was written under — present for both kinds, so
    /// reconstruction caches a pointer object exactly like a blob object.
    pub fn into_path(self) -> String {
        match self {
            RecoveryRecord::Blob { path } | RecoveryRecord::Pointer { path, .. } => path,
        }
    }
}

/// Encode the common record frame `b"S5" | path_len: u8 | path` and append
/// `extra` (empty for a blob, the value for a pointer). The length must be
/// `< 255`: `path_len == 255` is reserved for a future `S5`-prefixed format, so
/// a length `>= 255` is a **hard error in every build** (not a `debug_assert`),
/// guaranteeing the reserved escape can never be emitted — in release too. Real
/// paths are short (blob ~51 B, registry ~107 B), so this never trips in practice.
fn encode_record(path: &str, extra: &[u8]) -> Result<Vec<u8>> {
    let path_bytes = path.as_bytes();
    if path_bytes.len() >= u8::MAX as usize {
        return Err(anyhow!(
            "S5 record path is {} B; must be < 255 (255 is the reserved escape): {path}",
            path_bytes.len()
        ));
    }
    let mut v = Vec::with_capacity(RECORD_MAGIC.len() + 1 + path_bytes.len() + extra.len());
    v.extend_from_slice(&RECORD_MAGIC);
    v.push(path_bytes.len() as u8);
    v.extend_from_slice(path_bytes);
    v.extend_from_slice(extra);
    Ok(v)
}

/// Encode a **blob** recovery record: `b"S5" | path_len:u8 | path` (no value).
fn encode_recovery_metadata(path: &str) -> Result<Vec<u8>> {
    encode_record(path, &[])
}

/// Encode a **pointer** recovery record: `b"S5" | path_len:u8 | path | value`.
/// The trailing value (a serialized registry entry) is what marks this a pointer.
fn encode_pointer_metadata(path: &str, value: &[u8]) -> Result<Vec<u8>> {
    encode_record(path, value)
}

/// Decode a recovery record from an object's sealed metadata, or `None` for
/// foreign / malformed metadata (no `S5` magic, truncated, or non-UTF-8 path).
/// A trailing value after the path ⇒ pointer; none ⇒ blob.
pub fn decode_recovery_record(metadata: &[u8]) -> Option<RecoveryRecord> {
    let rest = metadata.strip_prefix(&RECORD_MAGIC[..])?;
    let (&path_len, rest) = rest.split_first()?;
    let (path, value) = rest.split_at_checked(path_len as usize)?;
    let path = String::from_utf8(path.to_vec()).ok()?;
    Some(if value.is_empty() {
        RecoveryRecord::Blob { path }
    } else {
        RecoveryRecord::Pointer {
            path,
            value: value.to_vec(),
        }
    })
}

/// Plaintext size of a sealed object: the sum of its slab lengths. Cheap — no
/// open, no download.
pub fn size_of(sealed: &SealedObject) -> u64 {
    sealed.slabs.iter().map(|s| s.length as u64).sum()
}

/// One object seen while enumerating the indexer (the rebuild / sync path,
/// [`SiaBackend::object_events`]).
pub struct EnumeratedObject {
    /// The object's **store path**, recovered from its sealed metadata, if it
    /// carried one in our format. `None` for a delete tombstone or an object
    /// pinned without S5 recovery metadata (e.g. by another app).
    pub recovered_path: Option<String>,
    /// The sealed object to cache. `None` for a delete tombstone.
    pub sealed: Option<SealedObject>,
    pub deleted: bool,
    /// Resume token: pass as `after` to fetch events strictly past this one.
    pub cursor: EnumCursor,
}

/// Opaque, persistable resume token for [`SiaBackend::object_events`] — the
/// `(updated_at, object_id)` pair the indexer paginates on, in sia-free form.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnumCursor {
    pub after_unix_nanos: i64,
    pub object_id: [u8; 32],
}

/// A live `sia_storage::Sdk` connected to one indexer under one AppKey.
///
/// `Clone` shares the underlying `Sdk` (connection + warmed host pool — `Sdk` is
/// itself `Clone`), so a cloned handle drives the same connection.
#[derive(Clone)]
pub struct SiaBackend {
    sdk: Sdk,
    upload_options: Option<UploadOptionsBuilder>,
}

impl std::fmt::Debug for SiaBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `Sdk` isn't `Debug`; only surface the upload options.
        f.debug_struct("SiaBackend")
            .field("upload_options", &self.upload_options)
            .finish_non_exhaustive()
    }
}

impl SiaBackend {
    /// Build a backend by reconnecting a previously-registered AppKey.
    ///
    /// Errors if the indexer doesn't recognise `app_key` — register first via
    /// [`crate::auth::register`].
    pub async fn open(
        indexer_url: &str,
        app_key: [u8; 32],
        upload_options: Option<UploadOptionsBuilder>,
        app_metadata: Option<AppMetadata>,
    ) -> Result<Self> {
        let metadata = app_metadata.unwrap_or_else(crate::auth::app_metadata);
        let builder = Builder::new(indexer_url, metadata)
            .map_err(|e| anyhow!("Builder::new({indexer_url}): {e:?}"))?;
        let key = AppKey::import(app_key);
        let sdk = builder
            .connected(&key)
            .await
            .map_err(|e| anyhow!("Builder::connected: {e:?}"))?
            .ok_or_else(|| {
                anyhow!(
                    "indexer at {indexer_url} does not recognise this AppKey — \
                     register first via auth::register"
                )
            })?;
        Ok(Self {
            sdk,
            upload_options,
        })
    }

    /// A sibling backend sharing this one's live `Sdk` (connection + warmed host
    /// pool — `Sdk` is `Clone`, sharing the underlying state) but uploading with
    /// different erasure-coding options. Lets one connection back multiple EC
    /// profiles (e.g. pack bodies vs manifests).
    pub(crate) fn with_upload_options(&self, upload_options: Option<UploadOptionsBuilder>) -> Self {
        Self {
            sdk: self.sdk.clone(),
            upload_options,
        }
    }

    fn upload_options(&self) -> UploadOptions {
        match &self.upload_options {
            Some(b) => b.build(),
            None => UploadOptions::default(),
        }
    }

    /// Upload `reader` as a new object, stamp `metadata`, pin it, and seal. The
    /// data-bearing primitive shared by [`upload`](Self::upload) (a blob) and
    /// [`mint_pointer`](Self::mint_pointer) (a pointer's placeholder body). Stays
    /// streaming — metadata is set *after* the upload, so nothing is teed off.
    async fn pin_with_metadata(
        &self,
        reader: ByteReader,
        metadata: Vec<u8>,
    ) -> Result<SealedObject> {
        let mut object = self
            .sdk
            .upload(Object::default(), reader, self.upload_options())
            .await
            .map_err(|e| anyhow!("Sdk::upload: {e:?}"))?;
        object.metadata = metadata;
        self.sdk
            .pin_object(&object)
            .await
            .map_err(|e| anyhow!("Sdk::pin_object: {e:?}"))?;
        Ok(object.seal(self.sdk.app_key()))
    }

    /// Re-stamp an existing object's `metadata` **in place** — open, set, re-pin
    /// via `update_object_metadata`: no data re-upload, `object_id` unchanged.
    /// The cheap-mutate primitive shared by [`rename`](Self::rename) and
    /// [`update_pointer`](Self::update_pointer).
    async fn repin_with_metadata(
        &self,
        sealed: &SealedObject,
        metadata: Vec<u8>,
    ) -> Result<SealedObject> {
        let mut object = sealed
            .clone()
            .open(self.sdk.app_key())
            .map_err(|e| anyhow!("SealedObject::open: {e:?}"))?;
        object.metadata = metadata;
        self.sdk
            .update_object_metadata(&object)
            .await
            .map_err(|e| anyhow!("Sdk::update_object_metadata: {e:?}"))?;
        Ok(object.seal(self.sdk.app_key()))
    }

    /// Upload from `reader`, seal `path` into the object's recovery metadata, pin
    /// it, and return the sealed object. `put_stream` stays streaming: the path
    /// is known up front, so nothing is teed off the stream.
    pub async fn upload(&self, reader: ByteReader, path: &str) -> Result<SealedObject> {
        self.pin_with_metadata(reader, encode_recovery_metadata(path)?)
            .await
    }

    /// Open `sealed` under our AppKey into an in-process [`Object`] — the
    /// **export** side of migration. Recovers the object's data key + slabs so
    /// another backend can re-pin it; the [`Object`] holds the cleartext data
    /// key, so keep it in-process and never serialize it to the wire.
    pub fn export_object(&self, sealed: &SealedObject) -> Result<Object> {
        sealed
            .clone()
            .open(self.sdk.app_key())
            .map_err(|e| anyhow!("SealedObject::open during migration export: {e:?}"))
    }

    /// Re-pin an [`Object`] exported from another backend onto *this* account,
    /// re-sealing it under our AppKey — the **import** side of migration. No blob
    /// bytes move: `pin_object` references the existing sectors on their hosts.
    /// The source opened it under a *different* AppKey; re-sealing under ours is
    /// what bridges the two indexers. `object_id = blake2b(slabs)` is preserved.
    pub async fn import_object(&self, object: Object) -> Result<SealedObject> {
        self.sdk
            .pin_object(&object)
            .await
            .map_err(|e| anyhow!("Sdk::pin_object during migration import: {e:?}"))?;
        Ok(object.seal(self.sdk.app_key()))
    }

    /// Durably rename an object: re-seal its recovery metadata with `new_path`
    /// in place — no data re-upload, `object_id` unchanged. Also the "name after
    /// upload" primitive: stream a blob in under a temporary path, then rename to
    /// its final content-addressed path once the hash is known.
    pub async fn rename(&self, sealed: &SealedObject, new_path: &str) -> Result<SealedObject> {
        self.repin_with_metadata(sealed, encode_recovery_metadata(new_path)?)
            .await
    }

    /// Mint a new **pointer** object: a tiny placeholder whose *data is `path`*
    /// (so `object_id = blake2b(slabs)` is unique per key and stable across
    /// updates) and whose *metadata carries `value`*. One slab, paid once; every
    /// later update is metadata-only ([`SiaBackend::update_pointer`]). The
    /// placeholder body uses this backend's `upload_options` — keep them small
    /// (e.g. 3-of-12) since the data is throwaway and the durable part is the
    /// metadata pin record.
    pub async fn mint_pointer(&self, path: &str, value: &[u8]) -> Result<SealedObject> {
        let reader: ByteReader = Box::new(std::io::Cursor::new(path.as_bytes().to_vec()));
        self.pin_with_metadata(reader, encode_pointer_metadata(path, value)?)
            .await
    }

    /// Update an existing pointer's value **in place** — re-pin metadata only,
    /// no data re-upload, object id unchanged. The cheap path that makes a
    /// per-snap durable HEAD viable.
    pub async fn update_pointer(
        &self,
        sealed: &SealedObject,
        path: &str,
        value: &[u8],
    ) -> Result<SealedObject> {
        self.repin_with_metadata(sealed, encode_pointer_metadata(path, value)?)
            .await
    }

    /// Read a pointer's value out of a (cached) sealed object — a local
    /// open/decrypt, no host round-trip. `None` if the object isn't a pointer
    /// record (e.g. a blob, or foreign metadata).
    pub fn read_pointer(&self, sealed: &SealedObject) -> Result<Option<Vec<u8>>> {
        let object = sealed
            .clone()
            .open(self.sdk.app_key())
            .map_err(|e| anyhow!("SealedObject::open (pointer read): {e:?}"))?;
        Ok(match decode_recovery_record(&object.metadata) {
            Some(RecoveryRecord::Pointer { value, .. }) => Some(value),
            _ => None,
        })
    }

    /// Open a reader over the plaintext identified by `sealed`, from `offset`
    /// for at most `len` bytes.
    pub async fn download(
        &self,
        sealed: &SealedObject,
        offset: u64,
        len: Option<u64>,
    ) -> Result<ByteReader> {
        let object = sealed
            .clone()
            .open(self.sdk.app_key())
            .map_err(|e| anyhow!("SealedObject::open: {e:?}"))?;
        let opts = DownloadOptions {
            offset,
            length: len,
            ..DownloadOptions::default()
        };
        let reader = self
            .sdk
            .download(&object, opts)
            .map_err(|e| anyhow!("Sdk::download: {e:?}"))?;
        Ok(Box::new(reader))
    }

    /// Delete the object from the indexer (unpins it). The slabs it referenced
    /// are reclaimed separately by [`SiaBackend::prune`].
    pub async fn delete(&self, object_id: [u8; 32]) -> Result<()> {
        self.sdk
            .delete_object(&Hash256::from(object_id))
            .await
            .map_err(|e| anyhow!("Sdk::delete_object: {e:?}"))
    }

    /// Reclaim storage by pruning slabs no longer referenced by any pinned
    /// object — the deferred other half of `delete`/overwrite, which only
    /// unpin. Account-wide, so run it periodically or after bulk deletes, not
    /// once per delete.
    pub async fn prune(&self) -> Result<()> {
        self.sdk
            .prune_slabs()
            .await
            .map_err(|e| anyhow!("Sdk::prune_slabs: {e:?}"))
    }

    /// Mint a time-limited share URL for `sealed`.
    pub async fn share(&self, sealed: &SealedObject, valid_until: DateTime<Utc>) -> Result<String> {
        let object = sealed
            .clone()
            .open(self.sdk.app_key())
            .map_err(|e| anyhow!("SealedObject::open: {e:?}"))?;
        Ok(self
            .sdk
            .share_object(&object, valid_until)
            .map_err(|e| anyhow!("Sdk::share_object: {e:?}"))?
            .to_string())
    }

    /// Enumerate up to `limit` object events strictly after `after` (ascending
    /// by `(updated_at, id)`), each carrying the recovered store path and the
    /// sealed object. Drives [`crate::IndexdStore::reconstruct_from_indexer`].
    pub async fn object_events(
        &self,
        after: Option<EnumCursor>,
        limit: usize,
    ) -> Result<Vec<EnumeratedObject>> {
        let cursor = after.map(|c| ObjectsCursor {
            after: DateTime::<Utc>::from_timestamp_nanos(c.after_unix_nanos),
            id: Hash256::from(c.object_id),
        });
        let events = self
            .sdk
            .object_events(cursor, Some(limit))
            .await
            .map_err(|e| anyhow!("Sdk::object_events: {e:?}"))?;
        let app_key = self.sdk.app_key();

        let mut out = Vec::with_capacity(events.len());
        for ev in events {
            let cursor = EnumCursor {
                after_unix_nanos: ev.updated_at.timestamp_nanos_opt().unwrap_or(0),
                object_id: ev.id.into(),
            };
            let (recovered_path, sealed) = match ev.object {
                Some(obj) => (
                    // Both blob and pointer records carry the path, so a pointer
                    // object is cached under its path just like a blob — its
                    // value rides in the sealed metadata.
                    decode_recovery_record(&obj.metadata).map(RecoveryRecord::into_path),
                    Some(obj.seal(app_key)),
                ),
                None => (None, None),
            };
            out.push(EnumeratedObject {
                recovered_path,
                sealed,
                deleted: ev.deleted,
                cursor,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RecoveryRecord, decode_recovery_record, encode_pointer_metadata, encode_recovery_metadata,
    };

    #[test]
    fn blob_record_round_trips_and_rejects_foreign() {
        // A blob record carries the path and NO trailing value.
        for path in ["blob3/aa/bb/ccdd", "obao6/ee/ff", ""] {
            let encoded = encode_recovery_metadata(path).unwrap();
            assert_eq!(&encoded[..2], b"S5", "starts with the S5 magic");
            match decode_recovery_record(&encoded) {
                Some(RecoveryRecord::Blob { path: p }) => assert_eq!(p, path),
                _ => panic!("expected blob record for {path:?}"),
            }
        }
        // No magic, wrong magic, magic-without-length, and a path_len that
        // overruns the body all reject (a foreign object is left alone).
        assert!(decode_recovery_record(&[]).is_none());
        assert!(decode_recovery_record(b"XX\x00").is_none());
        assert!(decode_recovery_record(b"S5").is_none());
        assert!(decode_recovery_record(b"S5\xff").is_none());
    }

    #[test]
    fn pointer_record_round_trips_path_and_value() {
        // A pointer record = the same frame + a non-empty trailing value.
        for (path, value) in [
            ("registry/abcd", b"head-pointer-bytes".as_slice()),
            ("registry/0011ff", &[0xde, 0xad, 0xbe, 0xef]),
        ] {
            let encoded = encode_pointer_metadata(path, value).unwrap();
            match decode_recovery_record(&encoded) {
                Some(RecoveryRecord::Pointer { path: p, value: v }) => {
                    assert_eq!(p, path);
                    assert_eq!(v, value);
                }
                _ => panic!("expected pointer record for {path:?}"),
            }
        }
        // A path_len that overruns the body rejects (declares 4, only 2 follow).
        assert!(decode_recovery_record(b"S5\x04ab").is_none());
    }

    #[test]
    fn rejects_reserved_path_length() {
        use super::encode_record;
        // 254 B fits; 255 (the reserved escape) and longer are a hard error in
        // every build, so the `path_len == 255` escape can never be emitted.
        assert!(encode_record(&"a".repeat(254), &[]).is_ok());
        assert!(encode_record(&"a".repeat(255), &[]).is_err());
        assert!(encode_record(&"a".repeat(300), b"value").is_err());
    }
}
