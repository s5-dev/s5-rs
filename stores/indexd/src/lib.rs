//! `IndexdStore` — a [`Store`] backed by Sia hosts, coordinated through an
//! indexd service via `sia_storage::Sdk`.
//!
//! This module is the `Store` orchestration; the detail lives in its two
//! collaborators:
//!
//! - The `backend` module (`SiaBackend`) — the only place that touches Sia's
//!   `Object` / `SealedObject` wire types and the AppKey: uploads, downloads,
//!   shares, deletes, and indexer enumeration.
//! - The `cache` module (`SealedObjectCache`) — the only client state: a single
//!   `C: Store` mapping `path -> SealedObject`.
//!
//! Exercised end-to-end against a live indexer in `tests/real_indexd.rs`.
//!
//! # The model: the indexer is the source of truth, the cache is rebuildable
//!
//! Every upload seals a tiny recovery record (magic + store path) into the
//! object's indexer-side `metadata`, end-to-end encrypted — the `backend` module
//! has the wire format and why it rides there. That makes the full `path -> object`
//! mapping reconstructable by enumerating the indexer and reading each path back
//! out — self-describing, for *any* key, including non-content-addressed
//! registry entries (which is what lets an `IndexdStore` back a durable
//! `StoreRegistry`).
//!
//! So the local `cache` is a rebuildable index, not the system of record:
//! persistence is the caller's choice of `C` (a `MemoryStore` for an ephemeral,
//! enumeration-rebuilt cache; a durable `Store` to survive restarts), and
//! migrating between indexers is stateless — enumerate one, re-seal each object
//! under the new AppKey, re-pin, carrying nothing locally
//! ([`IndexdStore::migrate_to`]).
//!
//! # Notes
//!
//! - **Integrity.** `SealedObject::open` verifies the data and metadata
//!   signatures on every download and share, so a corrupt or hostile cache entry
//!   fails there and is rebuilt by enumeration, never trusted. (`size` and
//!   `object_id` derive from the cached sealed object without opening — fine for
//!   a local, rebuildable cache.)
//! - **Deletes are lazy.** An indexer delete event names only an object id (the
//!   path is sealed ciphertext), so [`IndexdStore::sync_from_indexer`] skips it;
//!   a from-empty [`IndexdStore::reconstruct_from_indexer`] is still clean, since
//!   a deleted object returns as a lone tombstone. The only residue is a
//!   peer-deleted path lingering in an already-warm cache until the next
//!   from-scratch rebuild.
//! - **The Sia data key stays random.** Incoming bytes are already S5-encrypted
//!   and S5 dedups blobs upstream, so a random per-object key suffices and
//!   avoids a convergent-encryption side channel at the Sia layer.

pub mod auth;
mod backend;
mod cache;

use std::any::Any;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::stream::Stream;
use s5_core::blob::location::BlobLocation;
use s5_core::store::{ReferenceMigrate, Store, StoreFeatures, StoreResult, Substrate};
use s5_core::{RegistryApi, StreamKey, StreamMessage};
use sia_storage::{Object, UploadOptions};
use tokio_util::io::{ReaderStream, StreamReader};

/// Re-exported so callers can supply a custom application identity to
/// [`auth::register`] / [`IndexdStore::open`] (e.g. their own product name +
/// logo in the OAuth dialog) instead of the S5 default ([`auth::app_metadata`]).
/// **The `id` salts the AppKey** — keep it [`app_id`] unless you intend a
/// different Sia account.
pub use sia_storage::{AppID, AppMetadata};

use crate::backend::{EnumCursor, SiaBackend};
use crate::cache::SealedObjectCache;

/// Page size for indexer enumeration ([`IndexdStore::reconstruct_from_indexer`]
/// / [`IndexdStore::sync_from_indexer`]).
const RECONSTRUCT_BATCH: usize = 256;

/// Outcome counts from enumerating the indexer
/// ([`IndexdStore::reconstruct_from_indexer`] / [`IndexdStore::sync_from_indexer`]).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SyncStats {
    /// Index entries (re)written from recovered live objects.
    pub restored: u64,
    /// Live objects skipped for lacking recoverable S5 path metadata.
    pub unrecognized_skipped: u64,
}

/// Outcome counts from [`IndexdStore::migrate_to`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MigrationStats {
    /// Entries re-pinned onto the target (no blob bytes re-uploaded).
    pub migrated: u64,
}

/// Pre-image used to derive the canonical S5-indexd AppID.
///
/// **Once any IndexdStore writes data, the byte string below must never
/// change** — it salts the AppKey HKDF, so a different pre-image yields a
/// different AppKey and existing data becomes unreachable.
pub const S5_INDEXD_APP_ID_PREIMAGE: &[u8] = b"s5.indexd.app.v1";

/// Canonical AppID for the S5 indexd integration: 32 bytes of
/// `blake3(S5_INDEXD_APP_ID_PREIMAGE)`.
pub fn app_id_bytes() -> [u8; 32] {
    *blake3::hash(S5_INDEXD_APP_ID_PREIMAGE).as_bytes()
}

/// Lowercase hex form of [`app_id_bytes`].
pub fn app_id_hex() -> String {
    hex::encode(app_id_bytes())
}

/// The S5 [`AppID`] (`app_id_bytes()` as a `Hash256`). Convenience for callers
/// building a custom [`AppMetadata`] that must keep the S5 app identity — the id
/// salts the AppKey, so changing it addresses a *different* Sia account.
pub fn app_id() -> AppID {
    AppID::from(app_id_bytes())
}

/// Default indexer URL — the public Sia Foundation indexer. Callers who
/// want a self-hosted or alternative indexer pass that URL explicitly.
pub const DEFAULT_INDEXER_URL: &str = "https://sia.storage";

/// Sia network id reported as this store's migration [`Substrate`]. Any two
/// indexd stores on the same network can re-pin each other's sectors, so this
/// is the network — not the indexer URL. Hardcoded to mainnet (what the public
/// indexer serves); a testnet store would mis-declare and its re-pin would fail
/// at import — make this configurable if testnets are ever used.
const SIA_SUBSTRATE_NETWORK: &str = "sia-mainnet";

/// Wraps a Sia backend to expose it as an S5 `Store`.
///
/// `C` is the single backing cache (a `path -> SealedObject` map plus a sync
/// cursor; persistence optional — see the crate and `cache` module docs).
/// `Clone` yields a sibling sharing the live connection and the local cache
/// (the cache is path-based over a `Clone` backing store), so a cloned handle —
/// e.g. for the background [`run_sync_loop`](IndexdStore::run_sync_loop) — sees
/// the same on-disk index. Requires `C: Clone`.
#[derive(Clone)]
pub struct IndexdStore<C: Store> {
    backend: SiaBackend,
    cache: SealedObjectCache<C>,
    config: IndexdConfig,
}

impl<C: Store> std::fmt::Debug for IndexdStore<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexdStore")
            .field("cache", &self.cache)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct IndexdConfig {
    /// Validity window for `Store::provide(path)` signed share URLs.
    /// Default 24 h.
    pub share_validity: Duration,
    /// `UploadOptions` for new uploads. `None` uses sia_storage's own
    /// default (10-of-30 erasure coding).
    pub upload_options: Option<UploadOptionsBuilder>,
    /// Indexer URL the SDK connects to.
    pub indexer_url: String,
    /// Per-request timeout for an indexer enumeration page
    /// ([`IndexdStore::reconstruct_from_indexer`] / [`sync_from_indexer`]). Bounds
    /// each `object_events` call so a degraded indexer fails fast instead of
    /// hanging the open / sync. Default 30 s.
    ///
    /// [`sync_from_indexer`]: IndexdStore::sync_from_indexer
    pub request_timeout: Duration,
    /// Block `open` on a full enumeration when the cache is **cold** (no persisted
    /// cursor), so the store never reports a false "not found" from an
    /// unpopulated cache. Warm opens (cursor present) never block — they have a
    /// usable cache and rely on the background sync loop for freshness. Default
    /// `true`; set `false` only when a caller manages reconstruction itself
    /// (e.g. a cold-path test).
    pub sync_on_open: bool,
}

/// Clone-able copy of `UploadOptions` (the real one carries a non-clone
/// progress callback).
#[derive(Debug, Clone)]
pub struct UploadOptionsBuilder {
    pub data_shards: u8,
    pub parity_shards: u8,
    pub max_inflight: usize,
}

impl UploadOptionsBuilder {
    pub fn build(&self) -> UploadOptions {
        UploadOptions {
            data_shards: self.data_shards,
            parity_shards: self.parity_shards,
            max_buffered_slabs: Some(self.max_inflight),
            ..UploadOptions::default()
        }
    }
}

impl Default for IndexdConfig {
    fn default() -> Self {
        Self {
            share_validity: Duration::from_secs(24 * 3600),
            upload_options: None,
            indexer_url: DEFAULT_INDEXER_URL.to_string(),
            request_timeout: Duration::from_secs(30),
            sync_on_open: true,
        }
    }
}

impl<C: Store> IndexdStore<C> {
    /// Open a store against a previously-registered AppKey.
    ///
    /// `app_key` is the 32-byte export returned by [`auth::register`] — the
    /// caller is responsible for having persisted it (in S5, the node's
    /// age-encrypted `stores` vault). `cache` is the single backing store
    /// (pass a `MemoryStore` for an ephemeral cache rebuilt via
    /// [`IndexdStore::reconstruct_from_indexer`]). Errors if the indexer
    /// doesn't recognise the key.
    /// `app_metadata` brands the indexer connection (`None` = the S5 default,
    /// [`auth::app_metadata`]); its `id` must match the AppKey's app identity.
    pub async fn open(
        config: IndexdConfig,
        app_key: [u8; 32],
        cache: C,
        app_metadata: Option<AppMetadata>,
    ) -> StoreResult<Self> {
        let backend = SiaBackend::open(
            &config.indexer_url,
            app_key,
            config.upload_options.clone(),
            app_metadata,
        )
        .await?;
        let store = Self {
            backend,
            cache: SealedObjectCache::new(cache),
            config,
        };
        // Not "ready" until the local `path -> object` cache mirrors the indexer:
        // a cache miss is reported as "not found", which is only true once we've
        // enumerated fully. Gate on the **reconstructed marker**, not on cursor
        // presence: `run_events` checkpoints the cursor *per page*, so an
        // interrupted first reconstruct leaves a partial cache WITH a cursor —
        // treating that as ready would answer real, un-enumerated keys with a
        // false "not found". A cold cache blocks here on a full enumeration; an
        // interrupted one resumes from its cursor (cheap — skips seen pages);
        // only a run that reaches the end marks the cache ready. **Warm**
        // (already-reconstructed) restarts skip this and rely on the background
        // sync loop ([`run_sync_loop`]) for peer-write freshness.
        //
        // [`run_sync_loop`]: IndexdStore::run_sync_loop
        if store.config.sync_on_open && !store.cache.is_reconstructed().await {
            store.sync_from_indexer().await?;
            store.cache.mark_reconstructed().await?;
        }
        Ok(store)
    }

    /// A sibling store that **shares this one's live connection and local
    /// cache**, but uploads new objects with different erasure-coding options.
    ///
    /// This is how a single `IndexdStore` (one indexer connection, one cache)
    /// backs *both* roles of a `PackingStore`: the pack bodies at one EC profile
    /// (e.g. 10-of-25) and the manifests at another (e.g. 3-of-12). The `Sdk` is
    /// `Clone` (shared host pool) and the cache is path-based, so both handles
    /// read/write the same on-disk index over disjoint paths.
    pub fn with_upload_options(&self, upload_options: Option<UploadOptionsBuilder>) -> Self
    where
        C: Clone,
    {
        Self {
            backend: self.backend.with_upload_options(upload_options.clone()),
            cache: self.cache.clone(),
            config: IndexdConfig {
                upload_options,
                ..self.config.clone()
            },
        }
    }

    /// The indexer object id this store maps `path` to — the stable
    /// `blake2b(slabs)` handle (`SealedObject::id()`), useful for diagnostics or
    /// asserting that a re-pin preserved object identity. Errors if `path` isn't
    /// locally indexed.
    pub async fn object_id(&self, path: &str) -> StoreResult<[u8; 32]> {
        Ok(self.cache.load(path).await?.id().into())
    }

    /// Core of `put_bytes` / `put_stream`: upload from a reader, then record the
    /// sealed object under the path — one atomic cache write.
    async fn put_reader(&self, path: &str, reader: backend::ByteReader) -> StoreResult<()> {
        // Overwrite semantics (match LocalStore / MemoryStore): remember the old
        // object, if any, so it can be reclaimed after the new one lands. The
        // check-then-act race only risks an orphaned object, never data loss.
        let old = self.cache.load(path).await.ok();

        let sealed = self.backend.upload(reader, path).await?;
        let new_id: [u8; 32] = sealed.id().into();

        // One record: `p/<path>` -> sealed object. It is self-contained (id and
        // size derive from it), so this single write is the whole index entry.
        self.cache
            .store(path, &sealed)
            .await
            .map_err(|e| anyhow!("indexd cache write for {path}: {e:?}"))?;

        // Best-effort reclaim of the overwritten object, if it was a different one.
        if let Some(old) = old {
            let old_id: [u8; 32] = old.id().into();
            if old_id != new_id
                && let Err(err) = self.backend.delete(old_id).await
            {
                tracing::warn!(
                    "put overwrote {path}; old object {} not reclaimed: {err:?}",
                    hex::encode(old_id)
                );
            }
        }
        Ok(())
    }

    /// Upsert a small mutable value at `path` as an indexer **metadata
    /// pointer**: the first write mints a tiny placeholder object (one slab);
    /// every later write re-pins its metadata in place (~ms, no slab). This is
    /// the cheap substrate for a durable, high-churn registry HEAD — writing a
    /// 172-byte HEAD as a fresh erasure-coded object would cost a whole slab
    /// (~40 s) *every* update; the pointer pays that once, at mint.
    pub async fn put_pointer(&self, path: &str, value: &[u8]) -> StoreResult<()> {
        // Framing is `b"S5" | path_len:u8 | path | value` = 3 B + path + value.
        // path_len == 255 is reserved for a future S5-prefixed format.
        if path.len() >= u8::MAX as usize {
            return Err(anyhow!(
                "indexd pointer path is {} B; path length must be < 255 (255 reserved)",
                path.len()
            ));
        }
        if 3 + path.len() + value.len() > backend::METADATA_LIMIT {
            return Err(anyhow!(
                "indexd pointer for {path} exceeds the {}-byte metadata limit \
                 (path {} + value {} + 3 B framing)",
                backend::METADATA_LIMIT,
                path.len(),
                value.len()
            ));
        }
        let sealed = match self.cache.load(path).await.ok() {
            Some(existing) => self.backend.update_pointer(&existing, path, value).await?,
            None => self.backend.mint_pointer(path, value).await?,
        };
        self.cache
            .store(path, &sealed)
            .await
            .map_err(|e| anyhow!("indexd pointer cache write for {path}: {e:?}"))
    }

    /// Read a metadata pointer's current value, or `None` if `path` isn't
    /// locally indexed or isn't a pointer object. Local: opens the cached sealed
    /// object to decrypt its metadata — no host round-trip.
    pub async fn get_pointer(&self, path: &str) -> StoreResult<Option<Bytes>> {
        match self.cache.load(path).await {
            Ok(sealed) => Ok(self.backend.read_pointer(&sealed)?.map(Bytes::from)),
            Err(_) => Ok(None),
        }
    }

    /// Enumerate the indexer from `start` (the very beginning if `None`),
    /// applying each event to the local cache and persisting the advancing
    /// checkpoint as it goes. Shared core of [`IndexdStore::reconstruct_from_indexer`] and
    /// [`sync_from_indexer`].
    ///
    /// - **Live object** carrying recoverable S5 metadata: read its **store
    ///   path** straight from the recovery record and write the sealed object
    ///   under `p/<path>` — self-describing.
    /// - **Delete event**: skipped (deletes are lazy — see the crate docs). It
    ///   names only an object id, and we keep no reverse index to map it back; a
    ///   from-empty reconstruct stays clean because a deleted object enumerates
    ///   only as a tombstone (no live object).
    /// - **Live object** without our metadata (e.g. another app's): skipped
    ///   (`unrecognized_skipped`).
    ///
    /// Idempotent and resumable: re-running re-derives the same entries, and the
    /// persisted checkpoint lets sync continue strictly past what it saw.
    async fn run_events(&self, mut after: Option<EnumCursor>) -> StoreResult<SyncStats> {
        let mut stats = SyncStats::default();
        loop {
            // Per-page timeout so a degraded/hung indexer fails fast instead of
            // blocking the open / sync indefinitely.
            let batch = tokio::time::timeout(
                self.config.request_timeout,
                self.backend.object_events(after, RECONSTRUCT_BATCH),
            )
            .await
            .map_err(|_| {
                anyhow!(
                    "indexd object_events timed out after {:?}",
                    self.config.request_timeout
                )
            })??;
            if batch.is_empty() {
                break;
            }
            let batch_len = batch.len();
            for obj in batch {
                after = Some(obj.cursor);
                if obj.deleted {
                    continue;
                }
                let Some(sealed) = obj.sealed else { continue };
                let Some(path) = obj.recovered_path else {
                    stats.unrecognized_skipped += 1;
                    continue;
                };
                self.cache.store(&path, &sealed).await?;
                stats.restored += 1;
            }
            // Checkpoint after each page so an interrupted run resumes mid-way.
            if let Some(cursor) = after {
                self.cache.store_cursor(&cursor).await?;
            }
            if batch_len < RECONSTRUCT_BATCH {
                break;
            }
        }
        Ok(stats)
    }

    /// Rebuild the local cache by enumerating the indexer **from the start** —
    /// the recovery / "indexer is the source of truth" path. For an empty cache
    /// this reconstructs the full `path -> object` mapping; it also leaves a
    /// checkpoint so a later [`IndexdStore::sync_from_indexer`] continues incrementally.
    ///
    /// **Self-describing:** each object's store path is read straight from its
    /// recovery metadata, so *any* key is recoverable — including
    /// non-content-addressed keys like registry entries (which is what lets an
    /// `IndexdStore` back a durable `StoreRegistry`).
    pub async fn reconstruct_from_indexer(&self) -> StoreResult<SyncStats> {
        self.run_events(None).await
    }

    /// Incrementally converge the local cache with the indexer: resume from the
    /// persisted checkpoint and apply only events since — picking up objects
    /// added (including by *another* client sharing this account). This is the
    /// steady-state "stay in sync" loop. Remote deletes are **not** applied
    /// (deletes are lazy — see the crate docs); a from-scratch
    /// [`IndexdStore::reconstruct_from_indexer`] reconciles them.
    ///
    /// With no checkpoint yet (never reconstructed) this falls back to a full
    /// pass, equivalent to [`IndexdStore::reconstruct_from_indexer`].
    pub async fn sync_from_indexer(&self) -> StoreResult<SyncStats> {
        let start = self.cache.load_cursor().await;
        self.run_events(start).await
    }

    /// Background loop: every `interval`, incrementally [`sync_from_indexer`] to
    /// pick up objects written by **other devices** on the same indexd account
    /// (the multi-device case — a user backing up from several machines to one
    /// `sia.storage` account). Each pass resumes from the cursor and is
    /// per-request-timeout-bounded; a failed pass is logged and retried next tick,
    /// so a transient indexer outage never kills the loop. Spawn it once per store
    /// (it shares the cache with every sibling handle).
    ///
    /// [`sync_from_indexer`]: IndexdStore::sync_from_indexer
    pub async fn run_sync_loop(self, interval: Duration)
    where
        C: 'static,
    {
        loop {
            tokio::time::sleep(interval).await;
            match self.sync_from_indexer().await {
                Ok(stats) if stats.restored > 0 => tracing::debug!(
                    restored = stats.restored,
                    "indexd background sync: pulled new objects"
                ),
                Ok(_) => {}
                Err(e) => tracing::warn!("indexd background sync failed (will retry): {e:?}"),
            }
        }
    }

    /// Reclaim Sia storage by pruning slabs no longer referenced by any pinned
    /// object — the deferred other half of `delete`/overwrite, which only unpin
    /// the object. This is what actually stops paying for deleted data. It is
    /// account-wide, so run it periodically or after bulk deletes, not once per
    /// delete.
    pub async fn prune(&self) -> StoreResult<()> {
        self.backend.prune().await
    }

    /// Migrate every entry to `target` by **re-pinning** — no blob bytes are
    /// re-uploaded; the sectors stay on the Sia hosts and `target` just
    /// references them. Source and target are **two different indexers, each
    /// running stock indexd with its own AppKey**: every object is opened under
    /// our AppKey (`SiaBackend::export_object`) and re-sealed under the
    /// target's during the re-pin (`SiaBackend::import_object`), so no shared
    /// key and no fork are needed. Because each sealed object is read straight
    /// from the local cache, this works even with **this store's indexer
    /// offline**. Paths and object ids are preserved (`object_id =
    /// blake2b(slabs)` is indexer-independent).
    ///
    /// **Scope:** this migrates every entry in *this store's local index* (what
    /// [`Store::list`] returns). In normal single-writer operation that is every
    /// object this store ever wrote, so nothing is missed. But if the store was
    /// opened cold (e.g. a fresh recovery cache that hasn't enumerated yet), call
    /// [`IndexdStore::reconstruct_from_indexer`](Self::reconstruct_from_indexer) first so the
    /// index is complete before migrating.
    ///
    /// `target` is another `IndexdStore` (with its own cache `C2`).
    pub async fn migrate_to<C2>(&self, target: &IndexdStore<C2>) -> StoreResult<MigrationStats>
    where
        C2: Store,
    {
        // Typed shortcut over the generic `s5_core::store::migrate`: two indexd
        // stores share the Sia substrate, so this takes the by-reference path.
        let report = s5_core::store::migrate(self, target).await?;
        Ok(MigrationStats {
            migrated: report.by_reference,
        })
    }
}

#[async_trait]
impl<C: Store> Store for IndexdStore<C> {
    fn features(&self) -> StoreFeatures {
        // Inherit from the cache — IndexdStore adds no FS semantics.
        self.cache.features()
    }

    fn migration_substrate(&self) -> Option<Substrate> {
        // The bytes live on Sia hosts, shared across all indexers on the
        // network — so any other indexd store can re-pin them by reference.
        Some(Substrate::SiaHosts {
            network: SIA_SUBSTRATE_NETWORK.to_string(),
        })
    }

    fn as_reference_migrate(&self) -> Option<&dyn ReferenceMigrate> {
        Some(self)
    }

    async fn exists(&self, path: &str) -> StoreResult<bool> {
        self.cache.exists(path).await
    }

    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()> {
        self.put_reader(path, Box::new(std::io::Cursor::new(bytes)))
            .await
    }

    async fn put_stream(
        &self,
        path: &str,
        stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<()> {
        // True streaming: pipe the stream straight into the upload, no
        // full-buffer materialization.
        self.put_reader(path, Box::new(StreamReader::new(stream)))
            .await
    }

    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        let sealed = self.cache.load(path).await?;
        let mut reader = self.backend.download(&sealed, offset, max_len).await?;
        let mut buf: Vec<u8> = Vec::new();
        tokio::io::copy(&mut reader, &mut buf)
            .await
            .map_err(|e| anyhow!("indexd download copy for {path}: {e}"))?;
        Ok(Bytes::from(buf))
    }

    async fn open_read_stream(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>>
    {
        // True streaming: wrap the download reader as a chunk stream.
        let sealed = self.cache.load(path).await?;
        let reader = self.backend.download(&sealed, offset, max_len).await?;
        Ok(Box::new(ReaderStream::new(reader)))
    }

    async fn delete(&self, path: &str) -> StoreResult<()> {
        if !self.cache.exists(path).await? {
            return Ok(()); // already gone — idempotent
        }
        // Reclaim the remote object when the entry decodes; a corrupt entry
        // can't name its object, but we still drop it locally either way (the
        // backend delete is best-effort — a failure doesn't keep the entry).
        match self.cache.load(path).await {
            Ok(sealed) => {
                let object_id: [u8; 32] = sealed.id().into();
                if let Err(err) = self.backend.delete(object_id).await {
                    tracing::warn!(
                        "indexd backend delete failed for {path}; dropping local index entry anyway: {err:?}"
                    );
                }
            }
            Err(err) => tracing::warn!(
                "indexd: corrupt cache entry for {path}; dropping it without reclaiming the remote object: {err:?}"
            ),
        }
        self.cache.remove(path).await
    }

    async fn size(&self, path: &str) -> StoreResult<u64> {
        let sealed = self.cache.load(path).await?;
        Ok(backend::size_of(&sealed))
    }

    async fn list(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>>
    {
        // The cache surfaces only the caller's paths (the `p/` entries with the
        // prefix stripped; the `s/cursor` checkpoint never appears).
        self.cache.list_paths().await
    }

    async fn rename(&self, old_path: &str, new_path: &str) -> StoreResult<()> {
        // Durable: re-seal the object's recovery metadata with the new path and
        // update it on the indexer in place — no data re-upload, object id
        // unchanged — then move the local cache entry. A later reconstruct
        // recovers it under the new path. (This is also the "name after upload"
        // primitive: stream a blob in under a temporary path, then rename to its
        // final content-addressed path once the hash is known.)
        let sealed = self.cache.load(old_path).await?;
        let renamed = self.backend.rename(&sealed, new_path).await?;
        self.cache.store(new_path, &renamed).await?;
        self.cache.remove(old_path).await
    }

    async fn sync(&self) -> StoreResult<()> {
        self.cache.sync().await
    }

    async fn provide(&self, path: &str) -> StoreResult<Vec<BlobLocation>> {
        let sealed = self.cache.load(path).await?;
        let valid_until = Utc::now()
            + chrono::Duration::from_std(self.config.share_validity)
                .unwrap_or_else(|_| chrono::Duration::seconds(24 * 3600));
        let url = self.backend.share(&sealed, valid_until).await?;
        Ok(vec![BlobLocation::Url(url)])
    }
}

/// Registry path for a stream key — matches `StoreRegistry`'s default layout
/// (`registry/<hex(storage_key)>`), so the durable HEAD is found and rebuilt the
/// same way whichever registry backing is in use.
fn registry_path(key: &StreamKey) -> String {
    format!("registry/{}", hex::encode(key.storage_key()))
}

/// An `IndexdStore` *is* a durable registry: each HEAD is a metadata pointer
/// (see [`IndexdStore::put_pointer`]). This is what lets the Sia store back the
/// recovery-critical registry **directly** — cheap per-snap updates, immediately
/// durable on the indexer (no pack/flush lag), and rebuilt by the same
/// `reconstruct_from_indexer` pass that rebuilds the blobs. The revision/LWW
/// logic lives on `StreamMessage`, so this mirrors `StoreRegistry` exactly but
/// over the pointer substrate instead of `put_bytes` (which would mint a fresh
/// erasure-coded slab on every update).
#[async_trait]
impl<C: Store> RegistryApi for IndexdStore<C> {
    async fn get(&self, key: &StreamKey) -> anyhow::Result<Option<StreamMessage>> {
        match self.get_pointer(&registry_path(key)).await? {
            Some(bytes) => Ok(Some(StreamMessage::deserialize(bytes)?)),
            None => Ok(None),
        }
    }

    async fn set(&self, message: StreamMessage) -> anyhow::Result<()> {
        let existing = self.get(&message.key).await?;
        if !message.should_store(existing.as_ref()) {
            return Ok(());
        }
        self.put_pointer(&registry_path(&message.key), message.serialize().as_ref())
            .await?;
        Ok(())
    }

    async fn delete(&self, key: &StreamKey) -> anyhow::Result<()> {
        <Self as Store>::delete(self, &registry_path(key)).await?;
        Ok(())
    }
}

#[async_trait]
impl<C: Store> ReferenceMigrate for IndexdStore<C> {
    async fn export_ref(&self, path: &str) -> StoreResult<Box<dyn Any + Send>> {
        // Open under our AppKey into a portable in-process `Object`, boxed
        // type-erased for the generic migrate path.
        let sealed = self.cache.load(path).await?;
        let opened = self.backend.export_object(&sealed)?;
        Ok(Box::new(opened))
    }

    async fn import_ref(&self, path: &str, handle: Box<dyn Any + Send>) -> StoreResult<()> {
        let opened = handle.downcast::<Object>().map_err(|_| {
            anyhow!(
                "indexd import_ref: migration handle was not a sia Object \
                 (mismatched substrate backend)"
            )
        })?;
        let sealed = self.backend.import_object(*opened).await?;
        self.cache.store(path, &sealed).await?;
        Ok(())
    }
}
