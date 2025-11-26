use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, anyhow};
use chrono::Utc;
use xxhash_rust::xxh3::xxh3_64;

use crate::{
    FSResult,
    dir::{DirRef, DirV1, ENCRYPTION_TYPE_XCHACHA20_POLY1305},
};

use super::DirActor;

// TODO perf testing showed that 262_144 might be better for local dirs
const MAX_DIR_BYTES_BEFORE_SHARD: usize = 65_536;

/// Maximum supported shard level. At level 8+, the bit shift would exceed
/// 64 bits, causing all entries to hash to bucket 0.
pub const MAX_SHARD_LEVEL: u8 = 7;

/// Computes the shard bucket index for a given entry name at the
/// provided shard level. Used for both logical sharding of directory
/// entries and debug views so that the hashing scheme stays consistent.
///
/// # Panics
/// Panics if `shard_level > MAX_SHARD_LEVEL` (7).
pub(crate) fn shard_bucket_for(name: &str, shard_level: u8) -> u8 {
    assert!(
        shard_level <= MAX_SHARD_LEVEL,
        "shard_level {} exceeds maximum {}",
        shard_level,
        MAX_SHARD_LEVEL
    );
    let h = xxh3_64(name.as_bytes());
    ((h >> (8 * (shard_level as usize))) & 0xFF) as u8
}

impl DirActor {
    pub(super) async fn check_auto_promote(&mut self, path: &str) -> FSResult<()> {
        if let Some((prefix, _)) = path.split_once('/') {
            // Count files with this prefix
            let prefix_slash = format!("{}/", prefix);
            let count = self
                .state
                .files
                .keys()
                .filter(|k| k.starts_with(&prefix_slash))
                .count();

            if count > crate::FS5_PROMOTION_THRESHOLD {
                self.promote_prefix(prefix).await?;
            }
        }
        Ok(())
    }

    /// Extracts a child directory snapshot for all files with the given
    /// `prefix/` from `self.state.files`, rewriting their paths to be
    /// relative to that prefix.
    pub(super) fn extract_child_dir_state(&mut self, prefix: &str) -> DirV1 {
        let prefix_slash = format!("{}/", prefix);
        let mut new_dir_state = DirV1::new();

        // Collect matching keys first to avoid borrowing issues while
        // mutating the BTreeMap.
        let matching_keys: Vec<String> = self
            .state
            .files
            .keys()
            .filter(|k| k.starts_with(&prefix_slash))
            .cloned()
            .collect();

        for file_path in matching_keys {
            if let Some(file_ref) = self.state.files.remove(&file_path) {
                let sub_path = file_path
                    .strip_prefix(&prefix_slash)
                    .expect("prefix verified")
                    .to_string();
                new_dir_state.files.insert(sub_path, file_ref);
            }
        }

        new_dir_state
    }

    /// Builds a new `DirRef` for a child directory under this context,
    /// including per-directory encryption keys and optional registry
    /// pointer when a signing key is present.
    pub(super) fn build_child_dir_ref(&self, enable_encryption: bool) -> DirRef {
        use chacha20poly1305::aead::{OsRng, rand_core::RngCore};
        use chacha20poly1305::{KeyInit, XChaCha20Poly1305};

        let now = Utc::now();
        let mut keys = BTreeMap::new();
        if enable_encryption {
            let key: [u8; 32] = XChaCha20Poly1305::generate_key(&mut OsRng).into();
            keys.insert(0x0e, key);
        }

        // Local-only contexts (no signing key) should keep directory
        // references hash-based so everything lives in the local meta
        // store. Only contexts that participate in registry updates
        // (have a signing key) should emit RegistryKey dir refs.
        let (ref_type, hash) = if self.context.signing_key.is_some() {
            let mut registry_pointer = [0u8; 32];
            OsRng.fill_bytes(&mut registry_pointer);
            (Some(crate::dir::DirRefType::RegistryKey), registry_pointer)
        } else {
            (None, [0u8; 32])
        };

        DirRef {
            encryption_type: if enable_encryption {
                Some(ENCRYPTION_TYPE_XCHACHA20_POLY1305)
            } else {
                None
            },
            extra: None,
            hash,
            ref_type,
            keys: if enable_encryption { Some(keys) } else { None },
            ts_seconds: Some(now.timestamp() as u32),
            ts_nanos: Some(now.timestamp_subsec_nanos()),
        }
    }

    pub(super) async fn promote_prefix(&mut self, prefix: &str) -> FSResult<()> {
        if self.state.dirs.contains_key(prefix) {
            return Ok(());
        }

        // Move `prefix/` files into a new child directory snapshot.

        let new_dir_state = self.extract_child_dir_state(prefix);
        let enable_encryption = self.context.encryption_type.is_some();

        let dir_ref = self.build_child_dir_ref(enable_encryption);
        self.state.dirs.insert(prefix.to_owned(), dir_ref);

        let optimized_state = self.optimize_dir_structure(new_dir_state).await?;
        self.open_dir(prefix, Some(optimized_state)).await?;

        Ok(())
    }

    pub(super) async fn optimize_dir_structure(&self, mut state: DirV1) -> FSResult<DirV1> {
        // Group files by their first path segment
        let mut groups: HashMap<String, Vec<String>> = HashMap::new();
        for path in state.files.keys() {
            if let Some((prefix, _)) = path.split_once('/') {
                groups
                    .entry(prefix.to_string())
                    .or_default()
                    .push(path.clone());
            }
        }

        for (prefix, paths) in groups {
            if paths.len() > crate::FS5_PROMOTION_THRESHOLD {
                // Promote this prefix
                let prefix_slash = format!("{}/", prefix);
                let mut child_state = DirV1::new();

                // Move matching files to child state
                for path in paths {
                    if let Some(file_ref) = state.files.remove(&path) {
                        let sub_path = path
                            .strip_prefix(&prefix_slash)
                            .expect("prefix verified")
                            .to_string();
                        child_state.files.insert(sub_path, file_ref);
                    }
                }

                // Recursively optimize the child state
                let optimized_child = Box::pin(self.optimize_dir_structure(child_state)).await?;

                // Save the child state to blob store
                let now = Utc::now();
                let (bytes, keys) = self.encode_child_dir_bytes_for_child(&optimized_child)?;
                let hash = self.context.meta_blob_store.import_bytes(bytes).await?;

                let enable_encryption = self.context.encryption_type.is_some();
                let dir_ref = DirRef {
                    encryption_type: if enable_encryption {
                        Some(ENCRYPTION_TYPE_XCHACHA20_POLY1305)
                    } else {
                        None
                    },
                    extra: None,
                    hash: hash.hash.into(),
                    ref_type: None, // Blake3Hash
                    keys,
                    ts_seconds: Some(now.timestamp() as u32),
                    ts_nanos: Some(now.timestamp_subsec_nanos()),
                };

                state.dirs.insert(prefix, dir_ref);
            }
        }

        Ok(state)
    }

    pub(super) async fn shard_if_needed(&mut self) -> FSResult<()> {
        // TODO(perf): consider a cheaper approximate size heuristic (e.g.
        // bytes-per-entry * entry count) if profiling shows the periodic
        // full serialization here is still a hotspot.
        // TODO: Account for encryption overhead in size threshold
        // Only perform the initial sharding transition once per directory.
        if self.state.header.shard_level.is_some() {
            return Ok(());
        }

        // Fast path: if we've recently measured a small directory and only a
        // few mutations have occurred since, skip serialization.
        const SHARD_SIZE_CHECK_OP_INTERVAL: u64 = 128;
        if self.last_serialized_len < MAX_DIR_BYTES_BEFORE_SHARD
            && self.shard_size_check_ops < SHARD_SIZE_CHECK_OP_INTERVAL
        {
            return Ok(());
        }

        let exact_len = self.state.to_bytes()?.len();
        self.last_serialized_len = exact_len;
        self.shard_size_check_ops = 0;

        if exact_len >= MAX_DIR_BYTES_BEFORE_SHARD {
            self.shard().await?;
        }
        Ok(())
    }

    /// Splits this directory into up to 256 sharded child directories when the
    /// serialized size grows too large. Shards are addressed via XXH3 over the
    /// immediate entry name and tracked in `DirHeader.shards`.
    pub(super) async fn shard(&mut self) -> FSResult<()> {
        use crate::dir::DirRef as DirRefType;

        tracing::debug!("shard");

        if self.state.header.shard_level.is_none() {
            let shard_level = match &self.context.link {
                crate::context::DirContextParentLink::DirHandle { shard_level, .. } => *shard_level,
                _ => 0,
            };
            self.state.header.shard_level = Some(shard_level);
            tracing::debug!("shard_level {shard_level}");

            let mut shard_states: Vec<DirV1> = (0..256).map(|_| DirV1::new()).collect();

            for (name, dir_ref) in &self.state.dirs {
                let h = xxh3_64(name.as_bytes());
                let index = ((h >> (8 * (shard_level as usize))) & 0xFF) as usize;
                shard_states[index]
                    .dirs
                    .insert(name.clone(), dir_ref.clone());
            }
            for (name, file_ref) in &self.state.files {
                let h = xxh3_64(name.as_bytes());
                let index = ((h >> (8 * (shard_level as usize))) & 0xFF) as usize;
                shard_states[index]
                    .files
                    .insert(name.clone(), file_ref.clone());
            }
            tracing::debug!("created new shard states");

            // Persist each shard snapshot immediately so that `DirHeader.shards`
            // always contains a valid hash. This avoids races where a parent
            // directory is saved before shard actors have published their
            // hashes via `UpdateDirRefHash`, which would otherwise leave
            // zeroed shard hashes in the on-disk snapshot.
            for (i, shard_state) in shard_states.into_iter().enumerate() {
                // Skip completely empty shards to avoid clutter.
                if shard_state.dirs.is_empty() && shard_state.files.is_empty() {
                    continue;
                }

                if self.state.header.shards.is_none() {
                    self.state.header.shards = Some(BTreeMap::new());
                }

                let now = Utc::now();
                let (bytes, keys) = self.encode_child_dir_bytes_for_child(&shard_state)?;
                let hash = self.context.meta_blob_store.import_bytes(bytes).await?;

                if let Some(ref mut shards_map) = self.state.header.shards {
                    let enable_encryption = self.context.encryption_type.is_some();
                    let dir_ref = DirRefType {
                        encryption_type: if enable_encryption {
                            Some(ENCRYPTION_TYPE_XCHACHA20_POLY1305)
                        } else {
                            None
                        },
                        extra: None,
                        hash: hash.hash.into(),
                        ref_type: None, // Blake3Hash
                        keys,
                        ts_seconds: Some(now.timestamp() as u32),
                        ts_nanos: Some(now.timestamp_subsec_nanos()),
                    };
                    shards_map.insert(i as u8, dir_ref);
                }

                // Spawn an actor for the shard so future mutations are
                // routed correctly. The actor will load its state from the
                // blob store using the hash we just wrote.
                let _ = self.open_dir_shard_impl(i as u8, None).await?;
            }

            if let Some(shards_map) = &self.state.header.shards
                && shards_map.is_empty()
            {
                self.state.header.shards = None;
            }

            // Clear local state as entries are now in shards
            self.state.dirs.clear();
            self.state.files.clear();

            // After sharding, the parent directory retains its logical
            // `dirs`/`files` maps so the API sees a flat view. Shards are
            // an internal storage/layout optimization and are aggregated in
            // `list_entries`.
        } else {
            return Err(anyhow!("already sharded; cannot shard again"));
        }
        Ok(())
    }

    pub(super) async fn open_dir_shard_impl(
        &mut self,
        shard_index: u8,
        initial_state: Option<DirV1>,
    ) -> anyhow::Result<crate::actor::DirActorHandle> {
        if let Some(handle) = self.dir_shard_handles.get(&shard_index) {
            return Ok(handle.clone());
        }

        let dir_ref = self
            .state
            .header
            .shards
            .as_ref()
            .and_then(|s| s.get(&shard_index))
            .context("shard not found")?;

        let link = match dir_ref.ref_type() {
            crate::dir::DirRefType::Blake3Hash => {
                let parent_shard_level =
                    self.state.header.shard_level.ok_or_else(|| {
                        anyhow!("missing shard level in parent when opening shard")
                    })?;
                let child_shard_level = parent_shard_level.checked_add(1).ok_or_else(|| {
                    anyhow!(
                        "shard level overflow: parent level {} exceeds maximum",
                        parent_shard_level
                    )
                })?;
                if child_shard_level > MAX_SHARD_LEVEL {
                    return Err(anyhow!(
                        "shard level {} exceeds maximum allowed level {}",
                        child_shard_level,
                        MAX_SHARD_LEVEL
                    ));
                }
                crate::context::DirContextParentLink::DirHandle {
                    shard_level: child_shard_level,
                    path: crate::context::DirHandlePath::Shard(shard_index),
                    handle: self.handle.clone().context("actor has no handle")?,
                    initial_hash: dir_ref.hash,
                }
            }
            _ => return Err(anyhow!("dir shards can only be blake3 hash dir refs")),
        };

        let context = self.context.with_new_ref(dir_ref, link);
        // TODO: Propagate autosave and ensure recursive save/dirty semantics are correct
        let handle =
            crate::actor::DirActorHandle::spawn(context, initial_state, self.autosave_debounce_ms);

        self.dir_shard_handles.insert(shard_index, handle.clone());
        Ok(handle)
    }
}
