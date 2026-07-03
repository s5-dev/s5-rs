//! Configuration types shared between the S5 node and its clients.
//!
//! These types are the subset of `S5NodeConfig` that can live in the
//! API crate without pulling in heavy store/blob dependencies.
//! The full `S5NodeConfig` (which adds `NodeConfigStore` and
//! cross-reference validation) remains in `s5_node::config`.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigIdentity {
    pub secret_key_file: Option<String>,
    pub secret_key: Option<String>,
    /// Optional key name from `[key.*]` — identity file is encrypted with this key.
    #[serde(default)]
    pub encrypted_with: Option<String>,
    /// Optional path to a 32-byte ed25519 seed file holding the
    /// **warm master signing key** (D17 cold/warm split: the
    /// operational signer named by the DID's cold pointer — the DID
    /// itself is the *cold* pubkey, which never touches a device).
    /// When set, the daemon loads the warm key from this file
    /// (generating a fresh random key on first boot if the file is
    /// missing). Age-encrypted at rest when `[key.main]` is configured.
    #[serde(default)]
    pub master_key_file: Option<String>,
    /// Optional path to the serialized **cold-pointer entry** binding
    /// this daemon's DID (cold pubkey) to its warm key — written by
    /// `vup onboard`/`vup recover`, republished by the daemon at
    /// startup, and shipped in the pairing handshake (it is
    /// self-certifying under the DID). When unset, defaults to a
    /// sibling of the warm key file named `identity_anchor.entry`; if
    /// no such file exists the daemon runs **self-anchored** (cold ==
    /// warm, dev/test mode — see `s5_node::identity_anchor`).
    #[serde(default)]
    pub anchor_entry_file: Option<String>,
    /// Optional path to the per-device keyset file
    /// (`device_keyset.cbor.age`), holding **three independent random**
    /// ed25519 seeds: the iroh transport secret, the device signing
    /// secret (vault registry writes), and the device ACL/read secret
    /// (F02 blob-fetch challenge responder). Age-encrypted to
    /// `[key.main]` at rest.
    ///
    /// When unset, defaults to a sibling of `secret_key_file` named
    /// `device_keyset.cbor.age`. Replaces the previous design where
    /// `device_signing` and `device_acl` were blake3-derived from the
    /// iroh secret (a single-file-leak compromise). See
    /// `s5_node::device_keyset`.
    #[serde(default)]
    pub keyset_file: Option<String>,
    /// Name of the `[store.<name>]` the daemon publishes the **bootstrap
    /// vaults** (`stores`, later `identity_secrets`) into at startup — the
    /// durable, recoverable store `vup recover` re-selects to read them back.
    /// Set to a remote store (indexd/s3); a local-only store cannot anchor
    /// paper recovery. When unset, the bootstrap vaults are not published and
    /// recovery is unavailable (the daemon logs this once at startup).
    #[serde(default)]
    pub bootstrap_store: Option<String>,
}

// ---------------------------------------------------------------------------
// Key
// ---------------------------------------------------------------------------

/// An age key used for vault encryption.
///
/// `public_key` is always present (age recipient string).
/// `identity_file` is optional — if present, s5 can decrypt with this key.
/// Keys without an identity file are encrypt-only (e.g. yubikey recipients
/// where the hardware key is not always available).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigKey {
    /// Age public key (recipient string, e.g. "age1abc...").
    pub public_key: String,
    /// Optional path to an age identity file for decryption.
    #[serde(default)]
    pub identity_file: Option<String>,
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Declares a local directory that s5 is *allowed* to read.
///
/// This is a security boundary — nothing outside declared sources can be
/// ingested, regardless of what remote orchestration requests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigSource {
    /// Filesystem paths to the source directories.
    /// Supports both a single path and multiple paths.
    pub paths: Vec<String>,

    /// Whether to include cache directories (CACHEDIR.TAG, node_modules, target/, ...).
    /// Default: false — skip known cache dirs.
    #[serde(default)]
    pub include_caches: bool,

    /// Whether to skip hidden files and directories (dotfiles/dotdirs).
    /// Default: false — include hidden entries.
    #[serde(default)]
    pub skip_hidden: bool,

    /// Whether to respect .gitignore / .ignore rules.
    /// Default: false — don't follow ignore rules, include everything.
    #[serde(default)]
    pub respect_ignore_files: bool,

    /// Glob patterns to exclude from this source.
    #[serde(default)]
    pub exclude: Vec<String>,

    /// Stay on the same filesystem — do not cross mount boundaries.
    /// Default: false.
    #[serde(default)]
    pub one_file_system: bool,

    /// Maximum number of files processed concurrently during ingest from
    /// this source. `None` keeps `s5_fs_local::BackupConfig`'s default
    /// (currently 8); `Some(n)` overrides it.
    ///
    /// Each in-flight file occupies one tokio task that streams the file,
    /// chunks it, computes BLAKE3 + Bao, runs zstd compression, and writes
    /// the resulting blobs. Bumping this past your core count typically
    /// stops paying off (zstd is the CPU hog) — pick it to match the box
    /// the indexer runs on, not a global default.
    #[serde(default)]
    pub max_concurrent_ops: Option<usize>,
    /// Follow symlinks during ingest: stat and import the target file's
    /// content instead of storing the symlink as metadata-only (target
    /// path string). When true, also descends into symlinks to directories.
    ///
    /// Default: `false`.
    #[serde(default)]
    pub follow_symlinks: bool,

    /// Detect deletions: tombstone snapshot entries whose source file no
    /// longer exists, so the snapshot mirrors the source instead of
    /// accumulating every file ever ingested.
    ///
    /// Default `false` keeps the historical additive behaviour (append-only
    /// archival). Set `true` for sources that delete files — required for
    /// a segment compactor, which removes superseded packs (left off,
    /// the published tree grows without bound and downstream cold-store GC
    /// can never reclaim the orphaned blobs). See `BackupConfig`.
    #[serde(default)]
    pub detect_deletions: bool,
}

// ---------------------------------------------------------------------------
// Vault
// ---------------------------------------------------------------------------

/// An FS5 vault — the canonical local metadata state.
///
/// A vault only advances via merges from task-produced snapshots.
/// Each vault creates a separate FS5 root at the configured `root_path`.
/// All vaults are encrypted — `key` is required.
///
/// Unknown keys are rejected (`deny_unknown_fields`) so typos — and the
/// retired `blob_stores`/`meta_targets` lists, replaced by
/// `data_store`/`meta_store` in decision D1 — fail loudly at load
/// instead of being silently ignored.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct NodeConfigVault {
    /// Absolute path to the FS5 root directory (contains `root.fs5.cbor`
    /// and the metadata store). Metadata is always local.
    pub root_path: String,

    /// Key name from `[key.*]` used to encrypt this vault's local state.
    /// The node must have the identity file for this key (can decrypt).
    pub key: String,

    /// Primary store for this vault's content blobs — the upload target
    /// for snapshots and the first read source. Must reference a declared
    /// `[store.*]` entry. When unset, the node-level `default_store`
    /// applies (which itself defaults to the sole `[store.*]` entry).
    /// See architecture decision D1.
    #[serde(default)]
    pub data_store: Option<String>,

    /// Optional preset string for FS5 pipeline configuration
    /// (e.g. "e2ee_prolly_chunked_zstd_dict_default_chacha20").
    #[serde(default)]
    pub preset: Option<String>,

    /// Names of `[key.*]` entries that form the vault's full recipient set
    /// for publish encryption. Every published Transparent Node is age-encrypted
    /// to all of these. Empty list = vault is local-only and cannot publish.
    #[serde(default)]
    pub recipients: Vec<String>,

    /// Names of `[source.*]` entries that feed into this vault on snap/backup.
    /// Empty list = no automatic source mapping (paths must be passed explicitly).
    #[serde(default)]
    pub sources: Vec<String>,

    /// Primary store for this vault's published meta blobs (the encrypted
    /// Transparent Node, export blobs). Distinct from `data_store` so meta
    /// can ride a relay/registry-adjacent store while bulk content goes to
    /// cold storage. When unset, resolves to the vault's data store. Must
    /// reference a declared `[store.*]` entry. See architecture decision D1.
    #[serde(default)]
    pub meta_store: Option<String>,

    /// Store FS5 tree nodes in plaintext (true) instead of encrypted (false,
    /// default). Set this only for content-store interop (e.g. Hugging Face
    /// Xet). The published Transparent Node is still age-encrypted to
    /// `recipients` when this alone is set — only the inner tree nodes are
    /// plaintext. Combine with `plaintext_published_tn` for fully-public
    /// publish-and-distribute.
    #[serde(default)]
    pub plaintext_tree: bool,

    /// Publish the Transparent Node in plaintext (true) instead of age-
    /// encrypted to `recipients` (false, default).
    ///
    /// Designed for the public-publisher / many-read-only-consumers pattern:
    /// one daemon writes the vault, an arbitrary set of consumers subscribes
    /// to its `(pubkey, vault_id)` and reads the TN by hash. No per-consumer
    /// recipient rotation, no shared age secret file. Pair with
    /// `plaintext_tree = true` so the inner tree blobs are also unencrypted
    /// — otherwise consumers couldn't decrypt anything past the TN.
    ///
    /// When this is true, the publisher may run with `recipients = []` (no
    /// age envelope at all). The TN blob's content is still BLAKE3-addressed
    /// and signed in its registry entry, so authenticity is unaffected.
    ///
    /// Consumers detect plaintext at read time (no flag plumbing needed):
    /// `fetch_previous_published_node` first tries age-decrypt; if that
    /// fails or no identity is available, it falls back to direct CBOR
    /// parse. Mixed deployments work transparently.
    #[serde(default)]
    pub plaintext_published_tn: bool,

    /// Use filesystem events (inotify/FSEvents) for immediate detection.
    /// Default: false.
    #[serde(default)]
    pub watch: bool,

    /// Scheduled backups: snap this vault every N seconds (M5 "cron-like
    /// schedule in config, daemon executes"). The daemon runs a full
    /// incremental-dedup snap on the interval, measured from when the
    /// previous scheduled snap COMPLETED (a slow snap never stacks).
    /// Unset = no schedule. Redundant when `watch = true` (the watcher
    /// already snaps within seconds of every change) — `watch` wins and
    /// the schedule is skipped with a warning.
    #[serde(default)]
    pub snap_interval_secs: Option<u64>,

    /// Identities authorised to participate in this vault.
    ///
    /// Each entry is either `"self"` (this node's own DID) or a name
    /// from a `[friend.<name>]` block. Resolution at startup pulls each
    /// identity's published `DidDocument` from the registry and derives
    /// the per-vault transport ACL (authorised iroh pubkeys) and
    /// encryption recipient set (age recipients). Empty list = vault is
    /// strictly local; `recipients` (legacy) is consulted instead.
    ///
    /// `members` is the **read** set (D11: capability = keyset membership):
    /// every member's `acl_keys[]` join the vault's authorised-read ACL and
    /// their `age_recipients` join the encryption set. **Write** capability
    /// is the subset named in [`writers`](Self::writers).
    #[serde(default)]
    pub members: Vec<String>,

    /// Members (a subset of [`members`](Self::members)) granted **write**
    /// capability — their `signers[]` are accepted as vault registry writers
    /// (D11). A member NOT listed here is read-only: it can connect, fetch,
    /// and decrypt, but its registry writes are rejected. `grant --write`
    /// adds here; `grant --read` (the default) does not. Empty = no remote
    /// writers (the owner still writes locally, ungated).
    #[serde(default)]
    pub writers: Vec<String>,

    /// First-match-wins per-key pipeline routing for this vault.
    /// Each entry maps a glob over the file's vault key (relative path
    /// from the source root, e.g. `"segments/feed.post/X.seg"`) to a
    /// pipeline + chunking override. Files whose key matches a route's
    /// glob are imported with that override stamped onto
    /// `entry.child_context`; files matching nothing fall through to
    /// the vault's default `TraversalContext` (governed by
    /// `plaintext_tree` and `preset`).
    ///
    /// Order matters — first match in declaration order wins, like
    /// `.gitignore` patterns.
    #[serde(default)]
    pub pipelines: Vec<PipelineRouteConfig>,

    /// 32-byte hex-encoded `vault_id` for non-publishing members.
    ///
    /// Publishers compute `vault_id` from `KEY_SLOT_RECOVERY` of the
    /// vault root on first publish and register it into
    /// `MembershipState::vault_id_by_name` — that's what
    /// `MembershipSubscriber` reads to know which `(peer, vault_id)`
    /// data keys to subscribe to. Read-only members never publish, so
    /// without an explicit hint here, MembershipSubscriber would skip
    /// every data-vault subscription for the vault.
    ///
    /// Set this on the consumer side to mirror the publisher's
    /// recovery-derived value. Hex format matches the publisher's log
    /// output (`hex::encode(vault_id)`); leave `None` on the publisher
    /// (it computes the canonical value at publish time).
    #[serde(default)]
    pub vault_id: Option<String>,

    /// Bound the published Transparent Node history chain to the last `N`
    /// entries (count-based; the `""` current-snapshot entry is always
    /// kept and not counted). History keys are RFC3339 timestamps, so the
    /// `BTreeMap<String, _>` orders them chronologically and the oldest
    /// `len - N` are dropped on each publish.
    ///
    /// `None` (default) = unbounded history — today's behavior, preserved
    /// for every existing vault. Dropping an entry unbinds its old
    /// encrypted-TN blob, making it eligible for the cold-store GC below.
    /// A publisher that enables `gc_*` MUST also set this, or GC reclaims
    /// nothing (every prior TN blob stays reachable via the chain).
    #[serde(default)]
    pub tn_history_keep: Option<usize>,

    /// Enable the periodic cold-store garbage-collection task for this
    /// vault. Spawned only when `true` (publisher-only; consumers leave it
    /// unset). Deletes blobs from `gc_store` that are unreachable from the
    /// current FS5 root, unpinned, and older than `gc_min_age_secs`.
    #[serde(default)]
    pub gc_enabled: bool,

    /// Name of the `[store.*]` backend the GC task prunes — the *cold*
    /// tier of the tiered store (e.g. `"media_cold"`), NOT the tiered
    /// facade. Required when `gc_enabled = true`.
    #[serde(default)]
    pub gc_store: Option<String>,

    /// Interval between GC passes, seconds. Default 86400 (24 h).
    #[serde(default)]
    pub gc_interval_secs: Option<u64>,

    /// Minimum blob mtime age, seconds, before a candidate is eligible for
    /// deletion. The grace gate that makes the live-publisher race
    /// structurally impossible (a blob written by an in-flight revision is
    /// far younger than this). Default 604800 (7 d).
    #[serde(default)]
    pub gc_min_age_secs: Option<u64>,

    /// When `true`, the GC task computes and reports candidates but deletes
    /// nothing. Default for first rollout; flip to `false` after one cycle
    /// confirms the candidate set is sane.
    #[serde(default)]
    pub gc_dry_run: bool,
}

/// One entry in a vault's pipeline routing table. See
/// `NodeConfigVault.pipelines`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct PipelineRouteConfig {
    /// Glob pattern matched against the vault key (relative path within
    /// the vault tree). Uses `globset` semantics — `**` for arbitrary
    /// depth, `*` for one path segment.
    pub glob: String,
    /// Override for the file's leaf encoding pipeline (compression,
    /// padding, encryption). Any field set as `Some` overrides the
    /// vault default; fields left `None` inherit. Omit the whole
    /// section to inherit the entire pipeline.
    #[serde(default)]
    pub pipeline: Option<BlobPipelineConfig>,
    /// Override for the chunking strategy. Omit to inherit (which for
    /// the vault default means Xet CDC).
    #[serde(default)]
    pub chunking: Option<FileChunkingConfig>,
    /// APPEND-ONLY hint (#3): files matching this glob only ever GROW (their
    /// leading bytes never change). When `true` AND `chunking` is `Fixed`, the
    /// incremental backup reuses the unchanged full-chunk prefix by reference
    /// and reads only the appended tail, instead of re-reading the whole file
    /// every publish. MUST be false for any rewritten-in-place file. Default
    /// false. (See `s5_fs_v2::Snapshot::import_file_append`.)
    #[serde(default)]
    pub append_only: bool,
}

/// Serde-friendly mirror of the runtime `s5_fs_v2::node::BlobPipeline`.
/// Used in TOML config; converted to the runtime type at vault load
/// time. Fields omitted = inherit from parent context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlobPipelineConfig {
    #[serde(default)]
    pub compression: Option<CompressionConfig>,
    /// `Some(true)` enables, `Some(false)` disables, `None` inherits.
    #[serde(default)]
    pub skip_when_unhelpful: Option<bool>,
    // Padding/encryption knobs intentionally omitted — no current
    // consumer needs to express them via TOML. Add when a caller asks.
    // Inheriting is always available (omit the field).
}

/// Serde-friendly mirror of `CompressionStrategy`. Only the variants
/// expressible cleanly via TOML are exposed; dictionary-from-preceding
/// is intentionally omitted (rarely useful, wire format may evolve).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CompressionConfig {
    /// Explicit "no compression" (the runtime
    /// `CompressionStrategy::Uncompressed` variant).
    Uncompressed,
    /// Zstd compression. `level` omitted = default level (the runtime
    /// `CompressionStrategy::Zstd` unit variant). `level` set = the
    /// runtime `CompressionStrategy::ZstdLevel { level }` variant —
    /// 1–22 normal, negative for fast mode (`zstd --fast=N`).
    Zstd {
        #[serde(default)]
        level: Option<i8>,
    },
}

/// Serde-friendly mirror of `FileChunkingStrategy`. Only the simple
/// strategies usable from TOML are exposed; `DataCdc` (Xet CDC with
/// tunable params) is the runtime default when `chunking` is omitted
/// entirely on a route, so there's no v1 reason to spell it out via
/// config. Add the variant when a caller asks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FileChunkingConfig {
    /// Single blob per file (up to the 4 GiB u32 ceiling).
    None,
    /// Fixed-size leaf chunks. Last chunk = remainder.
    Fixed { chunk_size: u32 },
}

/// A paired peer identified by a `did:s5:b...` reference. Looked up by
/// nickname from `vault.<name>.members` and other places that need to
/// name a peer without inlining its DID string.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigFriend {
    /// `did:s5:b<multibase(0xed||pk)>` — see s5_core::identity::Did.
    pub id: String,
    /// Optional 64-character hex-encoded iroh transport pubkey for this
    /// friend. Required for bootstrap-from-cold-cache dialing: since
    /// the four-key model (S2d+), master_pubkey ≠ iroh_pubkey, so the
    /// DID alone cannot be used as a QUIC dial target. When this field
    /// is present, the daemon seeds the friend's iroh pubkey into the
    /// membership state directly, letting `MembershipSubscriber` dial
    /// the peer immediately to fetch their identity bundle. When
    /// absent, the daemon must fall back on out-of-band delivery of
    /// the bundle (push from another peer, relay-mediated bootstrap)
    /// — until the bundle arrives, the peer is unreachable.
    ///
    /// Pre-`pair`-flow this is the only way to seed cross-peer dials.
    #[serde(default)]
    pub iroh_pubkey_hex: Option<String>,
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum NodeConfigRegistry {
    /// Local Redb-backed registry at the given path.
    ///
    /// `type = "local"` is the recommended default — uses Redb under the hood.
    Local {
        /// Absolute path to the registry data directory.
        path: String,
    },
    /// Explicit Redb backend (alias for `local` with a different name).
    Redb {
        /// Absolute path to the registry data directory.
        path: String,
    },
    /// Registry stored in a local blob store directory via `StoreRegistry`.
    StoreLocal {
        /// Absolute path to the registry data directory.
        path: String,
        /// Optional key prefix within the store; defaults to "registry".
        #[serde(default)]
        prefix: Option<String>,
    },
    /// In-memory registry for testing or ephemeral nodes.
    Memory,
    /// Registry backed by a named store (e.g. S3, Sia, local).
    ///
    /// References a `[store.*]` entry by name. Uses `StoreRegistry` internally,
    /// so the registry entries are stored as individual files under `prefix/`.
    ///
    /// ```toml
    /// [registry.remote]
    /// type = "store"
    /// store = "my-s3-bucket"
    /// prefix = "registry"
    /// ```
    Store {
        /// Name of a declared `[store.*]` entry.
        store: String,
        /// Optional key prefix within the store; defaults to "registry".
        #[serde(default)]
        prefix: Option<String>,
    },
    /// Multi registry: fans out writes to N backends in parallel.
    Multi {
        /// List of backend configurations.
        backends: Vec<NodeConfigRegistry>,
        /// Write policy: "all" (default), "any", or "quorum:N".
        #[serde(default)]
        write_policy: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Task
// ---------------------------------------------------------------------------

/// When the daemon fires a task on its own (the `automate` engine, Stage 7).
///
/// A `Manual` task only runs when something asks for it (`RunTask` /
/// `vup backup`). `Watch`/`Every` tasks are *automations*: the daemon's
/// `AutomationManager` reconciles them from `[task.*]` and keeps a live loop
/// running for each. Serialized as a bare snake_case string (`trigger =
/// "watch"`), defaulting to `Manual` so every pre-existing task stays manual.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum TaskTrigger {
    /// Runs only on explicit request. The default.
    #[default]
    Manual,
    /// The daemon watches the task's source paths and snaps on change
    /// (requires a `Backup` spec).
    Watch,
    /// The daemon re-runs the task every `interval_secs` seconds.
    Every,
}

/// A named task — the unit of work in s5.
///
/// Each task works in its own ephemeral FS5 working tree, produces a
/// snapshot, and merges it into the target vault.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigTask {
    /// Tasks to trigger on successful completion.
    #[serde(default)]
    pub then: Vec<String>,

    /// How the daemon fires this task on its own (Stage 7 `automate`).
    /// Sibling of the flattened `spec`; defaults to `Manual` so existing
    /// tasks are unaffected.
    #[serde(default)]
    pub trigger: TaskTrigger,

    /// Cadence for `trigger = "every"`, in seconds. Required (and only
    /// meaningful) for `Every` automations; ignored otherwise.
    #[serde(default)]
    pub interval_secs: Option<u64>,

    /// A paused automation stays configured but is not spawned by the
    /// reconciler. `automate pause`/`resume` flip this. No effect on
    /// `Manual` tasks.
    #[serde(default)]
    pub paused: bool,

    /// The task kind and its parameters.
    #[serde(flatten)]
    pub spec: TaskSpec,
}

/// Task specification — determines what the task does.
///
/// Used both in config (`[task.*]`) and over RPC (`RunTask`).
/// All fields reference named config entries (vaults, sources, stores, keys)
/// that the node resolves at execution time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum TaskSpec {
    /// Ingest files from a source into a vault.
    ///
    /// Scans source dirs, imports blobs to `blob_store`, builds metadata
    /// in an ephemeral working tree, produces a snapshot, and merges it
    /// into the vault.
    Ingest {
        /// Vault name — must reference a declared `[vault.*]`.
        vault: String,
        /// Source name — must reference a declared `[source.*]`.
        source: String,
        /// Store name for writing data blobs — must reference a declared `[store.*]`.
        blob_store: String,
        /// Optional path prefix in the FS5 tree.
        #[serde(default)]
        target_path: Option<String>,
    },
    /// Publish vault state — diff, replicate meta blobs, publish to registry.
    ///
    /// Diffs the current vault snapshot against the last published one,
    /// pushes new meta blobs, and publishes the snapshot hash to the registry.
    /// The published state is encrypted for the specified key recipients.
    Publish {
        /// Vault name — must reference a declared `[vault.*]`.
        vault: String,
        /// Key names from `[key.*]` — recipients for the published state.
        /// The published snapshot is encrypted for these recipients.
        keys: Vec<String>,
    },
    /// Convenience: ingest + publish in one task.
    Backup {
        /// Vault name — must reference a declared `[vault.*]`.
        vault: String,
        /// Source name — must reference a declared `[source.*]`.
        source: String,
        /// Store name for writing data blobs — must reference a declared `[store.*]`.
        blob_store: String,
        /// Key names from `[key.*]` — recipients for the published state.
        keys: Vec<String>,
        /// Optional path prefix in the FS5 tree.
        #[serde(default)]
        target_path: Option<String>,
        /// Incremental hint — set only by the daemon watch loop (see
        /// `s5_node::watch`) from coalesced filesystem events. When `Some`,
        /// apply ONLY these paths to the snapshot (upsert existing, tombstone
        /// vanished) instead of walking the whole source tree + the whole
        /// previous snapshot for deletion detection — O(changed paths) vs
        /// O(corpus). `None` (the default, and every RPC caller) is a full
        /// backup / reconcile. NOT a correctness substitute for the full path:
        /// inotify can miss mmap modifies and drop events on overflow, so the
        /// watch loop still runs periodic full (`None`) reconciles.
        #[serde(default)]
        changed_paths: Option<Vec<std::path::PathBuf>>,
    },
    /// Restore files from a vault snapshot to a local directory.
    Restore {
        /// Vault name — must reference a declared `[vault.*]`.
        vault: String,
        /// Local directory to restore into.
        target_path: String,
        /// Optional blob store override for reading file content.
        /// If not set, uses the vault's resolved data store (D1).
        #[serde(default)]
        blob_store: Option<String>,
        /// Optional snapshot selector (D20 `vault:#snap`). Resolves against
        /// the vault's published registry history to a past snapshot: a
        /// 1-based revision number (oldest published = 1), an exact ISO-8601
        /// timestamp, or a hash prefix (matched against the same hashes
        /// `list_snapshots` prints). `None` restores the current snapshot.
        #[serde(default)]
        snapshot: Option<String>,
        /// Optional subtree path prefix (D20 `vault:path`). Restores only that
        /// path within the snapshot, re-rooted so the subtree's contents land
        /// directly under `target_path`. `None` restores the whole tree.
        #[serde(default)]
        subtree: Option<String>,
    },
    /// Copy a vault (or subtree) into another vault — the D21 sharing
    /// primitive. A **shallow** copy reuses the source leaf ciphertext and
    /// inlines each leaf's per-blob key into the destination (the source
    /// master data key is never shared); `--deep` re-encrypts everything under
    /// the destination's own keys (true future-revocability).
    Copy {
        /// Source vault name — must reference a declared `[vault.*]`.
        src_vault: String,
        /// Optional source subtree (D20 `vault:path`). `None` copies the whole
        /// vault.
        #[serde(default)]
        src_path: Option<String>,
        /// Optional source snapshot selector (D20 `vault:#snap`). `None` copies
        /// the current snapshot.
        #[serde(default)]
        src_snap: Option<String>,
        /// Destination vault name — must reference a declared `[vault.*]`.
        dst_vault: String,
        /// Optional destination path prefix (D20 `vault:path`). `None` lands the
        /// copy at the destination root.
        #[serde(default)]
        dst_path: Option<String>,
        /// Store name for writing replicated data blobs + re-encoded nodes —
        /// must reference a declared `[store.*]` (typically the destination
        /// vault's data store). A shared store makes replication a no-op.
        blob_store: String,
        /// Key names from `[key.*]` — recipients the destination snapshot is
        /// published to.
        keys: Vec<String>,
        /// Deep copy: re-encrypt under the destination's keys instead of
        /// reusing source ciphertext + inlined keys.
        #[serde(default)]
        deep: bool,
        /// Acknowledge that a shallow copy widens who can decrypt the source
        /// data (the destination reader set differs from the source's). The
        /// daemon refuses a widening shallow copy without this (or `--deep`).
        #[serde(default)]
        confirm_widen: bool,
    },
}
