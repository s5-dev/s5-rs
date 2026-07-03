//! RPC protocol types for communicating with an S5 node.
//!
//! Follows the irpc `#[rpc_requests]` pattern. The macro generates
//! `S5NodeMessage` (the enum with attached channels) and implements
//! `irpc::Service` on `S5NodeProto`.
//
// TODO(audit): a couple of fields here are forced to `String`
// because postcard (irpc's wire format) cannot encode
// internally-tagged serde enums or `serde_json::Value`. Specifically
// `RunTask.spec_json` (TaskSpec is `#[serde(tag = "type")]`) and
// `PatchConfig.patch_json` (RFC 6902 patches). The workaround is
// fine but means the daemon JSON-parses a string that the CLI just
// JSON-serialised. If we ever consider a self-describing format
// for the control plane (rmp / cbor), these JSON-as-String fields
// are the strongest argument for it.

use irpc::channel::{mpsc, oneshot};
use irpc::rpc_requests;
use serde::{Deserialize, Serialize};

/// RPC protocol definition for an S5 node.
///
/// Convention: most operations return `Result<T, String>` — the `T` is
/// the success payload (often `()`), the `String` is a human-readable
/// error. Streaming RPCs return their own purpose-specific event enum.
#[derive(Debug, Serialize, Deserialize)]
#[rpc_requests(message = S5NodeMessage)]
pub enum S5NodeProto {
    /// Run a named task from the node's config, or an inline spec.
    /// Returns the spawned task's id + resolved spec on success.
    #[rpc(tx = oneshot::Sender<Result<SpawnedTask, String>>)]
    RunTask(RunTask),

    /// Stream task status. First event = current state (so single-shot
    /// status reads work too); subsequent events follow state changes
    /// until the task reaches a terminal state.
    #[rpc(tx = mpsc::Sender<TaskStatusResponse>)]
    WatchTaskStatus(WatchTaskStatus),

    /// Cancel a running task.
    #[rpc(tx = oneshot::Sender<Result<(), String>>)]
    CancelTask(CancelTask),

    /// List all tasks (running + recently completed).
    #[rpc(tx = oneshot::Sender<ListTasksResponse>)]
    ListTasks(ListTasks),

    /// Get the node's current configuration as JSON.
    #[rpc(tx = oneshot::Sender<GetConfigResponse>)]
    GetConfig(GetConfig),

    /// Apply a JSON Patch (RFC 6902). On success returns the patched
    /// config serialised as a JSON string.
    #[rpc(tx = oneshot::Sender<Result<String, String>>)]
    PatchConfig(PatchConfig),

    /// Get a high-level node status summary.
    #[rpc(tx = oneshot::Sender<GetStatusResponse>)]
    GetStatus(GetStatus),

    /// Walk the node's health signals: per-store reachability + staging
    /// gauges + configured schedules. Powers `vup doctor` and the
    /// `vup status` durability gauges. Always succeeds (a per-store probe
    /// failure is reported inside the response, not as an RPC error).
    #[rpc(tx = oneshot::Sender<GetHealthResponse>)]
    GetHealth(GetHealth),

    /// List vault snapshots.
    #[rpc(tx = oneshot::Sender<ListSnapshotsResponse>)]
    ListSnapshots(ListSnapshots),

    /// List a vault's tree contents (depth-bounded), optionally at a past
    /// snapshot and/or scoped to a subtree. Powers `vup list vault:[path]`.
    #[rpc(tx = oneshot::Sender<Result<ListTreeResponse, String>>)]
    ListTree(ListTree),

    /// Mount a vault as a FUSE filesystem on the daemon. Returns the
    /// mount handle the CLI uses to drive `UnmountVault`.
    #[rpc(tx = oneshot::Sender<Result<MountedVault, String>>)]
    MountVault(MountVault),

    /// Drop a previously-issued mount handle, which unmounts the FUSE
    /// session.
    #[rpc(tx = oneshot::Sender<Result<(), String>>)]
    UnmountVault(UnmountVault),

    /// Build a frozen-anonymous share URL for the current snapshot of
    /// a vault.
    #[rpc(tx = oneshot::Sender<Result<ExportedShare, String>>)]
    ExportVault(ExportVault),

    /// Sender-side: ask the daemon to mint a one-time pair token and
    /// hold a slot for redemption. Server-streaming. First event:
    /// `Minted { token }`. Then either `Redeemed { peer_did }` (after
    /// a peer presents the secret over `s5/pair/0`) or `Failed { error }`,
    /// followed by stream close. Replaces the prior `StartPair` +
    /// `WaitPair` two-RPC handshake — the secret_id plumbing went away
    /// because there is no longer anything to correlate across calls.
    #[rpc(tx = mpsc::Sender<PairEvent>)]
    Pair(Pair),

    /// Receiver-side: parse a token, dial the peer's iroh endpoint
    /// over `s5/pair/0`, present the secret, and return the peer's
    /// DID on success.
    #[rpc(tx = oneshot::Sender<Result<String, String>>)]
    RedeemPair(RedeemPair),

    /// Inviter-side device enrollment (D10): mint a one-time
    /// `vupd-…` token and hold a slot for redemption over
    /// `s5/enroll/0`. Server-streaming, same shape as `Pair`: first
    /// event `Minted { token }`, then `Admitted { … }` (after the
    /// listener admitted the joiner's keys, wrote its catalogue
    /// entry, and re-wrapped the special vaults) or `Failed`.
    #[rpc(tx = mpsc::Sender<DeviceInviteEvent>)]
    DeviceInvite(DeviceInvite),

    /// Read the device catalogue (`identity_secrets/devices`):
    /// label → the device's four pubkeys + age recipient. Labels are
    /// UI-only — never an authorization input.
    #[rpc(tx = oneshot::Sender<Result<ListDevicesResponse, String>>)]
    ListDevices(ListDevices),

    /// Revoke one of this identity's devices by catalogue label (D18):
    /// drop its four keys from the identity bundle, remove the
    /// catalogue entry, and re-wrap the special vaults to the surviving
    /// recipient set (+ paper). Routine `identity-rotation.md` §6.1
    /// removal ONLY — the compromised-case follow-ups (§6.2 warm
    /// rotation, store-credential rotation) are the CLI's checklist,
    /// not this RPC. Refuses to revoke the daemon's own device
    /// (self-revoke = wedge).
    #[rpc(tx = oneshot::Sender<Result<RevokeDeviceResponse, String>>)]
    RevokeDevice(RevokeDevice),

    // `AddFriend` and `GrantVault` are deliberate semantic shortcuts
    // over `PatchConfig` — both could be expressed as JSON patches
    // over `friend.*` / `vault.<>.members` and `PatchConfig` already
    // fires `membership_refresh`. They exist as dedicated verbs for
    // (a) better error messages ("petname collides with different
    // DID"), (b) atomicity in a single read-modify-write, and (c) a
    // place to hang capability flags later (read/write split, etc.).
    // Don't justify them as wire-format necessities — if a future
    // pass adds typed config-edit RPCs across the board, these can
    // either lead the migration or fold into it.
    /// Persist a `[friend.<petname>]` config entry naming the
    /// supplied DID. Idempotent on identical pairings; refuses
    /// petname collisions with a different DID.
    #[rpc(tx = oneshot::Sender<Result<(), String>>)]
    AddFriend(AddFriend),

    /// Append an existing `@petname` (must already be in
    /// `[friend.*]`) to `vault.<vault>.members`, persist, and fire
    /// a membership refresh.
    #[rpc(tx = oneshot::Sender<Result<(), String>>)]
    GrantVault(GrantVault),

    /// Consume an `s5://export/…` share URL: fetch the frozen encrypted
    /// Transparent Node from a configured store, decrypt it with the
    /// fragment secret, materialise a read-only local vault, and add a
    /// `[vault.<label>]` entry. Returns the resulting vault label.
    #[rpc(tx = oneshot::Sender<Result<String, String>>)]
    JoinExport(JoinExport),

    /// Snapshot of per-peer connection observation. Powers
    /// `vup debug peers` — what iroh pubkeys we've seen, on which
    /// ALPNs, when, which side. Always succeeds.
    #[rpc(tx = oneshot::Sender<DebugPeersResponse>)]
    DebugPeers(DebugPeers),

    /// Graceful shutdown.
    #[rpc(tx = oneshot::Sender<()>)]
    Shutdown(Shutdown),
}

// ── Request types ──────────────────────────────────────────────────

/// Run a task. Either by name (looked up in config) or with an inline spec.
#[derive(Debug, Serialize, Deserialize)]
pub struct RunTask {
    /// Task name from `[task.*]` in config. Mutually exclusive with `spec_json`.
    pub name: Option<String>,
    /// Inline task spec as JSON string (postcard can't handle internally-tagged enums).
    /// Mutually exclusive with `name`.
    pub spec_json: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WatchTaskStatus {
    pub task_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelTask {
    pub task_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListTasks;

#[derive(Debug, Serialize, Deserialize)]
pub struct Shutdown;

// ── Response types ─────────────────────────────────────────────────

/// Success payload of a `RunTask` RPC. Wrapped in `Result<_, String>`
/// on the wire — refusals (vault not in config, invalid spec, executor
/// full, …) come through as `Err`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnedTask {
    /// Unique task ID for status/cancel.
    pub task_id: u64,
    /// The resolved task spec as JSON string.
    pub spec_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatusResponse {
    pub task_id: u64,
    pub state: TaskState,
    /// Task progress states. Keys are state names (e.g. "bytes", "files_added").
    pub progress: Option<TaskProgressMap>,
}

/// Current state of a task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskState {
    /// Task is queued but not yet started.
    Pending,
    /// Task is actively running.
    Running,
    /// Task completed successfully.
    Completed,
    /// Task failed with an error message.
    Failed { error: String },
    /// Task was cancelled.
    Cancelled,
}

/// Type of a progress metric.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressType {
    Count,
    Bytes,
}

/// A single progress metric for a task.
///
/// Each metric has a label (used as map key), a type (count or bytes),
/// an optional total (None for counters like "skipped" that have no natural bound),
/// a current progress value, and a complete flag.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ProgressState {
    /// Machine-readable key (e.g., "bytes", "files_added", "files_skipped").
    pub label: String,
    /// Human-readable display label (e.g., "bytes uploaded", "files added").
    /// Falls back to `label` if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_label: Option<String>,
    /// Whether this metric is in bytes or a plain count.
    #[serde(rename = "type")]
    pub progress_type: ProgressType,
    /// Total expected value, or None if no natural bound.
    pub total: Option<u64>,
    /// Current value.
    pub progress: u64,
    /// Whether this metric has reached a terminal state.
    pub complete: bool,
}

impl ProgressState {
    pub fn new(label: String, progress_type: ProgressType, total: Option<u64>) -> Self {
        Self {
            label,
            display_label: None,
            progress_type,
            total,
            progress: 0,
            complete: false,
        }
    }

    pub fn add(&mut self, n: u64) {
        self.progress += n;
        if let Some(total) = self.total {
            self.complete = self.progress >= total;
        }
    }

    /// Set a human-readable display label.
    pub fn set_display_label(&mut self, display_label: &str) {
        self.display_label = Some(display_label.into());
    }

    /// Return the display label, falling back to the key label.
    pub fn display_label(&self) -> &str {
        self.display_label.as_deref().unwrap_or(&self.label)
    }
}

/// An ordered list of progress states (insertion order = display order).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskProgressMap(pub Vec<ProgressState>);

impl TaskProgressMap {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Add a bytes progress state.
    pub fn bytes(&mut self, label: &str, progress: u64, total: Option<u64>) -> &mut ProgressState {
        self.0.push(ProgressState {
            label: label.into(),
            display_label: None,
            progress_type: ProgressType::Bytes,
            total,
            progress,
            complete: total.is_some_and(|t| progress >= t),
        });
        self.0.last_mut().unwrap()
    }

    /// Add a count progress state.
    pub fn count(&mut self, label: &str, progress: u64, total: Option<u64>) -> &mut ProgressState {
        self.0.push(ProgressState {
            label: label.into(),
            display_label: None,
            progress_type: ProgressType::Count,
            total,
            progress,
            complete: total.is_some_and(|t| progress >= t),
        });
        self.0.last_mut().unwrap()
    }

    /// Get a state by label.
    pub fn get(&self, label: &str) -> Option<&ProgressState> {
        self.0.iter().find(|s| s.label == label)
    }

    /// Get a mutable state by label.
    pub fn get_mut(&mut self, label: &str) -> Option<&mut ProgressState> {
        self.0.iter_mut().find(|s| s.label == label)
    }

    /// Iterate over states.
    pub fn iter(&self) -> impl Iterator<Item = &ProgressState> {
        self.0.iter()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListTasksResponse {
    pub tasks: Vec<TaskStatusResponse>,
}

// ── Config management ─────────────────────────────────────────────

/// Request the node's current configuration as a JSON value.
#[derive(Debug, Serialize, Deserialize)]
pub struct GetConfig;

#[derive(Debug, Serialize, Deserialize)]
pub struct GetConfigResponse {
    /// The full config as a JSON string (serialized from S5NodeConfig).
    /// Postcard cannot serialize serde_json::Value, so we use a String.
    pub config_json: String,
}

/// Apply an RFC 6902 JSON Patch to the node's running configuration.
///
/// The node will:
/// 1. Serialize current config → JSON
/// 2. Apply the patch operations
/// 3. Deserialize back to S5NodeConfig
/// 4. Validate cross-references
/// 5. Persist to TOML on disk
/// 6. Hot-reload the in-memory config
#[derive(Debug, Serialize, Deserialize)]
pub struct PatchConfig {
    /// RFC 6902 JSON Patch operations as a JSON string.
    /// Postcard cannot serialize serde_json::Value, so we use a String.
    pub patch_json: String,
}

// ── Node status ───────────────────────────────────────────────────

/// Request a high-level node status summary.
#[derive(Debug, Serialize, Deserialize)]
pub struct GetStatus;

#[derive(Debug, Serialize, Deserialize)]
pub struct GetStatusResponse {
    /// Number of configured stores.
    pub store_count: usize,
    /// Number of configured vaults.
    pub vault_count: usize,
    /// Number of configured sources.
    pub source_count: usize,
    /// Currently running tasks.
    pub running_tasks: usize,
    /// Endpoint ID of the node.
    pub endpoint_id: String,
    /// Live automations (`[task.*]` with a non-`Manual` trigger) the daemon's
    /// reconciler is currently running, with per-loop health. Paused
    /// automations are NOT here (they aren't spawned) — the CLI merges this
    /// liveness with the config for the full list. Empty on a daemon with no
    /// automation manager wired (test harnesses).
    #[serde(default)]
    pub automations: Vec<AutomationStatus>,
    /// The most recent one-shot (manual) `Backup` the daemon ran, if any.
    /// The bare `automate` wizard reads it to offer "keep doing that?".
    #[serde(default)]
    pub last_backup: Option<LastBackup>,
}

/// Per-automation liveness surfaced by `GetStatus` (Stage 7). One entry per
/// running reconciled automation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationStatus {
    /// The `[task.<name>]` key.
    pub name: String,
    /// The vault the automation backs up (from its `Backup` spec).
    pub vault: String,
    /// Watch or Every (never Manual — those aren't automations).
    pub trigger: crate::config::TaskTrigger,
    /// Always `false` here (paused automations aren't spawned); present so
    /// the CLI can render a uniform table.
    pub paused: bool,
    /// The supervised loop is currently established and healthy.
    pub alive: bool,
    /// How many times the supervisor has restarted the loop after a failure.
    pub restarts: u64,
    /// Unix seconds of the last successful backup dispatch, if any.
    pub last_ok_unix: Option<u64>,
    /// The most recent loop failure, if any (kept until the next).
    pub last_error: Option<String>,
}

/// The `(vault, source)` of the most recent manual `Backup` — the seed the
/// `automate` wizard promotes into a live automation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastBackup {
    /// Vault that was backed up.
    pub vault: String,
    /// Source that was backed up.
    pub source: String,
}

/// Request a per-store health + staging walk. Powers `vup doctor` and the
/// `vup status` durability gauges.
#[derive(Debug, Serialize, Deserialize)]
pub struct GetHealth;

#[derive(Debug, Serialize, Deserialize)]
pub struct GetHealthResponse {
    /// One entry per configured `[store.*]`, in config (name) order.
    pub stores: Vec<StoreHealth>,
    /// Configured scheduled backups (`snap_interval_secs`), one per vault
    /// that sets one. Empty when nothing is scheduled.
    pub schedules: Vec<ScheduledRun>,
}

/// Health of one configured store: whether it answered a reachability probe
/// and, when it buffers writes, how much is staged-but-not-durable.
#[derive(Debug, Serialize, Deserialize)]
pub struct StoreHealth {
    /// The `[store.<name>]` this describes.
    pub name: String,
    /// The store answered a `blob_contains` probe. `false` + `error` when the
    /// probe failed (backend unreachable, or the store isn't resolved).
    pub reachable: bool,
    /// Probe-failure detail; `None` when `reachable`.
    pub error: Option<String>,
    /// Staging gauges when the backend buffers writes (a packing store);
    /// `None` for a direct store whose writes are durable on return.
    pub staging: Option<StagingGauges>,
}

/// Wire mirror of `s5_core::blob::StagingStats`: staged-but-not-durable bytes
/// and how stale the last successful flush is.
#[derive(Debug, Serialize, Deserialize)]
pub struct StagingGauges {
    /// Bytes in the staging WAL, not yet inside a durable pack.
    pub staged_bytes: u64,
    /// Seconds since the last pack flush completed (or since store open).
    pub since_last_flush_secs: u64,
    /// A pack upload is currently in flight.
    pub inflight: bool,
}

/// A configured scheduled backup surfaced to `vup status`.
#[derive(Debug, Serialize, Deserialize)]
pub struct ScheduledRun {
    /// The vault the schedule snaps.
    pub vault: String,
    /// The configured cadence, in seconds (`snap_interval_secs`).
    pub interval_secs: u64,
}

// ── Snapshots ─────────────────────────────────────────────────────

/// List snapshots for a vault.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListSnapshots {
    /// Vault name. If None, list snapshots from all vaults.
    pub vault: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListSnapshotsResponse {
    pub snapshots: Vec<SnapshotInfo>,
}

/// Summary information about a single vault snapshot.
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotInfo {
    /// Vault this snapshot belongs to.
    pub vault: String,
    /// Snapshot hash/ID (hex-encoded).
    pub hash: String,
    /// ISO 8601 UTC timestamp (e.g. `2025-06-15T12:34:56Z`), or `"current"` for
    /// the latest published snapshot.
    pub timestamp: String,
    /// Total number of files in the snapshot (if known).
    pub file_count: Option<u64>,
    /// Total size in bytes (if known).
    pub total_bytes: Option<u64>,
}

// ── Tree listing ──────────────────────────────────────────────────

/// List a vault's tree contents (`vup list vault:[path][#snap]`).
#[derive(Debug, Serialize, Deserialize)]
pub struct ListTree {
    /// Vault name to list.
    pub vault: String,
    /// Optional `#snap` selector (revision number / timestamp / hash
    /// prefix). `None` lists the current snapshot.
    pub snapshot: Option<String>,
    /// Optional subtree path prefix. Only entries under it are returned,
    /// re-rooted so the prefix itself is stripped from displayed paths.
    /// `None` lists from the vault root.
    pub subtree: Option<String>,
    /// Max tree depth to descend, relative to the listing root (1 = the
    /// immediate children only). `None` = unbounded (the whole subtree).
    pub max_depth: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListTreeResponse {
    /// Entries in prolly-tree key order (parents precede their children).
    pub entries: Vec<TreeEntry>,
}

/// One entry in a vault's tree listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeEntry {
    /// Path relative to the listing root, using `/` separators. No
    /// trailing slash on directories (see `is_dir`).
    pub path: String,
    /// True for directories, false for files / symlinks.
    pub is_dir: bool,
    /// Plaintext content size in bytes. `0` for directories and
    /// metadata-only entries.
    pub size: u64,
}

// ── Mount ─────────────────────────────────────────────────────────

/// Mount a vault on the daemon as a FUSE filesystem. The daemon owns
/// the `MountHandle`; the CLI receives a `mount_id` to drive
/// `UnmountVault` (or any other future mount-management RPC).
///
/// `mountpoint` must be an absolute path the daemon process can see
/// and `mkdir`-create. With `rw = true`, writes accumulate in the
/// daemon's in-memory overlay and a debounced flush (idle window
/// `debounce_ms`) folds bursts into a fresh snapshot, persists the
/// new vault root, and dispatches a `Publish` task — same flow as a
/// CLI-driven snap, but without round-tripping through RPC.
#[derive(Debug, Serialize, Deserialize)]
pub struct MountVault {
    pub vault: String,
    pub mountpoint: std::path::PathBuf,
    pub rw: bool,
    pub debounce_ms: u64,
}

/// Success payload of a `MountVault` RPC. Wrapped in `Result<_, String>`
/// on the wire — preflight failures (vault not in config, store type
/// unsupported, mount point taken, …) come through as `Err`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountedVault {
    /// Handle the CLI passes back on `UnmountVault`.
    pub mount_id: u64,
}

/// Drop the daemon-side mount handle for `mount_id`. The
/// `MountHandle` drop performs the actual FUSE unmount — both the
/// kernel mount and any rw-mode debounce loop attached to it go
/// away. Idempotent at the wire level: unmounting an unknown
/// `mount_id` returns an `Err` but doesn't disturb other mounts.
#[derive(Debug, Serialize, Deserialize)]
pub struct UnmountVault {
    pub mount_id: u64,
}

// ── Share / export ────────────────────────────────────────────────

/// Build a frozen-anonymous share URL for the named vault. See
/// `docs/reference/share-links.md` for the URL grammar.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportVault {
    /// Vault name to export.
    pub vault: String,
    /// Optional sub-path; whole-vault export only today, so this must
    /// be `None` until sub-tree export lands.
    pub path: Option<String>,
}

/// Success payload of an `ExportVault` RPC. Wrapped in
/// `Result<_, String>` on the wire — vault-missing / sub-tree-not-yet-
/// supported errors come through as `Err`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportedShare {
    /// The frozen-anonymous share URL.
    pub url: String,
    /// Hex-encoded CAS hash of the encrypted Transparent Node.
    pub blob_hash_hex: String,
}

// ── Pair ──────────────────────────────────────────────────────────

/// Sender-side: open a server-streaming pair session. The daemon
/// mints a one-time token immediately (first event = `Minted`) and
/// then awaits redemption over `s5/pair/0`. On redemption it sends
/// `Redeemed` with the receiver's DID; on cancellation/timeout/
/// daemon-restart it sends `Failed`. After either, the stream
/// closes.
#[derive(Debug, Serialize, Deserialize)]
pub struct Pair;

#[derive(Debug, Serialize, Deserialize)]
pub enum PairEvent {
    /// Token freshly minted; share with the peer.
    Minted { token: String },
    /// A peer presented the secret; pairing succeeded. Receiver-side
    /// CLI now interactively prompts for a petname and calls
    /// `AddFriend(petname, peer_did)`.
    Redeemed { peer_did: String },
    /// Pair attempt aborted before redemption (bad mint, daemon
    /// restart, etc.). Stream closes after this event.
    Failed { error: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RedeemPair {
    /// `vup1-…` encoded token from the sender side.
    pub token: String,
}

// ── Device enrollment (D10) ───────────────────────────────────────

/// Mint a one-time device-enroll token and await its redemption.
#[derive(Debug, Serialize, Deserialize)]
pub struct DeviceInvite {
    /// Catalogue label for the new device (UI petname only — NEVER an
    /// authorization input). `None` → auto-generated from the joiner's
    /// signing pubkey; collisions are uniquified daemon-side.
    pub label: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DeviceInviteEvent {
    /// Token freshly minted; type into `vup device join <token>` on the
    /// new device.
    Minted { token: String },
    /// The joiner presented the secret and the full device-add ran:
    /// bundle admission + catalogue entry + special-vault re-wrap.
    Admitted {
        /// Catalogue label the device landed under.
        label: String,
        /// Hex of the admitted device-signing pubkey (display only).
        signing_hex: String,
    },
    /// Enrollment aborted (bad mint, listener failure, daemon restart).
    /// Stream closes after this event; mint a fresh invite to retry.
    Failed { error: String },
}

/// Read the device catalogue.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListDevices;

#[derive(Debug, Serialize, Deserialize)]
pub struct ListDevicesResponse {
    pub devices: Vec<DeviceEntry>,
}

/// One catalogue record (public keys only; hex-encoded for display).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceEntry {
    /// UI petname (never an authorization input).
    pub label: String,
    pub signing_hex: String,
    pub acl_hex: String,
    pub iroh_hex: String,
    pub age_recipient: String,
}

/// Revoke a device by catalogue label.
#[derive(Debug, Serialize, Deserialize)]
pub struct RevokeDevice {
    /// Catalogue label of the device to revoke. The daemon resolves it
    /// to the key 4-tuple — the label itself never authorizes anything.
    pub label: String,
}

/// Success payload of a `RevokeDevice` RPC (routine removal done).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevokeDeviceResponse {
    /// Hex of the revoked device-signing pubkey (display only).
    pub signing_hex: String,
    /// Identity-bundle revision now current after the removal.
    pub bundle_revision: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddFriend {
    pub petname: String,
    pub did: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JoinExport {
    /// An `s5://export/<label>?m=<hex(hash)>#<age-secret>` URL.
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GrantVault {
    pub vault: String,
    /// Nickname previously recorded via `AddFriend`. Must already
    /// exist in `[friend.*]`.
    pub petname: String,
    /// Grant **write** capability (D11): the member joins `writers` in
    /// addition to `members`, so its `signers[]` are accepted as vault
    /// registry writers. `false` = read-only (the default): the member can
    /// connect, fetch, and decrypt, but its writes are rejected.
    #[serde(default)]
    pub write: bool,
}

// ── Debug ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct DebugPeers;

#[derive(Debug, Serialize, Deserialize)]
pub struct DebugPeersResponse {
    pub peers: Vec<DebugPeer>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DebugPeer {
    /// Hex-encoded ed25519 pubkey (== iroh `EndpointId` ==
    /// `did:s5:b…` master pubkey today).
    pub pubkey_hex: String,
    /// Per-ALPN observation history.
    pub alpns: Vec<DebugPeerAlpn>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DebugPeerAlpn {
    pub alpn: String,
    pub handshakes: u64,
    pub first_seen_unix: u64,
    pub last_seen_unix: u64,
    pub last_was_incoming: bool,
}
