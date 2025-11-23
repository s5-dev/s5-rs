# S5 – A Modular Content‑Addressed Storage Network on Iroh

S5 is a modular, high‑performance content‑addressed storage (CAS) network built on top of [Iroh](https://iroh.computer).

It gives you:

- A small, well‑factored Rust API for **content addressing, blob transport, and mutable registries**.
- **FS5**, an encrypted, content‑addressed filesystem with snapshot semantics.
- A **node + CLI** that wires everything together with pluggable **S5 store backends** (local FS, S3, Sia, in‑memory).

If you are familiar with IPFS or Iroh: think of S5 as a focused, Rust‑native toolkit for building distributed storage and sync flows, with strong separation between **wire‑stable protocol types** and **high‑level ergonomics**.

---

## Table of Contents

- [Core Ideas](#core-ideas)
- [Architecture Overview](#architecture-overview)
- [Workspace & Crate Map](#workspace--crate-map)
  - [Core protocol crate: `s5_core`](#core-protocol-crate-s5_core)
  - [Blob transport: `s5_blobs`](#blob-transport-s5_blobs)
  - [Filesystem layer: `s5_fs` (FS5)](#filesystem-layer-s5_fs-fs5)
  - [Registry transport: `s5_registry`](#registry-transport-s5_registry)
  - [Node & sync orchestration: `s5_node`](#node--sync-orchestration-s5_node)
  - [CLI: `s5_cli`](#cli-s5_cli)
  - [Storage backends: `blob_stores/*`](#storage-backends-blob_stores)
  - [Importers & tools](#importers--tools)
  - [Experimental / auxiliary crates](#experimental--auxiliary-crates)
- [Key Data Structures & Concepts](#key-data-structures--concepts)
  - [Content hashes & blob identifiers](#content-hashes--blob-identifiers)
  - [Blob locations & storage abstraction](#blob-locations--storage-abstraction)
  - [Streams & Registry](#streams--registry)
  - [Pins & pinning policy](#pins--pinning-policy)
  - [FS5 internals](#fs5-internals)
- [Typical Workflows](#typical-workflows)
  - [Running a node](#running-a-node)
  - [Importing data](#importing-data)
  - [File sync via encrypted FS5 snapshots](#file-sync-via-encrypted-fs5-snapshots)
- [How S5 Compares](#how-s5-compares)
  - [vs. IPFS](#vs-ipfs)
  - [vs. Iroh](#vs-iroh)
  - [vs. "just S3" or a plain object store](#vs-just-s3-or-a-plain-object-store)
- [Where to Start Reading the Code](#where-to-start-reading-the-code)
- [License](#license)

---

## Core Ideas

- **Everything is a blob**, addressed by a BLAKE3 hash (`Hash`, `BlobId`).
- **Metadata and higher‑level structures** (directories, registries, pins) are built on top of those blobs and share the same wire‑stable, CBOR‑encoded primitives.
- **Mutability is handled via streams/registries** (append‑only logs of signed messages) that point at immutable content.
- **FS5** exposes a content‑addressed directory tree with snapshot semantics; each snapshot is a single CBOR structure (`DirV1`).
- **Storage is pluggable**: the `Store` trait abstracts local FS, S3, Sia renterd, in‑memory, or your own backend. A `BlobStore` façade sits on top.
- **Networking is delegated to Iroh**: S5 defines **two small protocols** on top of Iroh (`s5_blobs` and `s5_registry`) rather than reinventing transport.


## Architecture Overview

At a high level:

- `s5_core` defines the **protocol types**, traits, and wire formats.
- S5 store backends in `blob_stores/*` implement the `Store` trait for concrete backends.
- `s5_blobs` exposes those stores over the network via Iroh.
- `s5_registry` exposes the registry over the network via Iroh.
- `s5_fs` (FS5) provides a directory‑tree abstraction on top of blobs and a registry.
- `s5_node` wires together config, stores, FS5 roots, registry, and Iroh endpoints into a runnable node with optional file sync.
- `s5_cli` is a thin CLI around `s5_node`, FS5, and the importers.

Typical data flow:

1. **Ingest**: an importer (local filesystem or HTTP) writes file contents into a `BlobStore` and stores `FileRef`s in an FS5 directory snapshot.
2. **Addressing**: the directory snapshot and blobs are addressed by BLAKE3 hashes (`Hash` / `BlobId`).
3. **Publication**: a registry stream entry points to the latest snapshot hash.
4. **Networking**: other nodes fetch blobs via `s5_blobs` and the registry state via `s5_registry` over Iroh.


## Workspace & Crate Map

This repository is a Cargo workspace (see `Cargo.toml`) with the following members:

```toml
[workspace]
members = [
  "blob_stores/local",
  "blob_stores/memory",
  "blob_stores/s3",
  "blob_stores/sia",
  "importers/local",
  "s5_blobs",
  "s5_cli",
  "s5_core",
  "s5_fs",
  "s5_node",
  "s5_registry",
  "importers/http_importer",
]
```

### Core protocol crate: `s5_core`

File: `s5_core/src/lib.rs`

Defines the **core S5 protocol surface** shared by all crates.

Modules:

- `hash` – BLAKE3‑based `Hash` type.
- `blob::identifier` – `BlobId`, the canonical blob identifier.
- `blob::location` – `BlobLocation` and related types (how to fetch content).
- `blob::store` – `BlobStore` façade on top of a `Store` impl.
- `store` – `Store`, `StoreFeatures`, `StoreResult` (pluggable storage backends).
- `stream::types` – `StreamKey`, `StreamMessage`, `MessageType`, `PublicKeyEd25519`.
- `stream::registry` – `RedbRegistry` (local registry implementation).
- `pins` – `Pins`, `PinContext`, `RegistryPinner`.
- `cbor` – token‑level CBOR utilities (`cbor::Value`, etc.).
- `bao` – helpers for BLAKE3/bao outboard data (streaming verification).

Key exports:

- Content addressing: `Hash`, `BlobId`.
- Locations: `BlobLocation`.
- Storage: `Store`, `StoreFeatures`, `StoreResult`, `BlobStore`, plus blob read/write traits `BlobsRead`, `BlobsWrite`.
- Registry: `RegistryApi`, `StreamKey`, `StreamMessage`, `MessageType`, `RedbRegistry`.
- Pinning: `Pins`, `PinContext`, `RegistryPinner`.

**Design note:** Types in `hash`, `blob::{identifier,location}`, and `stream::types` are treated as **wire‑stable** for the 1.0 protocol. Higher‑level helpers may evolve more freely.


### Blob transport: `s5_blobs`

File: `s5_blobs/src/lib.rs`

Iroh‑based protocol for fetching and serving blobs.

Exports:

- `Client` – high‑level RPC client implementing `s5_core::BlobsRead` and `BlobsWrite`.
- `BlobsServer` – server‑side handler exposing named `BlobStore`s over an Iroh `Endpoint`.
- `RemoteBlobStore` – remote `Store` implementation backed by `Client` (usable with `BlobStore`).
- `PeerConfigBlobs` – ACL/config for which stores a peer may access.
- `ALPN` – protocol identifier for negotiating the blobs protocol over Iroh.

When you run an S5 node, it instantiates a `BlobsServer` and registers it with Iroh’s `Router`. Remote nodes construct a `Client` / `RemoteBlobStore` to speak to it.


### Filesystem layer: `s5_fs` (FS5)

Files:

- `s5_fs/src/lib.rs` – crate root and public surface.
- `s5_fs/src/dir.rs` – `DirV1` snapshot structure and `FileRef` representation.
- `s5_fs/src/actor.rs` – single‑writer directory actor.
- `s5_fs/src/api.rs` – user‑facing FS5 API.
- `s5_fs/src/context.rs` – wiring: blob store, registry, encryption keys, signing keys.

Public API highlights:

- `FS5` – ergonomic façade for interacting with a directory tree.
- `DirContext` – binds an FS5 root to storage (local files, registry, encryption keys).
- `DirContextParentLink` – how directories link to parents / registries.
- `SigningKey` – Ed25519 signing key for registry‑backed dirs.
- `FileRef` – metadata + blob pointer for a file entry.
- `CursorKind` – used for paginated listings.
- `FSResult<T>` – `anyhow::Result<T>` alias.

Conceptually, FS5 has four layers (documented in `lib.rs`):

1. `dir` – pure CBOR‑encoded data structures (`DirV1`, `DirHeader`, `FileRef`, etc.).
2. `actor` – single‑threaded state machine owning one directory snapshot; processes commands sequentially.
3. `api` – high‑level async API (`FS5`) for applications.
4. `context` – wiring to blob store and registry, plus encryption and signing context.

Features:

- Content‑addressed metadata snapshots: each directory state is a CBOR `DirV1` tree.
- Optional **XChaCha20‑Poly1305 encryption** for directories.
- Registry‑backed directories using Ed25519 keys.
- Cursor‑based listing for large directories.
- Planned sharding support (see header fields in `DirHeader`).

See `s5_fs/README.md` for a quick start example.


### Registry transport: `s5_registry`

Files:

- `s5_registry/src/lib.rs` – entire crate.

Functionality:

- Defines an Iroh RPC protocol (`RpcProto`) for registry operations.
- Provides server/client/adapters around `s5_core::RegistryApi`:
  - `RegistryServer` – serves a local `RedbRegistry` over Iroh.
  - `Client` – RPC client.
  - `RemoteRegistry` – implements `RegistryApi` on top of `Client`.
- `ALPN` – `b"s5/registry/0"`, the registry protocol identifier.

This is the building block used by `s5_node` for syncing registry state across peers.


### Node & sync orchestration: `s5_node`

Files:

- `s5_node/src/lib.rs` – main node type and orchestration.
- `s5_node/src/config.rs` – TOML‑serializable node config types.
- `s5_node/src/sync.rs` – filesystem sync logic using FS5 + registry + blobs.

Key types:

- `S5Node` – running node instance:
  - `config: S5NodeConfig` – deserialized from TOML.
  - `stores: HashMap<String, BlobStore>` – named storage backends.
  - `registry: Option<RedbRegistry>` – local registry DB.
  - `endpoint: iroh::Endpoint` – Iroh endpoint.
  - `router: iroh::protocol::Router` – protocol router.
- `S5NodeConfig` (in `config.rs`):
  - `identity: NodeConfigIdentity` – secret key (inline or file‑based).
  - `store: BTreeMap<String, NodeConfigStore>` – named stores (Sia, local, S3, memory).
  - `peer: BTreeMap<String, NodeConfigPeer>` – known peers + blobs ACL config.
  - `sync: BTreeMap<String, NodeConfigSync>` – named file sync jobs.
  - `registry_path: Option<String>` – optional override for registry DB location.

Config enums:

- `NodeConfigStore` – `SiaRenterd`, `Local`, `S3`, `Memory`.
- `NodeConfigPeer` – `id` (EndpointId string) + `blobs: PeerConfigBlobs`.
- `NodeConfigSync` – `local_path`, `via_untrusted` (peer route), `shared_secret`, optional `interval_secs`.

Functions:

- `S5Node::new` – constructs a node from config and optional registry.
- `S5Node::run_file_sync` – runs one‑shot or continuous FS5 snapshot syncs based on `sync.*` config entries.
- `S5Node::shutdown` – stops router.
- `create_store` – helper to instantiate a `BlobStore` from a `NodeConfigStore`.
- `run_node` – entrypoint used by the CLI: opens the registry, creates `S5Node`, logs endpoint info, runs file sync, waits for `Ctrl+C`.

The sync logic (see `sync.rs`) uses the following pattern:

- Derive symmetric keys from `shared_secret` (`SyncKeys`, `derive_sync_keys`).
- Open a **plaintext** FS5 root on the local filesystem.
- Open a **remote encrypted** FS5 root backed by Iroh blobs + remote registry.
- Repeatedly `push_snapshot` (local → remote) and `pull_snapshot` (remote → local).


### CLI: `s5_cli`

Files:

- `s5_cli/src/main.rs` – CLI entrypoint.
- `s5_cli/src/init_config.rs` – helpers for initializing/patching node config.

Key pieces:

- Uses `clap` to provide a small set of commands:
  - `s5 config …` – initialize or edit node configs (see `CmdConfig`).
  - `s5 import http` – import from HTTP using `HttpImporter`.
  - `s5 import local` – import from local filesystem using `LocalFileSystemImporter`.
  - `s5 start` – start a node using the configured stores/peers/syncs.
- Config placement uses `directories::ProjectDirs` under an OS‑specific base, e.g. usually something like:
  - Config: `~/.config/s5/S5/nodes/<name>.toml`.
  - Data: `~/.local/share/s5/S5/...` (FS5 roots, temp state, etc.).
- For imports, the CLI opens an FS5 root (`DirContext::open_local_root`) and passes it along with a target `BlobStore` (`s5_node::create_store`) to the importer.


### S5 store backends: `blob_stores/*`

All S5 store backends implement `s5_core::store::Store` and are typically wrapped in `BlobStore` for higher‑level use.

#### `blob_stores/local` – `LocalStore`

File: `blob_stores/local/src/lib.rs`

- Stores blobs on a local filesystem under a `base_path`.
- Respects `StoreFeatures` (case sensitivity, directory size hints, `supports_rename`).
- Ensures paths are relative and safe via `resolve_path`.
- Implements all core `Store` methods (`put_stream`, `exists`, `put_bytes`, `open_read_*`, `size`, `list`, `delete`, `rename`, `provide`).
- Helper: `LocalStore::to_blob_store()` to get a `BlobStore` directly.

#### `blob_stores/memory` – `MemoryStore`

File: `blob_stores/memory/src/lib.rs`

- Simple in‑memory `Store` backed by a `DashMap<String, Bytes>`.
- Useful for tests, ephemeral caching, or prototyping.
- Implements the full `Store` trait; `provide` returns empty `BlobLocation` lists (no network exposure by itself).

#### `blob_stores/s3` – `S3Store`

File: `blob_stores/s3/src/lib.rs`

- S3‑compatible object store backend using the `s3` crate.
- Config: `S3StoreConfig { endpoint, region, bucket_name, access_key, secret_key }`.
- `provide` returns `BlobLocation::Url` with a presigned GET URL.
- `supports_rename` is `false` (S3 has no native rename; users should copy/delete at a higher layer).
- Some methods (`size`, `list`) are `todo!()` and may evolve.

#### `blob_stores/sia` – `SiaStore`

Files:

- `blob_stores/sia/src/config.rs` – configuration for Sia renterd.
- `blob_stores/sia/src/store.rs` – `SiaStore` backend implementation.
- `blob_stores/sia/src/lib.rs` – crate root, error types.

Highlights:

- Integrates with the Sia network via `renterd`.
- Enforces certain renterd settings (no erasure coding/packing yet) via a domain‑specific `Error` enum.
- Intended for decentralized, off‑machine storage with S5’s addressing and registry on top.


### Importers & tools

#### `importers/local` – `LocalFileSystemImporter`

File: `importers/local/src/lib.rs`

- Batches import from the local filesystem into an FS5 root + blob store.
- Structure:
  - Holds `FS5`, a `BlobStore`, and a concurrency limit.
  - `import_path(PathBuf)` walks the directory tree using `walkdir` and processes files concurrently.
  - `process_entry` compares size + mtime of each file against existing `FileRef` entries in FS5 and only re‑imports on change.
  - Uses `BlobStore::import_file` to store file contents and then writes a `FileRef` into FS5 (`file_put`).

#### `importers/http_importer` – `HttpImporter`

File: `importers/http_importer/src/lib.rs`

- Recursively imports content from an HTTP(s) endpoint into FS5 + blob store.
- Features:
  - Rate‑limits concurrent requests via a `Semaphore`.
  - `import_url(Url)`:
    - Does a `HEAD` request to determine `Content-Length` and `Last-Modified`.
    - Compares against a stored `FileRef` (size/timestamp); skips unchanged content.
    - If `Content-Type: text/html`, parses the HTML (`scraper`) and recursively follows links inside the same prefix.
    - Otherwise, streams the body into `BlobStore::import_stream` and stores a new `FileRef` with timestamps in FS5.

Both importers are used by the CLI (`s5_cli`) but can be embedded directly into your own applications.


### Experimental / auxiliary crates

#### `s5_fuse` – FS5 FUSE mount (not in workspace by default)

File: `s5_fuse/src/main.rs`, `s5_fuse/README.md`

- Experimental FUSE driver that exposes an FS5 root as a POSIX filesystem.
- Supports:
  - Read‑only directory listing and file reads.
  - Basic writes as whole‑file atomics (write on `release`), plus `mkdir`.
- Many operations are intentionally unimplemented or simplified (see `s5_fuse/README.md`).


## Key Data Structures & Concepts

### Content hashes & blob identifiers

Defined in `s5_core`:

- `Hash` (`hash::Hash`): 32‑byte BLAKE3 hash, used for content addressing.
- `BlobId` (`blob::identifier::BlobId`): S5’s canonical blob identifier, built on top of `Hash` with additional structure (e.g. type tags).

These are treated as wire‑stable protocol types and appear in all network interactions.


### Blob locations & storage abstraction

Defined primarily in `s5_core::blob` and `s5_core::store`:

- `BlobLocation` – describes where/how content can be fetched:
  - URLs (e.g. S3 presigned URLs).
  - Iroh endpoints.
  - Sia objects, etc.
- `Store` – async trait for key/value blob storage. Backends implement this for local FS, S3, Sia, memory, or your own system.
- `StoreFeatures` – describes capabilities (`supports_rename`, `case_sensitive`, `recommended_max_dir_size`).
- `BlobStore` – façade on top of a `Store` that exposes higher‑level blob import/read operations (`import_file`, `import_stream`, etc.) and integrates with blob locations.


### Streams & Registry

Defined in `s5_core::stream` and surfaced via `s5_registry`:

- `StreamKey` – typed key for a stream (e.g. local or public Ed25519 keys).
- `StreamMessage` – signed, append‑only message carrying payloads.
- `MessageType` – identifies message semantics (registry vs others).
- `RegistryApi` – async trait for storing/retrieving `StreamMessage`s by key.
- `RedbRegistry` – local `RegistryApi` implementation using `redb`.
- `RemoteRegistry` – `RegistryApi` implementation backed by the network (`s5_registry::Client`).

The **registry** is used for mutable pointers to immutable content (e.g. “latest FS5 snapshot for this key”). Streams provide revision numbers, signatures, and message payloads.


### Pins & pinning policy

Defined in `s5_core::pins`:

- `Pins` – tracks which blobs are pinned and by whom.
- `PinContext` – identifies a logical pin context (e.g. application or sub‑component).
- `RegistryPinner` – helper that follows registry entries and ensures the referenced blobs stay pinned.

This layer is where you’d implement garbage collection, “keep alive until X”, and other retention policy logic.


### FS5 internals

Defined in `s5_fs`:

- `DirV1` (`dir` module) – CBOR‑encoded directory snapshot:
  - Contains entries, metadata, optional sharding info, and encryption markers.
- `FileRef` – points from a directory entry to a blob (`BlobId`) plus metadata (size, timestamps, previous versions, etc.).
- `FS5` – async façade:
  - `file_put`, `file_put_sync`, `file_get`, `delete`, `list`, `batch`, `save`, etc.
  - `list` returns entries plus a `CursorKind` for pagination.
- `DirContext` – how FS5 is bound to:
  - Local meta files (where `DirV1` snapshots live).
  - Blob store used for file content.
  - Registry (for registry‑backed directories).
  - Encryption keys and signing keys.

From the caller’s perspective, FS5 feels like a mutable filesystem; internally, each mutation produces a new immutable snapshot addressed by its hash, and optional registry pointers make that snapshot discoverable.


## Typical Workflows

### Running a node

Prerequisites:

- Rust (latest stable).

Install the CLI:

```bash
cargo install --path s5_cli
```

Initialize node config (creates a TOML under your config dir):

```bash
s5 config init
```

Start the node:

```bash
s5 start
```

This will:

- Load `S5NodeConfig` from the config directory.
- Construct configured blob stores (`Local`, `S3`, `Sia`, `Memory`).
- Open a `RedbRegistry` database.
- Bind an Iroh endpoint and spawn `BlobsServer` + `RegistryServer`.
- Optionally run `sync.*` jobs (see below) and then wait for `Ctrl+C`.


### Importing data

Local filesystem import:

```bash
# Import files from ./my-data into the default store
s5 import local ./my-data
```

This:

- Opens the local FS5 root for the node.
- Constructs a `BlobStore` for the chosen store (default is `default`).
- Walks `./my-data`, importing changed files into the blob store and storing `FileRef`s in FS5.

HTTP import:

```bash
# Recursively mirror a directory-like HTTP listing
s5 import http https://example.com/dir/
```

This:

- Uses `HttpImporter` to crawl the HTTP directory tree.
- Uses `HEAD` to detect changes and `GET` to stream content into the blob store.
- Stores `FileRef`s in FS5 with timestamps from `Last-Modified`.


### File sync via encrypted FS5 snapshots

Node file sync is configured in the node TOML (`sync.*` sections plus `peer.*`). Conceptually:

- `sync.<name>.local_path` – local directory to sync.
- `sync.<name>.via_untrusted` – route via named peers.
- `sync.<name>.shared_secret` – pre‑shared secret used to derive symmetric keys.
- `sync.<name>.interval_secs` – if set, run continuous sync at this interval; otherwise, run once per startup.

At runtime (`S5Node::run_file_sync`):

- Derive `SyncKeys` from `shared_secret` (`derive_sync_keys`).
- Open a plaintext FS5 root over `local_path`.
- Dial the peer via Iroh (using its `EndpointId` string) and construct:
  - `s5_blobs::Client` / `RemoteBlobStore`.
  - `s5_registry::RemoteRegistry`.
- Open an encrypted FS5 root backed by the remote store + registry.
- Perform `push_snapshot` + `pull_snapshot` between plaintext and encrypted FS roots.

This yields “end‑to‑end encrypted file sync via untrusted nodes” with FS5 snapshots as the unit of replication.


## How S5 Compares

### vs. IPFS

- **Library‑first vs daemon‑first**: S5 is designed as a set of Rust crates you embed; the provided node is thin and opinionated rather than a monolithic daemon.
- **Transport**: S5 leans on Iroh (QUIC + BLAKE3 + modern NAT traversal) rather than the full libp2p stack.
- **Storage model**: S5’s `Store` trait makes BYO storage (local FS, S3, Sia renterd, custom backends) first‑class, instead of treating external pinning services as out‑of‑band.
- **Filesystem & registry**: FS5 and the registry are explicit, modular crates you can use separately.

### vs. Iroh

- **Relationship**: S5 is built *on top of* Iroh, not a competitor.
- **What Iroh provides**:
  - Endpoints, QUIC transport, and hole punching.
  - Low‑level blob transport primitives.
- **What S5 adds**:
  - Protocol types (`Hash`, `BlobId`, `BlobLocation`, `StreamMessage`, etc.) and a registry model.
  - A pluggable storage abstraction (`Store`, `BlobStore`) with concrete backends.
  - FS5: an encrypted, content‑addressed filesystem with snapshot semantics.
  - Node orchestration (`s5_node`) and CLI tooling (`s5_cli`).
  - Importers for local files and HTTP directories.

### vs. "just S3" or a plain object store

- **Content addressing and dedup**: S5’s addressing is hash‑first (`Hash`, `BlobId`), not path‑first.
- **Global addressing**: The same blob hash can be located via multiple backends (`BlobLocation`) and announced over Iroh.
- **Higher‑level structures**: Directories, registries, and pins live above blobs with well‑defined CBOR schemas.
- **End‑to‑end encryption**: FS5 encrypted directories and encrypted sync flows run *on top* of untrusted object storage.


## Where to Start Reading the Code

For an experienced Rust + CAS/Iroh developer, here is a suggested path:

1. **Protocol surface**: `s5_core/src/lib.rs` and its modules:
   - `hash`, `blob::{identifier, location, store}`, `store`, `stream`, `pins`, `cbor`.
2. **Filesystem**: `s5_fs/src/dir.rs` and `s5_fs/src/api.rs` to see how `DirV1` and `FS5` are structured.
3. **Networking**:
   - `s5_blobs/src/lib.rs` – how blobs are exposed over Iroh.
   - `s5_registry/src/lib.rs` – registry RPC protocol.
4. **Node orchestration**:
   - `s5_node/src/config.rs` – full TOML config schema.
   - `s5_node/src/lib.rs` – `S5Node`, `run_node`, `run_file_sync`.
5. **CLI & UX**: `s5_cli/src/main.rs` – how the CLI composes node + importers.
6. **Storage backends & importers**:
   - `blob_stores/local/src/lib.rs`, `blob_stores/s3/src/lib.rs`, `blob_stores/sia/src/lib.rs`.
   - `importers/local/src/lib.rs`, `importers/http_importer/src/lib.rs`.

From there, you can follow types (`BlobStore`, `FS5`, `StreamKey`, etc.) through your editor to see how the pieces compose in more detail.


## License

Licensed under either of:

- Apache License, Version 2.0,
- MIT license,

at your option.

See the root of this workspace for the full license texts (`LICENSE-APACHE`, `LICENSE-MIT`).

---

This project is supported by a [Sia Foundation](https://sia.tech/grants) grant.
