# S5 Workflows: Practical Tutorials

This page shows short, end-to-end examples of how to use S5 for several interesting workflows.

Assumptions:

- You have Rust and `cargo` installed.
- You are in the root of this repository.
- You install the CLI once via:

```bash
cargo install --git https://github.com/s5-dev/s5-rs s5_cli
```

Most examples assume you already ran:

```bash
s5 config init
```

which creates `~/.config/s5/local.toml`.

---

## 1. Friend / Community Storage Patterns

### 1.1 Mutual backup circles

**Goal:** A small group of friends each run a node and back up each other’s data. If one node disappears, others still hold encrypted copies.

#### Step 1: Everyone runs a storage node

Each friend (alice, bob, carol) does:

```bash
s5 config init
s5 start --node local
# Note: endpoint id: 3htfn7skordxnacpg6g4zotmyztl6zrequmu66ldvvyxan7gwneq
```

They share their endpoint ids out-of-band.

Each person then creates a dedicated storage node config, e.g. for Alice:

```bash
mkdir -p ~/.config/s5/nodes
cp ~/.config/s5/local.toml ~/.config/s5/nodes/storage-alice.toml
```

Edit `~/.config/s5/nodes/storage-alice.toml`:

```toml
name = "alice-storage"

[identity]
secret_key_file = "/srv/s5/alice-identity.key"

[store.backups]
type = "local"
base_path = "/srv/s5/alice-backups"

[peer.bob]
id = "6xk3nfwqh2gaqrpwlozndmjq5cxb7fwlnt4ve77jkacmz5px3w4a"
[peer.bob.blobs]
readable_stores = ["backups"]
store_uploads_in = "backups"

[peer.carol]
id = "4mpbr2ehjs5vxqdlfkopz6n3aty2cwqhgurms7oefncqvdk3zr5q"
[peer.carol.blobs]
readable_stores = ["backups"]
store_uploads_in = "backups"
```

Start the storage node:

```bash
s5 start --node storage-alice
```

Bob and Carol do the same, granting each other access.

#### Step 2: Each person configures backups to multiple peers

On Alice’s laptop (`~/.config/s5/local.toml`):

```toml
[peer.bob-storage]
id = "2qwerty7uiopa3sdfghjklzxcvbnm5tgbnjmk8lpoi9uytre1wqa"

[peer.carol-storage]
id = "8zxcvbnm2asdfghjk5qwertyuiop4lkjhgfdsa9mnbvcxz7poiuq"

# Backup home directory to Bob this month
[sync.home_to_bob]
local_path = "/home/alice"
via_untrusted = ["bob-storage"]
shared_secret = "alice-home-backup"

# Same dataset, alternate peer (disabled by default)
# [sync.home_to_carol]
# local_path = "/home/alice"
# via_untrusted = ["carol-storage"]
# shared_secret = "alice-home-backup"
```

Alice can switch which peer is active by commenting/uncommenting the corresponding `sync.*` entries. Bob and Carol mirror the pattern.

Because content is encrypted and content-addressed, data backed up via multiple friends is deduplicated and still recoverable if one friend’s node disappears.

---

### 1.2 Blind "dead-drop" inbox

**Goal:** Let others send you encrypted files without you (the storage operator) being able to read them. You only relay ciphertext.

#### Operator: wildcard storage node

Create `~/.config/s5/nodes/dropbox.toml`:

```toml
name = "dropbox"

[identity]
secret_key_file = "/srv/s5/dropbox-identity.key"

[store.incoming]
type = "local"
base_path = "/srv/s5/incoming-blobs"

[peer."*"]
# Wildcard: applies when no explicit peer.<name>.id matches
id = "*"

[peer."*".blobs]
# Accept uploads from any unknown node into this store
store_uploads_in = "incoming"
# Allow reads from the same store, but access is still
# limited by per-node pins.
readable_stores = ["incoming"]
```

Start it and share the endpoint id:

```bash
s5 start --node dropbox
# endpoint id: 9plmokn4ijbuhvgycftxdrzesawq2qazxswedcvfrtgbnhyujmkiq
```

#### Sender: one-way encrypted drop folder

Sender’s `~/.config/s5/local.toml`:

```toml
[peer.dropbox]
id = "9plmokn4ijbuhvgycftxdrzesawq2qazxswedcvfrtgbnhyujmkiq"

[sync.drop]
local_path = "/home/sender/drop-folder"
via_untrusted = ["dropbox"]
# Only sender + intended recipient devices know this.
shared_secret = "sender-recipient-secret"
```

Sender runs:

```bash
mkdir -p /home/sender/drop-folder
cp secret-doc.pdf /home/sender/drop-folder/
s5 start --node local
```

Recipient configures the same `peer.dropbox` + `sync.drop` (same `shared_secret`), but with their own `local_path` to pull and decrypt the files.

---

## 2. Filesystem + Registry Workflows

### 2.1 Time-travel workspaces

**Goal:** Use FS5 snapshots as checkpoints of a project directory, giving you Git-like time travel for *all* files, including large binaries.

#### Step 1: Track a project directory with FS5 sync

On your dev machine, in `~/.config/s5/local.toml`:

```toml
[peer.self-backend]
# Could be a local node, a NAS, or a friend’s node
id = "7backend3nodeq4idstring5exampleforworkflows6docsa"

[sync.project]
local_path = "/home/dev/projects/my-app"
via_untrusted = ["self-backend"]
shared_secret = "my-app-history-secret"
```

Run:

```bash
s5 start --node local
```

Each sync tick produces an encrypted FS5 snapshot on the backend node.

#### Step 2: Record snapshot hashes for checkpoints

You can check the current snapshot hash for your sync job using the CLI:

```bash
s5 snapshots head --sync project
# Output: sync.project head: hash=...snapshot-hash-1... revision=42
```

At each logical checkpoint (e.g. before a refactor), record this hash into a file, e.g. `snapshots.txt` inside your Git repo:

```text
2025-01-01 pre-refactor  hash=...snapshot-hash-1...
2025-01-10 post-refactor hash=...snapshot-hash-2...
```

To "time travel", you can restore that specific snapshot into a separate directory:

```bash
s5 snapshots restore --root ./old-version --peer self-backend --hash ...snapshot-hash-1...
```

This rehydrates the FS5 tree at that point, independent of Git’s notion of history.

---

### 2.2 Encrypted shared folders with selective sharing

**Goal:** Create shared folders where membership is simply "who knows the shared_secret". The storage operator never sees plaintext.

#### Operator: storage node

As in the friend-storage guides: run a node with a store, and give each participant read/upload access via `peer.<name>.blobs`.

#### Members: shared folder config

Each member’s `~/.config/s5/local.toml`:

```toml
[peer.friends]
id = "5friends2storage3nodeid7exampleforshareddocumentsq"

[sync.shared_photos]
local_path = "/home/user/SharedPhotos"
via_untrusted = ["friends"]
# Everyone in the group uses the same secret here.
shared_secret = "shared-photos-secret"
```

Everyone who uses the same `shared_secret` and `via_untrusted` peer ends up on the same encrypted FS5 root. The operator only stores encrypted blobs and registry state; they never see your folder contents.

---

### 2.3 "Infinite" Home Media Server (Tiered Storage)

**Goal:** Keep your media library accessible via a small local cache, while bulk data lives on a larger machine or cloud backend.

#### Storage node (NAS / server)

Configure a node named `media` with a large local or S3/Sia-backed store in `~/.config/s5/nodes/media.toml`:

```toml
name = "media"

[identity]
secret_key_file = "/srv/s5/media-identity.key"

[store.media]
type = "local"
base_path = "/srv/s5/media-blobs"
```

Start it and record its endpoint id.

#### Laptop / client

Configure the media node as a peer and a sync job in `~/.config/s5/local.toml`:

```toml
[peer.media]
id = "6media4server2nodeid9exampleformediaworkflowdocsaq"

[sync.media_library]
local_path = "/home/user/Videos"
via_untrusted = ["media"]
shared_secret = "media-library-secret-between-my-devices"
```

Run `s5 start --node local`. Your laptop will encrypt and upload blobs to the `media` node.

**Innovation:** Because S5 is content-addressed, you can delete the *blobs* from your laptop's local cache while keeping the *FS5 metadata* (directory structure). When you open a video, S5 fetches the necessary chunks on-demand from the NAS.

---

### 2.4 Large-Asset "Git" Companion

**Goal:** Keep huge assets (video, game art, datasets) out of Git, but still versioned and shared alongside code.

#### Setup

1. Decide on a project-local directory for large assets, e.g. `project/assets/`.
2. Configure `sync.assets` in your S5 node config to mirror `project/assets/` via a friend or team-operated S5 node.
3. Add an `assets.s5` file in your Git repo to record the current FS5 snapshot hash for `assets/`.

#### Workflow

- **Update assets:**
  - Modify files under `project/assets/`.
  - Run `s5 start --node local` (or a single sync cycle) to push a new snapshot.
  - Check the new hash: `s5 snapshots head --sync assets`
  - Record the new snapshot hash in `assets.s5` and commit that file to Git.

- **Checkout on another machine:**
  - `git pull` to get code + `assets.s5`.
  - Read the hash from `assets.s5`.
  - Run `s5 snapshots restore --root project/assets --peer friend --hash <HASH>`

This keeps Git fast and lean while S5 handles large, deduplicated asset storage.

---

## 3. Data Publishing and Archiving

### 3.1 Static site hosting over S5

**Goal:** Host a static site as an FS5 snapshot backed by S5 blobs, addressable by a single hash.

#### Step 1: Import the static site

Assume your static site is built to `./public/`.

```bash
# Make sure local store exists in your config, e.g. [store.web]
s5 import --target-store web local ./public --concurrency 4
```

This writes file contents as blobs and an FS5 directory describing `./public`.

#### Step 2: Pin and record the snapshot hash

Using FS5 APIs (or a future CLI), obtain and pin the snapshot hash for the site root. Share that hash with others as the identifier of your site.

#### Step 3: Serve the site

A serving node can:

- Open the FS5 root for the snapshot hash.
- Mount it via FUSE (`s5_fuse`) or export it to a local directory.
- Point any HTTP static file server at that directory.

Anyone who knows the hash and can reach the S5 blobs/registry can reconstruct and serve the same site.

---

### 3.2 Personal / group web archive

**Goal:** Mirror and preserve parts of the web into S5, then share them or sync them with friends.

#### Step 1: Import web content

```bash
s5 import --target-store web http \
  https://example.com/interesting/ --concurrency 8
```

The HTTP importer:

- Downloads files as blobs into the `web` store.
- Populates FS5 with paths and metadata.

#### Step 2: Share via a storage node

Run a node that exposes the `web` store to your friends via `peer.<name>.blobs`. They can:

- Add that node as a peer.
- Configure a `sync.web_archive` entry pointing at a local FS5 root.

Each friend ends up with a local, deduplicated copy of the archived content, even if they sync from different entry points or at different times.

---

## 4. Dev / Infra Workflows

### 4.1 Content-addressed build cache (Decentralized Docker Registry)

**Goal:** Share large build artifacts (binaries, Docker images, models) across CI and developer machines using S5 as a content-addressed cache.

#### Shared cache node

Configure a node `build-cache` with a suitable store (local/S3/etc.) and grant CI + dev nodes access via `peer.ci`, `peer.dev`.

`~/.config/s5/nodes/build-cache.toml`:

```toml
name = "build-cache"

[identity]
secret_key_file = "/srv/s5/build-cache-identity.key"

[store.cache]
type = "local"
base_path = "/srv/s5/build-cache-blobs"

[peer.ci]
id = "3cinode5buildcache7exampleidstringfordocumentationq"
[peer.ci.blobs]
readable_stores = ["cache"]
store_uploads_in = "cache"

[peer.dev]
id = "8devnode4buildcache2exampleidstringfordocumentationq"
[peer.dev.blobs]
readable_stores = ["cache"]
store_uploads_in = "cache"
```

Start it and record its endpoint id for CI/dev machines.

#### CI: upload artifacts and record mapping

After a build:

```bash
HASH=$(s5 blobs upload --peer build-cache target/release/my-app \
  | awk '{print $3}' | sed 's/hash=//')

# Optionally write mapping from input-hash -> HASH into S5's registry
# using a small tool that stores CBOR under a well-known registry key.
```

#### Dev machines: check cache before building

Developers configure the same `peer.build-cache` and then:

```bash
s5 blobs download --peer build-cache --out ./bin/my-app "$HASH"
chmod +x ./bin/my-app
```

This gives you a language-agnostic remote build cache layered on S5.

---

### 4.2 Dataset distribution / ML workloads

**Goal:** Distribute large datasets to multiple consumers with deduplication and partial download.

#### Step 1: Dataset node

1. Store the raw dataset as blobs (local import or via a custom tool).
2. Create an FS5 "manifest" directory where each file is a small descriptor pointing to one or more dataset blobs (e.g. one JSON per sample).
3. Pin that manifest snapshot on a `dataset` node and expose it via `peer.*.blobs`.

#### Step 2: Consumers

1. Add the dataset node as a peer.
2. Configure `sync.dataset_manifest` pointing to a local path (e.g. `~/datasets/my-dataset/manifest`).
3. Use your ML/data tooling to:
   - Read the manifest.
   - For each required sample, fetch the associated blob(s) via `s5_blobs::Client` or `s5 blobs download`.

Because data is content-addressed and deduplicated, restructured manifests or partial mirrors reuse the same underlying blobs.

---

## 5. Experimental Patterns

### 5.1 Append-only logs / event feeds

**Goal:** Maintain append-only logs or activity feeds across devices via an untrusted relay, verifying all content by hash.

#### Step 1: Log structure

- Represent the log as a CBOR or CBOR+FS5 structure:
  - A "head" record in the registry mapping `log:<name>` → latest log blob hash.
  - Each log blob contains events and a pointer to the previous blob (linked list).

#### Step 2: Appending

When a device appends events:

1. Read the current head from the registry.
2. Build a new log blob with `prev = old_head` and the new events.
3. Upload it via `s5_blobs::Client` and store its hash.
4. Update the registry key `log:<name>` to point to the new head.

Other devices can follow the chain backwards, verifying each blob by hash and ignoring any inconsistent or malicious entries from the relay.

---

### 5.2 Cold storage for local-only apps

**Goal:** Let a local app keep all its state in a local FS5 root, with periodic encrypted snapshots to a remote S5 node for disaster recovery.

#### Step 1: App + FS5

- The app uses FS5 as its metadata store (or you wrap its data directory in an FS5 root).
- Periodically, you call into FS5 to save the directory state.

#### Step 2: Sync snapshots to a cold-storage node

Configure a `sync.app_state` entry in your S5 node config:

```toml
[peer.cold]
id = "2coldstore5backupnode7exampleidstringfordocumentsaq"

[sync.app_state]
local_path = "/home/user/.local/share/my-app-state"
via_untrusted = ["cold"]
shared_secret = "app-state-secret-between-my-devices"
```

Run `s5 start --node local` periodically or as a daemon. This pushes encrypted snapshots to the cold-storage node.

#### Step 3: Recovery

On a new machine or after data loss:

1. Restore your S5 config (or re-run `s5 config init` and re-add `peer.cold` and `sync.app_state`).
2. Run `s5 start --node local` to pull the latest snapshot.
3. Point the app at the restored FS5 root / directory.

Your app continues where it left off, with all data integrity-checked and encrypted in transit and at rest on the cold-storage node.

---

## 6. Air-Gapped Sneakernet Sync

**Goal:** Move data between secure, disconnected machines using physical media while preserving integrity and encryption.

### Source (secure) machine

1. Mount a USB drive at `/mnt/usb`.
2. Add a store pointing at it in `~/.config/s5/local.toml`:

```toml
[store.usb]
type = "local"
base_path = "/mnt/usb/s5-blobs"
```

3. Import the data you want to transfer:

```bash
s5 import --target-store usb local /path/to/secure/data --concurrency 4
```

4. Safely unmount and carry the USB to the destination machine.

### Destination machine

1. Mount the USB at `/mnt/usb`.
2. Configure the same `store.usb` entry.
3. Use S5 (either via FS5 snapshots or the `blobs` CLI) to read data from the `usb` store into a local FS5 root or plain directory.

Because everything is content-addressed with BLAKE3, S5 verifies the integrity of all data read from the USB. Corruption or tampering manifests as hash mismatches instead of silent data loss.
