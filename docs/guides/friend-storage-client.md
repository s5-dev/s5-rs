# Using a Friend's S5 Storage Node as a Client

This guide shows how to configure your own S5 node to use a **remote storage node run by a friend** (or yourself on another machine) that exposes a blob store for you.

You will:

- Use FS5 locally (optionally via FUSE) over a directory such as `~/Documents`.
- Sync encrypted FS5 snapshots to the remote storage service.
- Rely on S5’s pinning system so that only your node can access or delete your blobs on the service.

---

## 1. Information you need from your friend

Ask the friend who is running the storage node for:

1. Their node **endpoint id** string, for example:

```text
endpoint id: 7sorb2nswfc5lkuv3idxwmz4gkzj3efpulk3dvnlqacvds5nxf4q
```

2. A recommended peer name (e.g. `friend`).

Your friend will also need **your** node’s endpoint id (see below) to grant you access.


Some operators may use a wildcard configuration on their side (a special `[peer."*"]` entry) to accept uploads from unknown nodes into a dedicated store. As a client you do not need to do anything special for this: you still configure `[peer.friend]` with the operator’s endpoint id and let them decide whether to serve you via an explicit ACL or a wildcard rule.

---

## 2. Install and initialize S5 locally

Install the CLI from this repository:

```bash
cargo install --git https://github.com/s5-dev/s5-rs s5_cli
```

Initialize your default node config:

```bash
s5 config init
```

This creates a config file like:

- `~/.config/s5/local.toml`

We’ll use the node name `local` throughout this guide; you can choose another name if you prefer (just keep it consistent with the `--node` flag). For other node names, configs will live under `~/.config/s5/nodes/<name>.toml`.

---

## 3. Start your node once to get your endpoint id

Run your node:

```bash
s5 start --node local
```

On startup you’ll see logs similar to:

```text
s5_node online
endpoint id: 3htfn7skordxnacpg6g4zotmyztl6zrequmu66ldvvyxan7gwneq
endpoint addr: ...
```

This single **endpoint id** string is what your friend/operator needs to add you as a peer.

Send that endpoint id string to them. Once they have added you to their `peer.*` ACLs, you can stop your node (Ctrl+C) and continue with configuration.


---

## 4. Configure your local store and remote peer

Open your node config file, e.g.:

```text
~/.config/s5/local.toml
```


### 4.1 Local plaintext blob store

Add (or verify) a local store for your plaintext data:

```toml
name = "alice"

[identity]
# Keep what `s5 config init` created, or customize
secret_key_file = "/home/alice/.config/s5/identity.key"

[store.default]
type = "local"
base_path = "/home/alice/.local/share/s5/blobs"
```

This is where your local FS5 root will store plaintext blobs.

### 4.2 Remote friend storage peer

Add a `peer` entry pointing at your friend’s node. Use the endpoint id string they shared with you:

```toml
[peer.friend]
# Friend's endpoint id string, exactly as they share it with you
id = "7sorb2nswfc5lkuv3idxwmz4gkzj3efpulk3dvnlqacvds5nxf4q"

# You can omit the `blobs` section if you are only a client.
# If you want to serve blobs to your friend, configure it here.
```

The key `friend` is an arbitrary name; you’ll reference it from your sync configuration.


---

## 5. Configure encrypted FS5 sync via your friend

To sync a local directory (e.g. `~/Documents`) using your friend’s storage node, add a `sync` entry to your config.

Example:

```toml
[sync.documents]
# Local path you want FS5 to mirror
local_path = "/home/alice/Documents"

# Route via your friend’s node (key from [peer.friend])
via_untrusted = ["friend"]

# Shared secret between **your own devices**; used to derive encryption and signing keys.
# Do NOT share this secret with your friend or storage operator.
shared_secret = "some-long-random-secret-here"


# Optional: run continuous sync every 60 seconds. If omitted, sync runs once per start.
interval_secs = 60
```

What this does on each sync cycle:

1. Opens a **plaintext** FS5 root over `/home/alice/Documents`.
2. Connects to the `friend` node using Iroh + the `s5_blobs` and `s5_registry` protocols.
3. Opens an **encrypted** FS5 root backed by the provider's blob store and registry.
4. Pushes and pulls FS5 snapshots between plaintext and encrypted roots.

All content stored remotely is encrypted and addressed by BLAKE3 hashes; your friend (or any storage operator) never sees your filesystem structure or plaintext data.

---

## 6. Using FS5 locally

Once configured, you can treat `/home/alice/Documents` as your working tree:

- Create, modify, and delete files as usual.
- FS5 turns each state of the directory into a content-addressed snapshot.
- The sync job uploads new/changed blobs to the provider when it runs.

### Mounting via FUSE

You can also mount your FS5 filesystem to access it like a normal drive.

**Option 1: CLI Command**

```bash
mkdir ~/mnt/s5
s5 mount ~/mnt/s5
```

This mounts your node's default FS5 root.

**Option 2: Node Configuration**

Add a `[fuse.<name>]` section to your `local.toml` to mount automatically when `s5 start` runs:

```toml
[fuse.docs]
root_path = "/home/alice/.local/share/s5/roots/local.fs5"
mount_path = "/home/alice/mnt/s5"
auto_unmount = true
```

---

## 7. Running the sync

Start your node with the updated config:

```bash
s5 start --node local
```

On each sync tick (every `interval_secs` seconds, if configured):

1. Your node exports a snapshot of the plaintext FS5 root (`/home/alice/Documents`).
2. It uploads any new/changed blobs to the provider’s node via the `s5_blobs` protocol.
3. The storage node stores those blobs and pins them under your node id.
4. The registry state and encrypted FS5 metadata are also updated remotely.

If you change files on another device configured with the same `shared_secret` and peer settings, snapshots will converge via the paid storage service.

---

## 8. Access control and deletion (conceptual)

On the storage node side, uploads, downloads, and deletes are tied to your **node id** via S5’s pinning layer:

- **Uploads**: Your node uploads blobs using `s5_blobs::Client`. The storage node tags each blob with `PinContext::NodeId(<your-node-id>)`.
- **Downloads**: The storage node only serves blobs to your node if they are pinned by your `PinContext::NodeId`.
- **Deletes**: When you (or a CLI tool such as `s5 blobs delete`) issue a delete request for a blob:
  - The storage node removes your pin.
  - If no pins remain for that blob, it is deleted from the storage node’s backend.

Today, you mostly interact with this via FS5 snapshots and sync. For more direct control, the CLI also exposes blob-level operations (e.g. `s5 blobs upload`, `s5 blobs download`, `s5 blobs delete`) built on top of the same protocol.



---

## 9. Summary

- You run your own S5 node and FS5 filesystem locally.
- A friend (or community operator) runs an S5 node with a configured blob store.
- You configure a `peer` and `sync` section to point your node at that storage node.
- FS5 snapshots and blobs are encrypted, content-addressed, and synced over Iroh.
- The storage node tracks which blobs belong to which node via pins, only serving and deleting blobs for the node that pinned them.

With this setup, you get end-to-end encrypted, content-addressed storage on top of a storage node run by someone you know (or yourself elsewhere), while retaining a clear separation of roles between your local filesystem, your node, and the remote storage operator.

