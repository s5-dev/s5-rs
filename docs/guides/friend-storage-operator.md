# Operating an S5 Storage Service for Other Users

This guide walks through running an S5 node as a **storage service** that other users can sync their encrypted FS5 data to.

The service:

- Stores blobs in your chosen backend (local disk, S3, Sia, memory).
- Tags each uploaded blob with the uploading node’s public key using S5’s pinning system (`PinContext::NodeId`).
- Only allows a node to download/delete blobs that are pinned by that node.
- Deletes blobs automatically once no nodes are pinning them.

---

## 1. Prerequisites

- Rust toolchain installed.
- This repository checked out and built.
- `s5_cli` installed:

```bash
cargo install --git https://github.com/s5-dev/s5-rs s5_cli
```

All examples assume a Unix-like system; adjust paths as needed for your OS.

---

## 2. Create a dedicated storage node config

First, use the CLI to ensure the default config directory exists:

```bash
s5 config init
```

This creates a config file like:

- `~/.config/s5/local.toml`

For a storage service, it’s usually nicer to have a dedicated node name, e.g. `storage`:

1. Copy the file:

    ```bash
    mkdir -p ~/.config/s5/nodes
    cp ~/.config/s5/local.toml ~/.config/s5/nodes/storage.toml
    ```

2. Edit `~/.config/s5/nodes/storage.toml` to suit your deployment.

We’ll refer to this file as **`storage.toml`** in the rest of the guide.

---

## 3. Define a paid storage backend

In `storage.toml`, add a store that will hold customers’ data. For a local filesystem backend:

```toml
name = "paid-storage"

[identity]
# Either keep what `s5 config init` created, or point at a managed key file
secret_key_file = "/srv/s5/identity.key"

[store.paid]
type = "local"
base_path = "/srv/s5/paid-blobs"
```

You can instead use `s3` or `sia_renterd` by changing the `type` and providing the corresponding config (see `blob_stores/*` READMEs for details).

The important part is that you have a named store (here: `paid`) where blobs will live.

---

## 4. Start the storage node and record its endpoint id

Start your storage node:

```bash
s5 start --node storage
```

On startup you should see log lines like:

```text
s5_node online
endpoint id: 5wkqc3dhpfrcxsq7pbcdyzqapua2tnxt3h6p4euoonpyvdm6z6na
endpoint addr: ...
```

This single **endpoint id** string is used everywhere:

- Clients use it in their `sync.via_untrusted` configs.
- You use the exact same string in your `peer.*.id` ACL entries.

Leave this node running; it will accept connections from client nodes.


---

## 5. Add a friend to your ACLs

Each friend runs their own S5 node. To grant someone access, you need:


1. Their node’s `endpoint id (acl)` string (they can obtain it from their own `s5 start` logs).
2. To add a `peer.<name>` entry for them in your `storage.toml`.

### 5.1 Ask your friend for their endpoint id

Have your friend run:

```bash
s5 start --node local
```

and send you the log line:

```text
endpoint id: 3htfn7skordxnacpg6g4zotmyztl6zrequmu66ldvvyxan7gwneq
```

### 5.2 Add a `peer` entry for the friend

In `storage.toml`, add a section like this for a friend named `alice`:

```toml
[peer.alice]
# MUST match Alice's endpoint id string exactly as printed in her logs
id = "3htfn7skordxnacpg6g4zotmyztl6zrequmu66ldvvyxan7gwneq"


[peer.alice.blobs]
# Stores Alice may read from on your node
readable_stores = ["paid"]
# Store to accept uploads into from Alice
store_uploads_in = "paid"
```

Semantics:

- `readable_stores`: which of your blob stores this peer may query and download from.
- `store_uploads_in`: which store incoming uploads from this peer are written to.

You can add more peers in the same way (`peer.bob`, `peer.carol`, …), with different ACLs and stores if you like.

> Note: the current implementation allows unlimited uploads from peers you’ve configured this way. Future versions can add quota checks before accepting uploads.

---

## 6. Optional wildcard peer for anonymous uploads

If you want to accept uploads from **unknown nodes** without pre-registering each one, you can configure a wildcard peer.

Add a peer entry with `id = "*"`:

```toml
[peer."*"]
# Special wildcard: applies when no explicit peer.<name>.id matches
id = "*"

[peer."*".blobs]
# Store to accept uploads into from any unknown peer
store_uploads_in = "anon_uploads"

# Optional: allow them to read blobs from specific stores.
# Even with this set, downloads are still limited by per-node pins,
# so an anonymous node can only read blobs it uploaded (or explicitly pinned).
readable_stores = ["anon_uploads"]
```

Semantics and safety:

- The wildcard entry is only used **if no exact `peer.<name>.id` match is found** for the connecting node.
- Uploads from unknown nodes will still be tagged with their actual node id via `PinContext::NodeId`.
- Downloads for unknown nodes are still gated by pins, so they can only read blobs they themselves pinned.
- There is **no listing API** exposed via `PeerConfigBlobs`, so wildcard peers cannot enumerate all stored content.

If you don’t need anonymous uploads, simply omit the `[peer."*"]` section.

---

## 7. What happens on upload, download, and delete

With the S5 node and `s5_blobs`/`s5_core` integration in place, the flow looks like this.

### 7.1 Uploads

When a client (Alice) syncs to your node:

1. Her FS5 instance computes a snapshot and imports file contents as blobs.
2. For the encrypted FS5 root backed by your node, these imports go through `s5_blobs::Client`.
3. Your `BlobsServer` accepts the `UploadBlob` RPC:
  - Checks that `peer.alice.blobs.store_uploads_in` is set.
  - Streams the bytes into your `paid` `BlobStore`.
  - Verifies the hash and size.
  - Uses the registry-backed pinner to add a pin with `PinContext::NodeId(<alice-node-id>)`.

Result: every blob Alice uploads is both **stored** and **pinned** under her node id.

### 7.2 Downloads

When Alice downloads (e.g. during `pull_snapshot` or a direct blob read):

1. Her node calls `download` on the `s5_blobs` client.
2. Your `BlobsServer` handles `DownloadBlob`:
  - Looks up `peer.alice` by the remote node id.
  - Checks whether the requested blob is pinned by `PinContext::NodeId(<alice-node-id>)`.
  - If not pinned for Alice, the download is denied.

This enforces "only the uploader may read their own blobs" (modulo any other nodes you intentionally pin for).

### 7.3 Deletes and garbage collection

When Alice (or a future CLI tool on her node) explicitly deletes a blob via the `s5_blobs` client:

1. Her node calls a `delete_blob(hash)` RPC.
2. Your `BlobsServer`:
  - Removes Alice’s `PinContext::NodeId(<alice-node-id>)` for the hash.
  - If no other pinners remain for the hash, deletes the blob from all configured stores.

If other nodes are still pinning the same blob (e.g. shared data), it will remain stored until their pins are also removed.

---

## 8. Operational tips

- **Backups**: Treat the `base_path` of your `paid` store (and the `registry` directory) as important data; snapshot or back it up according to your service’s guarantees.
- **Monitoring**: Monitor disk usage under `base_path` and log volume. Because the system is content-addressed, blobs will be automatically deduplicated, but you still need capacity planning.
- **Onboarding**: For each new user:
 1. Ask for their `endpoint id (acl)`.
 2. Add a `[peer.<name>]` entry.
 3. Provide them with your **Display** `endpoint id` and a suggested `peer` name (e.g. `paid`).
 4. Optionally agree on a `shared_secret` string for FS5 sync.

With this in place, your S5 node acts as a multi-tenant, encrypted, content-addressed storage service that clients can sync to over Iroh.
