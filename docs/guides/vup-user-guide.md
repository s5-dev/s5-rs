# Vup Vault — user guide

**The 15-minute vault:** install, run one command, write 12 words on paper,
click once in a browser — and you have a client-side-encrypted backup on the
[Sia](https://sia.tech) decentralized storage network that survives the
total loss of your machine.

This guide covers the current `vup` CLI (1.0.0-beta). Command reference:
[`vup_cli/README.md`](../../vup_cli/README.md) and `vup help`; workflows and
recipes: [`../reference/cli-workflows.md`](../reference/cli-workflows.md).

---

## 1. Install

```sh
cargo install s5_vup        # from crates.io (or: cargo install --path vup_cli from a checkout)
```

This installs the `vup` binary. Everything else — the background daemon,
storage drivers, FUSE support — is inside it; `vup` starts its daemon
automatically the first time a command needs one.

Requirements: Linux or macOS (Windows is best-effort). FUSE mounts need
`fuse3` installed. No renterd, no Sia node, no server of your own.

## 2. Onboard (one time)

```sh
vup onboard
```

The wizard walks you through, in order:

1. **Your recovery phrase — 12 words.** Write them on paper. They are the
   *only* thing that can recover your data after losing every device. Vup
   will never show them again, and nobody — not the storage provider, not
   the indexer — can reset or recover them for you.
2. **Storage backend.** Press Enter for the default indexer
   (`https://sia.storage`) or point at any indexd-compatible service. A
   browser window opens for a one-click OAuth registration; the resulting
   app key is stored locally (age-encrypted).
3. **Always-on service (recommended).** The wizard offers to install vup
   as a permanent background service (systemd/launchd) so scheduled and
   watch-mode backups run unattended and vup starts with the system —
   and prints the `vup service install` / `vup service uninstall`
   commands either way (§7).
4. **Summary.** The wizard prints where everything lives:

   | Line | What it holds | Safe to delete? |
   |---|---|---|
   | `Config directory` (`~/.config/s5/`) | your identity keys and `config.toml` | **No** — this is your device identity (recoverable via the 12 words, but deleting it means running a recovery) |
   | `Data directory` (`~/.local/share/s5/`) | daemon runtime state (`service.lock`), local store data | No while the daemon runs |
   | `Backup store` | the remote store backups go to | (remote) |
   | `Index cache` (`~/.cache/s5/…`) | rebuildable local caches of remote state | Mostly — **except** the `…-staging` directory, which spools backup data that has not reached the network yet; don't touch it while backups are running |

## 3. Back up

The vault is addressed by a **trailing colon** (`docs:`); the verb comes
first. One command creates the vault, records what feeds it, and takes the
first snapshot:

```sh
vup backup ~/Documents docs:      # create vault "docs", map the source, snapshot once
```

The first time, vup offers to create `docs:` if it doesn't exist, records
`~/Documents` as its source, and runs one snapshot. After that:

```sh
vup backup docs:                  # re-run docs:' saved mapping
vup backup                        # re-run every vault's mapping
```

- Snapshots are **incremental** (unchanged files are skipped; identical
  content is deduplicated) and **encrypted on your device** — the network
  only ever sees ciphertext.
- The progress counter shows bytes *staged* (queued locally) — the daemon
  packs them into large chunks and uploads in the background; the backup's
  final sync barrier completes only once everything is durable remotely.
- `vup history docs:` lists snapshots; `vup list docs:` shows the vault's
  contents as a tree; `vup list` (no ref) shows vaults + stores; `vup status`
  shows vaults, stores and active tasks; `vup tasks` follows running work
  (`vup tasks <id>` for one).

> The colon is the *only* vault marker. A bare token like `~/Documents` is
> always a literal local path, so `vup backup PATH docs:` is unambiguous:
> the one argument with a colon is the destination vault.

### Automatic backups

Turn a one-shot backup into a standing automation. These are daemon-side and
persist across reboots once vup runs as a service (§7):

```sh
vup automate add docs: --watch    # snap within seconds of any change
vup automate add docs: --every 1h # snap on a fixed cadence (1h, 30m, 15s…)
```

Pick the one that fits — `--watch` for change-driven, `--every` for a
predictable interval. Manage them with:

```sh
vup automate list                 # every automation + live status
vup automate show docs-watch      # one in detail
vup automate pause docs-watch     # stop running, stay configured
vup automate resume docs-watch
vup automate rm docs-watch
```

Bare `vup automate` is a wizard: right after your first backup it offers to
keep that vault backed up automatically.

## 4. Restore

Restore rebuilds the recorded filesystem into a directory you name. The
target is **required** — vup never guesses a destination or overwrites in
place:

```sh
vup restore docs: ./restored              # whole vault, latest snapshot
vup restore docs:Photos ./photos          # just the Photos subtree, re-rooted
vup restore docs:#3 ./old                  # the whole vault at snapshot 3
vup restore docs:report.md#2026-06-01 ./x  # one path at a dated snapshot
```

Restore runs to completion in the foreground with progress, verifying every
block against its content hash as it downloads. A non-empty target is refused
unless you pass `--force`.

## 5. Disaster recovery — the 12 words

Machine stolen, disk dead, everything gone:

```sh
vup recover
```

Type the 12 words, re-authorize the storage account (one browser click),
and your identity, configuration, vault list and snapshots are back —
then `vup restore docs: ./restored` returns your files, hash-verified. This
exact flow is exercised against the production network as part of the test
suite (and was drilled live: 51/51 files bit-identical after a full
device wipe).

## 6. More devices, sharing

**Second device (same identity):**

```sh
# on the existing device            # on the new device
vup device invite                   vup device join <code>
```

The new device gets the same vaults and keys; both snap and sync through
the same stores. `vup device ls` lists devices; `vup device revoke @label`
removes one (and prints the honest checklist for the *compromised-device*
case — removal alone is not enough if someone else holds its keys).

**Other people:**

```sh
vup friend pair                     # exchange one-time token, save as @alice
vup grant docs: @alice              # read access (--write for write)
vup who docs:                       # list members
vup revoke docs: @alice             # take it back
```

**Anonymous share links** (no pairing, one frozen snapshot):

```sh
vup share docs:                     # → s5://export/docs?m=...#<key>
```

The decryption key lives in the URL fragment (`#…`) — servers never see it.
Send the link over a private channel; anyone with it can read that snapshot
(and nothing after it). On the receiving side:

```sh
vup join s5://export/docs?m=...#<key>   # materialise a read-only vault
vup restore docs: ./out                  # pull its files
```

**Share just a subtree** (composes a separate share-vault so revoking it
never touches your source):

```sh
vup share docs:Photos               # mint a share-vault, copy Photos in, export it
vup share docs:Photos --live        # …and keep it updated as Photos changes
vup share docs:Photos --deep        # re-encrypt under the share-vault's own keys
```

vup prints the honesty facts before it composes: a shallow copy widens who
can decrypt the copied blobs, "revoke" re-keys the share-vault's metadata
(not blobs already disclosed), and there is no write-only capability — use
`--deep` for true future-revocability.

**Copy between vaults** — the primitive sharing is built on:

```sh
vup copy docs: archive:             # copy a whole vault into another
vup copy docs:Photos album: --deep  # copy a subtree, re-encrypting it
```

Shallow by default (reuses the source ciphertext, inlines per-blob keys);
`--deep` re-encrypts under the destination's keys. A shallow copy that would
let new readers decrypt the source data asks before proceeding.

**Browse as a filesystem:**

```sh
vup mount docs: /mnt/docs           # read-only FUSE mount; --rw for writable
```

## 7. Run the daemon as a service

vup is designed to have its daemon permanently alive on every device —
that's what runs your automations (watch + schedule), mounts, and serves
your other devices and friends. Onboarding offers this; you can also do it
anytime:

```sh
vup service install      # systemd user unit (Linux) / LaunchAgent (macOS):
                         # starts now, starts at boot, restarts on failure
vup service status
vup service uninstall    # fully reversible; data and config untouched
```

The generated unit embeds the current binary and config paths — re-run
`vup service install` after moving or updating vup. Static templates for
packagers live in [`packaging/`](../../packaging/). Without a service, any
`vup` command still auto-starts a session daemon on demand (it just
doesn't survive reboots).

## 8. Add more storage

Onboarding sets up one store. Add others any time — a local directory, an
S3-compatible bucket, or another Sia backend:

```sh
vup store add local cold --path /mnt/backup
vup store add s3 offsite --endpoint https://… --bucket b --access-key … --secret-key …
vup store add sia sia2               # prompts for the 12 words + one-click OAuth
vup store list                       # what's configured (the default is marked)
vup store info cold                  # backend config, who uses it
vup store rm cold                    # refused while a vault still references it
```

Point a vault at a store by name via `vup config` (`vault.<name>.data_store`).
Flags supply everything non-interactively; anything omitted is prompted.

## 9. When something goes wrong

- `vup doctor` — one-line-per-signal health walk: daemon reachable, each
  store reachable, staging drained (is your latest backup actually durable?),
  the OS service active, and observed peers.
- `vup status` — stores, vaults, running tasks, and the log directory.
- Logs: `~/.cache/s5/logs/node.log.<date>` (daily rotation, 7 kept). The
  daemon always logs at debug level for its own subsystems.
- **"uploads appear STALLED"** in the log means staged data is not
  reaching the store (network/indexer trouble). Staged data is crash-safe
  on disk: the daemon retries automatically, and anything pending is
  re-queued on the next start. Uploads time out and retry rather than
  hanging forever.
- **"account not found"** from the indexer: beta indexers occasionally
  purge accounts. Re-run `vup store add sia` (or `vup onboard`) to
  re-register; your data keys never depended on the account.
- `vup shutdown` stops the daemon cleanly (it drains pending uploads for
  up to 45 s, then exits; anything left resumes next start).

## 10. What to trust, in one paragraph

Everything is encrypted on your device before upload: file content and
metadata with per-vault keys (ChaCha20, with integrity enforced by BLAKE3
content addressing — every read re-verifies the hash), and the vault
roots / key material age-sealed (X25519 + ChaCha20-Poly1305) to your
device and paper keys. Stores and indexers only ever see ciphertext and
sizes. Your identity
is a DID anchored to a cold key derived from the 12 words; day-to-day
operations use a rotatable warm key, so the paper phrase never has to
touch a networked machine except during recovery. There is no account to
reset, no vendor to trust, and no lock-in: the formats are open
(S5/FS5), the code is open source, and any S5-compatible store works as
a backend.
