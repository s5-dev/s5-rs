# CLI workflows and grammar

**Status:** reference — locks the user-facing `vup` CLI vocabulary, the
D20 reference grammar, command semantics, and the core workflows. Verified
against `vup_cli/src` at the current commit.

Related: `identity-model.md` (DIDs and the identity
behind the `@` prefix), [`configuration.md`](./configuration.md) (the
per-vault schema the verbs read/write), `snapshot-publication.md`
(the on-wire format every backup produces), [`key-recovery.md`](./key-recovery.md)
(paper recovery behind `vup recover`), [`share-links.md`](./share-links.md)
(the URL grammar `vup share` produces and `vup join` consumes).

## Architectural rule (above all the verbs)

**The `vup` CLI is a pure UI for `s5_node`.** Everything happens in the
daemon — mount, backup, copy, publish, pair, registry writes, store I/O.
The CLI parses the reference grammar, sends RPCs, prints responses. No
vault state, no key handling, no filesystem access in the CLI process.

The only exceptions are the **bootstrap verbs** — `onboard`, `recover`,
`device join`, and a top-level `join <vupd-…>` device code — which *create*
the config the daemon needs and therefore run without (and must never
auto-spawn) a daemon. Every other verb auto-starts a session daemon on
first use if one is not already running.

## Design goals (in priority order)

1. **Power-user typing minimisation.** This CLI gets typed dozens of times
   a day by the same users; the frequent verbs are short and single-letter
   aliased.
2. **Safety by default.** No silent capability grants; no implicit or
   surprise-overwrite targets (`restore` always names its destination).
3. **Consistent grammar across verbs.** One reference grammar, verb-first,
   `VERB SRC DST`.
4. **Universally safe across shells.** No reference needs quoting in bash,
   zsh, fish, nushell, cmd.exe, or PowerShell — except a bare `#hash`
   (see Shell safety).

## The D20 reference grammar

Every positional data argument is a **reference**. There are three
reference forms — vault-scoped (`vault:[path][#snap]`), a bare `#hash`, and
a literal local path — with the **colon as the only vault marker**.
Identities (for `grant` / `who` / `friend`) carry an `@` prefix:

| Reference | Meaning | Example |
|---|---|---|
| `vault:` | a whole configured vault, live head | `docs:` |
| `vault:path` | a path inside a vault, live head | `docs:Photos/2024` |
| `vault:path#snap` | a path at a past snapshot | `docs:report.md#2026-06-01` |
| `vault:#snap` | a whole vault at a past snapshot | `docs:#3` |
| `#hash` | a vault-free immutable snapshot, read-only | `#b3a9…f2` |
| `@identity` | a paired friend (petname or DID) | `@alice` |
| *local path* | a literal filesystem path, always | `~/Music`, `./out`, `/etc` |

Rules (see `vup_cli/src/refs.rs`):

- **The colon marks a vault; local paths are literal.** A bare token with
  no colon (`docs`, `report#3.md`) is a local path, never a vault, and is
  never split on `#`. To address a local file's history, go through its
  vault: `docs:report.md#3`.
- Tokens starting with `./`, `../`, `/`, `~`, or a Windows drive (`C:\`,
  `D:/`) are always local paths. A **single letter** before a colon is the
  drive-letter space and is treated as a local path — reach a real
  colon-containing local name as `./weird:file` (rclone's rule).
- **User vault names**: `[a-z0-9][a-z0-9._-]*`, length 2–64. Single
  letters are reserved for drive letters. **System vaults** start with `_`
  (`_config:`, `_identity:`); creating a user vault named `_*` is refused,
  as are the reserved words `default none self me paper recovery all`.
- **`#snap` selectors** ride the reference, so there is no `--snap` flag on
  any verb — the snapshot is part of the thing you are naming.
- A **bare `#hash`** is reserved: it parses as a vault-free immutable
  snapshot, but no verb consumes one yet — address a past snapshot today
  through its vault (`docs:#3`, `docs:path#snap`).

### Shell safety

- `:` and `@` are safe everywhere, unquoted.
- A **bare `#hash`** must stay attached to avoid the shell comment rule:
  `docs:#3` is one token and safe; a standalone `# 3` becomes a comment in
  bash/zsh. When in doubt, quote a leading-`#` reference: `'#b3a9f2'`.

## Verb taxonomy

Verb-first, always. The vault is never in command position. Frequent
**data verbs** are top-level and take references; infrequent **management
verbs** live under noun namespaces (`vault`, `store`, `device`, `friend`,
`service`).

### Data verbs (top-level, `VERB SRC DST`, D20 references)

| Verb | Alias | Form | Action |
|---|---|---|---|
| `backup` | `b` | `[SRC…] vault:[path]` | Fidelity-in snapshot: capture perms/mtimes/symlinks into an incremental, encrypted, published snapshot. |
| `restore` | `r` | `vault:[path][#snap] TARGET` | Fidelity-out: rebuild the exact recorded filesystem into `TARGET` (required). |
| `copy` | `c` | `SRC DST [--deep]` | Copy contents between vaults (whole vault or subtree; both ends are vault references). |
| `list` | `l`, `ls` | `[REF] [--all]` | No arg: vaults + stores overview; `vault:` → tree; `--all` includes `_system` vaults. |
| `history` | `h` | `[vault:]` | List a vault's snapshots. |
| `mount` | `m` | `vault: DIR [--rw]` | Daemon-side FUSE mount; read-only by default. |
| `share` | `s` | `vault:[path][#snap]` | Make a share link (frozen whole-vault, or a composed subtree share). |
| `join` | `j` | `URL \| CODE` | Consume a share URL, or enroll this device from a `vupd-…` code. |

### Automation

```
vup automate                     # bare = context-aware wizard
vup automate add vault:[path] --watch | --every DURATION
vup automate list | show NAME | pause NAME | resume NAME | rm NAME
```
`automate` = `auto` = `a`. An automation is a persisted daemon task that
re-runs a vault's backup mapping on a filesystem watch or a fixed cadence.

### Sharing & access

```
vup share vault:[path]           # frozen whole-vault, or composed subtree share
vup grant vault: @id [--write]   # read by default; --write adds write
vup revoke vault: @id            # remove read + write (alias: k / kick)
vup who [vault:]                 # a vault's members and their capabilities
vup join URL | CODE              # share links AND device-enroll codes
```

### Management namespaces (infrequent, wizard-friendly)

```
vup vault  create | drop | rename  NAME
vup store  add sia|s3|local NAME [flags] | list | info NAME | rm NAME
vup device invite [--label L] | join CODE | list | revoke @label
vup friend pair [TOKEN] | list | forget @id
vup service install | uninstall | status
vup config [VAULT] [--json | --patch STR | --patch-file FILE]
```

### Ops

| Verb | Alias | Action |
|---|---|---|
| `status` | | Endpoint, store/vault/source/task counts, log dir, durability gauges, scheduled backups. |
| `doctor` | `d` | One-line-per-signal health walk (absorbs the old `debug peers`). |
| `tasks` | `t` | List node tasks, or follow/inspect one by id (`tasks ID`). |
| `cancel` | `x` | Cancel a running task by id. |
| `shutdown` | | Stop the daemon cleanly (drains pending uploads, then exits). |
| `onboard` | `o` | First-run setup wizard. |
| `recover` | | Disaster recovery from the paper phrase. |

### The single-letter map

Single letters are permanent muscle memory — one per most-used verb:

`a` automate · `b` backup · `c` copy · `d` doctor · `g` grant ·
`h` history · `j` join · `k` revoke (the old `kick`) · `l` list ·
`m` mount · `o` onboard · `r` restore · `s` share · `t` tasks ·
`w` who · `x` cancel.

`recover`, `status`, `config`, `shutdown` and the namespaces (`vault`,
`store`, `device`, `friend`, `service`) are rare or wizard-shaped and
carry no single letter.

### Legacy aliases (hidden, through the beta)

The old subject-first `+vault <verb> …` form is rewritten to the
verb-first `<verb> vault: …` form before parsing, so old muscle memory
keeps working: `vup +music backup` → `vup backup music:`. The old verbs
`new`/`drop` (→ `vault create`/`vault drop`), `export` (→ `share`),
`task-status` (→ `tasks ID`), `peers` (→ `friend list`), `pair`/`unpair`
(→ `friend pair`/`friend forget`) survive as hidden aliases. `snap` and
`add` are **gone** — folded into `backup`.

## Global flags & exit codes

- `--config <path>` — use a specific node config file.
- `-y`, `--yes` — answer confirmations and accept prompt defaults
  (required for non-interactive/scripted use).
- `-v` / `-q` — raise / lower CLI log verbosity.

Exit codes:
- `0` success.
- `1` generic error (network, store, daemon).
- `2` bad usage (unknown verb, missing/invalid argument — from the parser).
- `3` a confirmation or prompt was required but could not be answered
  (no TTY and no `--yes`). Prompts with no possible default — S3
  credentials, the recovery phrase — always need a TTY and exit 3 otherwise.

Output is quiet on no-op and parseable by default (one item per line, no
table-drawing characters).

## Workflows

### A. First-run setup

```
$ vup onboard
```

The wizard generates your keys, asks where to store backups (a local
directory, an S3-compatible bucket, or Sia via an indexd service with a
one-click browser OAuth), writes `config.toml`, offers to install the
always-on service (§K), and prints your **12-word recovery phrase once**.
Write it on paper: it is the only thing that can recover your data after
losing every device. Onboarding also creates a first vault, `backup:`.

Next: back up a folder.

```
$ vup backup ~/Documents backup:
```

### B. Back up a directory

`backup` is fidelity-in. With source paths and a destination `vault:` it
records the source→vault mapping and takes one snapshot; the vault
reference is required (no auto-created default vault beyond onboarding's
`backup:`). On a TTY it offers to create the vault if it does not exist.

```
$ vup backup ~/Music music:        # create/refresh the mapping, snapshot once
$ vup backup music:                # re-run music:'s saved mapping
$ vup backup                       # re-run every vault's mapping
```

Snapshots are incremental (unchanged files skipped, identical content
deduplicated) and encrypted on your device — the store only ever sees
ciphertext. Inspect with:

```
$ vup history music:               # snapshots (hash, files, size, date)
$ vup list music:                  # contents as an indented tree
$ vup list                         # vaults + stores overview
$ vup status                       # counts, staged/durable gauges, schedules
$ vup tasks                        # running work (tasks <id> to follow one)
```

Keep it running on its own (daemon-side; survives reboots once the service
is installed, §K):

```
$ vup automate add music: --watch    # snap within seconds of any change
$ vup automate add music: --every 1h # snap on a fixed cadence
$ vup automate list                  # every automation + live status
$ vup automate pause music-watch     # stop running, stay configured
```

Bare `vup automate` is a wizard: right after a first backup it offers to
keep that vault backed up automatically.

### C. Restore

`restore` is fidelity-out, backup's exact mirror. The target directory is
a **required positional** — vup never guesses a destination or overwrites
in place; a non-empty target is refused without `--force`. The `#snap`
selector and a subtree path both ride the reference:

```
$ vup restore music: ./restored               # whole vault, latest snapshot
$ vup restore music:Live ./live               # just the Live subtree, re-rooted
$ vup restore music:#3 ./old                  # the whole vault at snapshot 3
$ vup restore music:track.flac#2026-06-01 ./x # one path at a dated snapshot
```

Restore runs in the foreground, verifying every block against its content
hash as it downloads.

### D. Pair another of your own devices

A device shares your identity (`vup device …`); a friend has their own DID
(`vup friend …`).

```
# On the existing device:            # On the new device:
$ vup device invite                  $ vup device join <vupd-…code>
```

`device invite` mints a one-time enrollment code and waits; `device join`
enrolls this machine, creating its config (run it instead of `vup onboard`).
The new device gets the same vaults and keys. `vup device list` shows
enrolled devices; `vup device revoke @label` removes one and prints the
compromised-device checklist (removal alone is not sufficient if someone
else holds its keys).

### E. Pair with someone else, and share a vault

```
$ vup friend pair                    # mint a one-time token, print it, wait
# …or, on the other side, redeem the token:
$ vup friend pair <token>
```

Either side runs `friend pair`; the side with no token mints one and
blocks, the side with a token redeems it. Both then interactively name the
friend (`@alice`) and save the DID under `[friend.*]`. Then grant access:

```
$ vup grant docs: @alice             # read access
$ vup grant docs: @alice --write     # read + write
$ vup who docs:                       # members and their [rw]/[ro] caps
$ vup revoke docs: @alice             # remove read + write
```

`revoke` is honest: future snapshots exclude the member and honest nodes
stop serving them blobs, but data they already fetched stays readable —
content keys are not rotated (the restic-comparable threat model; see
`acl-and-revocation.md`).

`vup friend list` shows paired friends; `vup friend forget @alice` drops
one (refused while they are still a vault member — revoke first).

### F. Share links

A **whole-vault** share is a frozen anonymous export of the current
snapshot — no future updates, no individual revocation, the URL is the
capability:

```
$ vup share docs:
  …
    s5://export/docs?m=…#<key>
```

A **subtree** share composes a separate share-vault + copy + (optional)
automation, so revoking it never touches your source. vup prints the
honesty facts first (a shallow copy widens who can decrypt the copied
blobs; "revoke" re-keys the share-vault's metadata, not blobs already
disclosed; there is no write-only capability):

```
$ vup share docs:Photos               # mint a share-vault, copy Photos in, export
$ vup share docs:Photos --live        # …and keep it updated as Photos changes
$ vup share docs:Photos --deep        # re-encrypt under the share-vault's own keys
$ vup share docs:Photos --name pics --every 30m   # explicit name + cadence
```

On the receiving side:

```
$ vup join s5://export/docs?m=…#<key>   # materialise a read-only vault
$ vup restore docs: ./out               # pull its files
```

`join` also consumes a `vupd-…` device-enrollment code (it dispatches to
`device join`). See [`share-links.md`](./share-links.md) for the URL grammar.

### G. Copy between vaults

`copy` moves contents from one vault into another — whole vault or subtree
— the D21 sharing primitive that `share` is built on. Both ends are vault
references (to pull files out to local disk, use `restore`). Shallow by
default (reuses the source ciphertext and inlines each blob's per-blob key;
the source master key is never shared); `--deep` re-encrypts under the
destination's own keys. A shallow copy that would let new readers decrypt
the source data prints the reader-set delta and asks before proceeding.

```
$ vup copy docs: archive:                  # whole vault → vault (content-addressed)
$ vup copy docs:Old/ archive:2025/         # graft a subtree into another vault
$ vup copy docs:Old/ archive:2025/ --deep  # …re-encrypted under archive:'s keys
$ vup copy album:Photos#3 gallery:         # a past snapshot's subtree, grafted
```

### H. Mount as a filesystem

```
$ vup mount docs: /mnt/docs           # read-only FUSE mount (daemon-side)
$ vup mount docs: /mnt/docs --rw      # writable; debounced flush + publish
```

The mount runs on the daemon and stays active; the CLI verb drives its
lifecycle and unmounts on Ctrl-C. `--debounce-ms` tunes the write-burst
idle window (default 2000). Mounting is whole-vault at the live head today
(a `#snap` or subtree selector on the reference is not yet honoured).

### I. Disaster recovery — the 12 words

Machine stolen, disk dead, everything gone:

```
$ vup recover
```

Type the 12 words, re-authorize the storage account (one browser click for
Sia), and your identity, configuration, vault list, and snapshots come
back. Then `vup restore <vault>: ./out` returns the files, hash-verified.
This requires a durable bootstrap store (e.g. Sia) to have been configured;
a local-only setup has nothing off-machine to recover from (onboarding
warns about this).

### J. Add more storage

Onboarding sets up one store. Stores are configured rarely and referenced
by name from `vault.<name>.data_store` / `meta_store` (set via `vup config`).

```
$ vup store add local cold --path /mnt/backup
$ vup store add s3 offsite --endpoint https://… --bucket b \
      --access-key … --secret-key …
$ vup store add sia sia2               # prompts for the 12 words + one-click OAuth
$ vup store list                       # configured stores
$ vup store info cold                  # backend config, vaults using it
$ vup store rm cold                    # refused while a vault still references it
```

Flags supply everything non-interactively; anything omitted is prompted
(a TTY is required, or exit 3).

### K. Run the daemon as a service

vup is designed to keep its daemon permanently alive — that is what runs
automations, mounts, and serving. Onboarding offers this; you can also:

```
$ vup service install      # systemd user unit (Linux) / LaunchAgent (macOS)
$ vup service status
$ vup service uninstall    # fully reversible; data and config untouched
```

Without a service, any `vup` command still auto-starts a session daemon on
demand — it just does not survive reboots.

### L. When something looks wrong

```
$ vup doctor       # daemon reachable? each store reachable? staging drained?
                   # (is the latest backup actually durable?) service active?
                   # observed peers.
$ vup status       # stores, vaults, running tasks, log dir, durability gauges
$ vup tasks 42     # follow / inspect a task; vup cancel 42 to stop it
$ vup config docs: # inspect a vault's config block (--json / --patch to edit)
$ vup shutdown     # stop the daemon (drains pending uploads, then exits)
```

Logs live at `~/.cache/s5/logs/node.log.<date>` (daily rotation, 7 kept).

## Safety properties

1. **No implicit vault in command position.** Every data verb names its
   vault by reference. The harmless read verbs (`history`, `who`) default
   to the sole configured vault when given no reference, and error if the
   choice is ambiguous.
2. **Restore never surprises.** The target is a required positional; a
   non-empty target needs `--force`.
3. **`grant` is keyset membership (D11).** `--read` (default) adds the
   friend to the vault's read ACL and decryption recipients; `--write`
   also admits its registry writes. There is no separate capability flag
   system.
4. **`revoke` is honest.** Future snapshots exclude the member; nodes stop
   serving them blobs and reject their future registry writes; already
   fetched ciphertext stays readable (keys are not rotated).
5. **Pairing needs out-of-band trust.** The one-time token travels a
   channel separate from any URL.
6. **Paper recovery is shown once, during `onboard`**, and never persisted.
7. **Mount and automations run daemon-side.** CLI exits do not stop them.
8. **Copy across a trust boundary is explicit.** A shallow copy that
   widens who can decrypt the source data confirms first (or `--yes`).

## Vault name validation

- Lowercase ASCII letters, digits, `.`, `_`, `-`.
- 2–64 characters (single letters are reserved for drive letters).
- Must start with a lowercase letter or a digit (user vaults).
- Reserved words: `default`, `none`, `self`, `me`, `paper`, `recovery`,
  `all`. Names starting with `_` are reserved for system vaults
  (`_config:`, `_identity:`).

## URL grammar

`vup share` produces, and `vup join` consumes, URLs of the form:

```
s5://export/<vault-label>?m=<snapshot>#<secret>
```

The decryption secret lives in the fragment (`#…`), so servers never see
it. For the full grammar and future intents see
[`share-links.md`](./share-links.md).

## What lives outside this document

- **On-wire publication format** — `snapshot-publication.md`.
- **Identity model (DIDs, devices, friends)** — `identity-model.md`.
- **Configuration schema** — [`configuration.md`](./configuration.md).
- **Paper recovery operational details** — [`key-recovery.md`](./key-recovery.md).
- **ACL and revocation semantics** — `acl-and-revocation.md`.
