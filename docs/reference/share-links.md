# Share-link URL grammar

**Status:** spec — locks the URL form for `vup share <vault>:` and
the `vup join` consumer. The current scope covers the *frozen
anonymous* share intent; `bond`, `grant=read`, `grant=write`,
sub-tree shares, and recovery URLs slot in as additional intents
and parameters under the same grammar.

Related:
[`cli-workflows.md`](./cli-workflows.md) (the user-facing verbs that
produce and consume these URLs),
`snapshot-publication.md` (the snapshot +
vault root publication that share links target),
`identity-model.md` (DID-based identities for
non-anonymous intents).

## Goals

1. **RFC 3986-valid URLs.** The earlier sketch in cli-workflows.md
   used two `#` characters in one URL, which is not legal — only one
   fragment is allowed. This spec keeps fragment use to its proper
   role (the secret) and moves the snapshot identifier into a query
   parameter. Existing tools (terminals, browsers, link previews,
   chat clients) won't truncate or rewrite the URL.
2. **Universally safe across shells.** No quoting required in bash,
   zsh, fish, nushell, cmd.exe, or PowerShell.
3. **Secret in the fragment.** Browsers and HTTP servers do not send
   fragments to remote endpoints. Pasting a share URL into a chat
   message that previews the link does not leak the secret to the
   preview-fetching server.
4. **One canonical form per intent.** Different verbs produce
   structurally identical URLs distinguished only by the path
   component. A consumer parsing `s5://export/...` knows immediately
   what to do.

## URL grammar (current baseline)

```
s5://<intent>/<vault-label>?<params>#<secret>
```

| Component | Current values | Example |
|---|---|---|
| `<intent>` | `export` (frozen anonymous read-only) | `export` |
| `<vault-label>` | Suggested local nickname for the consumer (e.g. the producer's vault name). Recipients may rename. Same character set as vault names: `[a-z0-9_-]{1,64}`. | `music` |
| `<params>` | `m=<hex>` — encrypted vault-root blob hash (64 hex chars) | `m=abc123…` |
| `<secret>` | The age-x25519 secret key in its native Bech32 form (`AGE-SECRET-KEY-1...`) | `AGE-SECRET-KEY-1...` |

Concrete:

```
s5://export/music?m=9f3a4b8c2d1e5f6a7b8c9d0e1f2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c#AGE-SECRET-KEY-1V0EQK…
```

The Bech32 character set (lowercase + digits + `-`) is URL-safe; no
percent-encoding required for the secret.

## Intents

| Intent | Status | Meaning |
|---|---|---|
| `export` | **shipped** | Frozen, anonymous read-only snapshot. Recipient downloads the blob `m`, age-decrypts with the fragment secret, restores. No future updates; not revocable. |
| `pair` | optional | URL-form alternative to the OTP token. Encodes the inviter's iroh pubkey + one-time code. Equivalent to `vup friend pair TOKEN`. |

A consumer encountering an unknown intent should refuse rather than
assume — intents are mutually-exclusive semantics, not flags.

## Mechanism: `export` (frozen anonymous)

Producer side (`vup share <vault>:`):

1. **Generate ephemeral age recipient.** A fresh
   `age::x25519::Identity` per export call. The secret is what goes
   in the URL fragment; the recipient pubkey is added to the meta
   recipient set for this single export blob.
2. **Re-encrypt the current vault root.** Read the local vault root
   (the vault's fs root node — see
   `snapshot-publication.md` § What gets
   published — age-decrypted with the vault's recipients), re-encrypt
   the same CBOR-serialized node with the vault's existing recipients
   **plus** the ephemeral recipient. Same content, enlarged recipient
   set.
3. **Upload to `vault.local`** and any configured `vault.relays`
   (per `publish.meta` policy). The new blob is content-addressed by
   `M = BLAKE3(age_bytes)` — different from the previous published M
   because age headers are nondeterministic.
4. **Emit the URL** with `<intent>=export`, `<vault-label>` = the
   producer's local vault name (the recipient may rename on
   import), `m=<hex(M)>`, fragment = the ephemeral age secret.

Consumer side (`vup join s5://export/...`):

1. Parse the URL → `(vault_label, M, age_secret)`.
2. Download blob `M` from a configured store.
3. age-decrypt with `age_secret` → vault root fs node (with its
   `TraversalContext` slots).
4. Walk the snapshot, restore (or open read-only).

Properties:

- **Frozen.** The URL targets a specific blob hash; future snaps by
  the producer don't reach this recipient (the producer's regular
  recipient list doesn't include the ephemeral key). Revocation =
  letting the blob age out of the producer's stores; in practice
  capability transfer through a URL is irrevocable.
- **Anonymous.** No identity exchange. The producer doesn't know
  who has the URL; the recipient doesn't authenticate to anyone.
- **Whole-vault by default.** Sub-tree shares (`vup share
  <vault>:<path>`) compose a share-vault + copy (+ optional
  automation); passing `--deep` re-encrypts the named subtree under the
  share-vault's own keys before the age wrapper — covered separately
  under sub-tree shares below.

## Sub-tree shares

`vup share <vault>:<path>` produces a URL that unlocks *only* a subtree
of the snapshot, not the whole vault. It is **composed** from existing
primitives (D21) rather than a bespoke crypto path: mint a share-vault,
`vup copy` the subtree into it (shallow by default, `--deep` to
re-encrypt), optionally keep it live with a scheduled copy, then export
the share-vault via the whole-vault mechanism above. Because the
share-vault is a separate vault, revoking the share re-keys *it* and
never touches your source.

With `--deep`, the copy re-encrypts the subtree under the share-vault's
own keys, so the recipient receives *only* those keys and the rest of
the parent vault stays opaque. That re-encryption pass, sketched:

1. Walk the named subtree of the chosen snapshot.
2. Generate fresh `TraversalContext` keys (independent of the parent
   vault's keys).
3. Re-encrypt the subtree's nodes and leaves with the new keys
   (writes new content-addressed blobs).
4. Build a new vault root pointing at the re-encrypted subtree, using
   the new keys.
5. Generate an ephemeral age keypair for this share.
6. age-encrypt the new vault root to that ephemeral recipient only.
7. Compute `M = BLAKE3(age_bytes)`; upload to a CAS-accessible store.
8. Output URL with the ephemeral age secret in the fragment, same
   grammar as whole-vault export.

Cost: one re-encryption pass over the subtree at share time. For
small subtrees this is trivial; for large subtrees it is noticeable
but bounded. Cached if the same subtree is shared multiple times.

The recipient gets *only* the new keys via the URL — they never see
the parent vault's keys, so the rest of the vault stays opaque even
if the recipient later obtains parent-vault blob hashes (e.g., from
the same blob store). This is the property that makes per-subtree
sharing semantically meaningful.

## Discoverability of the blob

The URL gives the recipient `M` (what to fetch) and the secret (how
to decrypt) but not directly *where to fetch from*. Two ways the
recipient can find the blob, in order of preference:

1. **Pre-configured shared store.** Recipient has a `[store.*]`
   entry that the producer also uses (typical for friends/family
   pre-coordinated to the same Sia bucket / S3 bucket / NAS path).
   `vup join` tries each configured store.
2. **Public store hint** (optional `&store=<address>` parameter).
   Producer specifies an explicit store the recipient should query
   (e.g. an HTTPS S3 bucket).

For now only option 1 is implemented; the URL stays minimal.

## Implementation surface

- Producer logic: `s5_node::export::run_export` and the export-vault
  RPC; CLI verb `vup share` — `s5_vup::cmd::share::run_share`
  dispatches to `s5_vup::cmd::vault::run_export` for the whole-vault
  case and composes vault + copy + automation for a subtree.
- Consumer logic: `vup join` — `s5_vup::cmd::stubs::run_join` over the
  `s5_node::share::join_export` RPC handler.
- URL parser: `s5_node::share::ExportUrl`.

## Versioning

If the URL grammar ever needs a breaking change, add `&v=<n>` as a
query parameter. The default and only current version is `v=1`
(implicit when omitted). Consumers that don't understand a `v` value
they see should refuse the URL rather than guess.
