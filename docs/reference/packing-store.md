# Packing store — content-addressed pack bundling

**Status:** implemented (format + index + reconstruction + persistence + wasm),
plus a **decided-but-unimplemented** garbage-collection architecture (2026-06-30).
The packing store (`stores/packing`, crate `s5_store_packing`) bundles many small
content-addressed blobs into large packs before they reach a slow or expensive
backend, and serves reads back out of those packs.

Related: [`architecture-directions.md`](./architecture-directions.md)
(where packing/tiered backends sit in the store stack),
[`registry-durability.md`](./registry-durability.md) (the HEAD enumeration that
recovery — and GC — root on), [`snapshot-publication.md`](./snapshot-publication.md)
(snapshot trees = the reachability graph), [`compression-and-chunking.md`](./compression-and-chunking.md)
(why blobs are small in the first place).

---

## 1. Why packing exists

Sia stores data in **4 MiB sectors**, erasure-coded into slabs. A backup at S5's
default ~64 KiB chunking produces millions of tiny blobs; writing each as its own
erasure-coded object wastes a whole slab per blob (~40 s, ~0.002 MiB/s) and is
economically absurd. Packing bundles many small blobs into one large
(40–256 MiB) content-addressed **pack** so each upload fills real slabs — ~1 MiB/s
and bounded memory (staged blobs spill to disk, not RAM).

The same mechanism is useful anywhere small-blob-over-slow-backend is the shape:
it's a generic decorator, not Sia-specific.

## 2. What it is — a content-addressed `BlobsReadWrite`, not a `Store`

`PackingStore<B>` is a `BlobsRead + BlobsWrite + BlobsDelete` provider keyed by
the **blob's own BLAKE3 hash** — *not* a path-based `Store`. There are no
arbitrary string paths: a blob is addressed by its hash, the in-pack index key is
that hash's 12-byte prefix (`hash[..12]`), uploads hash their own input
(`blob_upload_bytes = Hash::new`, self-verifying), and `blob_sync` lives on
`BlobsWrite` so the vault flushes through the same `dyn Blobs` handle it reads/writes.

**Why not a `Store`?** A `Store` is a path→bytes map; packing is content-addressed
by construction. Modelling it as a `Store` forced a synthetic path namespace and
hid that the address *is* the hash. `s5_core` intentionally hides `BlobStore`
behind the `dyn BlobsRead/BlobsReadWrite` traits; the packing store is the payoff
— it plugs into the vault as `Arc<dyn Blobs>` directly, with no path-store view.

### The three stores it composes

`PackingStore<B>` holds one generic backend and two `Arc<dyn Store>` locals:

| field | type | role |
|---|---|---|
| `blobs: B` | `B: BlobsReadWrite` | durable, content-addressed backend for whole **pack bodies** (e.g. a `BlobStore` over indexd/Sia). The hot read path; stays generic for monomorphization. |
| `index_cache` | `Arc<dyn Store>` | **local** cache of the pack index — per-pack manifests, the bulk snapshot, and `todo` markers. Rebuildable from the bodies; never load-bearing. |
| `staging` | `Arc<dyn Store>` | **local** scratch: blobs live under `hex(hash)` until a pack fills, then stream to `blobs` and are deleted. |

**Why only `B` is generic** (`PackingStore<B>`, not `<B, M, S>`): the index cache
and staging are always plain local stores, so the two extra type params were pure
verbosity. `Arc<dyn Store>` costs a vtable hop that is noise next to their disk
I/O. `B` stays generic because it's the perf-sensitive, content-addressed,
possibly-remote backend.

**Why staging is a `Store`, not a blob trait** (it *is* content-addressed in
spirit — keyed by `hex(hash)`):
- **The caller already knows the key.** Packing computes `Hash::new(&bytes)`
  before staging (it needs the key for the dedup check + the pending map), then
  `put_bytes(hex(hash), bytes)`. The content-addressed write API inverts control —
  `blob_upload_bytes` *re-derives* the hash — so routing staging through it means a
  redundant BLAKE3 pass over **every** staged blob, on the hottest path. The
  path-store API stores under the key we already hold.
- **It belongs with `index_cache`, not `blobs`.** The three deps split into one
  durable content-addressed backend (`blobs`) and two local keyed scratch/cache
  stores. `index_cache` *must* be a path `Store` (it holds non-hash keys: the
  snapshot, the todo sibling). Staging is its sibling; making it a blob trait
  splits the "local keyed byte-slots" concept for no gain.
- Staging is ephemeral (write → read-once → delete); it wants the minimal
  keyed-slot API, not the blob traits' self-hashing/verification machinery.

## 3. Pack body format — a prepended, self-describing header

A pack body is `header ++ data`. The header is **prepended**, not a trailer
(`stores/packing/src/manifest.rs`):

```
  byte 0   MAGIC = "S5.pro" 0x5b 'P'   = [53 35 2e 70 72 6f 5b 50]      8 B
  byte 8   reserved (zero, 3 B) | VERSION = 1 (byte 11)                 4 B
  byte 12  blob_count: u32 LE                                           4 B
  byte 16  member[i] = hash_prefix[12] | offset: u32 LE   (sorted by prefix)   N×16 B
  then     end_offset: u32 LE   (= total pack size)                     4 B
  then     zero-pad to the next 16 KiB boundary (DATA_ALIGN)
  aligned  data = blob0 ++ blob1 ++ … ++ blobN-1   (same order as members)
```

- **Offsets are absolute** → a reader does a direct ranged GET `[off[i] .. off[i+1])`,
  no cumulative sum; the last member runs to `end_offset`, which is also the pack
  size, so the header is **fully self-describing**.
- **`pack_hash` is NOT in the header** (it would be circular — the hash is over the
  whole body); it's recovered as the body's content address.

**Why prepended, not a trailer.** A reader resolves any member with pure ranged
GETs from offset 0 — read the 16 B fixed prefix for the count, then the
`16 + N·16 + 4` B header region, then the member's slice. **No pack-size lookup,
no tail seek, never a full-pack download.** That's exactly what a cold device or a
share-recipient (holding only a `pack_hash`) needs. A trailer's only advantage is
append-friendliness, which doesn't apply: packs are written once (immutable) and
every member is known before the flush, so the header is computed up front.

**Why the data start is 16 KiB-aligned but blobs are not individually padded.**
Aligning the data start lets one generous front GET grab the whole header for a
typical pack. Per-blob alignment would only waste space: **Sia's 4 MiB sectors
dominate read granularity**, so sub-sector alignment buys nothing on the durable
path. `u32` offsets cap a pack at 4 GiB — fine for 256 MiB targets; asserted at flush.

## 4. The in-memory index

Lookup answers "which pack holds blob `H`, and at what offset/length." The index
(`Index` in `lib.rs`) is four structures (three lookup tiers + the discovery set):

| tier | structure | holds |
|---|---|---|
| `bulk` | boomphf MPHF `key → slot` + `pack_idx: Vec<u32>` | settled packs, folded into the perfect hash |
| `recent` | `HashMap<Key, u32>` | packs flushed since the last rebuild (the MPHF is static) |
| `pending` | `HashMap<Key, PendingBlob>` | blobs staged, not yet packed |
| `todo` | `HashSet<[u8;32]>` | pack hashes **known but not yet read** (see §6) |

`locate` tries the bulk candidate, then the recent map; `pending` answers reads of
not-yet-packed blobs.

### Why a minimal perfect hash (and why boomphf specifically)

The naive index is a `HashMap<prefix → pack>`, ~50 B/key → ~800 MB at 16M blobs
(a 1 TiB / 64 KiB backup) — too much for a phone. A benchmark
(recorded in the storage-rewrite handoff) compared, at 16M blobs / 8889 packs:

| candidate | hit | miss | bits/key | portable? |
|---|---|---|---|---|
| per-pack BinaryFuse8 + binary search | 57 µs | 132 µs | 9 | ✅ |
| per-pack BinaryFuse16 + binary search | 48 µs | 97 µs | 18 | ✅ |
| **ptrhash** | **44 ns** | **49 ns** | 2.99 | ❌ (gxhash needs HW AES) |
| **ph (FMPH)** | 307 ns | 288 ns | 2.80 | ✅ |
| **boomphf (BBHash)** | **88 ns** | **97 ns** | 3.00 | ✅ |

- **Per-pack probabilistic filters don't scale.** They're O(packs) per lookup, and
  the false-positive rate *compounds* (bf8's 0.4% × 8889 packs ≈ 36 wasted binary
  searches per miss). A full 16M-blob restore would spend ~13 min in index lookups
  with filters vs ~0.5 s with an MPHF. A global MPHF is O(1) in pack count.
- **`ptrhash` rejected for production:** its `gxhash` dependency `compile_error!`s
  without hardware AES, so it's un-buildable on phones / non-AES targets without
  global `RUSTFLAGS`. (A target-gated `ptrhash` cargo feature is a noted TODO; the
  44 ns vs 88 ns gap is invisible end-to-end next to the blob's network read.)
- **`ph` (FMPH) rejected:** smallest, but ~3.5× slower lookups *and* observed
  nondeterministic `None` for in-set keys across runs — a correctness smell for a
  recovery-critical index.
- **`boomphf` chosen:** portable pure-Rust, robust, 88 ns (≈650× over filters). Use
  `try_hash` (returns `Option`), never `hash` (which *panics* on non-members).

The lookup is dwarfed by the blob's actual read, so the win is **RAM, not speed**.

### Why there is no separate `verify` array

An MPHF maps *any* key to *some* slot, so a non-member must be rejected. The early
design carried `verify: Vec<Key>` (slot → key, 12 B/key) to do that. It was
**redundant**: `locate` already binary-searches the candidate pack's sorted member
table to get the offset — and that search *is* the membership check (a non-member
routes to an arbitrary pack whose search misses). So `Bulk` is just
`{ mphf, pack_idx }` (~4.4 B/key, down from ~16.4). The one subtlety: `locate` must
fall through to the `recent` map on a candidate miss, because without `verify` the
MPHF always returns *some* bulk slot, which would otherwise shadow a recent-only
key. `contains` and staging reconciliation go through `locate` (definitive), never
a bare candidate — a false positive there would wrongly drop a staged blob.

### Rebuild cadence

New packs land in `recent`; once it exceeds `RECENT_REBUILD_KEYS` (200k) it folds
into a freshly-built `bulk` MPHF. The build is CPU-bound, so it runs off a blocking
thread (`spawn_blocking` on native; inline on wasm — see §7) over a *snapshot* of
the keys; the write lock is held only to swap the result in. **Known limitation:**
each rebuild rebuilds the whole MPHF, so total build work over a growing backup is
superlinear (~N²/200k). It is off-lock background CPU, not latency, and the
persisted snapshot (§5) removes the *restart* cost — a tiered/incremental MPHF is a
deferred scalability option, not a current need.

### Duplicate keys can never panic the build (why the store stays openable)

`boomphf::Mphf::new` **panics** on a duplicate key (it retries a bounded number of
times, then aborts). A key *can* legitimately appear in two packs: two devices
packing the same content-addressed blob beside different neighbours before they
sync, or a pack re-enriched after a `todo` marker survived a failed delete. Left
unguarded, one duplicate would make `rebuild_index` panic and the whole store
**un-openable** — catastrophic, since it lands on the recovery path. So `build_bulk`
folds its input to unique keys *before* the MPHF build (last-wins on the pack index;
both packs hold identical bytes for a shared key, so either serves the read). That
one choke point makes the "each key → one MPHF slot" invariant structurally true no
matter how a duplicate arose. As a second line of defence, `load_todos` skips
re-adding an already-enriched pack, so the common (single-device) case never
produces a duplicate in the first place, and the write path errors rather than
duplicating (see §6). The MPHF is also probed only with `try_hash` (returns
`Option`), never `hash` (which *panics* on non-members).

## 5. Persistence — the index cache (browser-safe, never load-bearing)

The **durable** index is each pack body's prepended header (§3). The `index_cache`
store is purely a *local cache* of that index so warm restarts are fast; it can be
lost and rebuilt from the bodies. It holds:

- **Per-pack manifests** at `manifests/<path_for_hash(pack_hash)>` — one pack's
  member table, written on flush.
- **A consolidated bulk snapshot** at `manifests/index-snapshot.v1` — postcard of
  `{ version, packs, mphf }`, written after each rebuild. On a warm open it's
  installed wholesale (no per-pack reads, no MPHF rebuild) and only the *tail* of
  per-pack manifests flushed since the snapshot is folded in. The slot arrays
  (`pack_idx`) are *not* persisted — they're reconstructed from the MPHF + packs on
  load (one `try_hash` per member; skips the costly `Mphf::new`), halving the blob.
- **`todo/` markers** at `manifests.todo/<hex(pack_hash)>` — discovered-but-not-yet-
  enriched packs (§6), in a sibling namespace so the per-pack scan skips them.

> Naming: the field is `index_cache` / config `index_cache_prefix`; the on-disk key
> prefix stays `"manifests/"` for cache continuity. The old "manifests store"
> framing was misleading — there are no durable manifest *objects*, only this cache.

**Why a `Store` blob, not mmap/epserde.** The packing store must also work in the
browser (to read shared packed files), where `mmap` and file-backed zero-copy
deserialization don't exist. Persisting to a portable `Store` blob via `postcard`
(a `no_std`/wasm-friendly serde format) works on any cache backend — memory, local
disk, IndexedDB, or a composed tier. The snapshot is a *pure cache*: a missing,
stale, version-mismatched, or corrupt blob is ignored and the index rebuilds from
per-pack manifests (and ultimately from headers). It is never a durability
dependency, so correctness never rests on it.

## 6. Reconstruction — discover cheap, enrich async

A cold/wiped device opens with an empty index cache, so the membership index is
empty and packed vault roots would read as "not found." Recovery rebuilds the
index from pack **headers**. The model is the same "a cache must not lie" principle
as `IndexdStore` sync-on-open, one layer down — split into a cheap discovery step
and an async enrichment step:

- **`note_pack_hashes(hashes)`** — record each unknown pack hash as a persisted
  `todo/` marker + an in-memory `todo` entry. No header reads, no blocking. Fed the
  blob backend's enumeration (`blob3/<hash>`) once the backend itself is synced. (A
  `PackingStore` over `B: BlobsReadWrite` cannot enumerate `B` — enumeration is the
  backend's job; the indexd layer owns it and passes the hashes in.)
- **`enrich()`** — drain `todo` concurrently (front-bytes header read per pack),
  write each manifest, fold it into the live index. Single-flighted so a storm of
  read-misses triggers one drain.

### Each todo resolves to a terminal state — and the magic check is what guarantees it

`enrich_one` reads the first 16 B, then the exact header region, and classifies:

- **`Enriched`** — valid header → write manifest, `add_pack`, delete the marker.
- **`NotAPack`** — bad magic / version / decode → **delete the marker** (terminal).
- **`Transient`** — the header couldn't be *read* (network/I/O) → **keep the marker**,
  retry on a later pass.

The `S5.pro` magic check is not just hygiene: a non-pack hash (a foreign object on
a shared account) can *never* enrich, so if a magic failure merely "skipped with a
warning" and left the marker, `todo` would never drain → every negative answer
would block and re-fetch the bad hash forever (see below). Dropping on a definitive
non-pack is what lets `todo` reach empty; keeping on a transient failure is what
avoids confusing "couldn't read it" with "isn't a pack."

### Honest-negative lookups (the correctness rule)

While `todo` is non-empty, a lookup miss might be a blob living in an un-enriched
pack. So:

- **A positive answer is always trustworthy immediately** — a hit in `bulk`/`recent`
  serves at once, even with packs pending (a blob can legitimately live in several
  packs after a dedup miss).
- **A negative answer is only trustworthy once `todo` is empty** — `blob_download`
  miss, `blob_contains == false`, and the `blob_upload_bytes` dedup check all first
  `enrich()` (drain the pending packs), then re-check (`locate_honest` /
  `contains_honest`). Steady state (`todo` empty) is a single read-lock, no work.

**Writes block on a negative too** (decided): a dedup miss while packs are pending
would re-stage — and re-upload to Sia — a blob that already lives in an un-enriched
pack. A redundant Sia slab is exactly what packing exists to avoid, and recovery
rarely overlaps an active backup, so the warmup stall is a non-issue.

### When enrichment can't finish: error, don't lie (the retryable-negative rule)

`enrich()` is best-effort — a `Transient` failure *keeps* the marker, so `todo` can
still be non-empty when it returns. A negative lookup then has three cases, and the
third is a deliberate design decision:

1. **found after enrich** → the positive (always trustworthy, never delayed).
2. **not found, `todo` now empty** → an *authoritative* negative (`Ok(false)` /
   `Ok(None)`): every known pack was read and none holds the key.
3. **not found, `todo` still non-empty** → some known packs are *unreadable* right
   now (a Sia/network blip kept their markers). The blob might live in one of them,
   so absence is **unprovable**. The lookup returns a **retryable error**
   (`unreadable_packs_error`) — never a false "not found."

Why error rather than answer `false`? Two reasons, both about not lying on the
recovery path:

- **A false "not found" is silent data loss.** During a transient backend outage a
  `vup recover` read of a genuinely-present packed root would report "gone" instead
  of "backend unavailable — retry." The error carries the truth; the retained `todo`
  markers re-enrich on a later read, so the store self-heals once the backend
  returns.
- **On the write path a false negative mints a duplicate pack.** The dedup check
  treats "not present" as "new," re-stages, and re-uploads → a second pack now
  carries the same blob. That is precisely the condition that could otherwise
  **panic the MPHF build** (§4). Erroring the write leaves staging untouched, so no
  duplicate is created — the retryable-negative rule and the `build_bulk` dedup
  guard reinforce each other.

We deliberately do **not** retry-with-backoff *inside* the lookup: retry policy and
timeouts belong to the caller, and blocking a read indefinitely on a dead backend is
worse than a clean, classified error. Positives are never blocked or delayed — only
would-be negatives pay the enrichment/uncertainty cost, and only while packs remain
unread.

Open of `create_raw_store` therefore **does not block**: it `note_pack_hashes`(the
enumerated bodies) then enriches in a detached task; the negative-gate is the
correctness backstop, so `vup recover` reading a packed root simply waits for the
pack it needs. `reconstruct_from_headers` is kept as a thin `note + enrich` wrapper
for one-shot "rebuild now" callers (benches/tests).

## 7. WebAssembly compatibility

The crate compiles to `wasm32-unknown-unknown` so a browser can read shared packed
files. Three native-only things are gated:

- **boomphf** is taken with `default-features = false` (drops the `parallel`
  feature and its `rayon` dependency, which doesn't build on wasm). We only ever
  call the single-threaded `Mphf::new`, never `new_parallel`, so nothing is lost.
- **`tokio`'s `fs` feature** is a `cfg(not(target_arch = "wasm32"))` target dep; its
  only user, `blob_upload_file`, is already wasm-gated.
- **`rebuild_index`** uses `spawn_blocking` on native and an **inline** build on
  wasm (no thread pool there; a browser share-reader indexes only a handful of
  packs, so the build is tiny — owner-scale rebuild is a native concern).

`postcard`'s `alloc` feature provides `to_allocvec` and is wasm-friendly.

## 8. Garbage collection — architecture and rationale

> **Status: decided, NOT implemented.** `blob_delete` and `blob_retain` return
> `NotSupported`. This section records the agreed architecture and, importantly,
> *why* the simple/conservative design is the correct one rather than a concession.

### 8.1 From first principles: deletion in a decentralized store

To delete safely you must know a blob is referenced by **no** retained root. In a
distributed, multi-writer, content-addressed-with-dedup system there are exactly
two ways to know that:

1. **Track liveness explicitly** — pinning / reference counts.
2. **Recompute liveness** — mark-sweep: walk every retained root, delete the rest.

**Why not pinning / refcounts.** In a decentralized setting this is not merely
"extra state" — it's distributed *mutable* state. Counts must be
incremented/decremented by every writer across every device, transactionally; a
lost decrement leaks, a lost increment or double-decrement is **silent data loss**.
Getting it right needs consensus or CRDT counters — exactly what a lazily-syncing
multi-device account doesn't have. So refcounts here are not just ugly, they're
*unsound* without machinery that defeats the point.

**Why not diff-based GC** ("walk-diff old vs new vault, delete what was removed").
Seductive but unsafe: a Merkle diff is cheap *because* it skips subtrees whose hash
is unchanged — and "unchanged subtrees" is exactly where a surviving reference to a
"removed" blob hides. With vault-scoped (and cross-vault) content dedup, "removed
from path X" ≠ "dead." Turning a diff candidate into a confirmed-dead requires
answering "is this hash referenced *anywhere* retained?" — which is the full
reachable set again. So diff-then-confirm collapses into a full mark with extra
steps; the diff is only useful as an *input to refcounting*, which we reject.

### 8.2 Mark-sweep is the native fit

Mark-sweep's one hard requirement is a **complete, consistent enumeration of all
live roots** at sweep time. This architecture already has that primitive: the
**registry** is the authoritative set of all HEADs, and walking it is *the same
walk recovery already does* (`reconstruct_from_indexer` → all HEADs → their trees).
So GC and cold-recovery share one mechanism; GC is not a new global-view system.

Two properties seal it:

- **Crash-idempotent, fail-safe.** A mark-sweep can be interrupted, re-run, or run
  twice — worst case is *leaked garbage* (reclaimed next pass), **never lost data**.
  Refcounts have no such property.
- **The concurrency window has a stateless fix.** The only gap is a write landing
  after the root set was enumerated. A **generation / timestamp floor** closes it:
  never reclaim a pack newer than the moment the roots were enumerated. No
  coordination — just "don't touch recent packs," set old enough that every device
  has provably synced those roots.

**Granularity is per-account, not per-vault.** Reachable = union over *all* retained
HEADs in the account; a per-vault sweep would wrongly reclaim a blob another vault
in the same account still references (cross-vault dedup is real). One account = one
owner (one AppKey), so there is no cross-*owner* pack sharing and thus no
multi-party-consent problem.

**Shares are not a liveness source** (decided). The mark is pure snapshot-tree
reachability; pruning a snapshot breaks any outstanding share of a blob unique to
it, by design. This keeps the mark simple (no share registry, no extra source).

### 8.3 Why GC is *purely* a cost optimization — the keyed-convergent-encryption insight

This is the realization that makes the conservative design correct rather than a
compromise. Every blob is encrypted, and the per-blob key is derived from **the
plaintext hash *and* a per-vault secret** (keyed convergent encryption), not the
plaintext hash alone. Keys are *reference-scoped*: the only places a blob's key
exists are in the parent reference that points at it (which carries it) and
derivable by a holder of both the plaintext and the secret. Decryptability flows
top-down from a root.

Consequences:

- **Orphaning ≈ erasure for non-secret-holders.** Update the root to drop a subtree
  and the orphaned ciphertext becomes inert to anyone lacking the vault secret —
  the bytes remain on disk, but there is no path to the key. The storage operator
  only ever saw ciphertext, so orphaning reveals nothing new; a prior-key-holder
  could always have copied the data before deletion (no system protects against
  that). Physical deletion adds no confidentiality the encryption + reference-drop
  didn't already provide.
- **The per-vault secret is what makes it robust** — and is why this is *not* the
  weak form of convergent encryption. Plain convergent encryption (key = plaintext
  hash alone) is confirmable: guess the plaintext, recompute the key, decrypt.
  Mixing in the secret defeats that — an adversary who guesses the plaintext still
  can't derive the key. So confidentiality does **not** depend on content entropy.
  It also scopes **dedup to per-vault** (same plaintext in two vaults → different
  secret → different ciphertext → different address), deliberately trading global
  dedup — itself a privacy leak — for confidentiality. (Load-bearing assumption: the
  combine is a keyed hash / KDF such as BLAKE3 keyed mode, not a naïve concat.)

Therefore **GC is a storage-cost optimization only** — not a confidentiality
mechanism, not on any correctness or security path. Which flips the risk calculus:

- The risk is maximally asymmetric toward safe: wrongly deleting a live blob is
  unrecoverable data loss; failing to delete a dead blob costs a few cents of
  storage. So the design must be **maximally conservative** — "leak rather than
  lose," floor-gated, full-mark-only-when-convenient.
- There is **no justification for risky machinery** (distributed refcounts,
  incremental diff-GC) to optimize a cost. Mark-sweep-with-floor run rarely is the
  *correct ceiling* of effort, not a shortcut.
- GC is **opt-in and lazy**: a deployment can run with monotonic growth and lose
  nothing but money. This is why `blob_delete`/`blob_retain` stay `NotSupported`
  defaults and were not rushed.

### 8.4 The mechanism / policy split

- **Packing = mechanism.** It exposes (when built) `blob_retain(reachable, floor)`
  + the sweep/compact primitives, policy-agnostic — zero knowledge of snapshots,
  vaults, registries, or shares. It is *handed* the complete reachable set and a
  floor, and reclaims below the floor. Its only GC-specific bookkeeping is stamping
  each pack with a generation/timestamp so the floor is meaningful. To delete pack
  bodies it needs `B: BlobsReadWrite + BlobsDelete` (the indexd `BlobStore` already
  implements delete).
- **Vault = policy.** It computes the complete reachable set (walk all retained
  HEADs — the recovery primitive) and chooses the floor and retention policy. The
  liveness *definition* (full mark) is fixed; *how* the set is produced (full walk
  now; refcount-derived deltas only if a real corpus proves the walk too slow) is a
  vault concern, fully separable from the packing mechanism.

### 8.5 Planned phasing (when implemented)

- **Phase A — whole-dead-pack sweep (cheap, no re-upload).** Any pack with *zero*
  reachable members is entirely dead → delete its body + manifest, drop it from the
  index, then `rebuild_index` + re-snapshot. Handles the common case (prune old
  snapshots → whole packs die) at delete-only cost.
- **Phase B — partial compaction (expensive, threshold-gated).** A pack with some
  live + some dead members → rewrite the live ones into a fresh pack, upload, delete
  the old. Each rewrite is a new Sia slab, so gate on `dead_fraction ≥ threshold`.

Because removing/replacing packs shifts the `packs` Vec positions that `bulk`/
`recent` index, a retain pass ends with a full `rebuild_index`; so GC is a
maintenance pass run under the flush lock / on idle, not concurrent with an active
backup.

## 8b. Resource bounds — RAM & disk

"Simple by default, powerful when needed": the store must never surprise a phone
*or* a PiB owner. The one genuinely unbounded resource — the staging spool — is now
capped; the rest are bounded and scale with a knob.

**Staging spool (disk) — the backpressure watermark.** `blob_upload_bytes` returns
as soon as a blob is durably *staged locally*; the pack flush to the backend is
async. So without a bound, an ingest faster than the sustained upload rate spools
the whole backlog to local disk (bounded only by free space — we saw 3.6 GiB pile
up before a publish barrier). The fix: a live `staged_bytes` counter + a watermark
`staging_max_packs × max_group_size` (default 4 × 256 MiB = **1 GiB**). Once staging
reaches it, `blob_upload_bytes` blocks (nudging the flush loop) until `flush_group`
uploads a pack and decrements the counter — so ingest **self-throttles to the upload
rate** instead of filling the disk. Requires a running flush loop (the daemon spawns
`run_upload_loop`) or `flush_on_put`; `staging_max_packs = 0` disables it. This is
the only place a backend→ingest backpressure signal belongs.

**Everything else is bounded** (formula → number at 16M blobs ≈ 1 TiB @ 64 KiB):

| Resource | Bound | ~ at 16M blobs |
|---|---|---|
| Upload RAM | `max_inflight × total_shards × 4 MiB` (knob, default 8 × 30) | ~960 MiB |
| Resident index | `packs` member tables + bulk MPHF/`pack_idx` ≈ ~20 B/blob | ~330 MiB |
| Rebuild transient | sort-dedup over `(key,pack_idx)` pairs (16 B) + unique keys — **no hash table** | ~512 MiB (off-lock) |
| Snapshot save/load | whole postcard blob in one buffer | ~330 MiB (transient) |
| Recovery (cold boot) | pack-hash Vec + `todo` set, by *pack* count | ~hundreds MB at PiB |

Notes: the rebuild dedup deliberately **sorts** rather than building a
`HashMap`/`HashSet` over every key — at 10M+ keys a hash table is a multi-hundred-MB
transient that sorting avoids. The `max_inflight` figure is the sia-SDK default (a
documented per-device knob: lower on phones). A corrupt/huge member-count can't force
a giant read — the header fetch is a ranged GET clamped to the actual body size, then
`decode_header` errors.

**Known RAM cliff (not yet fixed): whole-file buffering.** `FileChunkingStrategy`'s
default is `None` (one blob per file), so a large file ingested without a chunking
route becomes a single in-RAM `Bytes` — worst case `max_concurrent_ops × largest_file`.
The Gearhash CDC chunker (64 KiB target, `MAX_CHUNK_SIZE`-clamped) exists and
`node.rs` flags it as the intended default; making CDC the default (never `None` above
a size cap) closes this. See §9.

## 9. Deferred / open work

- **Default to Gearhash CDC chunking (never unbounded `None`)** — bounds ingest RAM
  to `max_concurrent_ops × MAX_CHUNK_SIZE`; today the enum default is `None` (whole
  file → one blob), the RAM cliff above. The CDC chunker already exists.
- **Lazy / on-disk member tables** — the resident index is dominated by the per-pack
  member tables (~16 B/member). Keeping only the MPHF + `pack_idx` resident (~4.4
  B/key) and reading a candidate pack's member table from the on-disk manifest cache
  on demand would cut resident RAM ~5× at the cost of a local read per cold lookup.
- **Chunked snapshot save/load** — serialize/read the bulk snapshot in bounded windows
  (or re-derive member tables from per-pack manifests on load) so it isn't a single
  ~hundreds-MB buffer at extreme scale.
- **Pack-upload cold-start timeout/retry (B3)** — the first-ever upload on a fresh Sia
  account can stall before hosts warm / contracts form, with no timeout → apparent
  hang. Warm-before-first-flush or a per-pack timeout surfacing a retryable error (the
  background flush loop then retries once warm).

- **GC** (§8) — designed, unimplemented. Phase A first.
- **boomphf mmap/epserde or `ptrhash` cargo feature** — only if profiling on a real
  10M+ blob corpus shows the rebuild or load cost matters; the snapshot already
  removes the restart cost.
- **Tiered/incremental MPHF** — to make rebuild work linear rather than ~N²/200k;
  off-lock background CPU only, so low priority.
- **Prune snapshot-covered per-pack manifests** — saves ~2× index-cache disk but
  removes the per-pack fallback when a snapshot is corrupt; the disk dup is small
  relative to actual data, so deprioritized.
- **Single speculative header GET.** Reading a pack header is currently two ranged
  GETs from offset 0 (16 B fixed prefix → member count → the exact header region).
  Since the data region starts on a 16 KiB boundary, one speculative ~16 KiB front
  GET would cover the entire header in a *single* round-trip for any pack whose
  member table fits under 16 KiB (the common case), falling back to a second GET
  only for very large member counts. Halves header-read round-trips on the
  enrich/recovery and share-read paths. Low effort, bounded; not yet done.

## 10. Where things live

| What | Where |
|---|---|
| Pack body format | `stores/packing/src/manifest.rs` |
| Index, MPHF, reconstruction, persistence, GC stubs | `stores/packing/src/lib.rs` |
| Bin-packing heuristic | `stores/packing/src/binpack.rs` |
| Sia store wiring (discover + background enrich) | `s5_node/src/lib.rs` `create_raw_store` |
| Live recovery bench | `stores/indexd/tests/bench_indexd.rs` (`bench_recover_from_headers`) |
