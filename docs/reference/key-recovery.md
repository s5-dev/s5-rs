# Key recovery

**Status:** design — defines what survives loss of devices and what
the recovery flows actually do. Aligned 2026-05-19 with the four-key
identity model; the key model and the registry/ACL verifier are
specified canonically in `identity-model.md` /
`acl-and-revocation.md` and are not
restated here.

Related: `identity-model.md` (DID, four-key
model, identity_secrets vault),
`acl-and-revocation.md` (verify chain,
suspend/evict, two-tier re-key),
`snapshot-publication.md`
(`recovery_secret` in vault root → recovery registry entry),
[`configuration.md`](./configuration.md) (per-vault `relays` for
offline-availability of recovery material).

## What can be lost

Three failure modes, each with its own recovery path:

| Lost | What survives | How to recover |
|---|---|---|
| One device | Other devices still work | New device set up via identity_secrets vault; identity bundle updated to drop lost device |
| All devices, paper retained | Paper recovery seed | New device set up from paper; identity_secrets vault decrypts; DID master key restored |
| Paper + all devices | Public CAS blobs (encrypted, useless without keys) | Nothing recoverable |

The architecture assumes the user keeps the paper recovery seed
offline. Without it, full device-loss is unrecoverable. There is no
server-side key escrow.

**Per-identity scope.** A node holding multiple owned identities has
one paper passphrase + recovery flow per identity. Losing the paper
for `personal` does not affect `work` (or vice versa). This is the
deliberate isolation property of multi-identity (see
`identity-model.md` § Multi-identity); sharing
one paper across identities would defeat the separation.

## Two layers of "recovery"

The phrase covers two distinct things, often conflated:

1. **Identity recovery** — recover the DID (master signing key) so
   you can publish updates, add new devices, write to vaults.
   Required when all devices are lost.
2. **Vault content recovery** — recover the data inside a specific
   vault (read its files, restore them). Required after device loss
   *or* after losing access to a vault for any other reason.
3. **Storage-access recovery** — regain the credentials to *reach* a
   backend (the indexd AppKey, S3 keys). Lower-stakes than the other two:
   these protect availability, not confidentiality (see
   `identity-model.md` § Special vaults), so they're
   recovered by decrypting the warm `stores` vault with the paper key — or,
   for the default account, re-derived from `stores_seed` as a fallback.

These have different mechanisms. Identity recovery uses `identity_secrets`
(decrypted with paper). Vault content recovery uses the paper recovery age
key directly as a vault recipient — no `identity_secrets` needed. Storage
access uses the `stores` vault (or the default-account fallback seed).

## The paper recovery age key

On `vup onboard`, the user generates a paper passphrase. That passphrase
deterministically derives an **age recipient key** via a
memory-hard KDF (argon2id with conservative parameters):

```
recovery_age_secret = argon2id(
    passphrase,
    salt = "s5/recovery/v1",
    m_cost = 256 MiB,
    t_cost = 3,
)
recovery_age_pubkey = age::x25519::Identity::from(recovery_age_secret).recipient()
```

The pubkey is a permanent member of the user's identity bundle's
`age_recipients` array. So when anyone (the user, a peer they're a
member-of-vault with) encrypts a vault root for the user's DID, they
automatically wrap a copy for the recovery key alongside any device
keys.

The user writes the paper passphrase down (offline, geographically
separated from devices). The CLI shows it once during onboarding;
it's never persisted.

## Vault root has its own recovery secret

> **2026-06-25 discovery redesign.** The `recovery_secret` → `vault_id` →
> `recovery_signing_key` scheme below is **retired as the discovery mechanism**.
> Vaults are now found master-anchored (the identity bundle, `identity_secrets`,
> `stores`) or via the `identity_secrets` catalogue (user vaults) — see
> `special-vaults.md` and
> `snapshot-publication.md` § Discovery. So paper
> recovery is: `mnemonic → master pubkey + paper age key → identity_secrets
> (constant id) → catalogue → every vault`, not the relay-blob scan + recovery
> entry described in the flows below. A `recovery_secret` survives only for
> opt-in **public "if you know you know"** vaults; this section documents that
> retained case and the historical scheme.

The vault root is the vault's encrypted fs root node — an
`s5_fs_v2::Node` (Transparent variant) whose `TraversalContext.keys`
slot map carries the per-vault secrets at well-known slots:

| Slot | Const | Purpose |
|---|---|---|
| `0x10` | `KEY_SLOT_LEAF` | File content (leaf blob) encryption |
| `0x11` | `KEY_SLOT_NODE` | Metadata (prolly-tree node blob) encryption |
| `0x12` | `KEY_SLOT_RECOVERY` | Recovery seed — derives `vault_id` + `recovery_signing_key` |

This is the existing fs node format — no parallel CBOR dict. The
whole node is age-encrypted to the vault's recipients and stored as a
content-addressed blob; consumers age-decrypt it once and then have
the `TraversalContext` they need to traverse the snapshot.

`KEY_SLOT_RECOVERY` is a non-encryption slot — no blob pipeline
references it. It's pure derivation material:

```
recovery_secret       = vault_root.ctx.keys[KEY_SLOT_RECOVERY]
vault_id              = blake3("s5-vault-id"     || recovery_secret) [..16]
recovery_signing_seed = blake3("s5-recovery-sig" || recovery_secret)
recovery_signing_key  = ed25519_keypair_from_seed(recovery_signing_seed)
```

Two consequences fall out:

- **`vault_id`** is the 16-byte `VAULT_ID` field in registry-entry
  lookup keys for this vault. Knowing the vault root tells you which
  registry entries belong to it.
- **`recovery_signing_key`** is the writer of one canonical "current
  HEAD" registry entry per vault. Anyone with vault root can locate
  this entry without knowing any device pubkeys or DIDs.

## The recovery registry entry

Every snap publishes two registry entries:

1. **Per-device entry** under `(device_signing_pubkey, vault_id)` —
   signed by the device's **signing** key (verified `∈` the identity
   bundle's `signers[]`; see
   `acl-and-revocation.md` § Verify chain),
   used by peers in the normal sync path. (Not the iroh transport key
   — that collapse is retired.)
2. **Recovery entry** under `(recovery_pubkey, vault_id)` —
   signed by `recovery_signing_key`, used as the deterministic lookup
   for paper-only recovery.

The recovery entry's payload is the same as the per-device entry (the
current snapshot HEAD hash, encrypted using `keys[KEY_SLOT_NODE]` from
the vault root). It exists so that a recovery flow can find the
latest HEAD without enumerating any device or DID identities — pure
derivation from vault root.

If multiple devices snap concurrently, multiple recovery entries get
written. Revision LWW resolves: highest revision wins. A compromised
recipient writing garbage causes recovery readers to detect the
garbage (decryption fails or HEAD points at a non-existent blob) and
fall back to per-device entries.

## Recovery flows

### Vault content recovery (paper only, no identity_secrets vault)

Use case: "I lost everything except my paper passphrase, my `work:`
vault was being backed up to S3, I want my files back."

```
1. Derive recovery age key:
     recovery_age_secret = argon2id(paper_passphrase, ...)

2. Fetch the vault root blob from any source that holds it:
     - configured S3 bucket
     - peer who's a vault member (if iroh-reachable)
     - local backup
   The recovery age key was a recipient when the vault root was
   encrypted (because it's in the user's identity bundle), so it can
   decrypt.

3. age-decrypt vault root → fs root node with TraversalContext.keys
   slots: KEY_SLOT_LEAF, KEY_SLOT_NODE, KEY_SLOT_RECOVERY

4. Derive recovery_signing_key + vault_id from keys[KEY_SLOT_RECOVERY]

5. Look up recovery entry on any registry-bearing storage:
     entry = registry.get(recovery_pubkey, vault_id)

6. Decrypt entry payload with keys[KEY_SLOT_NODE] → current HEAD
   snapshot hash

7. Fetch snapshot blob; decrypt + traverse using the standard s5_fs_v2
   pipeline with the recovered TraversalContext (KEY_SLOT_LEAF for
   leaf blobs, KEY_SLOT_NODE for tree nodes)

8. Walk snapshot, restore content
```

This path uses zero device state, zero identity-vault state, zero
identity_secrets vault state. Just paper + a storage backend that holds the
encrypted blobs.

### Adding a new device (identity_secrets vault path)

Use case: "I bought a new laptop, want to set it up with my existing
identity."

```
1. On the new machine: `vup recover` — creates a fresh config from the
   paper phrase. (If an existing device is still alive, enroll the new
   one with `vup device join <code>` instead — a code minted by
   `vup device invite` on the old device — which needs no paper.)
2. Enter paper passphrase for that identity → derive recovery_age_secret
3. Fetch + decrypt the identity_secrets vault (recovery age key is a recipient)
4. Read DID master signing key from identity_secrets vault
5. New device generates fresh local keys for the capabilities its role
   needs — age + iroh + ACL + signing for a full device, fewer for a
   read-only/service node (no signing key) — or reuses the device's
   existing keys if joining a second/third identity
6. Sign updated identity bundle (with master key) appending the
   device's pubkeys to the matching bundle sets (age_recipients /
   iroh_pubkeys / acl_keys / signers) per its role
7. Publish updated identity bundle (registry HEAD update under
   DID master key)
8. (Optional) re-wrap identity_secrets vault to add this device's age key as a
   recipient — so future unlocks don't depend on paper
```

Now the device has the master key for that identity, can write to its
vaults (via the device's **signing** key, if its role has one), and
other peers will accept its signatures once they refresh the identity
bundle.

A node joining multiple identities runs this flow once per identity —
each identity has its own paper passphrase, master key, and bundle.
The device's iroh + age (+ ACL/signing, per role) keys are generated
once and reused across identities (one device → one transport key in
every bundle; see `identity-model.md`
§ Multi-identity for the correlation trade-off).

### Master key rotation

Use case: "I think the master key may have leaked; rotate the DID."

The DID encodes a specific pubkey, so rotating it strictly *changes
the DID*. To preserve continuity:

```
1. Generate new master key, new DID
2. Publish a "rotation event" entry under the OLD DID's registry
   stream — signed by the old master key — containing { new_did,
   timestamp, sig_by_new_did }
3. Publish a normal identity bundle under the NEW DID
4. Peers walking the old DID's chain see the rotation event,
   follow it to the new DID, and consider the old DID retired
5. Vaults the user is a member of need their recipient lists updated
   (replace old DID with new) — done by the user via
   `vup grant <vault>: @new-did` and `vup revoke <vault>: @old-did`
```

Old encrypted content remains decryptable by old keys (the master
key didn't encrypt anything; it only signed). The cost of rotation is
publishing one rotation event + updating recipient lists across the
user's vaults.

## Hardware-backed factors (optional)

A user may want stronger protection on the identity_secrets vault than "any
device can decrypt." The shape: add additional recipients to the
identity_secrets vault (yubikey, BIP39-derived hardware-wallet keys, threshold
shamir splits) and remove device age recipients that the user doesn't
trust to hold the master key.

The identity layer doesn't impose policy here. Default config is "all
devices unlock the identity_secrets vault"; power users can swap this for any
subset.

Crucially this hardening is confined to `identity_secrets` (the cold tier).
The warm `stores` vault stays `{all devices, paper}`, so day-to-day storage
operations never need the hardware key — you don't want to touch a yubikey on
every daemon start just to talk to indexd. Keeping the two in *separate*
containers is exactly what makes "harden the master without locking storage"
possible — see `identity-model.md` § Special vaults.

## Threshold paper recovery (Shamir)

The paper passphrase can be split via Shamir secret sharing across
multiple physical locations. `k-of-n` reconstruction. Trades
single-location theft risk for the chance of losing too many shares.
Common shape: `k=2, n=3` across three locations.

This is operational (use `ssss-split` or any Shamir tool); s5 doesn't
need to know about the splitting. From the protocol's perspective the
recovery key is just whatever the user types in.

## What this doesn't solve

- **Loss of paper + all devices.** Unrecoverable. There is no escrow.
- **Compromise of paper.** The recovery age key has the same access
  as a daily-use device key — full read of every vault that lists the
  user's DID as a recipient. Compromise = treat as full identity
  compromise; rotate master key and all vault content keys.
- **Quantum-future read of harvest-now ciphertext.** age's X25519
  recipient is Shor-vulnerable. The argon2id KDF + paper passphrase
  layer doesn't help against a CRQC harvesting current ciphertext.
  Mitigation: PQ-hybrid recipients via age plugins (planned post-v1)
  added alongside the X25519 recipient.
- **Forward secrecy.** No vault-layer forward secrecy. Any compromise
  of vault content keys grants full historical read. Out of scope.
