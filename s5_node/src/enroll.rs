//! Device enrollment — `vup device invite` / `vup device join` (D10).
//!
//! Multi-device enrollment is paper-recovery's sibling: the new device
//! ends up running the same `bootstrap_from_identity` walk `vup
//! recover` runs — the two differ only in *how the warm master
//! arrives* (read from the re-wrapped `identity_secrets` escrow with
//! the new device's own age key, vs. located via the paper phrase).
//! This module is the transport half: a one-time invite token and a
//! narrow, ACL-bypassing iroh protocol on its own ALPN, mirroring the
//! friend-pairing handshake ([`crate::pair`]) it is modeled on.
//!
//! Crucial asymmetry vs. `pair`: the joining device **has nothing** —
//! no DID, no anchor, no keys of its own identity — so there is no
//! mutual proof-of-possession. Authentication is exactly the physical
//! transfer of the one-time token (the user carried it from the
//! inviting device to the new one), the same trust root device pairing
//! has everywhere. The joiner authenticates the *inviter* though: the
//! token carries the identity's DID (cold pubkey), and the response
//! ships the signed cold-pointer anchor entry, self-certifying under
//! that DID (`crate::identity_anchor`) — a MITM cannot substitute a
//! different identity without failing the anchor check.
//!
//! Inviter-side redemption performs the whole §6.1 device-add, *then*
//! acks:
//!
//! 1. [`crate::admission::admit_device_keys`] — union the four keys
//!    into the identity bundle (read-merge-write).
//! 2. Catalogue entry (`identity_secrets/devices`, label → keys;
//!    labels are UI-only, never authorization).
//! 3. [`crate::admission::rewrap_special_vaults`] — reseal
//!    `identity_secrets` + `config` to the recipient set *including*
//!    the new device's age key, so the joiner can complete its walk.
//!
//! Wire format on `s5/enroll/0` (big-endian length prefixes):
//!
//! ```text
//! Request  (joiner → inviter):
//!   secret(32) ‖ signing_pub(32) ‖ acl_pub(32) ‖ iroh_pub(32)
//!     ‖ age_len(2) ‖ age_recipient(age_len)
//! Response (inviter → joiner):
//!   ack(1)
//!     0x00 ok ‖ entry_len(2) ‖ anchor_entry ‖ grant_len(4) ‖ grant_json
//!     0x01 unknown/expired secret · 0x02 malformed request
//!     0x03 enrollment failed server-side
//! ```
//!
//! The grant JSON ([`EnrollGrant`]) carries the inviter's durable
//! bootstrap-store config (credentials inline — the exact bytes the
//! config vault would hand the joiner one step later, over a channel
//! encrypted to the endpoint key in the token) plus the paper age
//! recipient. Without it the joiner cannot reach the durable store at
//! all: the store credentials live *in* the config vault, which lives
//! *on* the store — and unlike `vup recover`, the joiner has no
//! mnemonic to re-derive a Sia AppKey from.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use rand::Rng;
use s5_core::blob::Blobs;
use s5_core::identity::{Did, DidMasterPubkey};
use s5_core::{RegistryApi, StreamMessage};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, oneshot};

use crate::admission::DeviceKeys;
use crate::config::NodeConfigStore;
use crate::identity_secrets_vault::IdentitySecretsVault;

/// ALPN for the enrollment handshake. Distinct from `s5/pair/0` (friend
/// pairing — a *different* trust ceremony) and from the control ALPN.
pub const ENROLL_ALPN: &[u8] = b"s5/enroll/0";

/// `vupd-<base64_url(version ‖ endpoint_id ‖ did_pubkey ‖ secret)>`.
const TOKEN_VERSION: u8 = 0x01;
const TOKEN_PREFIX: &str = "vupd-";
const TOKEN_LEN: usize = 1 + 32 + 32 + 32;

/// Upper bound on a shipped anchor entry (a v3 registry entry with the
/// 64-byte inline payload is ~220 bytes).
const MAX_ANCHOR_ENTRY_LEN: usize = 1024;
/// Upper bound on an age recipient string (bech32 `age1…` is 62 chars).
const MAX_AGE_RECIPIENT_LEN: usize = 256;
/// Upper bound on the grant JSON (a store config with inline creds).
const MAX_GRANT_LEN: usize = 64 * 1024;

/// Pending-enroll retention; generous so the user has time to carry the
/// token to the other device and finish its local key generation.
const PENDING_TTL: Duration = Duration::from_secs(60 * 30);

/// Bytes carried in a `vupd-…` enroll token.
#[derive(Clone)]
pub struct EnrollToken {
    /// Inviter's iroh endpoint id — the dial target (transport only).
    pub endpoint_id: [u8; 32],
    /// The identity's **DID** pubkey (cold anchor key). The joiner
    /// verifies the inviter's anchor entry against this.
    pub did_pubkey: [u8; 32],
    /// Fresh one-time secret; the joiner presents it on the wire.
    pub secret: [u8; 32],
}

impl EnrollToken {
    pub fn random_for(endpoint_id: [u8; 32], did_pubkey: [u8; 32]) -> Self {
        let mut secret = [0u8; 32];
        rand::rng().fill_bytes(&mut secret);
        Self {
            endpoint_id,
            did_pubkey,
            secret,
        }
    }

    pub fn encode(&self) -> String {
        let mut buf = [0u8; TOKEN_LEN];
        buf[0] = TOKEN_VERSION;
        buf[1..33].copy_from_slice(&self.endpoint_id);
        buf[33..65].copy_from_slice(&self.did_pubkey);
        buf[65..97].copy_from_slice(&self.secret);
        let body = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf);
        format!("{TOKEN_PREFIX}{body}")
    }

    pub fn decode(s: &str) -> Result<Self> {
        let body = s.trim().strip_prefix(TOKEN_PREFIX).ok_or_else(|| {
            anyhow!("not a vup device-enroll token (missing '{TOKEN_PREFIX}' prefix)")
        })?;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(body)
            .context("invalid base64 in enroll token")?;
        if bytes.len() != TOKEN_LEN {
            bail!(
                "enroll token has wrong length ({} bytes, expected {TOKEN_LEN})",
                bytes.len()
            );
        }
        if bytes[0] != TOKEN_VERSION {
            bail!(
                "unknown enroll token version {} (expected {TOKEN_VERSION})",
                bytes[0]
            );
        }
        let mut endpoint_id = [0u8; 32];
        endpoint_id.copy_from_slice(&bytes[1..33]);
        let mut did_pubkey = [0u8; 32];
        did_pubkey.copy_from_slice(&bytes[33..65]);
        let mut secret = [0u8; 32];
        secret.copy_from_slice(&bytes[65..97]);
        Ok(Self {
            endpoint_id,
            did_pubkey,
            secret,
        })
    }
}

/// What the inviter hands the joiner alongside the anchor entry, as
/// length-prefixed JSON. Extensible (new optional fields decode as
/// `None`/default on older joiners).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnrollGrant {
    /// `[store.<name>]` name of the inviter's durable bootstrap store.
    pub store_name: String,
    /// The bootstrap store's config, credentials inline — same shape
    /// the config vault syncs. The joiner rebuilds the store from this
    /// (adjusting device-local paths like caches) and completes the
    /// recovery walk against it.
    pub store: NodeConfigStore,
    /// The identity's paper recovery age recipient (`[key.recovery]`),
    /// so the joiner's config keeps sealing vault roots for paper.
    pub recovery_recipient: Option<String>,
}

/// Outcome handed to the inviter's `DeviceInvite` RPC stream once a
/// joiner has been fully enrolled (bundle + catalogue + re-wrap done).
#[derive(Clone, Debug)]
pub struct EnrolledDevice {
    /// Catalogue label the device landed under (caller-supplied or
    /// auto-generated; UI-only, never authorization).
    pub label: String,
    /// The four keys that were admitted.
    pub keys: DeviceKeys,
}

/// Pending inviter-side enroll entry.
#[derive(Debug)]
struct Pending {
    secret: [u8; 32],
    /// Caller-suggested catalogue label (None = auto from the signing
    /// pubkey).
    label: Option<String>,
    /// Fires with the enrolled device once redemption completes.
    redeem_tx: oneshot::Sender<EnrolledDevice>,
    created: Instant,
}

/// In-memory table of outstanding enroll tokens this daemon has minted
/// but not yet seen redeemed.
#[derive(Clone, Default, Debug)]
pub struct PendingEnrolls {
    inner: Arc<Mutex<Vec<Pending>>>,
}

impl PendingEnrolls {
    /// Mint + register a new one-time token. The caller awaits the
    /// returned receiver in the `DeviceInvite` RPC.
    pub async fn mint(
        &self,
        endpoint_id: [u8; 32],
        did_pubkey: [u8; 32],
        label: Option<String>,
    ) -> (EnrollToken, oneshot::Receiver<EnrolledDevice>) {
        let token = EnrollToken::random_for(endpoint_id, did_pubkey);
        let (tx, rx) = oneshot::channel();
        let mut guard = self.inner.lock().await;
        Self::reap_expired(&mut guard);
        guard.push(Pending {
            secret: token.secret,
            label,
            redeem_tx: tx,
            created: Instant::now(),
        });
        (token, rx)
    }

    /// Consume a presented secret (one-time): returns the label + the
    /// completion channel, or `None` if unknown/expired.
    async fn take(
        &self,
        secret: &[u8; 32],
    ) -> Option<(Option<String>, oneshot::Sender<EnrolledDevice>)> {
        let mut guard = self.inner.lock().await;
        Self::reap_expired(&mut guard);
        let idx = guard
            .iter()
            .position(|p| constant_time_eq(&p.secret, secret))?;
        let entry = guard.remove(idx);
        Some((entry.label, entry.redeem_tx))
    }

    fn reap_expired(entries: &mut Vec<Pending>) {
        let now = Instant::now();
        entries.retain(|p| now.duration_since(p.created) < PENDING_TTL);
    }
}

fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// Everything the inviter-side listener needs to enroll a device.
/// Snapshotted at daemon startup (same cadence as the pair identity).
pub struct EnrollContext {
    /// The WARM master — signs the bundle edit and the re-wrapped
    /// special-vault HEADs (the cold key never touches the daemon).
    pub warm: SigningKey,
    /// This identity's signed cold-pointer entry, shipped in-band so
    /// the joiner can verify DID → warm offline.
    pub anchor_entry: StreamMessage,
    pub registry: Arc<dyn RegistryApi + Send + Sync>,
    /// Every configured store (the bundle blob goes to all of them).
    pub stores: HashMap<String, Arc<dyn Blobs>>,
    /// The durable bootstrap store hosting `identity_secrets` + the
    /// config vault.
    pub escrow_store: Arc<dyn Blobs>,
    /// The identity's current `[key.*]` age recipients (device set +
    /// paper) — the new device's recipient is appended per enrollment.
    pub recipients: Vec<String>,
    /// age identity files that open the special vaults for the
    /// read-modify-write (this device's key).
    pub identity_files: Vec<String>,
    /// The grant shipped to every joiner.
    pub grant: EnrollGrant,
}

/// `ProtocolHandler` for `s5/enroll/0`: verifies the one-time secret,
/// runs the full §6.1 device-add (admit → catalogue → re-wrap), and
/// replies with the anchor entry + grant. ACL is bypassed for this ALPN
/// (the joiner is by definition not yet a member); the secret is the
/// auth.
#[derive(Clone)]
pub struct EnrollListener {
    pending: PendingEnrolls,
    ctx: Arc<EnrollContext>,
}

impl std::fmt::Debug for EnrollListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnrollListener").finish_non_exhaustive()
    }
}

impl EnrollListener {
    pub fn new(pending: PendingEnrolls, ctx: Arc<EnrollContext>) -> Self {
        Self { pending, ctx }
    }

    /// The inviter-side enrollment work, post secret check. Returns the
    /// catalogue label the device landed under.
    async fn enroll(&self, label: Option<String>, keys: &DeviceKeys) -> Result<String> {
        let ctx = &self.ctx;

        // 1. Bundle admission (read-merge-write; the authorization step).
        let revision = crate::admission::admit_device_keys(
            &ctx.warm,
            ctx.registry.as_ref(),
            &ctx.stores,
            keys,
        )
        .await
        .context("admitting device keys to the identity bundle")?;

        // Full recipient set from here on: existing devices + paper + the
        // new device (writers ⊆ readers — the joiner must be able to open
        // the escrow of the warm key it will sign with).
        let mut recipients = ctx.recipients.clone();
        if !recipients.iter().any(|r| r == &keys.age_recipient) {
            recipients.push(keys.age_recipient.clone());
        }

        // 2. Catalogue entry (UI-only label; uniquified, never authz).
        let vault = IdentitySecretsVault::new(
            ctx.warm.clone(),
            ctx.escrow_store.clone(),
            ctx.registry.clone(),
            recipients.clone(),
            ctx.identity_files.clone(),
        );
        let base = label.unwrap_or_else(|| format!("device-{}", hex::encode(&keys.signing[..4])));
        let catalogue = vault
            .read_devices()
            .await
            .context("reading the device catalogue")?;
        let label = free_label(&catalogue, &base, keys);
        vault
            .upsert_device(&label, keys)
            .await
            .context("writing the device catalogue entry")?;

        // 3. Re-wrap the special vaults to the expanded recipient set so
        // the joiner's own age key opens identity_secrets + config.
        crate::admission::rewrap_special_vaults(
            &ctx.warm,
            ctx.registry.as_ref(),
            ctx.escrow_store.clone(),
            &ctx.identity_files,
            &recipients,
        )
        .await
        .context("re-wrapping the special vaults for the new device")?;

        tracing::info!(
            label = label.as_str(),
            revision,
            signing = %hex::encode(&keys.signing[..4]),
            "enroll: device admitted + special vaults re-wrapped"
        );
        Ok(label)
    }
}

/// Pick a catalogue label: `base` if free (or already recording exactly
/// these keys — idempotent re-enroll), else `base-2`, `base-3`, …
fn free_label(
    catalogue: &crate::identity_secrets_vault::DeviceCatalogue,
    base: &str,
    keys: &DeviceKeys,
) -> String {
    match catalogue.get(base) {
        None => return base.to_string(),
        Some(existing) if existing == keys => return base.to_string(),
        Some(_) => {}
    }
    for n in 2u32.. {
        let candidate = format!("{base}-{n}");
        match catalogue.get(&candidate) {
            None => return candidate,
            Some(existing) if existing == keys => return candidate,
            Some(_) => continue,
        }
    }
    unreachable!("u32 label space exhausted")
}

impl iroh::protocol::ProtocolHandler for EnrollListener {
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let remote_id = conn.remote_id();
        tracing::info!(peer = %remote_id.fmt_short(), "enroll: incoming connection");

        let (mut send, mut recv) = match conn.accept_bi().await {
            Ok(streams) => streams,
            Err(e) => {
                tracing::warn!(peer = %remote_id.fmt_short(), "enroll: accept_bi failed: {e}");
                return Ok(());
            }
        };

        // Request head: secret(32) ‖ signing(32) ‖ acl(32) ‖ iroh(32).
        let mut head = [0u8; 128];
        if let Err(e) = recv.read_exact(&mut head).await {
            tracing::warn!(peer = %remote_id.fmt_short(), "enroll: short request: {e}");
            return Ok(());
        }
        let mut secret = [0u8; 32];
        secret.copy_from_slice(&head[..32]);
        let mut signing = [0u8; 32];
        signing.copy_from_slice(&head[32..64]);
        let mut acl = [0u8; 32];
        acl.copy_from_slice(&head[64..96]);
        let mut iroh_pub = [0u8; 32];
        iroh_pub.copy_from_slice(&head[96..128]);

        let age_recipient = match read_prefixed_u16(&mut recv, MAX_AGE_RECIPIENT_LEN).await {
            Ok(bytes) => match String::from_utf8(bytes)
                .map_err(anyhow::Error::from)
                .and_then(|s| {
                    s.parse::<age::x25519::Recipient>()
                        .map_err(|e| anyhow!("invalid age recipient: {e}"))?;
                    Ok(s)
                }) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(peer = %remote_id.fmt_short(), "enroll: bad age recipient: {e:#}");
                    let _ = send.write_all(&[0x02]).await;
                    let _ = send.finish();
                    return Ok(());
                }
            },
            Err(e) => {
                tracing::warn!(peer = %remote_id.fmt_short(), "enroll: {e:#}");
                let _ = send.write_all(&[0x02]).await;
                let _ = send.finish();
                return Ok(());
            }
        };
        let keys = DeviceKeys {
            signing,
            acl,
            iroh: iroh_pub,
            age_recipient,
        };

        // One-time secret check (consumes the pending entry).
        let Some((label, redeem_tx)) = self.pending.take(&secret).await else {
            tracing::warn!(
                peer = %remote_id.fmt_short(),
                "enroll: presented secret did not match any pending invite"
            );
            let _ = send.write_all(&[0x01]).await;
            let _ = send.finish();
            return Ok(());
        };

        // The actual §6.1 device-add. On failure the token stays consumed
        // (mint a fresh invite) — a half-applied enrollment must not be
        // retryable with the same, possibly-leaked secret.
        let label = match self.enroll(label, &keys).await {
            Ok(label) => label,
            Err(e) => {
                tracing::warn!(peer = %remote_id.fmt_short(), "enroll: failed: {e:#}");
                let _ = send.write_all(&[0x03]).await;
                let _ = send.finish();
                // Dropping redeem_tx signals failure to the invite RPC.
                drop(redeem_tx);
                return Ok(());
            }
        };

        // Ack + anchor entry + grant.
        let entry_bytes = self.ctx.anchor_entry.serialize();
        let grant_bytes = match serde_json::to_vec(&self.ctx.grant) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(peer = %remote_id.fmt_short(), "enroll: grant encode failed: {e}");
                let _ = send.write_all(&[0x03]).await;
                let _ = send.finish();
                drop(redeem_tx);
                return Ok(());
            }
        };
        let mut resp = Vec::with_capacity(1 + 2 + entry_bytes.len() + 4 + grant_bytes.len());
        resp.push(0x00);
        resp.extend_from_slice(&(entry_bytes.len() as u16).to_be_bytes());
        resp.extend_from_slice(&entry_bytes);
        resp.extend_from_slice(&(grant_bytes.len() as u32).to_be_bytes());
        resp.extend_from_slice(&grant_bytes);
        if let Err(e) = send.write_all(&resp).await {
            tracing::warn!(peer = %remote_id.fmt_short(), "enroll: write response failed: {e}");
        }
        let _ = send.finish();

        let _ = redeem_tx.send(EnrolledDevice {
            label: label.clone(),
            keys,
        });
        tracing::info!(peer = %remote_id.fmt_short(), label = label.as_str(), "enroll: complete");
        conn.closed().await;
        Ok(())
    }
}

/// What the joiner walks away from the handshake with.
#[derive(Clone, Debug)]
pub struct EnrollAccept {
    /// The identity's verified cold-pointer entry (persist to the anchor
    /// entry file; seed into the local registry).
    pub anchor_entry: StreamMessage,
    /// The warm master pubkey the anchor names — where `identity_secrets`
    /// and the config vault live.
    pub warm_pub: [u8; 32],
    /// Bootstrap-store config + paper recipient.
    pub grant: EnrollGrant,
}

/// Joiner side: dial `token.endpoint_id` over `s5/enroll/0`, present
/// the one-time secret + this device's four pubkeys, and receive the
/// verified anchor entry + grant. The anchor entry is checked
/// self-certifyingly against the token's DID (F01 signature at
/// deserialize + shape checks), so a MITM on the dial path cannot
/// substitute another identity.
pub async fn join_enroll(
    endpoint: &iroh::Endpoint,
    token: &EnrollToken,
    keys: &DeviceKeys,
) -> Result<EnrollAccept> {
    if keys.age_recipient.len() > MAX_AGE_RECIPIENT_LEN {
        bail!("age recipient string too long");
    }
    let target_id = iroh::EndpointId::from_bytes(&token.endpoint_id)
        .map_err(|e| anyhow!("invalid endpoint_id in enroll token: {e}"))?;
    let conn = endpoint
        .connect(iroh::EndpointAddr::from(target_id), ENROLL_ALPN)
        .await
        .map_err(|e| anyhow!("enroll: connect to inviter failed: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow!("enroll: open_bi failed: {e}"))?;

    let age_bytes = keys.age_recipient.as_bytes();
    let mut req = Vec::with_capacity(128 + 2 + age_bytes.len());
    req.extend_from_slice(&token.secret);
    req.extend_from_slice(&keys.signing);
    req.extend_from_slice(&keys.acl);
    req.extend_from_slice(&keys.iroh);
    req.extend_from_slice(&(age_bytes.len() as u16).to_be_bytes());
    req.extend_from_slice(age_bytes);
    send.write_all(&req)
        .await
        .map_err(|e| anyhow!("enroll: write request failed: {e}"))?;
    send.finish()
        .map_err(|e| anyhow!("enroll: finish send stream failed: {e}"))?;

    let mut ack = [0u8; 1];
    recv.read_exact(&mut ack)
        .await
        .map_err(|e| anyhow!("enroll: read ack failed (inviter closed early?): {e}"))?;
    match ack[0] {
        0x00 => {}
        0x01 => bail!("enroll: inviter rejected token (expired or already used)"),
        0x02 => bail!("enroll: inviter rejected our request as malformed"),
        0x03 => bail!("enroll: inviter-side enrollment failed (see its logs); mint a new invite"),
        other => bail!("enroll: inviter returned unknown ack {other:#x}"),
    }

    let entry_bytes = read_prefixed_u16(&mut recv, MAX_ANCHOR_ENTRY_LEN).await?;
    // F01 chokepoint: deserialize verifies the ed25519 signature under
    // the entry's embedded pubkey; the shape check pins it to the DID.
    let anchor_entry = StreamMessage::deserialize(Bytes::from(entry_bytes))
        .map_err(|e| anyhow!("enroll: inviter anchor entry undecodable: {e}"))?;
    let did = Did::from_pubkey(DidMasterPubkey::new(token.did_pubkey));
    let pointer = crate::identity_anchor::cold_pointer_from_entry(&did, &anchor_entry)
        .context("enroll: inviter anchor entry rejected")?;

    let grant_bytes = read_prefixed_u32(&mut recv, MAX_GRANT_LEN).await?;
    let grant: EnrollGrant =
        serde_json::from_slice(&grant_bytes).map_err(|e| anyhow!("enroll: bad grant JSON: {e}"))?;

    Ok(EnrollAccept {
        anchor_entry,
        warm_pub: pointer.warm_pub,
        grant,
    })
}

/// Joiner-side one-shot for callers without their own iroh plumbing
/// (the daemon-less `vup device join`): bind an ephemeral endpoint on
/// the device keyset's iroh secret — the same key advertised in
/// `keys.iroh`, so the transport identity matches the bundle entry —
/// run [`join_enroll`], and close the endpoint.
pub async fn join_as_new_device(
    keyset: &crate::device_keyset::DeviceKeyset,
    token: &EnrollToken,
    keys: &DeviceKeys,
) -> Result<EnrollAccept> {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .secret_key(keyset.iroh_secret_key())
        .bind()
        .await
        .map_err(|e| anyhow!("enroll: binding iroh endpoint failed: {e}"))?;
    let result = join_enroll(&endpoint, token, keys).await;
    endpoint.close().await;
    result
}

/// Read a `u16`-length-prefixed field.
async fn read_prefixed_u16(recv: &mut iroh::endpoint::RecvStream, max: usize) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 2];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| anyhow!("enroll: short read on length prefix: {e}"))?;
    let len = u16::from_be_bytes(len_buf) as usize;
    if len == 0 || len > max {
        bail!("enroll: field length {len} out of range (max {max})");
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| anyhow!("enroll: short read on field body: {e}"))?;
    Ok(buf)
}

/// Read a `u32`-length-prefixed field.
async fn read_prefixed_u32(recv: &mut iroh::endpoint::RecvStream, max: usize) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| anyhow!("enroll: short read on length prefix: {e}"))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 || len > max {
        bail!("enroll: field length {len} out of range (max {max})");
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| anyhow!("enroll: short read on field body: {e}"))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_round_trip() {
        let token = EnrollToken::random_for([7u8; 32], [8u8; 32]);
        let s = token.encode();
        assert!(s.starts_with(TOKEN_PREFIX));
        let parsed = EnrollToken::decode(&s).expect("decode");
        assert_eq!(parsed.endpoint_id, token.endpoint_id);
        assert_eq!(parsed.did_pubkey, token.did_pubkey);
        assert_eq!(parsed.secret, token.secret);
    }

    #[test]
    fn token_rejects_garbage_and_pair_tokens() {
        assert!(EnrollToken::decode("not-a-token").is_err());
        assert!(EnrollToken::decode("vupd-").is_err());
        assert!(EnrollToken::decode("vupd-AAAA").is_err());
        // A friend-pair token must not redeem as a device enroll.
        assert!(EnrollToken::decode("vup3-AAAA").is_err());
    }

    #[tokio::test]
    async fn pending_take_is_one_time_and_carries_the_label() {
        let pending = PendingEnrolls::default();
        let (token, _rx) = pending
            .mint([1u8; 32], [2u8; 32], Some("phone".to_string()))
            .await;
        let (label, _tx) = pending.take(&token.secret).await.expect("first take");
        assert_eq!(label.as_deref(), Some("phone"));
        assert!(
            pending.take(&token.secret).await.is_none(),
            "a redeemed secret must not redeem twice"
        );
        assert!(pending.take(&[0u8; 32]).await.is_none());
    }

    #[test]
    fn free_label_uniquifies_but_is_stable_for_the_same_keys() {
        let keys_a = DeviceKeys {
            signing: [1u8; 32],
            acl: [2u8; 32],
            iroh: [3u8; 32],
            age_recipient: "age1a".into(),
        };
        let mut keys_b = keys_a.clone();
        keys_b.signing = [9u8; 32];

        let mut catalogue = crate::identity_secrets_vault::DeviceCatalogue::new();
        assert_eq!(free_label(&catalogue, "phone", &keys_a), "phone");
        catalogue.insert("phone".to_string(), keys_a.clone());
        // Same keys, same base → same label (idempotent re-enroll).
        assert_eq!(free_label(&catalogue, "phone", &keys_a), "phone");
        // Different keys → uniquified.
        assert_eq!(free_label(&catalogue, "phone", &keys_b), "phone-2");
    }

    #[test]
    fn grant_json_round_trips() {
        let grant = EnrollGrant {
            store_name: "sia".to_string(),
            store: NodeConfigStore::from_backend(crate::config::NodeConfigStoreBackend::Local(
                s5_store_local::LocalStoreConfig {
                    base_path: "/data".to_string(),
                },
            )),
            recovery_recipient: Some("age1paper".to_string()),
        };
        let bytes = serde_json::to_vec(&grant).unwrap();
        let parsed: EnrollGrant = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.store_name, grant.store_name);
        assert_eq!(parsed.recovery_recipient, grant.recovery_recipient);
        assert_eq!(parsed.store, grant.store);
    }
}
