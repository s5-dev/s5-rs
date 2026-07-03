//! Friend pairing (`vup friend pair`, D16).
//!
//! `vup friend pair` (no args) on the sender side asks the daemon to mint a one-time
//! [`PairToken`] and block until a peer redeems it. The token encodes the
//! daemon's iroh endpoint id (a **dial target only**), its **DID pubkey**
//! (the cold anchor key — the authority root, D8/D17), and a fresh 32-byte
//! secret.
//!
//! `vup friend pair <token>` on the receiver side parses the token, dials the sender's
//! iroh endpoint over `s5/pair/0`, and runs a mutual **proof-of-possession**
//! handshake. Under the cold/warm split (D17) a device never holds the cold
//! key, so possession is proven with the **warm** master — and the binding
//! "this warm key speaks for that DID" travels *in-band* as the signed
//! cold-pointer entry (self-certifying under the DID pubkey; see
//! [`crate::identity_anchor`]), so neither side needs the other's registry
//! reachable during the handshake. Each side:
//!
//! 1. ships its DID pubkey + its anchor entry + `ed25519(warm,
//!    PAIR_POP_CONTEXT ‖ secret ‖ did_pubkey ‖ iroh_id)`;
//! 2. verifies the peer's anchor entry (F01 signature under the peer's DID
//!    pubkey + shape checks), extracts the warm pubkey it names, and checks
//!    the PoP under that warm key.
//!
//! A peer therefore cannot claim a DID unless it holds the warm key that
//! DID's cold pointer currently names. The verified anchor entry is handed
//! to the caller so it can be seeded into the local registry — after pairing,
//! the peer's DID resolves locally even before its own registry is reachable.
//!
//! Why a dedicated ALPN: the daemon's control ALPN (`s5/node/0`) bypasses
//! membership ACL (the local CLI uses ephemeral keys). The pair handshake gets
//! its own narrow, ACL-bypassing protocol whose auth is the one-time secret
//! plus the PoP signatures.
//!
//! Wire format on `s5/pair/0` (fixed head + length-prefixed anchor entry,
//! big-endian):
//!
//! ```text
//! Request  (receiver → sender):
//!   secret(32) ‖ receiver_did_pubkey(32) ‖ receiver_sig(64)
//!     ‖ entry_len(2) ‖ receiver_anchor_entry(entry_len)
//! Response (sender → receiver):
//!   ack(1) ‖ sender_did_pubkey(32) ‖ sender_sig(64)
//!     ‖ entry_len(2) ‖ sender_anchor_entry(entry_len)
//!     ack = 0x00 success · 0x01 unknown/expired secret · 0x02 bad proof
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use bytes::Bytes;
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use rand::Rng;
use s5_core::StreamMessage;
use s5_core::identity::{Did, DidMasterPubkey};
use tokio::sync::{Mutex, oneshot};

/// ALPN for the pair handshake. Distinct from `s5/node/0` so the
/// local-control surface stays untouched by remote pair callers.
pub const PAIR_ALPN: &[u8] = b"s5/pair/0";

/// Domain-separation tag for the pairing proof-of-possession signature.
/// v3: signed by the WARM master (D17), message binds the DID pubkey.
const PAIR_POP_CONTEXT: &[u8] = b"s5-pair-pop:v3";

/// `vup3-<base64_url(version ‖ endpoint_id ‖ did_pubkey ‖ secret)>`.
const TOKEN_VERSION: u8 = 0x03;
const TOKEN_PREFIX: &str = "vup3-";
const TOKEN_LEN: usize = 1 + 32 + 32 + 32;

/// Upper bound on a shipped anchor entry (a v3 vault registry entry is
/// ~220 bytes; the wire LEN field itself caps the payload at 255).
const MAX_ANCHOR_ENTRY_LEN: usize = 1024;

/// Verified outcome of a pairing: the peer's DID pubkey (authority root) plus
/// its iroh endpoint id (a dial hint).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PairedPeer {
    /// The peer's ed25519 **DID** pubkey — its cold anchor key (D17).
    /// Verified: the peer proved possession of the warm key this DID's
    /// in-band cold pointer names.
    pub did_pubkey: [u8; 32],
    /// The peer's iroh endpoint id, for dialing (not an authority principal).
    pub iroh_id: [u8; 32],
}

/// A verified pairing outcome plus the peer's cold-pointer entry — kept so
/// the caller can seed it into the local registry (the peer's DID then
/// resolves locally without its registry being reachable).
#[derive(Clone, Debug)]
pub struct VerifiedPair {
    pub peer: PairedPeer,
    pub anchor_entry: StreamMessage,
}

/// Pending-pair retention. Entries older than this are eligible to be reaped on
/// the next mint. Generous so a user has time to type a petname.
const PENDING_TTL: Duration = Duration::from_secs(60 * 30);

/// The signed bytes proving identity possession, bound to the pairing secret,
/// the claimed DID, and the signer's iroh transport key. Signed by the WARM
/// master (D17) — the verifier learns which warm key to check from the
/// in-band anchor entry.
fn pop_message(secret: &[u8; 32], did_pubkey: &[u8; 32], iroh_id: &[u8; 32]) -> Vec<u8> {
    let mut m = Vec::with_capacity(PAIR_POP_CONTEXT.len() + 96);
    m.extend_from_slice(PAIR_POP_CONTEXT);
    m.extend_from_slice(secret);
    m.extend_from_slice(did_pubkey);
    m.extend_from_slice(iroh_id);
    m
}

/// Verify a peer's identity claim: parse + F01/shape-verify its anchor entry
/// under the claimed DID, then check the PoP signature under the warm key the
/// anchor names. Returns the verified entry for registry seeding.
fn verify_peer_pop(
    did_pubkey: &[u8; 32],
    iroh_id: &[u8; 32],
    secret: &[u8; 32],
    sig: &[u8; 64],
    anchor_entry_bytes: Vec<u8>,
) -> Result<StreamMessage> {
    // StreamMessage::deserialize funnels through `StreamMessage::new` —
    // the F01 chokepoint — so the entry's ed25519 signature is verified
    // under its embedded pubkey here.
    let entry = StreamMessage::deserialize(Bytes::from(anchor_entry_bytes))
        .map_err(|e| anyhow!("pair: peer anchor entry undecodable: {e}"))?;
    let did = Did::from_pubkey(DidMasterPubkey::new(*did_pubkey));
    let pointer = crate::identity_anchor::cold_pointer_from_entry(&did, &entry)
        .context("pair: peer anchor entry rejected")?;
    let vk = VerifyingKey::from_bytes(&pointer.warm_pub)
        .map_err(|_| anyhow!("pair: anchor names an invalid warm pubkey"))?;
    let signature = Signature::from_bytes(sig);
    vk.verify_strict(&pop_message(secret, did_pubkey, iroh_id), &signature)
        .map_err(|_| anyhow!("pair: warm-key proof-of-possession failed"))?;
    Ok(entry)
}

/// Bytes carried in a `vup3-…` pair token.
#[derive(Clone)]
pub struct PairToken {
    /// Sender's iroh endpoint id — the dial target (transport only).
    pub endpoint_id: [u8; 32],
    /// Sender's ed25519 **DID** pubkey (cold anchor key). The receiver
    /// verifies the sender's anchor entry + PoP against this.
    pub did_pubkey: [u8; 32],
    /// Fresh 32-byte secret. Sender keeps it in its pending table; receiver
    /// presents it on the wire.
    pub secret: [u8; 32],
}

impl PairToken {
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
        let body = s
            .trim()
            .strip_prefix(TOKEN_PREFIX)
            .ok_or_else(|| anyhow!("not a vup pair token (missing '{TOKEN_PREFIX}' prefix)"))?;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(body)
            .context("invalid base64 in pair token")?;
        if bytes.len() != TOKEN_LEN {
            return Err(anyhow!(
                "pair token has wrong length ({} bytes, expected {TOKEN_LEN})",
                bytes.len()
            ));
        }
        if bytes[0] != TOKEN_VERSION {
            return Err(anyhow!(
                "unknown pair token version {} (expected {TOKEN_VERSION})",
                bytes[0]
            ));
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

/// Pending sender-side pair entry.
#[derive(Debug)]
struct Pending {
    secret: [u8; 32],
    /// Fires with the receiver's verified [`VerifiedPair`] on redemption.
    redeem_tx: Option<oneshot::Sender<VerifiedPair>>,
    created: Instant,
}

/// In-memory table of outstanding pair tokens this daemon has minted but not
/// yet seen redeemed.
#[derive(Clone, Default, Debug)]
pub struct PendingPairs {
    inner: Arc<Mutex<Vec<Pending>>>,
}

impl PendingPairs {
    /// Mint + register a new token. `endpoint_id` is the dial target;
    /// `did_pubkey` is this daemon's DID (embedded so the receiver can verify
    /// the sender's anchor + PoP). Caller awaits `wait_rx` in the `WaitPair`
    /// RPC.
    pub async fn mint(
        &self,
        endpoint_id: [u8; 32],
        did_pubkey: [u8; 32],
    ) -> (PairToken, oneshot::Receiver<VerifiedPair>) {
        let token = PairToken::random_for(endpoint_id, did_pubkey);
        let (tx, rx) = oneshot::channel();
        let mut guard = self.inner.lock().await;
        Self::reap_expired(&mut guard);
        guard.push(Pending {
            secret: token.secret,
            redeem_tx: Some(tx),
            created: Instant::now(),
        });
        (token, rx)
    }

    /// Match a presented secret; on success fire its channel with the receiver's
    /// verified pair and remove the entry. Returns `true` on redemption.
    async fn redeem(&self, secret: &[u8; 32], pair: VerifiedPair) -> bool {
        let mut guard = self.inner.lock().await;
        Self::reap_expired(&mut guard);
        let Some(idx) = guard
            .iter()
            .position(|p| constant_time_eq(&p.secret, secret))
        else {
            return false;
        };
        let mut entry = guard.remove(idx);
        if let Some(tx) = entry.redeem_tx.take() {
            let _ = tx.send(pair);
        }
        true
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

/// Read a `u16`-length-prefixed anchor entry from the stream.
async fn read_anchor_entry(recv: &mut iroh::endpoint::RecvStream) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 2];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| anyhow!("pair: short read on anchor length: {e}"))?;
    let len = u16::from_be_bytes(len_buf) as usize;
    if len == 0 || len > MAX_ANCHOR_ENTRY_LEN {
        bail!("pair: peer anchor entry length {len} out of range");
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| anyhow!("pair: short read on anchor entry: {e}"))?;
    Ok(buf)
}

/// `ProtocolHandler` for `s5/pair/0`. Verifies the receiver's anchor + warm
/// PoP, redeems the secret, and replies with this daemon's own anchor + PoP so
/// the receiver can verify us in turn. ACL is bypassed for this ALPN (the
/// secret + the proofs are the auth).
#[derive(Clone)]
pub struct PairListener {
    pending: PendingPairs,
    /// This daemon's WARM master signing key — signs our half of the mutual
    /// PoP (D17: the cold key never touches the daemon).
    warm: SigningKey,
    /// This daemon's DID pubkey (cold anchor key).
    our_did_pubkey: [u8; 32],
    /// This daemon's signed cold-pointer entry — shipped in-band so the peer
    /// can verify our DID→warm binding offline.
    our_anchor_entry: StreamMessage,
    /// This daemon's iroh endpoint id — bound into our PoP signature.
    our_iroh_id: [u8; 32],
}

impl std::fmt::Debug for PairListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PairListener").finish_non_exhaustive()
    }
}

impl PairListener {
    pub fn new(
        pending: PendingPairs,
        warm: SigningKey,
        our_did_pubkey: [u8; 32],
        our_anchor_entry: StreamMessage,
        our_iroh_id: [u8; 32],
    ) -> Self {
        Self {
            pending,
            warm,
            our_did_pubkey,
            our_anchor_entry,
            our_iroh_id,
        }
    }
}

impl iroh::protocol::ProtocolHandler for PairListener {
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let remote_id = conn.remote_id();
        let peer_iroh = *remote_id.as_bytes();
        tracing::info!(peer = %remote_id.fmt_short(), "pair: incoming connection");

        let (mut send, mut recv) = match conn.accept_bi().await {
            Ok(streams) => streams,
            Err(e) => {
                tracing::warn!(peer = %remote_id.fmt_short(), "pair: accept_bi failed: {e}");
                return Ok(());
            }
        };

        // Request head: secret(32) ‖ receiver_did_pubkey(32) ‖ receiver_sig(64).
        let mut buf = [0u8; 128];
        if let Err(e) = recv.read_exact(&mut buf).await {
            tracing::warn!(peer = %remote_id.fmt_short(), "pair: short request: {e}");
            return Ok(());
        }
        let mut secret = [0u8; 32];
        secret.copy_from_slice(&buf[..32]);
        let mut peer_did = [0u8; 32];
        peer_did.copy_from_slice(&buf[32..64]);
        let mut peer_sig = [0u8; 64];
        peer_sig.copy_from_slice(&buf[64..128]);
        let peer_anchor = match read_anchor_entry(&mut recv).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(peer = %remote_id.fmt_short(), "pair: {e:#}");
                return Ok(());
            }
        };

        // Verify the receiver's identity claim: anchor entry under its DID,
        // PoP under the warm key the anchor names, bound to its iroh id.
        let anchor_entry =
            match verify_peer_pop(&peer_did, &peer_iroh, &secret, &peer_sig, peer_anchor) {
                Ok(entry) => entry,
                Err(e) => {
                    tracing::warn!(
                        peer = %remote_id.fmt_short(),
                        "pair: receiver identity proof failed: {e:#}; rejecting"
                    );
                    let _ = send.write_all(&[0x02]).await;
                    let _ = send.finish();
                    return Ok(());
                }
            };

        let pair = VerifiedPair {
            peer: PairedPeer {
                did_pubkey: peer_did,
                iroh_id: peer_iroh,
            },
            anchor_entry,
        };
        let ok = self.pending.redeem(&secret, pair).await;
        if !ok {
            let _ = send.write_all(&[0x01]).await;
            let _ = send.finish();
            tracing::warn!(
                peer = %remote_id.fmt_short(),
                "pair: presented secret did not match any pending token"
            );
            return Ok(());
        }

        // Respond with our own anchor + PoP so the receiver can verify us.
        let our_sig = self
            .warm
            .sign(&pop_message(
                &secret,
                &self.our_did_pubkey,
                &self.our_iroh_id,
            ))
            .to_bytes();
        let entry_bytes = self.our_anchor_entry.serialize();
        let mut resp = Vec::with_capacity(97 + 2 + entry_bytes.len());
        resp.push(0x00);
        resp.extend_from_slice(&self.our_did_pubkey);
        resp.extend_from_slice(&our_sig);
        resp.extend_from_slice(&(entry_bytes.len() as u16).to_be_bytes());
        resp.extend_from_slice(&entry_bytes);
        if let Err(e) = send.write_all(&resp).await {
            tracing::warn!(peer = %remote_id.fmt_short(), "pair: write response failed: {e}");
        }
        let _ = send.finish();
        tracing::info!(peer = %remote_id.fmt_short(), "pair: redemption successful");
        conn.closed().await;
        Ok(())
    }
}

/// Receiver side: dial `token.endpoint_id`, prove possession of our identity
/// (warm PoP + our anchor entry), verify the sender's anchor + PoP against
/// `token.did_pubkey`, and return the sender's verified [`VerifiedPair`].
pub async fn redeem_pair_token(
    endpoint: &iroh::Endpoint,
    token: &PairToken,
    our_warm: &SigningKey,
    our_did_pubkey: [u8; 32],
    our_anchor_entry: &StreamMessage,
) -> Result<VerifiedPair> {
    let our_iroh = *endpoint.id().as_bytes();
    let target_id = iroh::EndpointId::from_bytes(&token.endpoint_id)
        .map_err(|e| anyhow!("invalid endpoint_id in pair token: {e}"))?;
    let target_addr = iroh::EndpointAddr::from(target_id);
    let conn = endpoint
        .connect(target_addr, PAIR_ALPN)
        .await
        .map_err(|e| anyhow!("pair: connect to peer endpoint failed: {e}"))?;

    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow!("pair: open_bi failed: {e}"))?;

    let our_sig = our_warm
        .sign(&pop_message(&token.secret, &our_did_pubkey, &our_iroh))
        .to_bytes();
    let entry_bytes = our_anchor_entry.serialize();
    let mut req = Vec::with_capacity(128 + 2 + entry_bytes.len());
    req.extend_from_slice(&token.secret);
    req.extend_from_slice(&our_did_pubkey);
    req.extend_from_slice(&our_sig);
    req.extend_from_slice(&(entry_bytes.len() as u16).to_be_bytes());
    req.extend_from_slice(&entry_bytes);
    send.write_all(&req)
        .await
        .map_err(|e| anyhow!("pair: write request failed: {e}"))?;
    send.finish()
        .map_err(|e| anyhow!("pair: finish send stream failed: {e}"))?;

    let mut resp = [0u8; 97];
    recv.read_exact(&mut resp)
        .await
        .map_err(|e| anyhow!("pair: read response failed (peer closed early?): {e}"))?;
    match resp[0] {
        0x00 => {}
        0x01 => bail!("pair: peer rejected token (likely expired or already used)"),
        0x02 => bail!("pair: peer rejected our identity proof"),
        other => bail!("pair: peer returned unknown ack {other:#x}"),
    }
    let mut sender_did = [0u8; 32];
    sender_did.copy_from_slice(&resp[1..33]);
    let mut sender_sig = [0u8; 64];
    sender_sig.copy_from_slice(&resp[33..97]);
    let sender_anchor = read_anchor_entry(&mut recv).await?;

    // The sender's DID pubkey MUST match the token (the DID we were handed),
    // and its anchor + warm PoP must verify bound to the iroh id we dialled.
    if sender_did != token.did_pubkey {
        bail!("pair: sender DID pubkey does not match the token's DID");
    }
    let anchor_entry = verify_peer_pop(
        &sender_did,
        &token.endpoint_id,
        &token.secret,
        &sender_sig,
        sender_anchor,
    )?;

    Ok(VerifiedPair {
        peer: PairedPeer {
            did_pubkey: sender_did,
            iroh_id: token.endpoint_id,
        },
        anchor_entry,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity_anchor::{ColdPointer, sign_cold_pointer};

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    /// A (cold, warm, anchor entry) triple as onboarding would mint it.
    fn identity(cold_seed: u8, warm_seed: u8) -> (SigningKey, SigningKey, StreamMessage) {
        let cold = key(cold_seed);
        let warm = key(warm_seed);
        let entry = sign_cold_pointer(
            &cold,
            &ColdPointer {
                warm_pub: warm.verifying_key().to_bytes(),
                next_cold_pub: [0u8; 32],
            },
            1,
        )
        .unwrap();
        (cold, warm, entry)
    }

    #[test]
    fn token_round_trip() {
        let token = PairToken::random_for([7u8; 32], [8u8; 32]);
        let s = token.encode();
        assert!(s.starts_with(TOKEN_PREFIX));
        let parsed = PairToken::decode(&s).expect("decode");
        assert_eq!(parsed.endpoint_id, token.endpoint_id);
        assert_eq!(parsed.did_pubkey, token.did_pubkey);
        assert_eq!(parsed.secret, token.secret);
    }

    #[test]
    fn token_rejects_garbage() {
        assert!(PairToken::decode("not-a-token").is_err());
        assert!(PairToken::decode("vup3-").is_err());
        assert!(PairToken::decode("vup3-AAAA").is_err());
        // Older token versions are rejected (v2 signed with the on-device
        // master, a key that no longer exists under the split).
        assert!(PairToken::decode("vup2-AAAA").is_err());
        assert!(PairToken::decode("vup1-AAAA").is_err());
    }

    #[test]
    fn pop_verifies_warm_holder_via_anchor() {
        let (cold, warm, entry) = identity(1, 2);
        let did_pub = cold.verifying_key().to_bytes();
        let iroh_id = [9u8; 32];
        let secret = [3u8; 32];
        let sig = warm
            .sign(&pop_message(&secret, &did_pub, &iroh_id))
            .to_bytes();
        let entry_bytes = entry.serialize().to_vec();

        // Valid: warm signature + matching anchor.
        assert!(verify_peer_pop(&did_pub, &iroh_id, &secret, &sig, entry_bytes.clone()).is_ok());
        // Wrong iroh binding → reject (an impostor replaying under a
        // different transport key can't reuse the signature).
        assert!(
            verify_peer_pop(&did_pub, &[0xFF; 32], &secret, &sig, entry_bytes.clone()).is_err()
        );
        // Wrong secret → reject.
        assert!(
            verify_peer_pop(&did_pub, &iroh_id, &[0u8; 32], &sig, entry_bytes.clone()).is_err()
        );
        // A DIFFERENT warm key signing (not the one the anchor names) → reject:
        // holding *a* key is not holding *the identity's* key.
        let intruder_sig = key(7)
            .sign(&pop_message(&secret, &did_pub, &iroh_id))
            .to_bytes();
        assert!(verify_peer_pop(&did_pub, &iroh_id, &secret, &intruder_sig, entry_bytes).is_err());
    }

    #[test]
    fn pop_rejects_anchor_for_a_different_did() {
        // Peer claims DID A but ships an anchor signed by DID B: the warm
        // PoP would verify under B's warm key, but the anchor-shape check
        // pins the entry to the claimed DID.
        let (_cold_a, warm_b, entry_b) = identity(1, 2);
        let other_did = key(5).verifying_key().to_bytes();
        let iroh_id = [9u8; 32];
        let secret = [3u8; 32];
        let sig = warm_b
            .sign(&pop_message(&secret, &other_did, &iroh_id))
            .to_bytes();
        assert!(
            verify_peer_pop(
                &other_did,
                &iroh_id,
                &secret,
                &sig,
                entry_b.serialize().to_vec()
            )
            .is_err()
        );
    }

    fn sample_pair(did_seed: u8) -> VerifiedPair {
        let (cold, _warm, entry) = identity(did_seed, did_seed.wrapping_add(1));
        VerifiedPair {
            peer: PairedPeer {
                did_pubkey: cold.verifying_key().to_bytes(),
                iroh_id: [10u8; 32],
            },
            anchor_entry: entry,
        }
    }

    #[tokio::test]
    async fn pending_redeem_fires_channel_with_verified_peer() {
        let pending = PendingPairs::default();
        let (token, rx) = pending.mint([3u8; 32], [4u8; 32]).await;
        let pair = sample_pair(1);
        assert!(pending.redeem(&token.secret, pair.clone()).await);
        assert_eq!(rx.await.expect("channel fires").peer, pair.peer);
    }

    #[tokio::test]
    async fn pending_redeem_rejects_unknown_secret() {
        let pending = PendingPairs::default();
        let (_token, _rx) = pending.mint([3u8; 32], [4u8; 32]).await;
        assert!(!pending.redeem(&[0u8; 32], sample_pair(1)).await);
    }

    #[tokio::test]
    async fn pending_redeem_consumes_entry() {
        let pending = PendingPairs::default();
        let (token, _rx) = pending.mint([3u8; 32], [4u8; 32]).await;
        assert!(pending.redeem(&token.secret, sample_pair(1)).await);
        assert!(!pending.redeem(&token.secret, sample_pair(1)).await);
    }
}
