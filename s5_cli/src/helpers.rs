use anyhow::{Context, Result, anyhow};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use s5_core::Hash;
use s5_node::config::{NodeConfigIdentity, S5NodeConfig};
use s5_node::identity;
use std::str::FromStr;

pub async fn build_endpoint(identity: &NodeConfigIdentity) -> Result<Endpoint> {
    let mut builder = Endpoint::builder();
    if let Some(sec) = identity::load_secret_key(identity) {
        builder = builder.secret_key(sec);
    }
    let endpoint = builder.bind().await?;
    Ok(endpoint)
}

pub fn peer_endpoint_addr(config: &S5NodeConfig, peer_name: &str) -> Result<EndpointAddr> {
    let peer = config
        .peer
        .get(peer_name)
        .with_context(|| format!("peer '{}' not found in node config", peer_name))?;
    let dial_str = &peer.id;
    let pid = EndpointId::from_str(dial_str)
        .with_context(|| format!("invalid endpoint id string '{}'", dial_str))?;
    Ok(EndpointAddr::from(pid))
}

pub fn parse_hash_hex(s: &str) -> Result<Hash> {
    let s = s.trim();
    if s.len() != 64 {
        return Err(anyhow!(
            "expected 64-character hex hash, got {} characters",
            s.len()
        ));
    }
    let bytes = hex::decode(s).context("failed to decode hex hash")?;
    if bytes.len() != 32 {
        return Err(anyhow!(
            "decoded hash has wrong length: {} bytes",
            bytes.len()
        ));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(Hash::from_bytes(arr))
}
