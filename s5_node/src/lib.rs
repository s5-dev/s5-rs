use crate::config::{NodeConfigStore, S5NodeConfig};
use anyhow::Context;
use iroh::{Endpoint, SecretKey, protocol::Router};
use s5_store_sia::SiaBlobStore;
use std::path::PathBuf;

pub mod config;

pub async fn run_node(config_file_path: PathBuf, config: S5NodeConfig) -> anyhow::Result<()> {
    let store = match config
        .store
        .get("default")
        .context("no default store present in node config")?
    {
        NodeConfigStore::SiaRenterd {
            bucket,
            worker_api_url,
            bus_api_url,
            password,
        } => SiaBlobStore::new(bucket, worker_api_url, bus_api_url, password),
    };

    let endpoint = Endpoint::builder()
        .secret_key(SecretKey::from_bytes(
            &std::fs::read(
                config_file_path
                    .parent()
                    .unwrap()
                    .join(config.identity.secret_key_file),
            )?
            .as_slice()
            .try_into()?,
        ))
        .discovery_n0()
        // TODO discovery_dht
        // TODO discovery_local_network
        .bind()
        .await?;

    let router = Router::builder(endpoint)
        // TODO .accept(iroh_blobs::ALPN, blobs)
        .spawn();

    tokio::signal::ctrl_c().await?;

    println!("Shutting down.");
    router.shutdown().await?;

    Ok(())
}
