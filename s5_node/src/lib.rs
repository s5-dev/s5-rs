use crate::config::{NodeConfigStore, S5NodeConfig};
use anyhow::Context;
use iroh::{Endpoint, SecretKey, protocol::Router};
use s5_core::{BlobStore, store::StoreResult};
use s5_store_local::LocalStore;
use s5_store_s3::S3Store;
use s5_store_sia::SiaStore;
use std::path::PathBuf;

pub mod config;

pub async fn create_store(config: NodeConfigStore) -> StoreResult<BlobStore> {
    let store: Box<dyn s5_core::store::Store + 'static> = match config {
        NodeConfigStore::SiaRenterd(config) => Box::new(SiaStore::create(config).await?),
        NodeConfigStore::Local(config) => Box::new(LocalStore::create(config)),
        NodeConfigStore::S3(config) => Box::new(S3Store::create(config)),
    };
    Ok(BlobStore::new(store))
}

pub async fn run_node(config_file_path: PathBuf, config: S5NodeConfig) -> anyhow::Result<()> {
    let store: BlobStore = create_store(
        config
            .store
            .get("default")
            .context("no default store present in node config")?
            .to_owned(),
    )
    .await?;

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
