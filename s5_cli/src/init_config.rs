use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::Context;
use clap::Subcommand;
use rand::RngCore;
use toml_edit::{DocumentMut, Item, Table};
use tracing::info;

#[derive(Subcommand)]
pub enum CmdConfig {
    /// Creates node config file if it doesn't exist and generates a keypair
    Init,
}

impl CmdConfig {
    pub fn run(self, node_config_file: PathBuf, local_data_dir: &Path) -> anyhow::Result<()> {
        let mut doc = if node_config_file.exists() {
            fs::read_to_string(&node_config_file)?
        } else {
            fs::create_dir_all(node_config_file.parent().unwrap())?;
            "".to_owned()
        }
        .parse::<DocumentMut>()
        .context("could not parse node config file")?;

        match self {
            Self::Init => {
                let secretkey_file = node_config_file.with_extension("secretkey");

                doc.entry("identity")
                    .or_insert(Item::Table(Table::new()))
                    .as_table_mut()
                    .unwrap()
                    .insert(
                        "secret_key_file",
                        secretkey_file
                            .file_name()
                            .unwrap()
                            .to_owned()
                            .into_string()
                            .unwrap()
                            .into(),
                    );

                let local_only_store_path = local_data_dir.join("local_only_store");
                let mut local_only_store_table = Table::new();
                local_only_store_table.insert("type", "local".into());
                local_only_store_table
                    .insert("base_path", local_only_store_path.to_str().unwrap().into());
                doc.entry("store")
                    .or_insert(Item::Table(Table::new()))
                    .as_table_mut()
                    .unwrap()
                    .insert("local_only_store", local_only_store_table.into());

                let mut fs_entry_local_table = Table::new();
                fs_entry_local_table.insert("type", "local".into());
                fs_entry_local_table.insert("dir", "local".into());
                doc.entry("fs")
                    .or_insert(Item::Table(Table::new()))
                    .as_table_mut()
                    .unwrap()
                    .entry("entry")
                    .or_insert(Item::Table(Table::new()))
                    .as_table_mut()
                    .unwrap()
                    .insert("local", fs_entry_local_table.into());

                if !secretkey_file.exists() {
                    info!("generating secure random secret key for node");
                    let mut bytes = [0u8; 32];
                    rand::rng().fill_bytes(&mut bytes);
                    fs::write(secretkey_file, bytes)?;
                }
            }
        }

        info!("writing to config file {node_config_file:?}");

        let tmp_path = node_config_file.with_extension("tmp");
        let mut tmp = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        tmp.write_all(doc.to_string().as_bytes())?;
        tmp.sync_all()?;
        std::fs::rename(&tmp_path, node_config_file)?;
        Ok(())
    }
}
