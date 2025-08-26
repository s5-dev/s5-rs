use anyhow::Context;
use clap::{Parser, Subcommand};
use clap_verbosity_flag::InfoLevel;
use directories::ProjectDirs;
use http_importer::HttpImporter;
use rand::RngCore;
use s5_node::config::{NodeConfigStore, S5NodeConfig};
use s5_store_sia::SiaBlobStore;
use std::{fs, io::Write, path::PathBuf};
use toml_edit::{DocumentMut, Item, Table};
use tracing::{debug, info};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// which s5 node this command should run on
    #[arg(short, long, value_name = "NAME", default_value = "local")]
    node: String,

    #[command(flatten)]
    verbosity: clap_verbosity_flag::Verbosity<InfoLevel>,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Modify the S5 Node's config
    Config {
        #[command(subcommand)]
        cmd: CmdConfig,
    },
    /// Import a HTTP url to the default blob store
    Import {
        // TODO support local fs paths
        url: String,

        /// max number of concurrent blob imports
        #[arg(short, long, value_name = "COUNT", default_value_t = 4)]
        concurrency: usize,
    },
    /// Start the S5 Node and serve files from the default blob store
    Start,
}

#[derive(Subcommand)]
enum CmdConfig {
    /// Creates node config file if it doesn't exist and generates a keypair
    Init,
}

impl CmdConfig {
    fn run(self, node_config_file: PathBuf) -> anyhow::Result<()> {
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
            .open(&tmp_path)?;
        tmp.write_all(doc.to_string().as_bytes())?;
        tmp.sync_all()?;
        std::fs::rename(&tmp_path, node_config_file)?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_max_level(cli.verbosity)
        .init();

    let dirs = ProjectDirs::from("com", "s5", "S5")
        .context("failed to determine config directory path")?;

    let node_config_file = dirs
        .config_dir()
        .join("nodes")
        .join(&cli.node)
        .with_extension("toml");

    match cli.cmd {
        Commands::Config { cmd } => cmd.run(node_config_file)?,
        _ => {
            let toml_content = fs::read_to_string(&node_config_file)?;
            let config: S5NodeConfig = toml::from_str(&toml_content)?;

            match cli.cmd {
                Commands::Import { url, concurrency } => {
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

                    let node_import_state_file = dirs
                        .data_dir()
                        .join("nodes")
                        .join(&cli.node)
                        .join("import_state.fs5.cbor");

                    let http_importer =
                        HttpImporter::new(node_import_state_file, store, concurrency);
                    http_importer.import_url(url.parse()?).await?;
                }
                Commands::Start => {
                    s5_node::run_node(node_config_file, config).await?;
                }
                _ => {}
            }
        }
    }

    Ok(())
}
