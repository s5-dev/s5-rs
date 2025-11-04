use anyhow::Context;
use clap::{Parser, Subcommand};
use clap_verbosity_flag::InfoLevel;
use directories::ProjectDirs;
use http_importer::HttpImporter;
use s5_fs::{DirContext, FS5};
use s5_importer_local::LocalFileSystemImporter;
use s5_node::config::S5NodeConfig;
use std::{
    fs,
    path::{ PathBuf},
};
use crate::init_config::CmdConfig;
mod init_config;

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
    /// Import data to the default blob store
    Import {
        #[arg(short, long, value_name = "STORE_NAME", default_value = "default")]
        target_store: String,
        #[command(subcommand)]
        cmd: ImportCmd,
    },
    /// Serve data (currently only web archives)
  /*   Serve {
        #[command(subcommand)]
        cmd: ServeCmd,
    }, */
    /// Start the S5 Node and serve all hashes from the default blob store
    Start,
}

#[derive(Subcommand)]
enum ImportCmd {
    Http {
        // TODO support local fs paths
        url: String,
        /// max number of concurrent blob imports
        #[arg(short, long, value_name = "COUNT", default_value_t = 4)]
        concurrency: usize,
    },
    Local {
        path: PathBuf,
        /// max number of concurrent blob imports
        #[arg(short, long, value_name = "COUNT", default_value_t = 4)]
        concurrency: usize,
    },
   /*  Warc {
        path: PathBuf,
    }, */
}

/* #[derive(Subcommand)]
enum ServeCmd {
    WebArchive {},
} */


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

    let local_data_dir = dirs.data_dir();

    match cli.cmd {
        Commands::Config { cmd } => cmd.run(node_config_file, local_data_dir)?,
        _ => {
            
            let toml_content = fs::read_to_string(&node_config_file)?;
            let config: S5NodeConfig = toml::from_str(&toml_content)?;


            // TODO support using custom fs meta path
                       let path = dirs
                                .data_dir()
                                .join("fs_roots")
                                .join("local.fs5");
                    let context = DirContext::open_local_root(path)?;
                    let fs =    FS5::open(context);

            match cli.cmd {
                Commands::Import { cmd, target_store } => {

                    let target_store  =
                            s5_node::create_store(
                                config
                                    .store
                                    .get(&target_store)
                                    .context(format!("store with name \"{target_store}\" not present in node config"))?
                                    .to_owned(),
                            )
                            .await?;
                        
                    

                    match cmd {
                        ImportCmd::Http { url, concurrency } => {
                            let http_importer = HttpImporter::create(
                                fs,
                                target_store,
                                concurrency,
                            )?;
                            http_importer.import_url(url.parse()?).await?;
                        }
                        ImportCmd::Local { path, concurrency } => {

                            // imported_local
                            // imported_http
                            // let root_dir = DirV1::open(fs).context("Failed to open FS5 directory state")?;

                            // let dir = root_dir.consume();

                            let importer = LocalFileSystemImporter::create(
                                fs,
                                target_store,
                                concurrency,
                            )?;
                            importer.import_path(path).await?;
                        }

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
