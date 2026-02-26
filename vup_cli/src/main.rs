mod cmd;
mod config;
mod vault;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use clap_verbosity_flag::InfoLevel;
use directories::ProjectDirs;
use std::path::PathBuf;

pub use config::VaultConfig;

#[derive(Parser)]
#[command(
    name = "vup",
    version,
    about = "Personal backup, sync, and archive tool built on S5"
)]
struct Cli {
    #[command(flatten)]
    verbosity: clap_verbosity_flag::Verbosity<InfoLevel>,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Configure the vault interactively (creates vault on first run)
    Config,

    /// Add directories to the vault for tracking (+ initial index)
    Add {
        /// Directories to add
        #[arg(required = true)]
        paths: Vec<PathBuf>,
    },

    /// Show vault status (re-indexes, then shows sources and targets)
    Status,

    /// Back up tracked files to a target
    Backup {
        /// Target name (required if multiple targets configured)
        #[arg(short, long)]
        target: Option<String>,
    },
}

fn config_path() -> Result<PathBuf> {
    let dirs = ProjectDirs::from("pro", "s5", "vup")
        .context("could not determine config directory")?;
    Ok(dirs.config_dir().join("config.toml"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_max_level(cli.verbosity)
        .init();

    let config_path = config_path()?;

    match cli.cmd {
        Commands::Config => cmd::run_config(&config_path).await,
        Commands::Add { paths } => cmd::run_add(paths, &config_path).await,
        Commands::Status => cmd::run_status(&config_path).await,
        Commands::Backup { target } => cmd::run_backup(&config_path, target.as_deref()).await,
    }
}
