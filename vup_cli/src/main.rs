mod cmd;
mod node;
mod recovery;

use anyhow::Result;
use clap::{Parser, Subcommand};
use clap_verbosity_flag::InfoLevel;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "vup",
    version,
    about = "Personal backup, sync, and archive tool built on S5"
)]
struct Cli {
    #[command(flatten)]
    verbosity: clap_verbosity_flag::Verbosity<InfoLevel>,

    /// Path to s5 node config file
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize vup: create config, keys, and directories
    Init,

    /// Show node status, configured stores, sources, and running tasks
    Status,

    /// Add a path to a source (creates the source if needed)
    Add {
        /// Paths to add
        paths: Vec<PathBuf>,
        /// Source name (default: "default")
        #[arg(long, short, default_value = "default")]
        source: String,
    },

    /// Run a full backup (ingest + publish)
    Backup {
        /// Vault name (default: from config)
        #[arg(long)]
        vault: Option<String>,
        /// Source name (default: from config)
        #[arg(long)]
        source: Option<String>,
        /// Blob store name (default: from vault config)
        #[arg(long)]
        blob_store: Option<String>,
        /// Encryption key names (default: vault key + recovery)
        #[arg(long, short)]
        key: Vec<String>,
    },

    /// Restore a vault snapshot to a local directory
    Restore {
        /// Vault name
        #[arg(long)]
        vault: String,
        /// Target directory for restored files
        #[arg(long)]
        target: String,
        /// Override blob store (default: use vault's blob_stores)
        #[arg(long)]
        blob_store: Option<String>,
    },

    /// Disaster recovery: restore from paper age key + remote store
    RemoteRestore {
        /// Vault name (must match the original vault)
        #[arg(long)]
        vault: String,
        /// The age secret key (AGE-SECRET-KEY-1...)
        #[arg(long)]
        age_secret_key: String,
        /// Blob store name for downloading
        #[arg(long)]
        blob_store: String,
        /// Target directory for restored files
        #[arg(long)]
        target: String,
    },

    /// List vault snapshots
    Snapshots {
        /// Vault name (omit for all vaults)
        vault: Option<String>,
    },

    /// Show or edit node configuration
    Config {
        /// Print current config as JSON
        #[arg(long)]
        json: bool,
        /// Apply a JSON Patch (RFC 6902) from a string
        #[arg(long)]
        patch: Option<String>,
        /// Apply a JSON Patch (RFC 6902) from a file
        #[arg(long)]
        patch_file: Option<PathBuf>,
    },

    /// Run a named task from node config
    RunTask {
        /// Task name (defined in [task.*] config)
        name: String,
    },

    /// Run an inline ingest task (walk + upload + persist)
    Ingest {
        /// Vault name
        #[arg(long)]
        vault: String,
        /// Source name
        #[arg(long)]
        source: String,
        /// Blob store name for file content
        #[arg(long)]
        blob_store: String,
    },

    /// Show status of a running or completed task
    TaskStatus {
        /// Task ID
        task_id: u64,
    },

    /// List all tasks
    Tasks,

    /// Cancel a running task
    Cancel {
        /// Task ID
        task_id: u64,
    },

    /// Shut down the running s5 node
    Shutdown,

    /// Run the s5 node daemon (internal — used by auto-start)
    #[command(hide = true)]
    #[command(name = "_daemon")]
    Daemon,
}

fn resolve_config(cli_override: Option<PathBuf>) -> Result<PathBuf> {
    match cli_override {
        Some(p) => Ok(p),
        None => node::default_config_path(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // For the daemon, write logs to ~/.cache/s5/logs/node.log
    let is_daemon = matches!(&cli.cmd, Commands::Daemon);

    if is_daemon {
        let log_dir = dirs::cache_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("."))
            .join("s5")
            .join("logs");
        std::fs::create_dir_all(&log_dir)?;

        let file_appender = tracing_appender::rolling::never(&log_dir, "node.log");
        let (file_writer, _guard) = tracing_appender::non_blocking(file_appender);

        tracing_subscriber::fmt()
            .with_max_level(cli.verbosity)
            .with_writer(file_writer)
            .with_ansi(false)
            .init();

        // Leak the guard to keep it alive for the entire program
        Box::leak(Box::new(_guard));
    } else {
        tracing_subscriber::fmt()
            .with_max_level(cli.verbosity)
            .init();
    }

    let config_path = resolve_config(cli.config)?;

    // Commands that don't need a shared node connection.
    match &cli.cmd {
        Commands::Init => return cmd::init::run_init(&config_path).await,
        Commands::Daemon => return node::run_daemon(&config_path).await,
        Commands::Shutdown => return cmd::run_shutdown(&config_path).await,
        _ => {}
    }

    // Commands that need a node connection — connect once, close cleanly.
    let client = node::ensure_node_running(&config_path).await?;
    let result = match cli.cmd {
        Commands::Init | Commands::Daemon | Commands::Shutdown => unreachable!(),

        Commands::Status => cmd::run_status(&client).await,

        Commands::Add { paths, source } => cmd::run_add(&client, &source, &paths).await,

        Commands::Snapshots { vault } => cmd::run_snapshots(&client, vault).await,

        Commands::Config {
            json,
            patch,
            patch_file,
        } => cmd::run_config(&client, json, patch, patch_file).await,

        // Task commands
        Commands::RunTask { name } => cmd::tasks::run_task_by_name(&client, &name).await,
        Commands::Ingest {
            vault,
            source,
            blob_store,
        } => cmd::tasks::run_ingest(&client, &vault, &source, &blob_store).await,
        Commands::Backup {
            vault,
            source,
            blob_store,
            key,
        } => {
            cmd::tasks::run_backup(
                &client,
                vault.as_deref(),
                source.as_deref(),
                blob_store.as_deref(),
                &key,
            )
            .await
        }
        Commands::Restore {
            vault,
            target,
            blob_store,
        } => cmd::tasks::run_restore_task(&client, &vault, &target, blob_store.as_deref()).await,
        Commands::RemoteRestore {
            vault,
            age_secret_key,
            blob_store,
            target,
        } => {
            cmd::tasks::run_remote_restore_task(
                &client,
                &age_secret_key,
                &vault,
                &blob_store,
                &target,
            )
            .await
        }
        Commands::TaskStatus { task_id } => cmd::tasks::task_status(&client, task_id).await,
        Commands::Tasks => cmd::tasks::list_tasks(&client).await,
        Commands::Cancel { task_id } => cmd::tasks::cancel_task(&client, task_id).await,
    };

    // Gracefully close the iroh endpoint before the runtime shuts down.
    client.close().await;

    result
}
