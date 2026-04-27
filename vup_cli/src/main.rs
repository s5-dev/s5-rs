mod cmd;
mod node;
mod progress;
mod recovery;
mod sigil;

use anyhow::Result;
use clap::{Parser, Subcommand};
use clap_verbosity_flag::InfoLevel;
use std::path::PathBuf;

use crate::cmd::{StoreCmd, VaultAction};

#[derive(Parser)]
#[command(
    name = "vup",
    version,
    about = "Personal backup, sync, and archive tool built on S5",
    long_about = "\
The vup CLI uses sigil-prefixed references:
  +vault         a configured vault   (e.g. `vup +music snap`)
  @identity      a paired peer        (e.g. `vup +music grant @alice -r`)
  #snap          a snapshot id        (e.g. `vup +music restore --snap #ab12…`)

Top-level verbs (no `+vault`):
  onboard ls new drop join peers unpair store

Vault-scoped verbs (after `+vault`):
  add info snap history restore mount grant pair export who kick

See `cli-workflows.md` for the full grammar and worked examples."
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
    // -- Top-level vocabulary ------------------------------------------------
    /// First-run setup wizard.
    #[command(alias = "o")]
    Onboard,

    /// List configured vaults.
    #[command(alias = "l")]
    Ls,

    /// Create a new vault.
    #[command(alias = "n")]
    New {
        /// Vault to create, e.g. `+music` or `music`.
        #[arg(value_parser = sigil::parse_vault_ref)]
        vault: String,
    },

    /// Delete a vault config (does not destroy stored data).
    #[command(alias = "d")]
    Drop {
        /// Vault to drop, e.g. `+music` or `music`.
        #[arg(value_parser = sigil::parse_vault_ref)]
        vault: String,
    },

    /// Consume a share/pair/grant URL.
    #[command(alias = "j")]
    Join {
        /// `s5://…` URL.
        url: String,
    },

    /// List known peer identities.
    #[command(alias = "p")]
    Peers,

    /// Forget a peer identity.
    #[command(alias = "u")]
    Unpair {
        /// Identity to forget (e.g. `@alice`).
        id: String,
    },

    /// Store sub-namespace (`add`, `ls`, `info`, `rm`, `allow`, `disallow`).
    Store {
        #[command(subcommand)]
        cmd: StoreCmd,
    },

    /// Vault-scoped action — invoked as `vup +<vault> <action>`.
    /// Hidden because users learn the `+vault` form.
    #[command(hide = true)]
    Vault {
        /// Vault name (already stripped of the `+` prefix by the sigil
        /// router). May be `all` for the wildcard form.
        name: String,
        #[command(subcommand)]
        action: VaultAction,
    },

    // -- Utility verbs (kept) ------------------------------------------------
    /// Show node status, configured stores, sources, and running tasks.
    Status,

    /// Show or edit node configuration.
    Config {
        /// Print current config as JSON.
        #[arg(long)]
        json: bool,
        /// Apply a JSON Patch (RFC 6902) from a string.
        #[arg(long)]
        patch: Option<String>,
        /// Apply a JSON Patch (RFC 6902) from a file.
        #[arg(long)]
        patch_file: Option<PathBuf>,
    },

    /// List all node tasks.
    Tasks,

    /// Show status of a running or completed task.
    TaskStatus {
        /// Task ID.
        task_id: u64,
    },

    /// Cancel a running task.
    Cancel {
        /// Task ID.
        task_id: u64,
    },

    /// Shut down the running s5 node.
    Shutdown,

    /// Run the s5 node daemon (internal — used by auto-start).
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
    // Sigil routing: rewrite `+<vault> …` into `vault <vault> …` so clap
    // dispatches to the hidden Vault subcommand.
    let mut argv: Vec<String> = std::env::args().collect();
    sigil::rewrite_vault_prefix(&mut argv);

    let cli = Cli::parse_from(&argv);

    let is_daemon = matches!(&cli.cmd, Commands::Daemon);

    if is_daemon {
        let log_dir = dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("s5")
            .join("logs");
        std::fs::create_dir_all(&log_dir)?;

        let file_appender = tracing_appender::rolling::never(&log_dir, "node.log");
        let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

        tracing_subscriber::fmt()
            .with_max_level(cli.verbosity)
            .with_writer(file_writer)
            .with_ansi(false)
            .init();

        // Keep the appender guard alive for the program's lifetime.
        Box::leak(Box::new(guard));
    } else {
        tracing_subscriber::fmt()
            .with_max_level(cli.verbosity)
            .init();
    }

    let config_path = resolve_config(cli.config)?;

    // The CLI is an RPC frontend; every verb routes through the running
    // daemon. Only two verbs are exempt:
    //   - `_daemon` IS the daemon (cannot connect to itself).
    //   - `onboard` is the bootstrap that creates the config the daemon
    //     needs to start.
    if matches!(&cli.cmd, Commands::Daemon) {
        return node::run_daemon(&config_path).await;
    }
    if matches!(&cli.cmd, Commands::Onboard) {
        return cmd::onboard::run_onboard(&config_path).await;
    }

    let client = node::ensure_node_running(&config_path).await?;
    let result = match cli.cmd {
        Commands::Daemon | Commands::Onboard => unreachable!("handled above"),

        // -- Utility ---------------------------------------------------------
        Commands::Shutdown => cmd::run_shutdown(&client).await,
        Commands::Status => cmd::run_status(&client).await,
        Commands::Config {
            json,
            patch,
            patch_file,
        } => cmd::run_config(&client, json, patch, patch_file).await,
        Commands::Tasks => cmd::tasks::list_tasks(&client).await,
        Commands::TaskStatus { task_id } => cmd::tasks::task_status(&client, task_id).await,
        Commands::Cancel { task_id } => cmd::tasks::cancel_task(&client, task_id).await,

        // -- Top-level vault lifecycle --------------------------------------
        Commands::Ls => cmd::lifecycle::run_ls(&client).await,
        Commands::New { vault } => cmd::lifecycle::run_new(&client, &vault).await,
        Commands::Drop { vault } => cmd::lifecycle::run_drop(&client, &vault).await,

        // -- Top-level identity / consume URL (stubs) -----------------------
        Commands::Join { url } => cmd::stubs::run_join(&client, &url).await,
        Commands::Peers => cmd::stubs::run_peers(&client).await,
        Commands::Unpair { id } => cmd::stubs::run_unpair(&client, &id).await,
        Commands::Store { cmd } => cmd::stubs::run_store(&client, cmd).await,

        // -- Vault-scoped: dispatch on action -------------------------------
        Commands::Vault { name, action } => dispatch_vault(&client, &name, action).await,
    };

    client.close().await;
    result
}

async fn dispatch_vault(
    client: &s5_node_api::S5NodeClient,
    vault: &str,
    action: VaultAction,
) -> Result<()> {
    use cmd::stubs as st;
    use cmd::vault as v;
    match action {
        VaultAction::Add { paths } => v::run_add(client, vault, &paths).await,
        VaultAction::Info => v::run_info(client, vault).await,
        VaultAction::Snap { watch } => v::run_snap(client, vault, watch).await,
        VaultAction::History => v::run_history(client, vault).await,
        VaultAction::Restore { snap } => {
            // Restore target defaults to <cwd>/restored/<vault>; the user
            // can override later (for now there's no --to flag).
            let target = std::env::current_dir()?.join("restored").join(vault);
            std::fs::create_dir_all(&target)?;
            v::run_restore(client, vault, snap.as_deref(), &target).await
        }
        VaultAction::Mount {
            path,
            rw,
            debounce_ms,
        } => v::run_mount(client, vault, &path, rw, debounce_ms).await,
        VaultAction::Grant { id, read, write } => {
            st::run_grant(client, vault, &id, read, write).await
        }
        VaultAction::Pair { id } => st::run_pair(client, vault, &id).await,
        VaultAction::Export { path } => v::run_export(client, vault, path.as_deref()).await,
        VaultAction::Who => st::run_who(client, vault).await,
        VaultAction::Kick { id } => st::run_kick(client, vault, &id).await,
    }
}
