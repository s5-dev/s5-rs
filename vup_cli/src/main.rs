mod cmd;
mod interact;
mod node;
mod progress;
mod recovery;
mod refs;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use clap_verbosity_flag::InfoLevel;
use std::path::PathBuf;

use crate::cmd::{DeviceCmd, FriendCmd, StoreCmd, VaultCmd};
use crate::refs::VaultRef;

#[derive(Parser)]
#[command(
    name = "vup",
    version,
    about = "Personal backup, sync, and archive tool built on S5",
    long_about = "\
The vup CLI uses the D20 reference grammar — the vault is addressed by a
trailing colon, verb first:

  vault:            a configured vault           (e.g. `vup backup ~/Docs docs:`)
  vault:path        a path inside a vault        (e.g. `vup restore docs:Photos ./out`)
  vault:path#snap   a path at a past snapshot    (e.g. `vup restore docs:#3 ./out`)
  #hash             a vault-free immutable snap   (read-only)
  @identity         a paired friend              (e.g. `vup grant docs: @alice --write`)

Common verbs: backup restore list history mount share copy automate join grant
              revoke who status doctor tasks config
              (+ namespaces: vault store device friend service)

The old `+vault <verb>` form still works as a hidden alias through the beta."
)]
struct Cli {
    #[command(flatten)]
    verbosity: clap_verbosity_flag::Verbosity<InfoLevel>,

    /// Path to s5 node config file
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    /// Answer yes to confirmations and accept prompt defaults
    /// (required for non-interactive/scripted use; see exit code 3).
    #[arg(long, short = 'y', global = true)]
    yes: bool,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    // -- Getting started -----------------------------------------------------
    /// First-run setup wizard.
    #[command(alias = "o")]
    Onboard,

    /// Recover an identity from its paper phrase (disaster recovery).
    Recover,

    // -- Data verbs (top-level, D20 refs) ------------------------------------
    /// Back up paths into a vault (fidelity-in): `[SRC…] vault:[path]`.
    /// With sources it persists the mapping and snapshots once; with no
    /// sources it re-runs the vault's — or every vault's — mapping.
    ///
    /// `snap` is a hidden legacy alias so `vup +vault snap` (rewritten to
    /// `snap vault:`) keeps snapshotting through the beta.
    #[command(alias = "b", alias = "snap")]
    Backup {
        /// `[SRC…] vault:[path]` — source paths then the destination vault.
        args: Vec<String>,
    },

    /// Restore a vault to a local directory (fidelity-out):
    /// `vault:[path][#snap] TARGET`. TARGET is required.
    #[command(alias = "r")]
    Restore {
        /// `vault:[path][#snap]` — the source vault reference.
        #[arg(value_parser = refs::vault_ref_arg)]
        reference: String,
        /// Local directory to restore into (required, no default).
        target: PathBuf,
        /// Restore into a non-empty target anyway.
        #[arg(long)]
        force: bool,
    },

    /// List vaults + stores (no arg), or a vault's contents.
    #[command(alias = "l", alias = "ls")]
    List {
        /// Optional `vault:[path][#snap]` or `#hash`.
        reference: Option<String>,
        /// Include `_system` vaults.
        #[arg(long)]
        all: bool,
    },

    /// List a vault's snapshots.
    #[command(alias = "h")]
    History {
        /// `vault:` reference (defaults to the sole vault).
        reference: Option<String>,
    },

    /// Mount a vault at a local directory (read-only by default).
    #[command(alias = "m")]
    Mount {
        /// `vault:` reference (live head; `#snap`/subtree mounts are not yet wired).
        #[arg(value_parser = refs::vault_ref_arg)]
        reference: String,
        /// Mount point (must already exist).
        dir: PathBuf,
        /// Mount read-write — writes accumulate in the daemon's overlay
        /// and a debounced flush + publish folds bursts into snapshots.
        #[arg(long)]
        rw: bool,
        /// Idle window before a write burst is published, ms (with --rw).
        #[arg(long, default_value_t = 2000)]
        debounce_ms: u64,
    },

    /// Make a share link. `vault:` is a frozen whole-vault export;
    /// `vault:path` composes a share-vault + copy + (optional) automation.
    #[command(alias = "s")]
    Share {
        /// `vault:[path][#snap]` reference.
        #[arg(value_parser = refs::vault_ref_arg)]
        reference: String,
        /// Subtree share: deep-copy (re-encrypt under the share-vault's own
        /// keys → true future-revocability) instead of the shallow default.
        #[arg(long)]
        deep: bool,
        /// Subtree share: name for the minted share-vault (default derived
        /// from the ref, e.g. `docs:Photos` → `docs-photos`).
        #[arg(long)]
        name: Option<String>,
        /// Subtree share: keep it updated with a scheduled copy automation.
        #[arg(long)]
        live: bool,
        /// Cadence for `--live`, e.g. `1h`, `30m` (default `1h`).
        #[arg(long, value_name = "DURATION")]
        every: Option<String>,
    },

    /// Copy contents between any refs: vault↔vault (the D21 sharing
    /// primitive), vault→local (get files/versions out), or local→vault
    /// (untracked ingest — `backup` without a persisted mapping).
    /// Shallow by default (reuses source ciphertext, inlines per-blob keys);
    /// `--deep` re-encrypts under the destination's keys.
    #[command(alias = "c")]
    Copy {
        /// Source — a `vault:[path][#snap]` ref or a local path.
        src: String,
        /// Destination — a `vault:[path]` ref or a local directory.
        dst: String,
        /// Deep copy (vault→vault only): re-encrypt everything under the
        /// destination's keys (brand-new ciphertext, true future-revocability).
        #[arg(long)]
        deep: bool,
    },

    /// Consume a share URL or a device-enrollment code.
    #[command(alias = "j")]
    Join {
        /// `s5://export/…` share URL or `vupd-…` device code.
        url: String,
    },

    /// Keep a backup running on its own (watch or schedule). Bare `automate`
    /// is a wizard; `add|list|show|pause|resume|rm` are explicit + scriptable.
    #[command(alias = "a", alias = "auto")]
    Automate {
        #[command(subcommand)]
        cmd: Option<cmd::automate::AutomateCmd>,
    },

    // -- Access --------------------------------------------------------------
    /// Grant a friend read (default) or write access to a vault.
    #[command(alias = "g")]
    Grant {
        /// `vault:` reference.
        #[arg(value_parser = refs::vault_ref_arg)]
        reference: String,
        /// Identity to grant (e.g. `@alice`).
        id: String,
        /// Read access (default).
        #[arg(long, short)]
        read: bool,
        /// Write access — interactive confirm required on a TTY.
        #[arg(long, short)]
        write: bool,
    },

    /// Revoke a member's access (read + write) to a vault.
    #[command(alias = "k", alias = "kick")]
    Revoke {
        /// `vault:` reference.
        #[arg(value_parser = refs::vault_ref_arg)]
        reference: String,
        /// Identity to revoke (e.g. `@alice`).
        id: String,
    },

    /// Show a vault's members and their capabilities.
    #[command(alias = "w")]
    Who {
        /// `vault:` reference (defaults to the sole vault).
        reference: Option<String>,
    },

    // -- Management namespaces ------------------------------------------------
    /// Manage vaults (create / drop / rename).
    Vault {
        #[command(subcommand)]
        cmd: VaultCmd,
    },

    /// Manage paired friends (pair / list / forget).
    Friend {
        #[command(subcommand)]
        cmd: FriendCmd,
    },

    /// Manage this identity's devices (invite / join / list / revoke).
    Device {
        #[command(subcommand)]
        cmd: DeviceCmd,
    },

    /// Manage storage backends (add / list / info / rm).
    Store {
        #[command(subcommand)]
        cmd: StoreCmd,
    },

    /// Run vup permanently as a background service (install / uninstall / status).
    Service {
        #[command(subcommand)]
        cmd: cmd::service::ServiceCmd,
    },

    // -- Ops -----------------------------------------------------------------
    /// Show node status, configured stores, sources, and running tasks.
    Status,

    /// Health walk: daemon reachable, stores + staging, scheduled backups,
    /// service, and observed peers.
    #[command(alias = "d")]
    Doctor,

    /// Show or edit configuration (`--json` / `--patch` escape hatch).
    Config {
        /// Optional vault to inspect.
        vault: Option<String>,
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

    /// List node tasks, or show/follow one by id.
    #[command(alias = "t")]
    Tasks {
        /// Task id (omit to list all).
        task_id: Option<u64>,
    },

    /// Cancel a running task.
    #[command(alias = "x")]
    Cancel {
        /// Task id.
        task_id: u64,
    },

    /// Shut down the running s5 node.
    Shutdown,

    // -- Hidden legacy aliases (through the beta) ----------------------------
    /// Hidden legacy alias of `vup vault create`.
    #[command(hide = true)]
    New {
        /// Vault to create (`+music` or `music`).
        vault: String,
    },
    /// Hidden legacy alias of `vup vault drop`.
    #[command(hide = true, name = "drop")]
    DropAlias {
        /// Vault to drop.
        vault: String,
    },
    /// Hidden legacy alias of `vup share` (subtree via `--path`).
    #[command(hide = true)]
    Export {
        /// `vault:[path]` reference.
        #[arg(value_parser = refs::vault_ref_arg)]
        reference: String,
    },
    /// Hidden legacy alias: pretty-print a vault's config block.
    #[command(hide = true)]
    Info {
        /// `vault:` reference.
        #[arg(value_parser = refs::vault_ref_arg)]
        reference: String,
    },
    /// Hidden legacy alias of `vup tasks <id>`.
    #[command(hide = true, name = "task-status")]
    TaskStatus {
        /// Task id.
        task_id: u64,
    },
    /// Hidden legacy alias of `vup friend list`.
    #[command(hide = true)]
    Peers,
    /// Hidden legacy alias of `vup friend pair`.
    #[command(hide = true)]
    Pair {
        /// Token from the friend's side. Omit to mint one and wait.
        token: Option<String>,
    },
    /// Hidden legacy alias of `vup friend forget`.
    #[command(hide = true)]
    Unpair {
        /// Identity to forget (e.g. `@alice`).
        id: String,
    },

    /// Run the s5 node daemon (internal — used by auto-start).
    #[command(hide = true, name = "_daemon")]
    Daemon,
}

/// Build the daemon's tracing filter.
///
/// Honours `RUST_LOG` if set; otherwise the node log always captures
/// full debug detail for the s5/vup/sia_storage side (per-shard Sia
/// uploads, packing dedup + flush decisions, indexd sync — the record
/// you want after the fact when something stalled), while the chatty
/// third-party dependencies (iroh internals, quinn, pkarr resolvers,
/// hickory DNS) stay clamped to warn. Operators who want raw iroh
/// detail can set e.g. `RUST_LOG=debug,iroh=debug` without losing the
/// s5-side context.
fn daemon_log_filter(verb: tracing::level_filters::LevelFilter) -> tracing_subscriber::EnvFilter {
    use tracing_subscriber::EnvFilter;

    if std::env::var("RUST_LOG").is_ok() {
        return EnvFilter::from_default_env();
    }

    // The file log is the flight recorder: never below debug (a `-v`
    // asking for trace still wins).
    let verb = verb.max(tracing::level_filters::LevelFilter::DEBUG);
    let baseline = verb.to_string().to_lowercase();
    let directives = [
        baseline.as_str(),
        "iroh=warn",
        "iroh_blobs=warn",
        "iroh_relay=warn",
        "iroh_quinn=warn",
        "iroh_quinn_proto=warn",
        "iroh_dns_node_info=warn",
        "iroh_metrics=warn",
        "iroh_net_report=warn",
        "iroh_dns_server=warn",
        "pkarr=warn",
        "mainline=warn",
        "quinn=warn",
        "quinn_proto=warn",
        "hickory_resolver=warn",
        "hickory_proto=warn",
        "h2=warn",
        "hyper=warn",
        "hyper_util=warn",
        "rustls=warn",
        "swarm_discovery=warn",
        "watchable=warn",
    ];
    EnvFilter::try_new(directives.join(",")).unwrap_or_else(|_| EnvFilter::new(baseline))
}

fn resolve_config(cli_override: Option<PathBuf>) -> Result<PathBuf> {
    match cli_override {
        Some(p) => Ok(p),
        None => node::default_config_path(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // D20 compatibility: rewrite the legacy subject-first `+vault <verb> …`
    // form into the verb-first `<verb> vault: …` form before clap parses.
    let mut argv: Vec<String> = std::env::args().collect();
    refs::rewrite_legacy_plus(&mut argv);

    let cli = Cli::parse_from(&argv);

    let is_daemon = matches!(&cli.cmd, Commands::Daemon);

    if is_daemon {
        let log_dir = dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("s5")
            .join("logs");
        std::fs::create_dir_all(&log_dir)?;

        // Daily rotation with bounded retention: the daemon always logs at
        // debug for the domain crates (drill directive), so an unrotated
        // node.log grows without bound. Files land as
        // `node.log.YYYY-MM-DD`, newest 7 kept.
        let file_appender = tracing_appender::rolling::Builder::new()
            .rotation(tracing_appender::rolling::Rotation::DAILY)
            .filename_prefix("node.log")
            .max_log_files(7)
            .build(&log_dir)
            .context("building the rotating node.log appender")?;
        let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

        tracing_subscriber::fmt()
            .with_env_filter(daemon_log_filter(cli.verbosity.tracing_level_filter()))
            .with_writer(file_writer)
            .with_ansi(false)
            .init();

        tracing::info!(log_dir = %log_dir.display(), "s5_node daemon log open (node.log.<date>, daily rotation, 7 kept)");

        // Keep the appender guard alive for the program's lifetime.
        Box::leak(Box::new(guard));
    } else {
        tracing_subscriber::fmt()
            .with_max_level(cli.verbosity)
            .init();
    }

    let config_path = resolve_config(cli.config)?;

    interact::set_assume_yes(cli.yes);

    let result = run_command(cli.cmd, &config_path).await;

    // Scripting contract (cli-workflows.md § Exit codes): a prompt that was
    // needed but couldn't be answered exits 3, not 1.
    if let Err(e) = &result
        && e.downcast_ref::<interact::NonInteractive>().is_some()
    {
        eprintln!("Error: {e:#}");
        std::process::exit(3);
    }
    result
}

/// Parse a token that must be a vault reference.
fn parse_vault(reference: &str) -> Result<VaultRef> {
    refs::parse_ref(reference)
        .and_then(|r| r.require_vault())
        .map_err(|e| anyhow::anyhow!(e))
}

async fn run_command(cmd: Commands, config_path: &std::path::Path) -> Result<()> {
    // The CLI is an RPC frontend; every verb routes through the running
    // daemon EXCEPT the bootstrap verbs, which create/own the config the
    // daemon needs and must never auto-spawn it.
    if matches!(&cmd, Commands::Daemon) {
        return node::run_daemon(config_path).await;
    }
    if matches!(&cmd, Commands::Onboard) {
        return cmd::onboard::run_onboard(config_path).await;
    }
    if matches!(&cmd, Commands::Recover) {
        return cmd::recover::run_recover(config_path).await;
    }
    // `device join` and a top-level `join <vupd-…>` both CREATE this
    // machine's config — daemon-less, like onboard/recover.
    if let Commands::Device {
        cmd: DeviceCmd::Join { code },
    } = &cmd
    {
        return cmd::device::run_join(config_path, code).await;
    }
    if let Commands::Join { url } = &cmd
        && url.starts_with("vupd-")
    {
        return cmd::device::run_join(config_path, url).await;
    }
    // `service` manages the daemon's lifecycle from OUTSIDE.
    if let Commands::Service { cmd } = &cmd {
        return cmd::service::run_service(cmd, config_path).await;
    }

    let client = node::ensure_node_running(config_path).await?;
    let result = dispatch(&client, cmd).await;
    client.close().await;
    result
}

async fn dispatch(client: &s5_node_api::S5NodeClient, cmd: Commands) -> Result<()> {
    match cmd {
        // Handled in run_command before the daemon connection.
        Commands::Daemon | Commands::Onboard | Commands::Recover | Commands::Service { .. } => {
            unreachable!("daemon-less, handled above")
        }

        // -- Data verbs -----------------------------------------------------
        Commands::Backup { args } => cmd::backup::run_backup(client, &args).await,
        Commands::Restore {
            reference,
            target,
            force,
        } => {
            let vref = parse_vault(&reference)?;
            cmd::vault::run_restore(client, &vref, &target, force).await
        }
        Commands::List { reference, all } => match reference {
            None => cmd::lifecycle::run_list(client, all).await,
            Some(r) => match refs::parse_ref(&r).map_err(|e| anyhow::anyhow!(e))? {
                refs::Ref::Vault { name, path, snap } => {
                    cmd::lifecycle::run_list_tree(client, &name, path, snap).await
                }
                refs::Ref::Hash(_) => anyhow::bail!(
                    "listing a bare `#<hash>` snapshot is not wired yet — list it through \
                     its vault instead, e.g. `vup list docs:#<snap>`"
                ),
                refs::Ref::Local(p) => anyhow::bail!(
                    "'{}' is a local path — to list a vault's contents add a trailing \
                     colon (e.g. `vup list docs:`)",
                    p.display()
                ),
            },
        },
        Commands::History { reference } => {
            let vault = resolve_vault_arg(client, reference).await?;
            cmd::vault::run_history(client, &vault.name).await
        }
        Commands::Mount {
            reference,
            dir,
            rw,
            debounce_ms,
        } => {
            let vref = parse_vault(&reference)?;
            if vref.snap.is_some() || vref.path.is_some() {
                anyhow::bail!(
                    "mounting a past snapshot or subtree isn't wired yet — mount the live vault \
                     (`{}:`) and browse inside it, or `restore {}` to a directory",
                    vref.name,
                    reference
                );
            }
            cmd::vault::run_mount(client, &vref.name, &dir, rw, debounce_ms).await
        }
        Commands::Share {
            reference,
            deep,
            name,
            live,
            every,
        } => {
            let vref = parse_vault(&reference)?;
            let opts = cmd::share::ShareOpts {
                deep,
                name,
                live,
                every,
            };
            cmd::share::run_share(client, &vref, opts).await
        }
        Commands::Copy { src, dst, deep } => {
            let src_ref = refs::parse_ref(&src).map_err(|e| anyhow::anyhow!(e))?;
            let dst_ref = refs::parse_ref(&dst).map_err(|e| anyhow::anyhow!(e))?;
            cmd::copy::run(client, src_ref, dst_ref, deep).await
        }
        Commands::Join { url } => cmd::stubs::run_join(client, &url).await,
        Commands::Automate { cmd } => cmd::automate::run_automate(client, cmd).await,

        // -- Access ---------------------------------------------------------
        Commands::Grant {
            reference,
            id,
            read,
            write,
        } => {
            let vref = parse_vault(&reference)?;
            cmd::stubs::run_grant(client, &vref.name, &id, read, write).await
        }
        Commands::Revoke { reference, id } => {
            let vref = parse_vault(&reference)?;
            cmd::membership::run_revoke(client, &vref.name, &id).await
        }
        Commands::Who { reference } => {
            let vault = resolve_vault_arg(client, reference).await?;
            cmd::membership::run_who(client, &vault.name).await
        }

        // -- Namespaces -----------------------------------------------------
        Commands::Vault { cmd } => match cmd {
            VaultCmd::Create { name } => {
                cmd::lifecycle::run_vault_create(client, &refs::strip_plus(&name)).await
            }
            VaultCmd::Drop { name } => {
                cmd::lifecycle::run_vault_drop(client, &refs::strip_plus(&name)).await
            }
            VaultCmd::Rename { old, new } => {
                cmd::lifecycle::run_vault_rename(
                    client,
                    &refs::strip_plus(&old),
                    &refs::strip_plus(&new),
                )
                .await
            }
        },
        Commands::Friend { cmd } => match cmd {
            FriendCmd::Pair { token } => cmd::stubs::run_pair_top_level(client, token).await,
            FriendCmd::List => cmd::membership::run_friend_ls(client).await,
            FriendCmd::Forget { id } => cmd::membership::run_friend_forget(client, &id).await,
        },
        Commands::Device { cmd } => match cmd {
            DeviceCmd::Invite { label } => cmd::device::run_invite(client, label).await,
            DeviceCmd::List => cmd::device::run_ls(client).await,
            DeviceCmd::Revoke { id } => cmd::device::run_revoke(client, &id).await,
            DeviceCmd::Join { .. } => unreachable!("daemon-less, handled above"),
        },
        Commands::Store { cmd } => cmd::store::run_store(client, cmd).await,

        // -- Ops ------------------------------------------------------------
        Commands::Status => cmd::run_status(client).await,
        Commands::Doctor => cmd::doctor::run_doctor(client).await,
        Commands::Config {
            vault,
            json,
            patch,
            patch_file,
        } => cmd::run_config(client, vault, json, patch, patch_file).await,
        Commands::Tasks { task_id } => match task_id {
            Some(id) => cmd::tasks::task_status(client, id).await,
            None => cmd::tasks::list_tasks(client).await,
        },
        Commands::Cancel { task_id } => cmd::tasks::cancel_task(client, task_id).await,
        Commands::Shutdown => cmd::run_shutdown(client).await,

        // -- Hidden legacy aliases ------------------------------------------
        Commands::New { vault } => {
            cmd::lifecycle::run_vault_create(client, &refs::strip_plus(&vault)).await
        }
        Commands::DropAlias { vault } => {
            cmd::lifecycle::run_vault_drop(client, &refs::strip_plus(&vault)).await
        }
        Commands::Export { reference } => {
            let vref = parse_vault(&reference)?;
            cmd::share::run_share(client, &vref, cmd::share::ShareOpts::default()).await
        }
        Commands::Info { reference } => {
            let vref = parse_vault(&reference)?;
            cmd::vault::run_info(client, &vref.name).await
        }
        Commands::TaskStatus { task_id } => cmd::tasks::task_status(client, task_id).await,
        Commands::Peers => cmd::membership::run_friend_ls(client).await,
        Commands::Pair { token } => cmd::stubs::run_pair_top_level(client, token).await,
        Commands::Unpair { id } => cmd::membership::run_friend_forget(client, &id).await,
    }
}

/// Resolve an optional `vault:` argument to a concrete vault, applying the
/// D20 default: no ref → the sole configured vault, else require an
/// explicit ref. Used by the harmless read verbs (`history`, `who`).
async fn resolve_vault_arg(
    client: &s5_node_api::S5NodeClient,
    reference: Option<String>,
) -> Result<VaultRef> {
    if let Some(r) = reference {
        return parse_vault(&r);
    }
    let resp = client.get_config().await?;
    let config: serde_json::Value = serde_json::from_str(&resp.config_json)?;
    let user_vaults: Vec<String> = config
        .get("vault")
        .and_then(|v| v.as_object())
        .map(|o| {
            o.keys()
                .filter(|n| !refs::is_system_vault(n))
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    match user_vaults.as_slice() {
        [one] => Ok(VaultRef {
            name: one.clone(),
            path: None,
            snap: None,
        }),
        [] => anyhow::bail!("no vaults configured — create one with `vup backup <path> <name>:`"),
        _ => anyhow::bail!(
            "multiple vaults configured — name one explicitly, e.g. `{}:`",
            user_vaults[0]
        ),
    }
}
