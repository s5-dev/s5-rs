use crate::init_config::CmdConfig;
use anyhow::Context;
use clap::{ArgAction, Parser, Subcommand};
use clap_verbosity_flag::InfoLevel;
use directories::ProjectDirs;
use std::path::PathBuf;

mod cmd;
mod helpers;
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
    /// Low-level blob operations against a configured peer
    Blobs {
        #[command(subcommand)]
        cmd: BlobsCmd,
    },
    /// Snapshot utilities for FS5/registry-backed roots
    Snapshots {
        #[command(subcommand)]
        cmd: SnapshotsCmd,
    },
    /// Mount an FS5 root via FUSE (through the integrated FUSE support in S5)
    Mount {
        /// Mount point for the FUSE filesystem
        mount_point: PathBuf,
        /// Optional override for the FS5 root directory; defaults to the node's fs_roots/<node>.fs5
        #[arg(long, value_name = "PATH")]
        root: Option<PathBuf>,
        /// Optional subdirectory inside the FS5 root to mount
        #[arg(long, value_name = "PATH")]
        subdir: Option<String>,
        /// Mount the filesystem as read-only
        #[arg(long, action = ArgAction::SetTrue)]
        read_only: bool,
        /// Allow root user to access filesystem
        #[arg(long, action = ArgAction::SetTrue)]
        allow_root: bool,
        /// Automatically unmount on process exit (if supported on this platform)
        #[arg(long, action = ArgAction::SetTrue)]
        auto_unmount: bool,
    },
    /// Print a tree of the FS5 root for debugging
    Tree {
        /// Optional directory path inside the FS5 root to start from
        #[arg(long, value_name = "PATH")]
        path: Option<String>,
    },
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
        /// Optional prefix to prepend to imported paths.
        /// If not provided, defaults to the full URL structure (scheme/host/path).
        #[arg(long)]
        prefix: Option<String>,
    },
    Local {
        path: PathBuf,
        /// max number of concurrent blob imports
        #[arg(short, long, value_name = "COUNT", default_value_t = 4)]
        concurrency: usize,
        /// Optional prefix to prepend to imported paths.
        /// If not provided, defaults to the absolute path of the source file.
        #[arg(long)]
        prefix: Option<String>,
        /// Show results from files/directories normally ignored by .gitignore, .ignore,
        /// .fdignore or global ignore files. Can be re-enabled with --ignore.
        #[arg(short = 'I', long = "no-ignore", action = ArgAction::SetTrue)]
        no_ignore: bool,
        /// Show results from files/directories normally ignored by VCS ignore files
        /// like .gitignore, git's global excludes or .git/info/exclude. Can be
        /// re-enabled with --ignore-vcs.
        #[arg(long = "no-ignore-vcs", action = ArgAction::SetTrue)]
        no_ignore_vcs: bool,
        /// Re-enable all ignore files after -I/--no-ignore.
        #[arg(long = "ignore", action = ArgAction::SetTrue)]
        ignore: bool,
        /// Re-enable VCS ignore files after --no-ignore-vcs.
        #[arg(long = "ignore-vcs", action = ArgAction::SetTrue)]
        ignore_vcs: bool,
        /// Show results from directories marked with CACHEDIR.TAG.
        /// Can be re-enabled with --ignore-cachedir.
        #[arg(long = "no-ignore-cachedir", action = ArgAction::SetTrue)]
        no_ignore_cachedir: bool,
        /// Re-enable CACHEDIR.TAG ignore after --no-ignore-cachedir.
        #[arg(long = "ignore-cachedir", action = ArgAction::SetTrue)]
        ignore_cachedir: bool,
        /// Skip metadata checks and always import files (fast path).
        #[arg(long, action = ArgAction::SetTrue)]
        always_import: bool,
    },
}

#[derive(Subcommand)]
enum BlobsCmd {
    /// Upload a local file as a blob to a peer
    Upload {
        /// Name of the peer in the node config (e.g. "paid")
        #[arg(short, long)]
        peer: String,
        /// Local file path to upload
        path: PathBuf,
    },
    /// Download a blob from a peer into a local file
    Download {
        /// Name of the peer in the node config (e.g. "paid")
        #[arg(short, long)]
        peer: String,
        /// Blob hash in hex (BLAKE3, 32 bytes)
        hash: String,
        /// Output file path to write the blob to
        #[arg(long)]
        out: PathBuf,
    },
    /// Delete (unpin) a blob on a peer
    Delete {
        /// Name of the peer in the node config (e.g. "paid")
        #[arg(short, long)]
        peer: String,
        /// Blob hash in hex (BLAKE3, 32 bytes)
        hash: String,
    },
    /// Perform conservative garbage collection on a local blob store
    /// used by this node. Only deletes blobs that have no pins in the
    /// node registry and are not reachable from the primary FS5 root
    /// (its current head and any snapshots).
    GcLocal {
        /// Name of the local store in the node config (e.g. "default")
        #[arg(long, value_name = "STORE_NAME", default_value = "default")]
        store: String,
        /// If set, only print which blobs would be deleted.
        #[arg(long, action = ArgAction::SetTrue)]
        dry_run: bool,
    },
    /// Verify that all blobs referenced from the primary FS5 root
    /// (current head and any local snapshots) exist in the given
    /// local store. This command is read-only and does not modify
    /// any data.
    VerifyLocal {
        /// Name of the local store in the node config (e.g. "default")
        #[arg(long, value_name = "STORE_NAME", default_value = "default")]
        store: String,
    },
}

#[derive(Subcommand)]
enum SnapshotsCmd {
    /// Show the current remote snapshot head for a sync job
    Head {
        /// Name of the sync entry in the node config (e.g. "project")
        #[arg(long, value_name = "NAME")]
        sync: String,
    },
    /// Download a raw directory snapshot blob from a peer
    Download {
        /// Name of the peer in the node config (e.g. "paid")
        #[arg(short, long)]
        peer: String,
        /// Snapshot blob hash in hex (BLAKE3, 32 bytes)
        hash: String,
        /// Output file path to write the snapshot bytes
        #[arg(long)]
        out: PathBuf,
    },
    /// Restore a directory snapshot into a local FS5 root
    Restore {
        /// Local directory that will host `root.fs5.cbor` and metadata blobs
        #[arg(long, value_name = "PATH")]
        root: PathBuf,
        /// Name of the peer in the node config (e.g. "paid")
        #[arg(short, long)]
        peer: String,
        /// Snapshot blob hash in hex (BLAKE3, 32 bytes)
        #[arg(long)]
        hash: String,
    },
    /// List snapshots for the local FS5 root backing this node
    ListFs,
    /// Create a new snapshot for the local FS5 root backing this node
    CreateFs,
    /// Delete a snapshot from the local FS5 root and unpin its hash
    DeleteFs {
        /// Snapshot name as listed by `s5 snapshots list-fs`
        #[arg(long, value_name = "NAME")]
        name: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_max_level(cli.verbosity)
        .init();

    // Use a simple layout for configs and data:
    // - Configs under:  ~/.config/s5/
    //   - Default node: ~/.config/s5/local.toml
    //   - Other nodes:  ~/.config/s5/nodes/<name>.toml
    // - Data under:     ~/.local/share/s5/
    let dirs =
        ProjectDirs::from("", "", "s5").context("failed to determine config directory path")?;

    let config_root = dirs.config_dir();
    let node_config_file = if cli.node == "local" {
        config_root.join("local.toml")
    } else {
        config_root
            .join("nodes")
            .join(&cli.node)
            .with_extension("toml")
    };

    let local_data_dir = dirs.data_dir();

    cmd::run_command(&dirs, &cli.node, node_config_file, local_data_dir, cli.cmd).await
}
