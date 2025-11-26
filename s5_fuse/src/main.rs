use clap::{Arg, ArgAction, Command};
use s5_core::BlobStore;
use s5_fs::{DirContext, FS5};
use s5_store_local::{LocalStore, LocalStoreConfig};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(true)
        .init();
    let matches = Command::new("s5-fuse")
        .about("Read-only FUSE for S5 FS")
        .arg(
            Arg::new("MOUNT_POINT")
                .required(true)
                .index(1)
                .help("Mount point for the FUSE filesystem"),
        )
        .arg(
            Arg::new("root")
                .long("root")
                .short('r')
                .value_name("PATH")
                .help("Directory containing root.fs5.cbor and blobs")
                .required(true),
        )
        .arg(
            Arg::new("store_path")
                .long("store-path")
                .value_name("PATH")
                .help("Path to the local blob store (defaults to root path if unset)"),
        )
        .arg(
            Arg::new("auto_unmount")
                .long("auto-unmount")
                .action(ArgAction::SetTrue)
                .help("Automatically unmount on process exit"),
        )
        .arg(
            Arg::new("allow-root")
                .long("allow-root")
                .action(ArgAction::SetTrue)
                .help("Allow root user to access filesystem"),
        )
        .get_matches();

    let mountpoint = PathBuf::from(matches.get_one::<String>("MOUNT_POINT").unwrap());
    let root = PathBuf::from(matches.get_one::<String>("root").unwrap());
    let store_path = matches
        .get_one::<String>("store_path")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.clone());
    let allow_root = matches.get_flag("allow-root");
    let auto_unmount = matches.get_flag("auto_unmount");

    // Open FS5 on the given root
    let ctx = match DirContext::open_local_root(&root) {
        Ok(ctx) => {
            info!("s5_fuse: DirContext::open_local_root succeeded");
            ctx
        }
        Err(e) => {
            info!("s5_fuse: DirContext::open_local_root failed: {e}");
            return Err(e);
        }
    };
    let fs5 = FS5::open(ctx);
    info!("s5_fuse: FS5::open created root handle");

    // Local blob store for reading file contents and metadata blobs
    let local = LocalStore::create(LocalStoreConfig {
        base_path: store_path.to_string_lossy().to_string(),
    });
    let store = BlobStore::new(local);

    s5_fuse::mount(&mountpoint, fs5, store, true, allow_root, auto_unmount).await
}
