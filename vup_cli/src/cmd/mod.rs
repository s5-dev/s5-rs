use std::path::{Path, PathBuf};

use anyhow::{Context, bail, Result};

use crate::config::{Source, VaultConfig};
use crate::vault;

// ── vup config ──────────────────────────────────────────────────────

/// Interactive configuration. Detects current state and walks the user
/// through whatever is missing (seed phrase, etc.).
pub async fn run_config(config_path: &Path) -> Result<()> {
    let mut cfg = VaultConfig::load(config_path)?;
    let is_new = cfg.vault.seed_phrase.is_none();

    if is_new {
        println!("No vault found — setting up a new one.");
        println!();

        let seed_phrase = s5_client::keys::generate_seed_phrase()
            .map_err(|e| anyhow::anyhow!("failed to generate seed phrase: {e}"))?;

        // Validate round-trip before showing to user
        let _keys = s5_client::DerivedKeys::from_seed_phrase(&seed_phrase)
            .map_err(|e| anyhow::anyhow!("generated phrase failed validation: {e}"))?;

        println!("Your recovery phrase (write this down and store it safely):");
        println!();
        println!("  {seed_phrase}");
        println!();
        println!("WARNING: This phrase is the ONLY way to recover your encrypted backups.");
        println!("         If you lose it, your data cannot be recovered.");
        println!();

        let confirmed = dialoguer::Confirm::new()
            .with_prompt("I have saved my recovery phrase")
            .default(false)
            .interact()?;

        if !confirmed {
            bail!("Aborted. Run `vup config` again when ready.");
        }

        cfg.vault.seed_phrase = Some(seed_phrase);
    } else {
        println!("Vault configured at {}", config_path.display());
        println!();
    }

    // Show current targets
    if !cfg.targets.is_empty() {
        println!("Current targets:");
        for (name, store) in &cfg.targets {
            println!("  {name}: {store:?}");
        }
        println!();
    }

    cfg.save(config_path)?;

    if is_new {
        println!();
        println!("Vault initialized at {}", config_path.display());
        println!("Next: run `vup add <directory>` to start tracking files.");
    }

    Ok(())
}

// ── vup add ─────────────────────────────────────────────────────────

/// Register source directories and run initial index.
pub async fn run_add(paths: Vec<PathBuf>, config_path: &Path) -> Result<()> {
    let mut cfg = VaultConfig::load(config_path)?;
    if cfg.vault.seed_phrase.is_none() {
        bail!("Vault not configured. Run `vup config` first.");
    }

    let mut added = Vec::new();
    for p in &paths {
        let abs = p
            .canonicalize()
            .with_context(|| format!("path does not exist: {}", p.display()))?;

        if !abs.is_dir() {
            bail!("{} is not a directory", abs.display());
        }

        if cfg.sources.iter().any(|s| s.path == abs) {
            println!("  already tracked: {}", abs.display());
            continue;
        }

        cfg.sources.push(Source { path: abs.clone() });
        println!("  added: {}", abs.display());
        added.push(abs);
    }

    cfg.save(config_path)?;

    // Re-index all sources (including newly added ones)
    let source_paths: Vec<PathBuf> = cfg.sources.iter().map(|s| s.path.clone()).collect();
    let fs = vault::open_index()?;
    let (files, bytes) = vault::reindex(&fs, &source_paths).await?;
    fs.shutdown().await?;

    println!();
    println!(
        "Indexed {} files ({})",
        files,
        vault::format_bytes(bytes),
    );

    Ok(())
}

// ── vup status ──────────────────────────────────────────────────────

/// Re-index then show vault status.
pub async fn run_status(config_path: &Path) -> Result<()> {
    if !config_path.exists() {
        println!("Vault not configured. Run `vup config` to get started.");
        return Ok(());
    }

    let cfg = VaultConfig::load(config_path)?;

    if cfg.vault.seed_phrase.is_none() {
        println!("Vault not configured. Run `vup config` to get started.");
        return Ok(());
    }

    // Re-index all sources
    let source_paths: Vec<PathBuf> = cfg.sources.iter().map(|s| s.path.clone()).collect();
    if !source_paths.is_empty() {
        let fs = vault::open_index()?;
        vault::reindex(&fs, &source_paths).await?;
        fs.shutdown().await?;
    }

    println!("Vault: {}", config_path.display());
    println!();

    // Sources
    println!("Sources ({}):", cfg.sources.len());
    if cfg.sources.is_empty() {
        println!("  (none) — run `vup add <path>` to track directories");
    } else {
        for s in &cfg.sources {
            // TODO: show file count + size per source, and backup status
            println!("  {}", s.path.display());
        }
    }
    println!();

    // Targets
    println!("Targets ({}):", cfg.targets.len());
    if cfg.targets.is_empty() {
        println!("  (none) — run `vup config` to add a backup target");
    } else {
        for (name, store) in &cfg.targets {
            println!("  {name}: {store:?}");
        }
    }

    Ok(())
}

// ── vup backup ──────────────────────────────────────────────────────

/// Re-index, diff against backup root, upload changed blobs.
pub async fn run_backup(config_path: &Path, target_name: Option<&str>) -> Result<()> {
    let cfg = VaultConfig::load(config_path)?;
    if cfg.vault.seed_phrase.is_none() {
        bail!("Vault not configured. Run `vup config` first.");
    }
    if cfg.sources.is_empty() {
        bail!("No sources tracked. Run `vup add <path>` first.");
    }

    // Resolve target
    let _target = match target_name {
        Some(name) => cfg
            .targets
            .get(name)
            .with_context(|| format!("target not found: {name}"))?,
        None => {
            if cfg.targets.len() == 1 {
                cfg.targets.values().next().unwrap()
            } else if cfg.targets.is_empty() {
                bail!("No targets configured. Run `vup config` to add one.");
            } else {
                bail!("Multiple targets configured. Specify one with --target <name>.");
            }
        }
    };

    // Re-index all sources
    let source_paths: Vec<PathBuf> = cfg.sources.iter().map(|s| s.path.clone()).collect();
    let index_fs = vault::open_index()?;
    let (files, bytes) = vault::reindex(&index_fs, &source_paths).await?;

    println!("Indexed {} files ({})", files, vault::format_bytes(bytes));

    // TODO: diff index vs backup root, upload changed blobs
    println!();
    println!("Backup upload not yet implemented. Index is up to date.");

    index_fs.shutdown().await?;
    Ok(())
}

