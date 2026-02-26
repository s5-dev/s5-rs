use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Top-level vault configuration, stored at ~/.config/vup/config.toml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultConfig {
    #[serde(default)]
    pub vault: VaultSettings,

    #[serde(default)]
    pub sources: Vec<Source>,

    /// Backup targets keyed by name (local stores only for now).
    #[serde(default)]
    pub targets: BTreeMap<String, LocalStoreConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultSettings {
    /// BIP39 seed phrase for key derivation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed_phrase: Option<String>,
}

impl Default for VaultSettings {
    fn default() -> Self {
        Self { seed_phrase: None }
    }
}

/// A source directory tracked by the vault
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    /// Absolute path to the source directory
    pub path: PathBuf,
}

/// Local store configuration.
///
/// Only local-filesystem stores are supported for now. Remote store types
/// (Sia renterd, S3, etc.) will be added in a future milestone.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalStoreConfig {
    /// Filesystem path where blobs are stored.
    pub base_path: String,
}

impl Default for VaultConfig {
    fn default() -> Self {
        Self {
            vault: VaultSettings::default(),
            sources: Vec::new(),
            targets: BTreeMap::new(),
        }
    }
}

impl VaultConfig {
    /// Load config from a TOML file, or return default if it doesn't exist.
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        if path.exists() {
            let content = std::fs::read_to_string(path)?;
            Ok(toml::from_str(&content)?)
        } else {
            Ok(Self::default())
        }
    }

    /// Save config to a TOML file, creating parent directories if needed.
    pub fn save(&self, path: &std::path::Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
}
