// server/src/cli/config.rs

use serde::Deserialize;
use anyhow::Result; // Removed 'anyhow' from the import list as it's not directly used
use std::path::PathBuf;

/// Represents the `[server]` section of the config.toml.
#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    /// The default port for the main GraphDB daemon.
    /// This can be overridden by CLI flags.
    pub port: Option<u16>,
    /// The host address for the server.
    pub host: String,
}

/// Represents the `[daemon]` section of the config.toml.
/// Contains configuration specific to the daemonization process.
#[derive(Debug, Deserialize)]
pub struct DaemonConfig {
    /// The base name for daemon processes (e.g., "graphdb-cli").
    pub process_name: String,
    /// The user to run the daemon as (optional).
    pub user: Option<String>,
    /// The group to run the daemon as (optional).
    pub group: Option<String>,
}

/// Represents the entire structure of the `config.toml` file.
#[derive(Debug, Deserialize)]
pub struct CliConfig {
    pub server: ServerConfig,
    pub daemon: DaemonConfig,
}

/// Loads the CLI configuration from `server/src/cli/config.toml`.
///
/// This function constructs the path to the config file relative to the
/// `CARGO_MANIFEST_DIR` (which is the root of the `server` crate).
/// It reads the file content and parses it using `toml` and `serde`.
///
/// # Returns
/// - `Ok(CliConfig)` if the configuration is loaded and parsed successfully.
/// - `Err(anyhow::Error)` if the file cannot be read or parsed.
pub fn load_cli_config() -> Result<CliConfig, anyhow::Error> {
    // Construct the path to the config file.
    // env!("CARGO_MANIFEST_DIR") gives the path to the Cargo.toml of the current crate (server).
    // We then append the relative path to config.toml within the src/cli directory.
    let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("cli")
        .join("config.toml");

    // Read the content of the config file into a string.
    let config_content = std::fs::read_to_string(&config_path)
        .map_err(|e| anyhow::anyhow!("Failed to read config file {}: {}", config_path.display(), e))?;

    // Parse the TOML content into the CliConfig struct.
    let config: CliConfig = toml::from_str(&config_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse config file {}: {}", config_path.display(), e))?;

    Ok(config)
}

