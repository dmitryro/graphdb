// storage_daemon_server/src/lib.rs

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::oneshot;
use tokio::signal; // Add this for Ctrl+C handling within the daemon process

// Declare the storage_client module.
pub mod storage_client;

// Re-export the necessary items from the storage_client module.
// This makes `StorageClient` directly accessible via `storage_daemon_server::StorageClient`
// for external crates, without needing `storage_daemon_server::storage_client::StorageClient`.
pub use storage_client::StorageClient;

// --- MOCK DEFINITIONS FOR StorageRequest and StorageResponse ---
// These are placeholders. You should replace them with your actual
// message formats (e.g., from protobuf definitions if you have them).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageRequest {
    pub key: String,
    pub value: Option<String>, // Value is optional for Get, required for Put
    pub operation: String,     // e.g., "GET", "PUT", "DELETE"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageResponse {
    pub success: bool,
    pub message: String,
    pub data: Option<String>, // Data returned for GET operations
}
// --- END MOCK DEFINITIONS ---


// --- Mocks/Placeholders for actual daemon logic ---

// This function represents the actual entry point for the storage daemon's server.
// It should contain the logic to set up and run your gRPC server, database connections,
// and other core storage functionalities.
pub async fn start_storage_daemon_server_real(
    port: u16,
    settings: StorageSettings, // Now passes the full settings
    shutdown_rx: oneshot::Receiver<()>, // Receiver for graceful shutdown
) -> Result<()> {
    println!("[Storage Daemon] Starting real storage daemon server on port {}", port);
    println!("[Storage Daemon] Data directory: {:?}", settings.data_directory);
    println!("[Storage Daemon] Log directory: {:?}", settings.log_directory);
    // Print the *actual* port being used, which may differ from settings.default_port
    println!("[Storage Daemon] Effective listening port: {}", port);
    println!("[Storage Daemon] Default port from config: {}", settings.default_port); // For verification of config value
    println!("[Storage Daemon] Cluster range: {}", settings.cluster_range);
    println!("[Storage Daemon] Max disk space: {} GB", settings.max_disk_space_gb);
    println!("[Storage Daemon] Min disk space: {} GB", settings.min_disk_space_gb);
    println!("[Storage Daemon] Use Raft for scale: {}", settings.use_raft_for_scale);

    // --- ACTUAL STORAGE DAEMON SERVER INITIALIZATION GOES HERE ---
    // Example: Bind to port, start gRPC server, initialize internal state, etc.
    // let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    // tonic::transport::Server::builder()
    //      .add_service(YourStorageServiceServer::new(YourServiceImpl))
    //      .serve_with_shutdown(listener, async {
    //           shutdown_rx.await.ok(); // Wait for shutdown signal
    //      })
    //      .await?;

    println!("[Storage Daemon] Running. Waiting for shutdown signal...");
    
    // In this mock, we just wait for the shutdown signal.
    // In a real server, the .serve() method would block until shutdown.
    shutdown_rx.await.map_err(|e| anyhow::anyhow!("Shutdown signal error: {}", e))?;
    
    println!("[Storage Daemon] Shutdown signal received. Exiting.");
    Ok(())
}

// --- Configuration Structs ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSettings {
    pub data_directory: PathBuf,
    pub log_directory: PathBuf,
    #[serde(default = "StorageSettings::default_default_port")] // Use default for this field
    pub default_port: u16,
    pub cluster_range: String,
    pub max_disk_space_gb: u64,
    pub min_disk_space_gb: u64,
    pub use_raft_for_scale: bool,
}

impl Default for StorageSettings {
    fn default() -> Self {
        StorageSettings {
            data_directory: PathBuf::from("/opt/graphdb/storage_data"),
            log_directory: PathBuf::from("/var/log/graphdb"),
            default_port: 8085, // This is the *application's* hardcoded fallback default
            cluster_range: "9000-9002".to_string(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
        }
    }
}

impl StorageSettings {
    // A helper for serde's default attribute, pointing to the default value.
    fn default_default_port() -> u16 {
        StorageSettings::default().default_port
    }

    /// Loads storage settings from a YAML file.
    /// Expects a structure like:
    /// ```yaml
    /// storage:
    ///    data_directory: "/path/to/data"
    ///    log_directory: "/path/to/logs"
    ///    # ... other settings
    /// ```
    pub fn load_from_yaml(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file from {:?}", path))?;
        
        // Deserialize into a wrapper struct that expects the "storage" key
        let wrapper: StorageSettingsWrapper = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse YAML from {:?}", path))?;
        
        Ok(wrapper.storage)
    }
}

// A wrapper struct to handle the top-level 'storage' key in the YAML config.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StorageSettingsWrapper {
    storage: StorageSettings,
}

// --- Helper Functions ---

/// Retrieves the default storage port from the `storage_config.yaml` file.
/// If the file cannot be loaded or parsed, it falls back to the hardcoded default
/// defined in `StorageSettings::default()`.
pub fn get_default_storage_port_from_config() -> u16 {
    // Assuming 'storage_config.yaml' is relative to the executable's current working directory.
    let config_path = PathBuf::from("storage_config.yaml"); 

    match StorageSettings::load_from_yaml(&config_path) {
        Ok(settings) => {
            println!("[Storage Daemon Config] Loaded default port {} from {:?}", settings.default_port, config_path);
            settings.default_port
        },
        Err(e) => {
            eprintln!("[Storage Daemon Config] Warning: Could not load or parse storage_config.yaml for default port: {}. Using application's hardcoded default {}.", e, StorageSettings::default().default_port);
            StorageSettings::default().default_port // Explicitly use the default from the struct
        }
    }
}

/// The public entry point for starting the storage daemon logic as a library.
/// This function is intended to be called by an external executable (e.g., `graphdb` CLI).
/// It loads configuration, applies CLI overrides, and then starts the daemon's core logic.
pub async fn run_storage_daemon(
    cli_port_override: Option<u16>,
    config_file_path: PathBuf,
) -> Result<()> {
    // Step 1: Load settings from the YAML file
    let mut settings = match StorageSettings::load_from_yaml(&config_file_path) {
        Ok(s) => {
            println!("[Storage Daemon] Loaded storage settings from {}.", config_file_path.display());
            s
        },
        Err(e) => {
            eprintln!("[Storage Daemon] Error loading configuration from {}: {}. Falling back to default settings.", config_file_path.display(), e);
            StorageSettings::default()
        }
    };

    // Step 2: Determine the *actual* port to listen on.
    // CLI argument takes highest precedence.
    // If no CLI argument, use the port from the loaded settings.
    // If settings were defaulted (because YAML was missing/invalid), it uses `StorageSettings::default().default_port`.
    let effective_port = if let Some(p) = cli_port_override {
        println!("[Storage Daemon] Using port from CLI argument: {}", p);
        p
    } else {
        println!("[Storage Daemon] Using default port from config/default: {}", settings.default_port);
        settings.default_port
    };

    // Basic port validation
    if effective_port < 1024 || effective_port > 65535 {
        return Err(anyhow::anyhow!(
            "Invalid effective port: {}. Port must be between 1024 and 65535.",
            effective_port
        ));
    }

    // Prepare a shutdown channel
    let (tx, rx) = oneshot::channel();
    
    // Spawn a task to listen for Ctrl+C and send a shutdown signal.
    // This is crucial when the daemon is running in a forked process.
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        println!("\n[Storage Daemon] Ctrl-C received. Initiating graceful shutdown...");
        let _ = tx.send(()); // Send shutdown signal
    });

    // Step 3: Call the actual daemon's server start function
    start_storage_daemon_server_real(effective_port, settings, rx).await?;

    Ok(())
}
