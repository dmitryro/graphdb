// server/src/cli/daemon_registry.rs
// Manages daemon metadata for GraphDB components using a Sled database.
// Updated: 2025-07-28 - Enhanced error logging and aligned with service_type usage.

use anyhow::{Result, anyhow};
use serde::{Serialize, Deserialize};
use sled::{Db, IVec};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use log::{info, debug, warn, error};
use lazy_static::lazy_static;

use crate::cli::config::DAEMON_REGISTRY_DB_PATH;

/// Struct to store metadata about a running daemon process.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DaemonMetadata {
    pub service_type: String, // "main", "rest", "storage"
    pub port: u16,
    pub pid: u32,
    pub ip_address: String, // For now, always "127.0.0.1"
    pub data_dir: Option<PathBuf>,
    pub config_path: Option<PathBuf>,
    pub engine_type: Option<String>, // e.g., "Sled", "RocksDB"
}

/// A centralized registry for managing daemon metadata using Sled.
pub struct DaemonRegistry {
    db: Arc<TokioMutex<Db>>,
}

impl DaemonRegistry {
    /// Initializes the DaemonRegistry, opening or creating the Sled database.
    pub fn new() -> Result<Self> {
        let db = sled::open(DAEMON_REGISTRY_DB_PATH)
            .map_err(|e| anyhow!("Failed to open daemon registry database: {}", e))?;
        info!("Daemon registry database opened at: {}", DAEMON_REGISTRY_DB_PATH);
        Ok(DaemonRegistry {
            db: Arc::new(TokioMutex::new(db)),
        })
    }

    /// Registers a new daemon process with its metadata.
    pub async fn register_daemon(&self, metadata: DaemonMetadata) -> Result<()> {
        let db = self.db.lock().await;
        let key = metadata.port.to_string();
        let serialized_metadata = serde_json::to_vec(&metadata)
            .map_err(|e| anyhow!("Failed to serialize daemon metadata for port {}: {}", metadata.port, e))?;
        db.insert(key.as_bytes(), serialized_metadata)
            .map_err(|e| anyhow!("Failed to insert daemon metadata for port {}: {}", metadata.port, e))?;
        db.flush_async().await
            .map_err(|e| anyhow!("Failed to flush daemon registry to disk for port {}: {}", metadata.port, e))?;
        info!("Registered {} daemon: port={}, pid={}", metadata.service_type, metadata.port, metadata.pid);
        Ok(())
    }

    /// Unregisters a daemon process by its port.
    pub async fn unregister_daemon(&self, port: u16) -> Result<()> {
        let db = self.db.lock().await;
        let key = port.to_string();
        if db.remove(key.as_bytes()).map_err(|e| anyhow!("Failed to remove daemon metadata for port {}: {}", port, e))?.is_some() {
            db.flush_async().await
                .map_err(|e| anyhow!("Failed to flush daemon registry to disk after removing port {}: {}", port, e))?;
            info!("Unregistered daemon on port: {}", port);
        } else {
            debug!("No daemon found to unregister on port: {}", port);
        }
        Ok(())
    }

    /// Retrieves metadata for a specific daemon by its port.
    pub async fn get_daemon_metadata(&self, port: u16) -> Result<Option<DaemonMetadata>> {
        let db = self.db.lock().await;
        let key = port.to_string();
        let result = db.get(key.as_bytes())
            .map_err(|e| anyhow!("Failed to retrieve daemon metadata for port {}: {}", port, e))?;
        match result {
            Some(ivec) => {
                let metadata: DaemonMetadata = serde_json::from_slice(&ivec)
                    .map_err(|e| {
                        error!("Failed to deserialize daemon metadata for port {}: {}", port, e);
                        anyhow!("Failed to deserialize daemon metadata for port {}: {}", port, e)
                    })?;
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }

    /// Retrieves metadata for all registered daemon processes.
    pub async fn get_all_daemon_metadata(&self) -> Result<Vec<DaemonMetadata>> {
        let db = self.db.lock().await;
        let mut all_metadata = Vec::new();
        for item in db.iter() {
            let (_, ivec) = item
                .map_err(|e| anyhow!("Failed to iterate daemon registry: {}", e))?;
            let metadata: DaemonMetadata = serde_json::from_slice(&ivec)
                .map_err(|e| {
                    error!("Failed to deserialize daemon metadata during iteration: {}", e);
                    anyhow!("Failed to deserialize daemon metadata during iteration: {}", e)
                })?;
            all_metadata.push(metadata);
        }
        Ok(all_metadata)
    }

    /// Clears all entries from the daemon registry.
    pub async fn clear_all_daemons(&self) -> Result<()> {
        let db = self.db.lock().await;
        db.clear()
            .map_err(|e| anyhow!("Failed to clear daemon registry: {}", e))?;
        db.flush_async().await
            .map_err(|e| anyhow!("Failed to flush daemon registry after clearing: {}", e))?;
        info!("Cleared all daemon entries from registry.");
        Ok(())
    }
}

lazy_static! {
    pub static ref GLOBAL_DAEMON_REGISTRY: DaemonRegistry = DaemonRegistry::new()
        .expect("Failed to initialize GLOBAL_DAEMON_REGISTRY");
}