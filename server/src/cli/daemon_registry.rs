// server/src/cli/daemon_registry.rs
// Manages daemon metadata for GraphDB components using a Sled database.
// Updated: 2025-07-29 - Added `last_seen` timestamp for cleanup and `Pid` for direct process lookup.
// Added bincode for serialization/deserialization.

use anyhow::{Result, anyhow};
use serde::{Serialize, Deserialize};
use sled::{Db, IVec};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use log::{info, debug, warn, error};
use lazy_static::lazy_static;
use chrono::{DateTime, Utc, TimeZone}; // For last_seen timestamp
use sysinfo::Pid; // For direct PID storage and lookup
use bincode::{encode_to_vec, decode_from_slice, config, Encode, Decode}; // Import Encode and Decode traits

use crate::cli::config::DAEMON_REGISTRY_DB_PATH;

/// Struct to store metadata about a running daemon process.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Encode, Decode)] // Add Encode and Decode derives
pub struct DaemonMetadata {
    pub service_type: String, // "main", "rest", "storage"
    pub port: u16,
    pub pid: u32, // Changed to u32 for direct sysinfo::Pid compatibility
    pub ip_address: String, // For now, always "127.0.0.1"
    pub data_dir: Option<PathBuf>,
    pub config_path: Option<PathBuf>,
    pub engine_type: Option<String>, // e.g., "Sled", "RocksDB" for storage, None for others
    #[serde(rename = "last_seen")] // Keep serde field name for compatibility with existing data if any
    pub last_seen_nanos: i64, // Store as nanoseconds timestamp for bincode compatibility
}

impl DaemonMetadata {
    /// Helper to get the last_seen timestamp as DateTime<Utc>.
    pub fn last_seen_datetime(&self) -> DateTime<Utc> {
        Utc.timestamp_nanos(self.last_seen_nanos)
    }
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
    /// If a daemon with the same port exists, it updates its metadata.
    pub async fn register_daemon(&self, mut metadata: DaemonMetadata) -> Result<()> {
        let db = self.db.lock().await;
        let key = metadata.port.to_string();
        metadata.last_seen_nanos = Utc::now().timestamp_nanos(); // Update last_seen_nanos on registration/update

        let bincode_config = config::standard(); // Use standard bincode config
        let serialized_metadata = encode_to_vec(&metadata, bincode_config) // Use encode_to_vec
            .map_err(|e| anyhow!("Failed to serialize daemon metadata for port {}: {}", metadata.port, e))?;
        
        db.insert(key.as_bytes(), serialized_metadata)
            .map_err(|e| anyhow!("Failed to insert/update daemon metadata for port {}: {}", metadata.port, e))?;
        db.flush_async().await
            .map_err(|e| anyhow!("Failed to flush daemon registry to disk for port {}: {}", metadata.port, e))?;
        info!("Registered/Updated {} daemon: port={}, pid={}, last_seen_nanos={}", metadata.service_type, metadata.port, metadata.pid, metadata.last_seen_nanos);
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
                let bincode_config = config::standard();
                let (metadata, _): (DaemonMetadata, usize) = decode_from_slice(&ivec, bincode_config) // Use decode_from_slice
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
        let bincode_config = config::standard();
        // Collect keys to remove problematic entries outside the iteration
        let mut keys_to_remove = Vec::new();

        for item in db.iter() {
            let (key_ivec, ivec) = match item {
                Ok(pair) => pair,
                Err(e) => {
                    // This error indicates an issue with fetching the item itself from Sled.
                    // It's less common than a deserialization error but should still be handled.
                    error!("Failed to iterate daemon registry item: {}", e);
                    continue; // Skip this problematic item
                }
            };
            let key_str = String::from_utf8_lossy(&key_ivec); // Get key for logging

            match decode_from_slice(&ivec, bincode_config) { // Use decode_from_slice
                Ok((metadata, _)) => {
                    all_metadata.push(metadata);
                },
                Err(e) => {
                    // This is the core fix: if deserialization fails, the data is corrupted.
                    // Log the error and mark the key for removal to prevent future failures.
                    error!("Failed to deserialize daemon metadata for key '{}' during iteration: {}. Marking for removal.", key_str, e);
                    keys_to_remove.push(key_ivec.to_vec()); // Store as Vec<u8> to avoid lifetime issues
                }
            }
        }

        // After iterating through all entries, remove the corrupted ones.
        // This is done outside the loop to avoid modifying the database while iterating,
        // which can lead to deadlocks or unexpected behavior with some database systems.
        // Iterate over a reference `&keys_to_remove` so that `keys_to_remove` is not moved.
        // This is the fix for the E0382 "borrow of moved value" error.
        for key in &keys_to_remove { 
            let port_str = String::from_utf8_lossy(key);
            warn!("Removing corrupted daemon registry entry for port '{}'.", port_str);
            db.remove(key)
                .map_err(|e| anyhow!("Failed to remove corrupted entry for port {}: {}", port_str, e))?;
        }
        // Only flush if changes were made (i.e., if corrupted entries were removed)
        if !keys_to_remove.is_empty() {
            db.flush_async().await
                .map_err(|e| anyhow!("Failed to flush daemon registry after removing corrupted entries: {}", e))?;
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
