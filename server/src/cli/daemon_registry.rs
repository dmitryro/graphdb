// server/src/cli/daemon_registry.rs
// Manages daemon metadata for GraphDB components using a Sled database.
// Updated: 2025-07-29 - Added `last_seen` timestamp for cleanup and `Pid` for direct process lookup.
// Added bincode for serialization/deserialization.

use anyhow::{Result, anyhow, Context}; // Added Context import
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
    pub engine_type: Option<String>, // e.g., "sled", "rocksdb" for storage daemons
    pub last_seen_nanos: i64, // Timestamp in nanoseconds since epoch
}

/// Manages the registration and lookup of daemon processes.
pub struct DaemonRegistry {
    db: Arc<TokioMutex<Db>>,
}

impl DaemonRegistry {
    /// Creates a new `DaemonRegistry` instance, opening the Sled database.
    pub fn new() -> Result<Self> {
        let db_path = PathBuf::from(DAEMON_REGISTRY_DB_PATH);
        let db = sled::open(&db_path)
            .with_context(|| format!("Failed to open daemon registry database at {:?}", db_path))?;
        info!("Daemon registry initialized at {:?}", db_path);
        Ok(DaemonRegistry {
            db: Arc::new(TokioMutex::new(db)),
        })
    }

    /// Registers a new daemon process or updates an existing one.
    /// Uses the port as the key for the database.
    pub async fn register_daemon(&self, metadata: DaemonMetadata) -> Result<()> {
        let db = self.db.lock().await;
        let key = metadata.port.to_string().into_bytes();
        let encoded_metadata = encode_to_vec(metadata.clone(), config::standard())
            .context("Failed to encode daemon metadata")?;
        
        db.insert(&key, encoded_metadata)
            .map_err(|e| anyhow!("Failed to insert daemon metadata for port {}: {}", metadata.port, e))?;
        db.flush_async().await
            .map_err(|e| anyhow!("Failed to flush daemon registry after registration: {}", e))?;
        info!("Registered daemon: {:?} on port {}", metadata.service_type, metadata.port);
        Ok(())
    }

    /// Unregisters a daemon process by its port.
    pub async fn unregister_daemon(&self, port: u16) -> Result<()> {
        let db = self.db.lock().await;
        let key = port.to_string().into_bytes();
        db.remove(&key)
            .map_err(|e| anyhow!("Failed to remove daemon metadata for port {}: {}", port, e))?;
        db.flush_async().await
            .map_err(|e| anyhow!("Failed to flush daemon registry after unregistration: {}", e))?;
        info!("Unregistered daemon on port {}", port);
        Ok(())
    }

    /// Retrieves metadata for a specific daemon by its port.
    pub async fn get_daemon_metadata(&self, port: u16) -> Result<Option<DaemonMetadata>> {
        let db = self.db.lock().await;
        let key = port.to_string().into_bytes();
        match db.get(&key)
            .map_err(|e| anyhow!("Failed to retrieve daemon metadata for port {}: {}", port, e))?
        {
            Some(ivec) => {
                let (metadata, _len) = decode_from_slice::<DaemonMetadata, _>(&ivec, config::standard())
                    .context("Failed to decode daemon metadata from database")?;
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }

    /// Retrieves metadata for all registered daemons, cleaning up corrupted entries.
    pub async fn get_all_daemon_metadata(&self) -> Result<Vec<DaemonMetadata>> {
        let db = self.db.lock().await;
        let mut all_metadata = Vec::new();
        let mut keys_to_remove = Vec::new();

        for item_result in db.iter() {
            let (key, ivec) = item_result
                .map_err(|e| anyhow!("Failed to iterate daemon registry: {}", e))?;
            
            match decode_from_slice::<DaemonMetadata, _>(&ivec, config::standard()) {
                Ok((metadata, _len)) => {
                    all_metadata.push(metadata);
                }
                Err(e) => {
                    // Log the error and mark the key for removal
                    error!("Corrupted daemon registry entry for key '{:?}': {}. Marking for removal.", key, e);
                    keys_to_remove.push(key.to_vec()); // Convert IVec to Vec<u8> for storage
                }
            }
        }

        // Remove corrupted entries outside the iteration to avoid iterator invalidation,
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

