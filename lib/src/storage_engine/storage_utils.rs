// lib/src/storage_engine/storage_utils.rs
// Created: 2025-07-04 - Added serialization utilities for Sled storage
// Fixed: 2025-07-04 - Corrected create_edge_key to use util::build for Identifier
// Updated: 2025-08-13 - Changed create_edge_key to accept SerializableUuid and return GraphResult<Vec<u8>>
// Fixed: 2025-08-14 - Corrected unresolved import of `Component` from `util`.
// Fixed: 2025-08-14 - Corrected the return value of `create_edge_key` to not use `.map_err` as `util_build_key` is infallible.

use std::path::{Path, PathBuf};
use log::{info, debug, warn, error, trace};
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use models::identifiers::SerializableUuid;
use models::util::{build as util_build_key, Component as UtilComponent};
use crate::config::{SledStorage};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY,  NonBlockingDaemonRegistry, DaemonMetadata};
use bincode::{encode_to_vec, decode_from_slice, config};
use uuid::Uuid;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::fs as tokio_fs;
use tokio::time::{sleep, Duration as TokioDuration};
use futures::future::{Future, FutureExt};

/// Helper to serialize a Vertex to bytes using bincode.
pub fn serialize_vertex(vertex: &Vertex) -> GraphResult<Vec<u8>> {
    encode_to_vec(vertex, config::standard())
        .map_err(|e| GraphError::SerializationError(e.to_string()))
}

/// Helper to deserialize bytes to a Vertex using bincode.
pub fn deserialize_vertex(bytes: &[u8]) -> GraphResult<Vertex> {
    decode_from_slice(bytes, config::standard())
        .map(|(val, _)| val)
        .map_err(|e| GraphError::DeserializationError(e.to_string()))
}

/// Helper to serialize an Edge to bytes using bincode.
pub fn serialize_edge(edge: &Edge) -> GraphResult<Vec<u8>> {
    encode_to_vec(edge, config::standard())
        .map_err(|e| GraphError::SerializationError(e.to_string()))
}

/// Helper to deserialize bytes to an Edge using bincode.
pub fn deserialize_edge(bytes: &[u8]) -> GraphResult<Edge> {
    decode_from_slice(bytes, config::standard())
        .map(|(val, _)| val)
        .map_err(|e| GraphError::DeserializationError(e.to_string()))
}

/// Helper to create a consistent byte key for an edge from its components.
/// This key is used for storage in key-value stores.
/// It uses the `util::build` function to ensure correct Identifier serialization.
pub fn create_edge_key(outbound_id: &SerializableUuid, edge_type: &Identifier, inbound_id: &SerializableUuid) -> Result<Vec<u8>, GraphError> {
    Ok(util_build_key(&[
        UtilComponent::Uuid(outbound_id.0),
        UtilComponent::Identifier(edge_type.clone()),
        UtilComponent::Uuid(inbound_id.0),
    ]))
}

/// Helper function to clean up legacy port-suffixed Sled directories
pub async fn cleanup_legacy_sled_paths(base_data_dir: &Path, current_port: u16) {
    info!("Cleaning up legacy port-suffixed Sled directories in {:?}", base_data_dir);
    
    if !base_data_dir.exists() {
        return;
    }
    
    if let Ok(entries) = tokio::fs::read_dir(base_data_dir).await {
        let mut entries = entries;
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Some(name) = entry.file_name().to_str() {
                // Look for directories matching pattern like "sled_8052", "sled_8053", etc.
                if name.starts_with("sled_") && name != "sled" {
                    if let Some(suffix) = name.strip_prefix("sled_") {
                        if let Ok(old_port) = suffix.parse::<u16>() {
                            let old_path = entry.path();
                            info!("Found legacy Sled directory: {:?} (port {})", old_path, old_port);
                            
                            // Try to force unlock any database locks first
                            if old_path.exists() {
                                if let Err(e) = SledStorage::force_unlock(&old_path).await {
                                    warn!("Failed to force unlock Sled database at {:?}: {}", old_path, e);
                                } else {
                                    info!("Successfully unlocked Sled database at {:?}", old_path);
                                }
                            }
                            
                            // Attempt to remove the entire legacy directory
                            match tokio::fs::remove_dir_all(&old_path).await {
                                Ok(_) => {
                                    info!("Successfully removed legacy Sled directory: {:?}", old_path);
                                    
                                    // Also clean up daemon registry entry for the old port if it exists
                                    if let Ok(_) = GLOBAL_DAEMON_REGISTRY.unregister_daemon(old_port).await {
                                        info!("Unregistered daemon registry entry for legacy port {}", old_port);
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to remove legacy Sled directory {:?}: {}", old_path, e);
                                    // If we can't remove it, at least try to clean up the registry
                                    if let Ok(_) = GLOBAL_DAEMON_REGISTRY.unregister_daemon(old_port).await {
                                        info!("Unregistered daemon registry entry for legacy port {} (directory removal failed)", old_port);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    } else {
        warn!("Could not read directory entries from {:?}", base_data_dir);
    }
    
    info!("Completed cleanup of legacy port-suffixed Sled directories");
}

// Helper to copy directory for same-engine path changes
pub async fn copy_dir(src: &Path, dst: &Path) -> io::Result<()> {
    use std::path::PathBuf;
    use tokio::fs as tokio_fs;
    
    let src_path = src.to_path_buf();
    let dst_path = dst.to_path_buf();
    
    copy_dir_recursive(src_path, dst_path).await
}

fn copy_dir_recursive(src: PathBuf, dst: PathBuf) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
    Box::pin(async move {
        tokio_fs::create_dir_all(&dst).await?;
        let mut entries = tokio_fs::read_dir(&src).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let ty = entry.file_type().await?;
            let target = dst.join(entry.file_name());
            
            if ty.is_dir() {
                copy_dir_recursive(entry.path(), target).await?;
            } else {
                tokio_fs::copy(entry.path(), target).await?;
            }
        }
        Ok(())
    })
}

