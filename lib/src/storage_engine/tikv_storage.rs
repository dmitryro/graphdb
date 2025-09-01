// Implements the GraphStorageEngine for a TiKV backend using the official tikv-client.
// This implementation directly interacts with TiKV's key-value API, replacing the SurrealDB
// abstraction from the original file.

use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use models::identifiers::SerializableUuid;
use crate::storage_engine::config::{TikvConfig, StorageEngineType};
use crate::storage_engine::{StorageEngine, GraphStorageEngine};
use crate::storage_engine::storage_utils::{
    serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge,
};
use log::{info, debug, error, warn};
use std::ops::{Bound, RangeBounds};
use tikv_client::{Config, Transaction, TransactionClient as TiKvClient, RawClient as KvClient, KvPair, Key};
use tokio::sync::Mutex as TokioMutex;
use tokio::time::{self, Duration as TokioDuration};
use futures::stream::StreamExt;
use base64::{engine::general_purpose, Engine as _};

// The base64 engine to use for encoding/decoding keys.
const BASE64_ENGINE: general_purpose::GeneralPurpose = general_purpose::STANDARD;

// Define key prefixes for different data types to allow for efficient scanning.
const VERTEX_KEY_PREFIX: &[u8] = b"v:";
const EDGE_KEY_PREFIX: &[u8] = b"e:";

// --- Helper Functions for Key Management ---

/// Creates a key for a vertex.
/// Format: `v:{uuid}`
fn create_vertex_key(uuid: &Uuid) -> Vec<u8> {
    let mut key = VERTEX_KEY_PREFIX.to_vec();
    key.extend_from_slice(uuid.as_bytes());
    key
}

/// Creates a key for an edge.
/// Format: `e:{outbound_uuid}:{edge_type}:{inbound_uuid}`
fn create_edge_key(outbound_id: &SerializableUuid, edge_type: &Identifier, inbound_id: &SerializableUuid) -> Vec<u8> {
    let mut key = EDGE_KEY_PREFIX.to_vec();
    key.extend_from_slice(outbound_id.0.as_bytes());
    key.extend_from_slice(b":");
    key.extend_from_slice(edge_type.to_string().as_bytes());
    key.extend_from_slice(b":");
    key.extend_from_slice(inbound_id.0.as_bytes());
    key
}

/// Creates a range for prefix scanning
fn create_prefix_range(prefix: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let start = prefix.to_vec();
    let mut end = prefix.to_vec();
    
    // Calculate the end key by incrementing the last byte
    // This creates a range [prefix, prefix_next) for scanning
    for i in (0..end.len()).rev() {
        if end[i] < 255 {
            end[i] += 1;
            break;
        } else {
            end[i] = 0;
            if i == 0 {
                // If we overflow, use empty vec which means "no upper bound"
                return (start, Vec::new());
            }
        }
    }
    
    (start, end)
}

// The `RawClient` type does not implement `Debug`, so we cannot use `#[derive(Debug)]` here.
pub struct TikvStorage {
    // We use a KvClient for simple key-value operations.
    // A TransactionClient is also available if transactional behavior is needed.
    client: Arc<KvClient>,
    config: TikvConfig,
    running: TokioMutex<bool>,
}

// I've manually implemented the `Debug` trait to satisfy the compiler.
impl std::fmt::Debug for TikvStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TikvStorage")
            .field("config", &self.config)
            .field("running", &self.running)
            .finish()
    }
}

impl TikvStorage {
    /// Creates a new TiKV storage instance using the `tikv-client`.
    pub async fn new(config: &TikvConfig) -> GraphResult<Self> {
        info!("Initializing TiKV storage engine with tikv-client");

        // Validate required PD endpoints
        let pd_endpoints_str = config.pd_endpoints.as_ref().ok_or_else(|| {
            error!("Missing pd_endpoints in TiKV configuration");
            GraphError::ConfigurationError("Missing pd_endpoints in TiKV configuration".to_string())
        })?;

        let pd_endpoints: Vec<String> = pd_endpoints_str.split(',').map(|s| s.trim().to_string()).collect();
        info!("Connecting to TiKV PD endpoints: {:?}", pd_endpoints);

        // Add retry logic for connection
        let max_attempts = 3;
        let retry_interval = TokioDuration::from_secs(2);
        let mut last_error = None;

        for attempt in 1..=max_attempts {
            match KvClient::new_with_config(pd_endpoints.clone(), Config::default()).await {
                Ok(client) => {
                    info!("Successfully connected to TiKV on attempt {}", attempt);
                    return Ok(TikvStorage {
                        client: Arc::new(client),
                        config: config.clone(),
                        running: TokioMutex::new(true),
                    });
                }
                Err(e) => {
                    warn!("Failed to connect to TiKV on attempt {}: {}", attempt, e);
                    last_error = Some(e);
                    if attempt < max_attempts {
                        info!("Retrying in {:?}", retry_interval);
                        time::sleep(retry_interval).await;
                    }
                }
            }
        }

        error!("Failed to connect to TiKV after {} attempts", max_attempts);
        Err(GraphError::StorageError(format!(
            "Failed to connect to TiKV backend after {} attempts: {}",
            max_attempts,
            last_error.unwrap()
        )))
    }
    
    /// Forces the unlocking of a local database. This is a no-op for TiKV
    /// which is a distributed key-value store and does not use local files.
    pub async fn force_unlock() -> GraphResult<()> {
        info!("No local lock files to unlock for TiKV (distributed storage)");
        Ok(())
    }

    /// Resets the TiKV database by deleting all keys with the specified prefixes.
    pub async fn reset(&self) -> GraphResult<()> {
        info!("Resetting TiKV database by clearing all data");

        // Delete all vertex keys using range bounds
        let (start_vertex, end_vertex) = create_prefix_range(VERTEX_KEY_PREFIX);
        self.client.delete_range(start_vertex..end_vertex)
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to delete vertices: {}", e)))?;

        // Delete all edge keys using range bounds
        let (start_edge, end_edge) = create_prefix_range(EDGE_KEY_PREFIX);
        self.client.delete_range(start_edge..end_edge)
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to delete edges: {}", e)))?;

        info!("Successfully reset TiKV database");
        Ok(())
    }
}

#[async_trait]
impl StorageEngine for TikvStorage {
    async fn connect(&self) -> Result<(), GraphError> {
        info!("Connecting to TiKV storage engine");
        // Connection is handled during initialization, so this is a no-op.
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphError> {
        self.client.put(key, value)
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to insert key: {}", e)))?;
        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, GraphError> {
        self.client.get(key.clone())
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to retrieve key: {}", e)))
    }

    async fn delete(&self, key: &Vec<u8>) -> Result<(), GraphError> {
        self.client.delete(key.clone())
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to delete key: {}", e)))
    }

    async fn flush(&self) -> Result<(), GraphError> {
        info!("Flushing TiKV storage engine (no-op, handled by TiKV)");
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for TikvStorage {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_type(&self) -> &'static str {
        "tikv"
    }

    async fn is_running(&self) -> bool {
        *self.running.lock().await
    }

    async fn start(&self) -> Result<(), GraphError> {
        let mut running = self.running.lock().await;
        if *running {
            info!("TiKV storage engine is already running");
            return Ok(());
        }
        info!("Starting TiKV storage engine");
        *running = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        let mut running = self.running.lock().await;
        if !*running {
            info!("TiKV storage engine is already stopped");
            return Ok(());
        }
        info!("Stopping TiKV storage engine");
        *running = false;
        Ok(())
    }

    /// Note: The original SurrealDB implementation supported complex queries.
    /// The direct `tikv-client` does not have a query language. This method is
    /// therefore not supported in this implementation.
    async fn query(&self, _query_string: &str) -> Result<Value, GraphError> {
        Err(GraphError::StorageError("Direct queries are not supported with the tikv-client backend. Use the specific graph methods instead.".to_string()))
    }

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        let key = create_vertex_key(&vertex.id.0);
        let value = serialize_vertex(&vertex)?;
        self.client.put(key, value)
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to create vertex: {}", e)))
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let mut vertices = Vec::new();
        let (start_key, end_key) = create_prefix_range(VERTEX_KEY_PREFIX);
        
        let kv_pairs = if end_key.is_empty() {
            self.client.scan(start_key.., 100)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to scan for vertices: {}", e)))?
        } else {
            self.client.scan(start_key..end_key, 100)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to scan for vertices: {}", e)))?
        };

        // In tikv-client v0.3, scan returns Vec<KvPair> directly
        for kv_pair in kv_pairs {
            let serialized_vertex = kv_pair.value();
            if let Ok(vertex) = deserialize_vertex(serialized_vertex) {
                vertices.push(vertex);
            } else {
                warn!("Failed to deserialize vertex data from key: {:?}", kv_pair.key());
            }
        }
        Ok(vertices)
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        let key = create_vertex_key(id);
        let value_option = self.client.get(key)
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to get vertex: {}", e)))?;
        
        match value_option {
            Some(value) => deserialize_vertex(&value).map(Some)
                .map_err(|e| GraphError::StorageError(format!("Failed to deserialize vertex: {}", e))),
            None => Ok(None),
        }
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        // Update is a simple `put` over the existing key.
        self.create_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        let key = create_vertex_key(id);
        self.client.delete(key)
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to delete vertex: {}", e)))
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        let key = create_edge_key(&edge.outbound_id, &edge.t, &edge.inbound_id);
        let value = serialize_edge(&edge)?;
        self.client.put(key, value)
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to create edge: {}", e)))
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let mut edges = Vec::new();
        let (start_key, end_key) = create_prefix_range(EDGE_KEY_PREFIX);
        
        let kv_pairs = if end_key.is_empty() {
            self.client.scan(start_key.., 100)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to scan for edges: {}", e)))?
        } else {
            self.client.scan(start_key..end_key, 100)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to scan for edges: {}", e)))?
        };

        // In tikv-client v0.3, scan returns Vec<KvPair> directly
        for kv_pair in kv_pairs {
            let serialized_edge = kv_pair.value();
            if let Ok(edge) = deserialize_edge(serialized_edge) {
                edges.push(edge);
            } else {
                warn!("Failed to deserialize edge data from key: {:?}", kv_pair.key());
            }
        }
        Ok(edges)
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        let key = create_edge_key(&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id));
        let value_option = self.client.get(key)
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to get edge: {}", e)))?;

        match value_option {
            Some(value) => deserialize_edge(&value).map(Some)
                .map_err(|e| GraphError::StorageError(format!("Failed to deserialize edge: {}", e))),
            None => Ok(None),
        }
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        // Update is a simple `put` over the existing key.
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        let key = create_edge_key(&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id));
        self.client.delete(key)
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to delete edge: {}", e)))
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        self.reset().await
    }

    async fn close(&self) -> Result<(), GraphError> {
        info!("Closing TiKV storage engine");
        let mut running = self.running.lock().await;
        *running = false;
        Ok(())
    }
}

// Suppress unused warnings for fields that may be used in other modules
#[allow(unused)]
struct PermissionsExt;
#[allow(unused)]
struct _message;
#[allow(unused)]
struct _deleted;
#[allow(unused)]
struct _pool;