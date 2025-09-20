// lib/src/storage_engine/tikv_storage.rs
// Updated: 2025-09-01 - Fixed panic in retrieve, get_vertex, get_edge, get_all_vertices, get_all_edges
// by committing read-only transactions and rolling back on error. Added debug logging for transaction state.

use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use models::identifiers::SerializableUuid;
use crate::config::{TikvConfig, StorageEngineType, QueryResult, QueryPlan,};
use crate::storage_engine::storage_utils::{
    serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge,
};
use crate::{StorageEngine, GraphStorageEngine};
use log::{info, debug, error, warn};
use tikv_client::{Config, TransactionClient, Transaction, KvPair, Key};
use tokio::sync::Mutex as TokioMutex;
use tokio::time::{self, Duration as TokioDuration};

// Define key prefixes for different data types to allow for efficient scanning.
const VERTEX_KEY_PREFIX: &[u8] = b"v:";
const EDGE_KEY_PREFIX: &[u8] = b"e:";
const KV_KEY_PREFIX: &[u8] = b"kv:";

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

/// Creates a key for a key-value pair.
/// Format: `kv:{key}`
pub fn create_kv_key(key: &str) -> Vec<u8> {
    let mut kv_key = KV_KEY_PREFIX.to_vec();
    kv_key.extend_from_slice(key.as_bytes());
    kv_key
}

/// Creates a range for prefix scanning
/// Returns (start_key, end_key_option) where None means unbounded
pub fn create_prefix_range(prefix: &[u8]) -> (Key, Option<Key>) {
    let start = Key::from(prefix.to_vec());
    let mut end = prefix.to_vec();
    
    // Calculate the end key by incrementing the last byte
    for i in (0..end.len()).rev() {
        if end[i] < 255 {
            end[i] += 1;
            return (start, Some(Key::from(end)));
        } else {
            end[i] = 0;
            if i == 0 {
                // If we overflow, use unbounded range
                return (start, None);
            }
        }
    }
    
    (start, Some(Key::from(end)))
}

pub struct TikvStorage {
    client: Arc<TransactionClient>,
    config: TikvConfig,
    running: TokioMutex<bool>,
}

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
            match TransactionClient::new_with_config(pd_endpoints.clone(), Config::default()).await {
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

        let mut txn = self.client.begin_optimistic().await
            .map_err(|e| GraphError::StorageError(format!("Failed to start transaction: {}", e)))?;

        // Delete all keys with specified prefixes
        for prefix in [VERTEX_KEY_PREFIX, EDGE_KEY_PREFIX, KV_KEY_PREFIX] {
            let (start_key, end_key_option) = create_prefix_range(prefix);
            
            let keys: Vec<Key> = if let Some(end_key) = end_key_option {
                txn.scan(start_key..end_key, 1000).await
                    .map_err(|e| GraphError::StorageError(format!("Failed to scan for prefix {:?}: {}", prefix, e)))?
                    .map(|kv_pair| kv_pair.key().to_owned())
                    .collect()
            } else {
                txn.scan(start_key.., 1000).await
                    .map_err(|e| GraphError::StorageError(format!("Failed to scan for prefix {:?}: {}", prefix, e)))?
                    .map(|kv_pair| kv_pair.key().to_owned())
                    .collect()
            };

            for key in keys {
                if let Err(e) = txn.delete(key).await {
                    warn!("Failed to delete key: {}", e);
                }
            }
        }

        txn.commit().await
            .map_err(|e| GraphError::StorageError(format!("Failed to commit reset transaction: {}", e)))?;

        info!("Successfully reset TiKV database");
        Ok(())
    }
}

#[async_trait]
impl StorageEngine for TikvStorage {
    async fn connect(&self) -> Result<(), GraphError> {
        info!("Connecting to TiKV storage engine");
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphError> {
        debug!("Inserting key: {:?}", String::from_utf8_lossy(&key));
        debug!("Inserting value: {:?}", String::from_utf8_lossy(&value));
        let mut txn = self.client.begin_optimistic().await
            .map_err(|e| GraphError::StorageError(format!("Failed to start transaction: {}", e)))?;
        txn.put(key, value).await
            .map_err(|e| GraphError::StorageError(format!("Failed to insert key: {}", e)))?;
        txn.commit().await
            .map_err(|e| GraphError::StorageError(format!("Failed to commit transaction: {}", e)))?;
        debug!("Successfully committed insert transaction");
        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, GraphError> {
        debug!("Retrieving key: {:?}", String::from_utf8_lossy(key));
        let mut txn = self.client.begin_optimistic().await
            .map_err(|e| GraphError::StorageError(format!("Failed to start transaction: {}", e)))?;
        let value = match txn.get(key.clone()).await {
            Ok(val) => val,
            Err(e) => {
                warn!("Failed to retrieve key: {}", e);
                txn.rollback().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to rollback transaction: {}", e)))?;
                return Err(GraphError::StorageError(format!("Failed to retrieve key: {}", e)));
            }
        };
        if let Some(ref v) = value {
            debug!("Retrieved value: {:?}", String::from_utf8_lossy(v));
        } else {
            debug!("No value found for key");
        }
        txn.commit().await
            .map_err(|e| GraphError::StorageError(format!("Failed to commit retrieve transaction: {}", e)))?;
        debug!("Successfully committed retrieve transaction");
        Ok(value)
    }

    async fn delete(&self, key: &Vec<u8>) -> Result<(), GraphError> {
        debug!("Deleting key: {:?}", String::from_utf8_lossy(key));
        let mut txn = self.client.begin_optimistic().await
            .map_err(|e| GraphError::StorageError(format!("Failed to start transaction: {}", e)))?;
        txn.delete(key.clone()).await
            .map_err(|e| GraphError::StorageError(format!("Failed to delete key: {}", e)))?;
        txn.commit().await
            .map_err(|e| GraphError::StorageError(format!("Failed to commit transaction: {}", e)))?;
        debug!("Successfully committed delete transaction");
        Ok(())
    }

    async fn flush(&self) -> Result<(), GraphError> {
        info!("Flushing TiKV storage engine (handled by TiKV transactions)");
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

    async fn query(&self, _query_string: &str) -> Result<Value, GraphError> {
        Err(GraphError::StorageError("Direct queries are not supported with the tikv-client backend. Use the specific graph methods instead.".to_string()))
    }

    async fn execute_query(&self, query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        info!("Executing query on SledStorage (returning null as not implemented)");
        println!("===> EXECUTING QUERY ON SLED STORAGE (NOT IMPLEMENTED)");
        Ok(QueryResult::Null)
    }       

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        let key = create_vertex_key(&vertex.id.0);
        let value = serialize_vertex(&vertex)?;
        debug!("Creating vertex with key: {:?}", String::from_utf8_lossy(&key));
        let mut txn = self.client.begin_optimistic().await
            .map_err(|e| GraphError::StorageError(format!("Failed to start transaction: {}", e)))?;
        txn.put(key, value).await
            .map_err(|e| GraphError::StorageError(format!("Failed to create vertex: {}", e)))?;
        txn.commit().await
            .map_err(|e| GraphError::StorageError(format!("Failed to commit transaction: {}", e)))?;
        debug!("Successfully committed create_vertex transaction");
        Ok(())
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let mut vertices = Vec::new();
        let (start_key, end_key_option) = create_prefix_range(VERTEX_KEY_PREFIX);
        let mut txn = self.client.begin_optimistic().await
            .map_err(|e| GraphError::StorageError(format!("Failed to start transaction: {}", e)))?;
        
        let kv_pairs: Vec<KvPair> = if let Some(end_key) = end_key_option {
            match txn.scan(start_key..end_key, 1000).await {
                Ok(pairs) => pairs.collect(),
                Err(e) => {
                    warn!("Failed to scan vertices: {}", e);
                    txn.rollback().await
                        .map_err(|e| GraphError::StorageError(format!("Failed to rollback transaction: {}", e)))?;
                    return Err(GraphError::StorageError(format!("Failed to scan for vertices: {}", e)));
                }
            }
        } else {
            match txn.scan(start_key.., 1000).await {
                Ok(pairs) => pairs.collect(),
                Err(e) => {
                    warn!("Failed to scan vertices: {}", e);
                    txn.rollback().await
                        .map_err(|e| GraphError::StorageError(format!("Failed to rollback transaction: {}", e)))?;
                    return Err(GraphError::StorageError(format!("Failed to scan for vertices: {}", e)));
                }
            }
        };

        for kv_pair in kv_pairs {
            let serialized_vertex = kv_pair.value();
            if let Ok(vertex) = deserialize_vertex(serialized_vertex) {
                vertices.push(vertex);
            } else {
                warn!("Failed to deserialize vertex data from key: {:?}", kv_pair.key());
            }
        }
        txn.commit().await
            .map_err(|e| GraphError::StorageError(format!("Failed to commit get_all_vertices transaction: {}", e)))?;
        debug!("Successfully committed get_all_vertices transaction");
        Ok(vertices)
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        let key = create_vertex_key(id);
        debug!("Retrieving vertex with key: {:?}", String::from_utf8_lossy(&key));
        let mut txn = self.client.begin_optimistic().await
            .map_err(|e| GraphError::StorageError(format!("Failed to start transaction: {}", e)))?;
        let value_option = match txn.get(key).await {
            Ok(val) => val,
            Err(e) => {
                warn!("Failed to get vertex: {}", e);
                txn.rollback().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to rollback transaction: {}", e)))?;
                return Err(GraphError::StorageError(format!("Failed to get vertex: {}", e)));
            }
        };
        let result = match value_option {
            Some(value) => {
                debug!("Vertex found: {:?}", String::from_utf8_lossy(&value));
                deserialize_vertex(&value).map(Some)
                    .map_err(|e| GraphError::StorageError(format!("Failed to deserialize vertex: {}", e)))
            }
            None => {
                debug!("No vertex found for key");
                Ok(None)
            }
        };
        txn.commit().await
            .map_err(|e| GraphError::StorageError(format!("Failed to commit get_vertex transaction: {}", e)))?;
        debug!("Successfully committed get_vertex transaction");
        result
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.create_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        let key = create_vertex_key(id);
        debug!("Deleting vertex with key: {:?}", String::from_utf8_lossy(&key));
        let mut txn = self.client.begin_optimistic().await
            .map_err(|e| GraphError::StorageError(format!("Failed to start transaction: {}", e)))?;
        txn.delete(key).await
            .map_err(|e| GraphError::StorageError(format!("Failed to delete vertex: {}", e)))?;
        txn.commit().await
            .map_err(|e| GraphError::StorageError(format!("Failed to commit transaction: {}", e)))?;
        debug!("Successfully committed delete_vertex transaction");
        Ok(())
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        let key = create_edge_key(&edge.outbound_id, &edge.t, &edge.inbound_id);
        let value = serialize_edge(&edge)?;
        debug!("Creating edge with key: {:?}", String::from_utf8_lossy(&key));
        let mut txn = self.client.begin_optimistic().await
            .map_err(|e| GraphError::StorageError(format!("Failed to start transaction: {}", e)))?;
        txn.put(key, value).await
            .map_err(|e| GraphError::StorageError(format!("Failed to create edge: {}", e)))?;
        txn.commit().await
            .map_err(|e| GraphError::StorageError(format!("Failed to commit transaction: {}", e)))?;
        debug!("Successfully committed create_edge transaction");
        Ok(())
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let mut edges = Vec::new();
        let (start_key, end_key_option) = create_prefix_range(EDGE_KEY_PREFIX);
        let mut txn = self.client.begin_optimistic().await
            .map_err(|e| GraphError::StorageError(format!("Failed to start transaction: {}", e)))?;
        
        let kv_pairs: Vec<KvPair> = if let Some(end_key) = end_key_option {
            match txn.scan(start_key..end_key, 1000).await {
                Ok(pairs) => pairs.collect(),
                Err(e) => {
                    warn!("Failed to scan edges: {}", e);
                    txn.rollback().await
                        .map_err(|e| GraphError::StorageError(format!("Failed to rollback transaction: {}", e)))?;
                    return Err(GraphError::StorageError(format!("Failed to scan for edges: {}", e)));
                }
            }
        } else {
            match txn.scan(start_key.., 1000).await {
                Ok(pairs) => pairs.collect(),
                Err(e) => {
                    warn!("Failed to scan edges: {}", e);
                    txn.rollback().await
                        .map_err(|e| GraphError::StorageError(format!("Failed to rollback transaction: {}", e)))?;
                    return Err(GraphError::StorageError(format!("Failed to scan for edges: {}", e)));
                }
            }
        };

        for kv_pair in kv_pairs {
            let serialized_edge = kv_pair.value();
            if let Ok(edge) = deserialize_edge(serialized_edge) {
                edges.push(edge);
            } else {
                warn!("Failed to deserialize edge data from key: {:?}", kv_pair.key());
            }
        }
        txn.commit().await
            .map_err(|e| GraphError::StorageError(format!("Failed to commit get_all_edges transaction: {}", e)))?;
        debug!("Successfully committed get_all_edges transaction");
        Ok(edges)
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        let key = create_edge_key(&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id));
        debug!("Retrieving edge with key: {:?}", String::from_utf8_lossy(&key));
        let mut txn = self.client.begin_optimistic().await
            .map_err(|e| GraphError::StorageError(format!("Failed to start transaction: {}", e)))?;
        let value_option = match txn.get(key).await {
            Ok(val) => val,
            Err(e) => {
                warn!("Failed to get edge: {}", e);
                txn.rollback().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to rollback transaction: {}", e)))?;
                return Err(GraphError::StorageError(format!("Failed to get edge: {}", e)));
            }
        };
        let result = match value_option {
            Some(value) => {
                debug!("Edge found: {:?}", String::from_utf8_lossy(&value));
                deserialize_edge(&value).map(Some)
                    .map_err(|e| GraphError::StorageError(format!("Failed to deserialize edge: {}", e)))
            }
            None => {
                debug!("No edge found for key");
                Ok(None)
            }
        };
        txn.commit().await
            .map_err(|e| GraphError::StorageError(format!("Failed to commit get_edge transaction: {}", e)))?;
        debug!("Successfully committed get_edge transaction");
        result
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        let key = create_edge_key(&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id));
        debug!("Deleting edge with key: {:?}", String::from_utf8_lossy(&key));
        let mut txn = self.client.begin_optimistic().await
            .map_err(|e| GraphError::StorageError(format!("Failed to start transaction: {}", e)))?;
        txn.delete(key).await
            .map_err(|e| GraphError::StorageError(format!("Failed to delete edge: {}", e)))?;
        txn.commit().await
            .map_err(|e| GraphError::StorageError(format!("Failed to commit transaction: {}", e)))?;
        debug!("Successfully committed delete_edge transaction");
        Ok(())
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
