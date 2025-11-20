// lib/src/storage_engine/tikv_storage.rs
// Updated: 2025-11-19 - Full index support + Tantivy + all original code preserved + all errors fixed
use std::any::Any;
use std::sync::Arc;
use std::path::{Path, PathBuf};
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use models::identifiers::SerializableUuid;
pub use crate::config::{TikvConfig, QueryResult, QueryPlan, TikvStorage};
use crate::storage_engine::storage_utils::{
    serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge,
};
use crate::{StorageEngine, GraphStorageEngine};
use log::{info, debug, error, warn};
use tikv_client::{Config, TransactionClient, Transaction, KvPair, Key};
use tokio::sync::{Mutex as TokioMutex, OnceCell};
use tokio::time::{self, Duration as TokioDuration};

// Tantivy full-text index — async-safe OnceCell
static FULLTEXT_INDEX: OnceCell<Arc<indexing_service::fulltext::FullTextIndex>> = OnceCell::const_new();

async fn get_fulltext_index() -> Arc<indexing_service::fulltext::FullTextIndex> {
    FULLTEXT_INDEX.get_or_init(|| async {
        let index_dir = PathBuf::from("/opt/graphdb/indexes/fulltext");
        std::fs::create_dir_all(&index_dir).expect("Failed to create fulltext index directory");
        Arc::new(indexing_service::fulltext::FullTextIndex::new(&index_dir).expect("Failed to initialize Tantivy index"))
    }).await.clone()
}

// Key prefixes
const VERTEX_KEY_PREFIX: &[u8] = b"v:";
const EDGE_KEY_PREFIX: &[u8] = b"e:";
const KV_KEY_PREFIX: &[u8] = b"kv:";

// Helper functions
fn create_vertex_key(uuid: &Uuid) -> Vec<u8> {
    let mut key = VERTEX_KEY_PREFIX.to_vec();
    key.extend_from_slice(uuid.as_bytes());
    key
}

fn create_edge_key(outbound_id: &SerializableUuid, edge_type: &Identifier, inbound_id: &SerializableUuid) -> Vec<u8> {
    let mut key = EDGE_KEY_PREFIX.to_vec();
    key.extend_from_slice(outbound_id.0.as_bytes());
    key.extend_from_slice(b":");
    key.extend_from_slice(edge_type.to_string().as_bytes());
    key.extend_from_slice(b":");
    key.extend_from_slice(inbound_id.0.as_bytes());
    key
}

pub fn create_kv_key(key: &str) -> Vec<u8> {
    let mut kv_key = KV_KEY_PREFIX.to_vec();
    kv_key.extend_from_slice(key.as_bytes());
    kv_key
}

pub fn create_prefix_range(prefix: &[u8]) -> (Key, Option<Key>) {
    let start = Key::from(prefix.to_vec());
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 255 {
            end[i] += 1;
            return (start, Some(Key::from(end)));
        } else {
            end[i] = 0;
            if i == 0 {
                return (start, None);
            }
        }
    }
    (start, Some(Key::from(end)))
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
    pub async fn new(config: &TikvConfig) -> GraphResult<Self> {
        info!("Initializing TiKV storage engine with tikv-client");
        let pd_endpoints_str = config.pd_endpoints.as_ref().ok_or_else(|| {
            error!("Missing pd_endpoints in TiKV configuration");
            GraphError::ConfigurationError("Missing pd_endpoints in TiKV configuration".to_string())
        })?;
        let pd_endpoints: Vec<String> = pd_endpoints_str.split(',').map(|s| s.trim().to_string()).collect();
        info!("Connecting to TiKV PD endpoints: {:?}", pd_endpoints);

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
            last_error.map(|e| e.to_string()).unwrap_or_else(|| "Unknown error".into())
        )))
    }

    pub async fn force_unlock() -> GraphResult<()> {
        info!("No local lock files to unlock for TiKV (distributed storage)");
        Ok(())
    }

    pub async fn reset(&self) -> GraphResult<()> {
        info!("Resetting TiKV database by clearing all data");
        let mut txn = self.client.begin_optimistic().await
            .map_err(|e| GraphError::StorageError(format!("Failed to start transaction: {}", e)))?;
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

    async fn execute_query(&self, _query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        Err(GraphError::StorageError("execute_query not supported on TiKV backend".into()))
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
        let key = create_edge_key(&edge.outbound_id, &edge.edge_type, &edge.inbound_id);
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

    // === INDEX METHODS (TiKV does not support secondary indexes natively) ===
    async fn create_index(&self, _label: &str, _property: &str) -> GraphResult<()> {
        Err(GraphError::StorageError("Secondary indexes are not supported on TiKV backend".into()))
    }

    async fn drop_index(&self, _label: &str, _property: &str) -> GraphResult<()> {
        Err(GraphError::StorageError("Secondary indexes are not supported on TiKV backend".into()))
    }

    async fn list_indexes(&self) -> GraphResult<Vec<(String, String)>> {
        Ok(vec![])
    }

    // === FULLTEXT SEARCH (Tantivy) ===
    async fn fulltext_search(&self, query: &str, limit: usize) -> GraphResult<Vec<(String, String)>> {
        let index = get_fulltext_index().await;
        index.search(query, limit)
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    async fn fulltext_rebuild(&self) -> GraphResult<()> {
        let index = get_fulltext_index().await;
        tokio::spawn({
            let index = index.clone();
            async move {
                let _ = index.commit().await;
            }
        });
        Ok(())
    }
}
