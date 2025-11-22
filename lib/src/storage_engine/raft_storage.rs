// lib/src/storage_engine/raft_storage.rs
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use async_trait::async_trait;
use serde_json::{ json, Value};
use uuid::Uuid;
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use crate::config::{StorageConfig, MemStoreForTypeConfig, QueryResult, QueryPlan};
use crate::{StorageEngine, GraphStorageEngine};
use log::info;
use tokio::sync::{ Mutex as TokioMutex, MutexGuard, OnceCell };
use openraft::{storage::Adaptor, RaftTypeConfig};
use openraft_memstore::{MemStore, TypeConfig as RaftMemStoreTypeConfig};

// Tantivy full-text index — async-safe OnceCell
static FULLTEXT_INDEX: OnceCell<Arc<indexing_service::fulltext::FullTextIndex>> = OnceCell::const_new();

async fn get_fulltext_index() -> Arc<indexing_service::fulltext::FullTextIndex> {
    FULLTEXT_INDEX.get_or_init(|| async {
        let index_dir = PathBuf::from("/opt/graphdb/indexes/fulltext");
        std::fs::create_dir_all(&index_dir).expect("Failed to create fulltext index directory");
        Arc::new(indexing_service::fulltext::FullTextIndex::new(&index_dir).expect("Failed to initialize Tantivy index"))
    }).await.clone()
}

#[derive(Debug)]
pub struct RaftStorage {
    config: StorageConfig,
    vertices: TokioMutex<HashMap<Uuid, Vertex>>,
    edges: TokioMutex<HashMap<(Uuid, Identifier, Uuid), Edge>>,
    kv_store: TokioMutex<HashMap<Vec<u8>, Vec<u8>>>,
    running: TokioMutex<bool>,
}

impl MemStoreForTypeConfig {
    pub fn new(inner: Arc<MemStore>) -> Self {
        Self { inner }
    }
}

impl RaftStorage {
    pub fn new(config: &StorageConfig) -> Self {
        RaftStorage {
            config: config.clone(),
            vertices: TokioMutex::new(HashMap::new()),
            edges: TokioMutex::new(HashMap::new()),
            kv_store: TokioMutex::new(HashMap::new()),
            running: TokioMutex::new(false),
        }
    }

    pub fn kv_store(&self) -> &TokioMutex<HashMap<Vec<u8>, Vec<u8>>> {
        &self.kv_store
    }

    pub fn reset(&mut self) -> GraphResult<()> {
        let mut vertices = self.vertices.blocking_lock();
        let mut edges = self.edges.blocking_lock();
        let mut kv_store = self.kv_store.blocking_lock();
        vertices.clear();
        edges.clear();
        kv_store.clear();
        Ok(())
    }

    pub fn kv_store_lock(&self) -> GraphResult<MutexGuard<HashMap<Vec<u8>, Vec<u8>>>> {
        Ok(self.kv_store.blocking_lock())
    }

    pub async fn into_openraft_adaptors(&self)
        -> (Adaptor<RaftMemStoreTypeConfig, Arc<MemStore>>, Adaptor<RaftMemStoreTypeConfig, Arc<MemStore>>)
    {
        let store: Arc<MemStore> = MemStore::new_async().await;
        Adaptor::new(store)
    }
}

#[async_trait]
impl StorageEngine for RaftStorage {
    async fn connect(&self) -> Result<(), GraphError> {
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphError> {
        let mut kv_store = self.kv_store.lock().await;
        kv_store.insert(key, value);
        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, GraphError> {
        let kv_store = self.kv_store.lock().await;
        Ok(kv_store.get(key).cloned())
    }

    async fn delete(&self, key: &Vec<u8>) -> Result<(), GraphError> {
        let mut kv_store = self.kv_store.lock().await;
        kv_store.remove(key);
        Ok(())
    }

    async fn flush(&self) -> Result<(), GraphError> {
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for RaftStorage {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn start(&self) -> GraphResult<()> {
        let mut running = self.running.lock().await;
        *running = true;
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        let mut running = self.running.lock().await;
        *running = false;
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "raft"
    }

    async fn is_running(&self) -> bool {
        *self.running.lock().await
    }

    async fn query(&self, query_string: &str) -> GraphResult<Value> {
        println!("Executing query against RaftStorage: {}", query_string);
        Ok(json!({
            "status": "success",
            "query": query_string,
            "result": "RaftStorage query execution placeholder"
        }))
    }

    async fn execute_query(&self, _query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        info!("Executing query on RaftStorage (returning null as not implemented)");
        println!("===> EXECUTING QUERY ON RAFT STORAGE (NOT IMPLEMENTED)");
        Ok(QueryResult::Null)
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let mut vertices = self.vertices.lock().await;
        vertices.insert(vertex.id.0, vertex);
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let vertices = self.vertices.lock().await;
        Ok(vertices.get(id).cloned())
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let mut vertices = self.vertices.lock().await;
        vertices.insert(vertex.id.0, vertex);
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let mut vertices = self.vertices.lock().await;
        vertices.remove(id);
        Ok(())
    }

    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let vertices = self.vertices.lock().await;
        Ok(vertices.values().cloned().collect())
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let mut edges = self.edges.lock().await;
        edges.insert((edge.outbound_id.0, edge.edge_type.clone(), edge.inbound_id.0), edge);
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let edges = self.edges.lock().await;
        Ok(edges.get(&(*outbound_id, edge_type.clone(), *inbound_id)).cloned())
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let mut edges = self.edges.lock().await;
        edges.remove(&(*outbound_id, edge_type.clone(), *inbound_id));
        Ok(())
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let edges = self.edges.lock().await;
        Ok(edges.values().cloned().collect())
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        let mut vertices = self.vertices.lock().await;
        let mut edges = self.edges.lock().await;
        vertices.clear();
        edges.clear();
        Ok(())
    }

    async fn close(&self) -> GraphResult<()> {
        self.flush().await?;
        info!("RaftStorage closed");
        Ok(())
    }

    // === INDEX METHODS (not supported in RaftStorage) ===
    async fn create_index(&self, _label: &str, _property: &str) -> GraphResult<()> {
        Err(GraphError::StorageError("Secondary indexes are not supported on RaftStorage backend".into()))
    }

    async fn drop_index(&self, _label: &str, _property: &str) -> GraphResult<()> {
        Err(GraphError::StorageError("Secondary indexes are not supported on RaftStorage backend".into()))
    }

    async fn list_indexes(&self) -> GraphResult<Vec<(String, String)>> {
        Ok(vec![])
    }

    // === FULLTEXT SEARCH (Tantivy via global index) ===
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

    // === REQUIRED INDEX COMMAND IMPLEMENTATION (Fix for E0609 & E0574) ===
    async fn execute_index_command(&self, command: &str, params: Value) -> GraphResult<QueryResult> {
        // The InMemoryStorage doesn't manage indexes and cannot delegate to a persistent layer.
        // It fulfills the trait requirement by returning a harmless success state.
        info!("Index command '{}' received by RaftStorage. Returning success stub.", command);

        // FIX: QueryResult is an ENUM (QueryResult::Success or QueryResult::Null).
        // We use the Success variant with a message to indicate the command was received but ignored.
        Ok(QueryResult::Success(
            format!("Index command '{}' successfully received but is a no-op in InMemoryStorage.", command)
        ))
    }
}
