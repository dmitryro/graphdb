use std::any::Any;
use std::sync::Arc;
use async_trait::async_trait;
use crate::config::{ TypeConfig, MemStoreForTypeConfig, QueryResult, QueryPlan }; // ensure this path matches where TypeConfig is defined
use crate::storage_engine::{GraphStorageEngine, StorageConfig, StorageEngine};
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use tokio::sync::Mutex; // Use Tokio's Mutex
use uuid::Uuid;
use log::info;

// OpenRaft adaptor/type import
use openraft::{storage::Adaptor, storage::RaftStorage as OpenRaftStorage, RaftTypeConfig }; 
use openraft_memstore::{MemStore, TypeConfig as RaftMemStoreTypeConfig};

/// In-memory Raft-friendly store that also implements your GraphStorageEngine/StorageEngine APIs.
///
/// NOTE: This struct holds your original graph storage code (vertices/edges/kv_store)
/// and also acts as the underlying store passed to `openraft::storage::Adaptor::new(store)`.
#[derive(Debug)]
pub struct RaftStorage {
    config: StorageConfig,
    vertices: Mutex<HashMap<Uuid, Vertex>>,
    edges: Mutex<HashMap<(Uuid, Identifier, Uuid), Edge>>,
    kv_store: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    running: Mutex<bool>,
}

/// Fix for MemStoreForTypeConfig: wrap MemStore
impl MemStoreForTypeConfig {
    pub fn new(inner: Arc<MemStore>) -> Self {
        Self { inner }
    }
}

impl RaftStorage {
    pub fn new(config: &StorageConfig) -> Self {
        RaftStorage {
            config: config.clone(),
            vertices: Mutex::new(HashMap::new()),
            edges: Mutex::new(HashMap::new()),
            kv_store: Mutex::new(HashMap::new()),
            running: Mutex::new(false),
        }
    }

    pub fn kv_store(&self) -> &Mutex<HashMap<Vec<u8>, Vec<u8>>> {
        &self.kv_store
    }

    pub fn reset(&mut self) -> GraphResult<()> {
        let mut vertices = self.vertices.blocking_lock(); // Use blocking_lock for sync context
        let mut edges = self.edges.blocking_lock();
        let mut kv_store = self.kv_store.blocking_lock();

        vertices.clear();
        edges.clear();
        kv_store.clear();

        Ok(())
    }

    // Public method to access kv_store
    pub fn kv_store_lock(&self) -> GraphResult<tokio::sync::MutexGuard<HashMap<Vec<u8>, Vec<u8>>>> {
        Ok(self.kv_store.blocking_lock())
    }

    /// Produce the (log_store, state_machine) adaptor pair backed by a tested MemStore.
    /// This function does *not* try to turn the graph RaftStorage into an openraft store.
    /// Instead it creates a separate openraft::MemStore and returns the adaptors created from it.
    ///
    /// Usage:
    /// let store = RaftStorage::new(&config);
    /// let (log_store, state_machine) = store.into_openraft_adaptors().await;
    
    /// Produce the (log_store, state_machine) adaptor pair backed by a tested MemStore.
    pub async fn into_openraft_adaptors(&self) 
        -> (Adaptor<RaftMemStoreTypeConfig, Arc<MemStore>>, Adaptor<RaftMemStoreTypeConfig, Arc<MemStore>>) 
    {
        // Create underlying MemStore (async)
        let store: Arc<MemStore> = MemStore::new_async().await;

        // Adaptor::new(store) returns a tuple (log_store, state_machine)
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

    async fn get_edge(
        &self,
        outbound_id: &Uuid,
        edge_type: &Identifier,
        inbound_id: &Uuid
    ) -> Result<Option<Edge>, GraphError> {
        let edges = self.edges.lock().await;
        Ok(edges.get(&( *outbound_id, edge_type.clone(), *inbound_id )).cloned())
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        let mut vertices = self.vertices.lock().await;
        let mut edges = self.edges.lock().await;
        vertices.clear();
        edges.clear();
        Ok(())
    }

    async fn start(&self) -> Result<(), GraphError> {
        let mut running = self.running.lock().await;
        *running = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        let mut running = self.running.lock().await;
        *running = false;
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "in-memory"
    }

    async fn is_running(&self) -> bool {
        *self.running.lock().await
    }

    async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        Ok(serde_json::json!({
            "status": "success",
            "query": query_string,
            "result": "In-memory query execution placeholder"
        }))
    }

    async fn execute_query(&self, query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        info!("Executing query on RaftStorage (returning null as not implemented)");
        println!("===> EXECUTING QUERY ON Raft STORAGE (NOT IMPLEMENTED)");
        Ok(QueryResult::Null)
    }       

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        let mut vertices = self.vertices.lock().await;
        vertices.insert(vertex.id.0, vertex);
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        let vertices = self.vertices.lock().await;
        Ok(vertices.get(id).cloned())
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.create_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        let mut vertices = self.vertices.lock().await;
        vertices.remove(id);
        Ok(())
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let vertices = self.vertices.lock().await;
        Ok(vertices.values().cloned().collect())
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        let mut edges = self.edges.lock().await;
        edges.insert(
            (edge.outbound_id.into(), edge.edge_type.clone(), edge.inbound_id.into()),
            edge,
        );
        Ok(())
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        let mut edges = self.edges.lock().await;
        edges.remove(&( *outbound_id, edge_type.clone(), *inbound_id ));
        Ok(())
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let edges = self.edges.lock().await;
        Ok(edges.values().cloned().collect())
    }

    async fn close(&self) -> Result<(), GraphError> {
        self.flush().await?;
        info!("RaftStorage closed");
        Ok(())
    }
}
