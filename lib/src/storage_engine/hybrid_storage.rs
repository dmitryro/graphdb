// lib/src/storage_engine/hybrid_storage.rs
// Updated: 2025-09-09 - Fixed E0308 and E0560 by correcting StorageConfig initialization:
// - Wrapped PathBuf fields in Some for data_directory and config_root_directory.
// - Removed non-existent connection_string field.
// - Converted log_directory to Option<PathBuf>.
// - Provided a u64 value for max_open_files.

use async_trait::async_trait;
use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use models::errors::GraphError;
use models::{Edge, Identifier, Vertex};
use serde_json::Value;
use uuid::Uuid;
use log::{info, debug, error, warn};

use crate::storage_engine::{GraphStorageEngine, StorageEngine};
use crate::storage_engine::inmemory_storage::InMemoryStorage;
use crate::config::config::{StorageConfig, StorageEngineType, QueryResult, QueryPlan,};
use tokio::sync::Mutex as TokioMutex;

#[derive(Debug)]
pub struct HybridStorage {
    pub inmemory: Arc<InMemoryStorage>,
    pub persistent: Arc<dyn GraphStorageEngine + Send + Sync>,
    pub running: Arc<TokioMutex<bool>>,
    pub engine_type: StorageEngineType,
}

impl HybridStorage {
    pub fn new(persistent: Arc<dyn GraphStorageEngine + Send + Sync>) -> Self {
        let config = StorageConfig {
            storage_engine_type: StorageEngineType::InMemory,
            data_directory: Some(PathBuf::from("/tmp/graphdb_inmemory")), // Fixed: Wrapped in Some
            config_root_directory: Some(PathBuf::from("/tmp/graphdb_config")), // Fixed: Wrapped in Some
            log_directory: Some(PathBuf::from("/tmp/graphdb_logs")), // Fixed: Converted to PathBuf and wrapped in Some
            default_port: 0,
            cluster_range: String::new(),
            max_disk_space_gb: 100,
            min_disk_space_gb: 1,
            use_raft_for_scale: false,
            engine_specific_config: None,
            max_open_files: 1000, // Fixed: Provided u64 value
        };
        HybridStorage {
            inmemory: Arc::new(InMemoryStorage::new(&config)),
            persistent,
            running: Arc::new(TokioMutex::new(false)),
            engine_type: StorageEngineType::Hybrid,
        }
    }
}

#[async_trait]
impl StorageEngine for HybridStorage {
    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphError> {
        self.inmemory.insert(key.clone(), value.clone()).await?;
        self.persistent.insert(key, value).await?;
        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, GraphError> {
        if let Some(value) = self.inmemory.retrieve(key).await? {
            Ok(Some(value))
        } else {
            let value = self.persistent.retrieve(key).await?;
            if let Some(v) = &value {
                self.inmemory.insert(key.to_vec(), v.clone()).await?;
            }
            Ok(value)
        }
    }

    async fn delete(&self, key: &Vec<u8>) -> Result<(), GraphError> {
        self.inmemory.delete(key).await?;
        self.persistent.delete(key).await?;
        Ok(())
    }

    async fn connect(&self) -> Result<(), GraphError> {
        self.inmemory.connect().await?;
        self.persistent.connect().await?;
        Ok(())
    }

    async fn flush(&self) -> Result<(), GraphError> {
        self.inmemory.flush().await?;
        self.persistent.flush().await?;
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for HybridStorage {
    fn get_type(&self) -> &'static str {
        "Hybrid"
    }

    async fn start(&self) -> Result<(), GraphError> {
        self.inmemory.start().await?;
        self.persistent.start().await?;
        let mut running = self.running.lock().await;
        *running = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        self.inmemory.stop().await?;
        self.persistent.stop().await?;
        let mut running = self.running.lock().await;
        *running = false;
        Ok(())
    }

    async fn is_running(&self) -> bool {
        *self.running.lock().await && self.inmemory.is_running().await && self.persistent.is_running().await
    }

    async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        self.persistent.query(query_string).await
    }

    async fn execute_query(&self, query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        info!("Executing query on SledStorage (returning null as not implemented)");
        println!("===> EXECUTING QUERY ON HYBRID STORAGE (NOT IMPLEMENTED)");
        Ok(QueryResult::Null)
    }

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.inmemory.create_vertex(vertex.clone()).await?;
        self.persistent.create_vertex(vertex).await?;
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        if let Some(vertex) = self.inmemory.get_vertex(id).await? {
            Ok(Some(vertex))
        } else {
            let vertex = self.persistent.get_vertex(id).await?;
            if let Some(v) = &vertex {
                self.inmemory.create_vertex(v.clone()).await?;
            }
            Ok(vertex)
        }
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.inmemory.update_vertex(vertex.clone()).await?;
        self.persistent.update_vertex(vertex).await?;
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        self.inmemory.delete_vertex(id).await?;
        self.persistent.delete_vertex(id).await?;
        Ok(())
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let vertices = self.persistent.get_all_vertices().await?;
        for vertex in &vertices {
            self.inmemory.create_vertex(vertex.clone()).await?;
        }
        Ok(vertices)
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.inmemory.create_edge(edge.clone()).await?;
        self.persistent.create_edge(edge).await?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        if let Some(edge) = self.inmemory.get_edge(outbound_id, edge_type, inbound_id).await? {
            Ok(Some(edge))
        } else {
            let edge = self.persistent.get_edge(outbound_id, edge_type, inbound_id).await?;
            if let Some(e) = &edge {
                self.inmemory.create_edge(e.clone()).await?;
            }
            Ok(edge)
        }
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.inmemory.update_edge(edge.clone()).await?;
        self.persistent.update_edge(edge).await?;
        Ok(())
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        self.inmemory.delete_edge(outbound_id, edge_type, inbound_id).await?;
        self.persistent.delete_edge(outbound_id, edge_type, inbound_id).await?;
        Ok(())
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let edges = self.persistent.get_all_edges().await?;
        for edge in &edges {
            self.inmemory.create_edge(edge.clone()).await?;
        }
        Ok(edges)
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        self.inmemory.clear_data().await?;
        self.persistent.clear_data().await?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn close(&self) -> Result<(), GraphError> {
        self.inmemory.close().await?;
        self.persistent.close().await?;
        let mut running = self.running.lock().await;
        *running = false;
        Ok(())
    }
}
