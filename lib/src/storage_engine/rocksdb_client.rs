// Extended RocksDBClient implementation with ZMQ support while preserving original functionality
use std::any::Any;
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use rocksdb::{DB, ColumnFamily, Options, DBCompressionType, WriteBatch, WriteOptions};
use models::{Vertex, Edge, Identifier, identifiers::SerializableUuid};
use models::errors::{GraphError, GraphResult};
pub use crate::config::{QueryResult, RocksDBClient, RaftCommand, RocksDBClientMode};
use crate::storage_engine::storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};
use crate::storage_engine::{ GraphStorageEngine, StorageEngine };
use crate::config::{ QueryPlan };
use uuid::Uuid;
use log::{info, error, debug};
use serde_json::{json, Value};

impl RocksDBClient {
    // Keep the original new() method unchanged
    pub async fn new(db_path: PathBuf) -> GraphResult<Self> {
        let cf_names = vec![
            "data", "vertices", "edges", "kv_pairs",
            "raft_log", "raft_snapshot", "raft_membership", "raft_vote"
        ];
        let mut cf_opts = Options::default();
        cf_opts.create_if_missing(true);
        cf_opts.set_compression_type(DBCompressionType::Zstd);
        let cfs = cf_names
            .iter()
            .map(|name| rocksdb::ColumnFamilyDescriptor::new(*name, cf_opts.clone()))
            .collect::<Vec<_>>();
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        Self::force_unlock(db_path.clone()).await?;

        let db_path_clone = db_path.clone();
        let db = tokio::task::spawn_blocking(move || {
            DB::open_cf_descriptors(&db_opts, &db_path_clone, cfs)
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to spawn blocking task: {}", e)))?
        .map_err(|e| GraphError::StorageError(format!("Failed to open RocksDB: {}", e)))?;

        Ok(Self {
            inner: Arc::new(TokioMutex::new(Arc::new(db))),
            db_path,
            is_running: false,
            mode: Some(RocksDBClientMode::Direct), // Add this field to the struct
        })
    }

    // Keep the original new_with_db() method unchanged
    pub async fn new_with_db(db_path: PathBuf, db: Arc<DB>) -> GraphResult<Self> {
        Ok(Self {
            inner: Arc::new(TokioMutex::new(db)),
            db_path,
            is_running: false,
            mode: Some(RocksDBClientMode::Direct),
        })
    }

    /// Creates a new RocksDBClient that connects to an existing daemon via ZMQ
    /// instead of opening the database directly. This prevents lock conflicts.
    pub async fn new_with_port(port: u16) -> GraphResult<Self> {
        info!("Creating RocksDBClient that connects to daemon on port {}", port);
        
        // Create a minimal client structure for ZMQ mode
        // We don't actually open a database in this case
        let dummy_path = PathBuf::from(format!("/tmp/rocksdb-client-zmq-{}", port));
        
        // Test the ZMQ connection by performing a ping
        if Self::ping_daemon(port).await.is_ok() {
            info!("Successfully connected to RocksDB daemon on port {}", port);
            
            // Create a dummy DB handle that won't be used
            let dummy_db = tokio::task::spawn_blocking(move || {
                let opts = Options::default();
                DB::open_default(&dummy_path)
            })
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to create dummy DB: {}", e)))?
            .map_err(|e| GraphError::StorageError(format!("Failed to create dummy DB: {}", e)))?;

            Ok(Self {
                inner: Arc::new(TokioMutex::new(Arc::new(dummy_db))),
                db_path: PathBuf::from(format!("/tmp/rocksdb-client-zmq-{}", port)),
                is_running: false,
                mode: Some(RocksDBClientMode::ZMQ(port)),
            })
        } else {
            Err(GraphError::StorageError(format!("Failed to connect to RocksDB daemon on port {}", port)))
        }
    }

    // Keep all original methods unchanged
    pub async fn force_unlock(db_path: PathBuf) -> GraphResult<()> {
        let lock_path = db_path.join("LOCK");
        if let Err(e) = rocksdb::DB::destroy(&rocksdb::Options::default(), &lock_path) {
            if !e.to_string().contains("No such file or directory") {
                return Err(GraphError::StorageError(format!("Failed to unlock database: {}", e)));
            }
        }
        Ok(())
    }

    // Keep original apply_raft_entry unchanged
    pub async fn apply_raft_entry(&self, data: Vec<u8>) -> GraphResult<()> {
        let db = self.inner.lock().await;
        let cf = (*db).cf_handle("kv_pairs")
            .ok_or_else(|| GraphError::StorageError("Missing column family: kv_pairs".to_string()))?;
        let (key, value) = data.split_at(data.len() / 2);
        (*db).put_cf(&cf, key, value).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    // Enhanced insert_into_cf that supports both modes
    pub async fn insert_into_cf(&self, cf_name: &str, key: &[u8], value: &[u8]) -> GraphResult<()> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                // Original direct database access logic
                let db = self.inner.lock().await;
                let cf = (*db).cf_handle(cf_name)
                    .ok_or_else(|| GraphError::StorageError(format!("Missing column family: {}", cf_name)))?;
                (*db).put_cf(&cf, key, value)
                    .map_err(|e| GraphError::StorageError(format!("Failed to insert into {}: {}", cf_name, e)))?;
                (*db).flush_wal(true)
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush WAL: {}", e)))?;
                Ok(())
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                // ZMQ communication logic
                self.insert_into_cf_zmq(*port, cf_name, key, value).await
            }
            None => {
                info!("Will be implemented later.");
                Ok(()) 
            }
        }
    }

    // Enhanced retrieve_from_cf that supports both modes
    pub async fn retrieve_from_cf(&self, cf_name: &str, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                // Original direct database access logic
                let db = self.inner.lock().await;
                let cf = (*db).cf_handle(cf_name)
                    .ok_or_else(|| GraphError::StorageError(format!("Missing column family: {}", cf_name)))?;
                let result = (*db).get_cf(&cf, key)
                    .map_err(|e| GraphError::StorageError(format!("Failed to retrieve from {}: {}", cf_name, e)))?;
                Ok(result)
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                // ZMQ communication logic
                self.retrieve_from_cf_zmq(*port, cf_name, key).await
            }
            None => {
                info!("Will be implemented later.");
                Ok(None)            }
        }
    }

    // Keep all original vertex/edge methods unchanged - they use insert_into_cf/retrieve_from_cf internally
    pub async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let uuid = SerializableUuid(vertex.id.0);
        let key = uuid.0.as_bytes();
        let value = serialize_vertex(&vertex)?;
        self.insert_into_cf("vertices", key, &value).await
    }

    pub async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let uuid = SerializableUuid(*id);
        let key = uuid.0.as_bytes();
        let result = self.retrieve_from_cf("vertices", key).await?;
        match result {
            Some(v) => Ok(Some(deserialize_vertex(&v)?)),
            None => Ok(None),
        }
    }

    pub async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.create_vertex(vertex).await
    }

    pub async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                // Original direct database access logic
                let db = self.inner.lock().await;
                let cf = (*db).cf_handle("vertices")
                    .ok_or_else(|| GraphError::StorageError(format!("Missing column family: vertices")))?;
                let mut batch = WriteBatch::default();
                let uuid = SerializableUuid(*id);
                batch.delete_cf(&cf, uuid.0.as_bytes());
                (*db).write(batch)
                    .map_err(|e| GraphError::StorageError(format!("Failed to delete vertex: {}", e)))?;
                (*db).flush_wal(true)
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush after delete: {}", e)))?;
                Ok(())
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                let uuid = SerializableUuid(*id);
                let key = uuid.0.as_bytes();
                self.delete_from_cf_zmq(*port, "vertices", key).await
            }
            None => {
                info!("Will be implemented later.");
                Ok(()) 
            }

        }
    }

    // Keep all other original methods unchanged...
    pub async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let key = create_edge_key(
            &SerializableUuid(edge.outbound_id.0),
            &edge.t,
            &SerializableUuid(edge.inbound_id.0)
        )?;
        let value = serialize_edge(&edge)?;
        self.insert_into_cf("edges", &key, &value).await
    }

    pub async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let key = create_edge_key(
            &SerializableUuid(*outbound_id),
            edge_type,
            &SerializableUuid(*inbound_id)
        )?;
        let result = self.retrieve_from_cf("edges", &key).await?;
        match result {
            Some(v) => Ok(Some(deserialize_edge(&v)?)),
            None => Ok(None),
        }
    }

    pub async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    pub async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                // Original logic
                let db = self.inner.lock().await;
                let cf = (*db).cf_handle("edges")
                    .ok_or_else(|| GraphError::StorageError(format!("Missing column family: edges")))?;
                let mut batch = WriteBatch::default();
                let key = create_edge_key(
                    &SerializableUuid(*outbound_id),
                    edge_type,
                    &SerializableUuid(*inbound_id)
                )?;
                batch.delete_cf(&cf, &key);
                (*db).write(batch)
                    .map_err(|e| GraphError::StorageError(format!("Failed to delete edge: {}", e)))?;
                (*db).flush_wal(true)
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush after delete: {}", e)))?;
                Ok(())
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                let key = create_edge_key(
                    &SerializableUuid(*outbound_id),
                    edge_type,
                    &SerializableUuid(*inbound_id)
                )?;
                self.delete_from_cf_zmq(*port, "edges", &key).await
            }
            None => {
                info!("Will be implemented later.");
                Ok(()) 
            }
        }
    }

    // Keep original get_all_* methods - add ZMQ support later if needed
    pub async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let db = self.inner.lock().await;
        let cf = (*db).cf_handle("vertices")
            .ok_or_else(|| GraphError::StorageError(format!("Missing column family: vertices")))?;
        let iter = (*db).iterator_cf(&cf, rocksdb::IteratorMode::Start);
        let mut vertices = Vec::new();
        for res in iter {
            let (_, value) = res.map_err(|e| GraphError::StorageError(format!("Failed to iterate vertices: {}", e)))?;
            vertices.push(deserialize_vertex(&value)?);
        }
        Ok(vertices)
    }

    pub async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let db = self.inner.lock().await;
        let cf = (*db).cf_handle("edges")
            .ok_or_else(|| GraphError::StorageError(format!("Missing column family: edges")))?;
        let iter = (*db).iterator_cf(&cf, rocksdb::IteratorMode::Start);
        let mut edges = Vec::new();
        for res in iter {
            let (_, value) = res.map_err(|e| GraphError::StorageError(format!("Failed to iterate edges: {}", e)))?;
            edges.push(deserialize_edge(&value)?);
        }
        Ok(edges)
    }

    // Keep all original control methods unchanged
    pub async fn clear_data(&self) -> GraphResult<()> {
        let cfs = ["vertices", "edges", "kv_pairs"];
        let db = self.inner.lock().await;
        let mut batch = WriteBatch::default();
        for cf_name in cfs.iter() {
            let cf = (*db).cf_handle(cf_name)
                .ok_or_else(|| GraphError::StorageError(format!("Missing column family: {}", cf_name)))?;
            let iter = (*db).iterator_cf(&cf, rocksdb::IteratorMode::Start);
            for res in iter {
                let (key, _) = res.map_err(|e| GraphError::StorageError(format!("Failed to iterate {}: {}", cf_name, e)))?;
                batch.delete_cf(&cf, key);
            }
        }
        (*db).write(batch)
            .map_err(|e| GraphError::StorageError(format!("Failed to clear data: {}", e)))?;
        (*db).flush_wal(true)
            .map_err(|e| GraphError::StorageError(format!("Failed to flush after clear: {}", e)))?;
        Ok(())
    }

    pub async fn connect(&mut self) -> GraphResult<()> {
        info!("Connecting to RocksDB");
        self.is_running = true;
        Ok(())
    }

    pub async fn start(&mut self) -> GraphResult<()> {
        info!("Starting RocksDB");
        self.is_running = true;
        Ok(())
    }

    pub async fn stop(&mut self) -> GraphResult<()> {
        info!("Stopping RocksDB");
        self.is_running = false;
        Ok(())
    }

    pub async fn close(&mut self) -> GraphResult<()> {
        info!("Closing RocksDB");
        self.is_running = false;
        Ok(())
    }

    pub async fn flush(&self) -> GraphResult<()> {
        let db = self.inner.lock().await;
        (*db).flush_wal(true)
            .map_err(|e| GraphError::StorageError(format!("Failed to flush WAL: {}", e)))?;
        Ok(())
    }

    pub async fn execute_query(&self) -> GraphResult<QueryResult> {
        info!("Executing query on RocksDBClient (not implemented)");
        Ok(QueryResult::Null)
    }

    // New ZMQ helper methods
    async fn ping_daemon(port: u16) -> GraphResult<()> {
        let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", port);
        let addr = format!("ipc://{}", socket_path);

        // Check if socket file exists
        if !tokio::fs::metadata(&socket_path).await.is_ok() {
            return Err(GraphError::StorageError(format!("IPC socket file {} does not exist", socket_path)));
        }

        let request = json!({ "command": "ping" });
        let request_data = serde_json::to_vec(&request)
            .map_err(|e| GraphError::StorageError(format!("Failed to serialize ping request: {}", e)))?;

        let response = tokio::task::spawn_blocking({
            let addr = addr.clone();
            let request_data = request_data.clone();
            move || -> Result<Value, GraphError> {
                let context = zmq::Context::new();
                let socket = context.socket(zmq::REQ)
                    .map_err(|e| GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e)))?;

                socket.set_rcvtimeo(5000)
                    .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
                socket.set_sndtimeo(5000)
                    .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;

                socket.connect(&addr)
                    .map_err(|e| GraphError::StorageError(format!("Failed to connect to {}: {}", addr, e)))?;

                socket.send(&request_data, 0)
                    .map_err(|e| GraphError::StorageError(format!("Failed to send ping: {}", e)))?;

                let mut msg = zmq::Message::new();
                socket.recv(&mut msg, 0)
                    .map_err(|e| GraphError::StorageError(format!("Failed to receive ping response: {}", e)))?;

                let response: Value = serde_json::from_slice(&msg)
                    .map_err(|e| GraphError::StorageError(format!("Failed to deserialize ping response: {}", e)))?;

                Ok(response)
            }
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("ZMQ task failed: {}", e)))??;

        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            Ok(())
        } else {
            Err(GraphError::StorageError("Ping failed".to_string()))
        }
    }

    async fn send_zmq_request(&self, port: u16, request: Value) -> GraphResult<Value> {
        let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", port);
        let addr = format!("ipc://{}", socket_path);

        let request_data = serde_json::to_vec(&request)
            .map_err(|e| GraphError::StorageError(format!("Failed to serialize request: {}", e)))?;

        tokio::task::spawn_blocking({
            let addr = addr.clone();
            let request_data = request_data.clone();
            move || -> Result<Value, GraphError> {
                let context = zmq::Context::new();
                let socket = context.socket(zmq::REQ)
                    .map_err(|e| GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e)))?;

                socket.set_rcvtimeo(15000)
                    .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
                socket.set_sndtimeo(10000)
                    .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;

                socket.connect(&addr)
                    .map_err(|e| GraphError::StorageError(format!("Failed to connect to {}: {}", addr, e)))?;

                socket.send(&request_data, 0)
                    .map_err(|e| GraphError::StorageError(format!("Failed to send request: {}", e)))?;

                let mut msg = zmq::Message::new();
                socket.recv(&mut msg, 0)
                    .map_err(|e| GraphError::StorageError(format!("Failed to receive response: {}", e)))?;

                let response: Value = serde_json::from_slice(&msg)
                    .map_err(|e| GraphError::StorageError(format!("Failed to deserialize response: {}", e)))?;

                Ok(response)
            }
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("ZMQ task failed: {}", e)))?
    }

    async fn insert_into_cf_zmq(&self, port: u16, cf_name: &str, key: &[u8], value: &[u8]) -> GraphResult<()> {
        let request = json!({
            "command": "set_key",
            "key": String::from_utf8_lossy(key),
            "value": String::from_utf8_lossy(value),
            "cf": cf_name
        });

        let response = self.send_zmq_request(port, request).await?;

        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            Ok(())
        } else {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            Err(GraphError::StorageError(format!("ZMQ insert failed: {}", error_msg)))
        }
    }

    async fn retrieve_from_cf_zmq(&self, port: u16, cf_name: &str, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        let request = json!({
            "command": "get_key",
            "key": String::from_utf8_lossy(key),
            "cf": cf_name
        });

        let response = self.send_zmq_request(port, request).await?;

        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            if let Some(value) = response.get("value") {
                if value.is_null() {
                    Ok(None)
                } else {
                    let value_str = value.as_str()
                        .ok_or_else(|| GraphError::StorageError("Invalid value format".to_string()))?;
                    Ok(Some(value_str.as_bytes().to_vec()))
                }
            } else {
                Ok(None)
            }
        } else {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            Err(GraphError::StorageError(format!("ZMQ retrieve failed: {}", error_msg)))
        }
    }

    async fn delete_from_cf_zmq(&self, port: u16, cf_name: &str, key: &[u8]) -> GraphResult<()> {
        let request = json!({
            "command": "delete_key",
            "key": String::from_utf8_lossy(key),
            "cf": cf_name
        });

        let response = self.send_zmq_request(port, request).await?;

        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            Ok(())
        } else {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            Err(GraphError::StorageError(format!("ZMQ delete failed: {}", error_msg)))
        }
    }
}

// Add the missing trait implementations
#[async_trait]
impl StorageEngine for RocksDBClient {
    async fn connect(&self) -> Result<(), GraphError> {
        info!("Connecting to RocksDBClient");
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphError> {
        self.insert_into_cf("kv_pairs", &key, &value).await
    }

    async fn retrieve(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, GraphError> {
        self.retrieve_from_cf("kv_pairs", key).await
    }

    async fn delete(&self, key: &Vec<u8>) -> Result<(), GraphError> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                let db = self.inner.lock().await;
                let cf = (*db).cf_handle("kv_pairs")
                    .ok_or_else(|| GraphError::StorageError(format!("Missing column family: kv_pairs")))?;
                let mut batch = WriteBatch::default();
                batch.delete_cf(&cf, key);
                (*db).write(batch)
                    .map_err(|e| GraphError::StorageError(format!("Failed to delete key: {}", e)))?;
                (*db).flush_wal(true)
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush after delete: {}", e)))?;
                Ok(())
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                self.delete_from_cf_zmq(*port, "kv_pairs", key).await
            }
            None => {
                let db = self.inner.lock().await;
                let cf = (*db).cf_handle("kv_pairs")
                    .ok_or_else(|| GraphError::StorageError(format!("Missing column family: kv_pairs")))?;
                let mut batch = WriteBatch::default();
                batch.delete_cf(&cf, key);
                (*db).write(batch)
                    .map_err(|e| GraphError::StorageError(format!("Failed to delete key: {}", e)))?;
                (*db).flush_wal(true)
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush after delete: {}", e)))?;
                Ok(())
            }
        }
    }

    async fn flush(&self) -> Result<(), GraphError> {
        self.flush().await
    }
}

#[async_trait]
impl GraphStorageEngine for RocksDBClient {
    async fn start(&self) -> Result<(), GraphError> {
        info!("Starting RocksDBClient");
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        info!("Stopping RocksDBClient");
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => "rocksdb_client",
            Some(RocksDBClientMode::ZMQ(_)) => "rocksdb_client_zmq",
            None => "rocksdb_client",
        }
    }

    async fn is_running(&self) -> bool {
        self.is_running
    }

    async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        match &self.mode {
            Some(RocksDBClientMode::ZMQ(port)) => {
                let request = json!({ "command": "query", "query": query_string });
                let response = self.send_zmq_request(*port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    Ok(response.get("value").cloned().unwrap_or(Value::Null))
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ query failed: {}", error_msg)))
                }
            }
            _ => Err(GraphError::StorageError("RocksDBClient query not implemented for direct access".to_string())),
        }
    }

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.create_vertex(vertex).await
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        self.get_vertex(id).await
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.update_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        self.delete_vertex(id).await
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        self.get_all_vertices().await
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.create_edge(edge).await
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        self.get_edge(outbound_id, edge_type, inbound_id).await
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.update_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        self.delete_edge(outbound_id, edge_type, inbound_id).await
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        self.get_all_edges().await
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        self.clear_data().await
    }

    async fn execute_query(&self, query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        match &self.mode {
            Some(RocksDBClientMode::ZMQ(port)) => {
                let request = json!({ "command": "execute_query", "query_plan": query_plan });
                let response = self.send_zmq_request(*port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    let result = response.get("value")
                        .map(|v| serde_json::from_value(v.clone()))
                        .transpose()
                        .map_err(|e| GraphError::StorageError(format!("Failed to deserialize query result: {}", e)))?
                        .unwrap_or(QueryResult::Null);
                    Ok(result)
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ execute_query failed: {}", error_msg)))
                }
            }
            _ => {
                info!("Executing query on RocksDBClient (returning null as not implemented)");
                Ok(QueryResult::Null)
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn close(&self) -> Result<(), GraphError> {
        info!("Closing RocksDBClient");
        Ok(())
    }
}