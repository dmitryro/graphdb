use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use rocksdb::{DB, ColumnFamily, Options, DBCompressionType, WriteBatch, WriteOptions};
use models::{Vertex, Edge, Identifier, identifiers::SerializableUuid};
use models::errors::{GraphError, GraphResult};
pub use crate::config::{QueryResult, RocksDBClient, RaftCommand};
use crate::storage_engine::storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};
use uuid::Uuid;
use log::{info, error};

impl RocksDBClient {
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
        })
    }

    pub async fn new_with_db(db_path: PathBuf, db: Arc<DB>) -> GraphResult<Self> {
        Ok(Self {
            inner: Arc::new(TokioMutex::new(db)),
            db_path,
            is_running: false,
        })
    }

    pub async fn force_unlock(db_path: PathBuf) -> GraphResult<()> {
        let lock_path = db_path.join("LOCK");
        if let Err(e) = rocksdb::DB::destroy(&rocksdb::Options::default(), &lock_path) {
            if !e.to_string().contains("No such file or directory") {
                return Err(GraphError::StorageError(format!("Failed to unlock database: {}", e)));
            }
        }
        Ok(())
    }

    pub async fn apply_raft_entry(&self, data: Vec<u8>) -> GraphResult<()> {
        let db = self.inner.lock().await;
        let cf = (*db).cf_handle("kv_pairs")
            .ok_or_else(|| GraphError::StorageError("Missing column family: kv_pairs".to_string()))?;
        let (key, value) = data.split_at(data.len() / 2);
        (*db).put_cf(&cf, key, value).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    pub async fn insert_into_cf(&self, cf_name: &str, key: &[u8], value: &[u8]) -> GraphResult<()> {
        let db = self.inner.lock().await;
        let cf = (*db).cf_handle(cf_name)
            .ok_or_else(|| GraphError::StorageError(format!("Missing column family: {}", cf_name)))?;
        (*db).put_cf(&cf, key, value)
            .map_err(|e| GraphError::StorageError(format!("Failed to insert into {}: {}", cf_name, e)))?;
        (*db).flush_wal(true)
            .map_err(|e| GraphError::StorageError(format!("Failed to flush WAL: {}", e)))?;
        Ok(())
    }

    pub async fn retrieve_from_cf(&self, cf_name: &str, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        let db = self.inner.lock().await;
        let cf = (*db).cf_handle(cf_name)
            .ok_or_else(|| GraphError::StorageError(format!("Missing column family: {}", cf_name)))?;
        let result = (*db).get_cf(&cf, key)
            .map_err(|e| GraphError::StorageError(format!("Failed to retrieve from {}: {}", cf_name, e)))?;
        Ok(result)
    }

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
}
