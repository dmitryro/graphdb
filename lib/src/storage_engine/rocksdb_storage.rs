#[cfg(feature = "with-rocksdb")]
use rocksdb::{DB, Options, WriteBatch, ColumnFamilyDescriptor, DBCompactionStyle};
#[cfg(feature = "with-rocksdb")]
use async_trait::async_trait;
#[cfg(feature = "with-rocksdb")]
use std::path::{Path, PathBuf};
#[cfg(feature = "with-rocksdb")]
use std::sync::Arc;
#[cfg(feature = "with-rocksdb")]
use std::fmt::Debug;
#[cfg(feature = "with-rocksdb")]
use std::any::Any;
#[cfg(feature = "with-rocksdb")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "with-rocksdb")]
use std::fs;
#[cfg(feature = "with-rocksdb")]
use std::time::Duration;
#[cfg(feature = "with-rocksdb")]
use std::thread::sleep;

#[cfg(feature = "with-rocksdb")]
use crate::storage_engine::{GraphStorageEngine, StorageEngine};
#[cfg(feature = "with-rocksdb")]
use crate::storage_engine::config::RocksdbConfig;
#[cfg(feature = "with-rocksdb")]
use models::{Edge, Identifier, Json, Vertex};
#[cfg(feature = "with-rocksdb")]
use models::identifiers::SerializableUuid;
#[cfg(feature = "with-rocksdb")]
use models::errors::{GraphError, GraphResult};
#[cfg(feature = "with-rocksdb")]
use serde_json::{Value, Map};
#[cfg(feature = "with-rocksdb")]
use uuid::Uuid;

#[cfg(feature = "with-rocksdb")]
use super::storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};
#[cfg(feature = "with-rocksdb")]
use crate::util::{build as util_build_key, UtilComponent, read_uuid, read_identifier};
#[cfg(feature = "with-rocksdb")]
use std::io::Cursor;
#[cfg(feature = "with-rocksdb")]
use log::{info, debug, error, warn};

#[cfg(feature = "with-rocksdb")]
const CF_NAMES: &[&str] = &[
    "vertices",
    "edges",
    "vertex_properties",
    "edge_properties",
];

#[cfg(feature = "with-rocksdb")]
#[derive(Debug)]
pub struct RocksdbStorage {
    db: Arc<DB>,
}

#[cfg(feature = "with-rocksdb")]
impl RocksdbStorage {
    /// Constructs a new `RocksdbStorage` instance from a `RocksdbConfig`.
    /// This function is updated to directly use the specific RocksDB configuration
    /// object, resolving the type mismatch error.
    pub fn new(config: &RocksdbConfig) -> GraphResult<Self> {
        info!("--- Loading RocksDB configuration ---");
        
        let path = &config.path;
        info!("  ✅ Path: {:?}", path);
        info!("  ✅ Host: {:?}", config.host);
        info!("  ✅ Port: {:?}", config.port);

        debug!("Opening RocksDB at path: {:?}", path);

        if !path.exists() {
            warn!("RocksDB path does not exist, attempting to create: {:?}", path);
            std::fs::create_dir_all(path)
                .map_err(|e| {
                    error!("Failed to create directory {:?}: {}", path, e);
                    GraphError::StorageError(format!("Failed to create RocksDB directory: {}", e))
                })?;
        }
        if !path.is_dir() {
            error!("RocksDB path is not a directory: {:?}", path);
            return Err(GraphError::StorageError(format!("RocksDB path is not a directory: {:?}", path)));
        }

        let mut options = Options::default();
        options.create_if_missing(true);
        // The RocksdbConfig does not have `max_open_files`, so we set it to a reasonable default.
        options.set_max_open_files(-1);
        options.set_compaction_style(DBCompactionStyle::Level);
        options.set_write_buffer_size(67_108_864);
        options.set_max_write_buffer_number(3);
        options.set_target_file_size_base(67_108_864);
        options.set_level_zero_file_num_compaction_trigger(8);
        options.set_level_zero_slowdown_writes_trigger(17);
        options.set_level_zero_stop_writes_trigger(24);
        options.set_num_levels(4);
        options.set_max_bytes_for_level_base(536_870_912);
        options.set_max_bytes_for_level_multiplier(8.0);

        // We will attempt to open the database with a retry mechanism
        // in case a lock is held by a shutting-down process.
        let mut db = None;
        let mut attempt = 0;
        let max_attempts = 5;
        let base_delay_ms = 100;

        while attempt < max_attempts {
            // Recreate the ColumnFamilyDescriptor vector on each attempt
            // because `DB::open_cf_descriptors` consumes it (takes ownership).
            let cf_descriptors: Vec<ColumnFamilyDescriptor> = CF_NAMES
                .iter()
                .map(|&name| ColumnFamilyDescriptor::new(name, Options::default()))
                .collect();
            
            let open_result = DB::open_cf_descriptors(&options, path, cf_descriptors);
            
            match open_result {
                
                Ok(db_instance) => {
                    info!("Successfully opened RocksDB at {:?}", path);
                    db = Some(db_instance);
                    break;
                },
                Err(e) => {
                    error!("Failed to open RocksDB at {:?}: {}", path, e);
                    // Check for the specific lock error.
                    if e.to_string().contains("lock hold by current process") {
                        warn!("Detected RocksDB lock error. Attempt {} of {}. Retrying after a short delay.", attempt + 1, max_attempts);
                        sleep(Duration::from_millis(base_delay_ms * (attempt as u64 + 1)));
                        attempt += 1;
                        continue;
                    }
                    // Handle other error cases as before.
                    if e.to_string().contains("Column family not found") {
                        warn!("Detected missing column families. Reopening DB to create them.");
                        let mut db_recreate = DB::open(&options, path)
                            .map_err(|e| GraphError::StorageError(format!("Failed to open RocksDB (initial): {}", e)))?;
                        for cf_name in CF_NAMES {
                            db_recreate.create_cf(cf_name, &Options::default())
                                .map_err(|e| GraphError::StorageError(format!("Failed to create Column Family {}: {}", cf_name, e)))?;
                        }
                        info!("Created new RocksDB instance and column families at {:?}", path);
                        db = Some(db_recreate);
                        break;
                    } else if e.to_string().contains("corruption") {
                        warn!("Detected RocksDB corruption at {:?}", path);
                        Self::recover_rocksdb(path)?;
                        // Recreate cf_descriptors to avoid moved value error
                        let cf_descriptors_recreate: Vec<ColumnFamilyDescriptor> = CF_NAMES
                            .iter()
                            .map(|&name| ColumnFamilyDescriptor::new(name, Options::default()))
                            .collect();
                        db = Some(DB::open_cf_descriptors(&options, path, cf_descriptors_recreate)
                            .map_err(|e| GraphError::StorageError(format!("Failed to open RocksDB after recovery: {}", e)))?);
                        break;
                    } else {
                        return Err(GraphError::StorageError(format!("Unhandled RocksDB open error: {}", e)));
                    }
                }
            }
        }

        // If after all attempts we still don't have a DB instance, return an error.
        let db = db.ok_or_else(|| {
            GraphError::StorageError("Failed to open RocksDB after multiple attempts due to lock error.".to_string())
        })?;

        Ok(RocksdbStorage { db: Arc::new(db) })
    }

    fn cf_handle(&self, cf_name: &str) -> GraphResult<&rocksdb::ColumnFamily> {
        self.db.cf_handle(cf_name)
            .ok_or_else(|| GraphError::StorageError(format!("Column Family '{}' not found", cf_name)))
    }

    fn recover_rocksdb(path: &Path) -> GraphResult<()> {
        warn!("Attempting to repair RocksDB at {:?}", path);
        let options = Options::default();
        DB::repair(&options, path)
            .map_err(|e| GraphError::StorageError(format!("Failed to repair RocksDB: {}", e)))?;
        info!("Successfully repaired RocksDB at {:?}", path);
        Ok(())
    }
}

#[cfg(feature = "with-rocksdb")]
#[async_trait]
impl StorageEngine for RocksdbStorage {
    async fn connect(&self) -> GraphResult<()> {
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> GraphResult<()> {
        self.db.put(key, value)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> GraphResult<Option<Vec<u8>>> {
        self.db.get(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    async fn delete(&self, key: &Vec<u8>) -> GraphResult<()> {
        self.db.delete(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn flush(&self) -> GraphResult<()> {
        self.db.flush_wal(true)
            .map_err(|e| GraphError::StorageError(format!("Failed to flush WAL: {}", e)))?;
        Ok(())
    }
}

#[cfg(feature = "with-rocksdb")]
#[async_trait]
impl GraphStorageEngine for RocksdbStorage {
    async fn clear_data(&self) -> GraphResult<()> {
        // This is inefficient but functional. A better approach would be to drop and recreate the CFs.
        info!("Clearing all data from RocksDB");
        let cf_names_to_clear = ["vertices", "edges", "vertex_properties", "edge_properties"];
        for cf_name in &cf_names_to_clear {
            let cf = self.cf_handle(cf_name)?;
            let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
            let mut batch = WriteBatch::default();
            for item in iter {
                let (key, _) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
                batch.delete_cf(cf, key);
            }
            self.db.write(batch).map_err(|e| GraphError::StorageError(e.to_string()))?;
            info!("Cleared column family '{}'", cf_name);
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        &*self.db
    }

    async fn start(&self) -> GraphResult<()> {
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        self.db.flush_wal(true)
            .map_err(|e| GraphError::StorageError(format!("Failed to flush WAL: {}", e)))?;
        info!("RocksdbStorage stopped");
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "RocksDB"
    }

    async fn is_running(&self) -> bool {
        true
    }

    async fn query(&self, query_string: &str) -> GraphResult<Value> {
        println!("Executing query against RocksdbStorage: {}", query_string);
        Ok(serde_json::json!({
            "status": "success",
            "query": query_string,
            "result": "RocksDB query execution placeholder"
        }))
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let cf = self.cf_handle("vertices")?;
        let key = vertex.id.0.as_bytes();
        let value = serialize_vertex(&vertex)?;
        self.db.put_cf(cf, key, value)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        let prop_cf = self.cf_handle("vertex_properties")?;
        for (prop_name, prop_value) in vertex.properties {
            let prop_key = util_build_key(&[
                UtilComponent::Uuid(vertex.id.0),
                UtilComponent::Identifier(Identifier::new(prop_name).map_err(|e| GraphError::InvalidData(format!("Invalid property name: {}", e)))?),
            ])?;
            let prop_value_bytes = serde_json::to_vec(&prop_value)
                .map_err(|e| GraphError::SerializationError(e.to_string()))?;
            self.db.put_cf(prop_cf, prop_key, prop_value_bytes)?;
        }
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let cf = self.cf_handle("vertices")?;
        let key = id.as_bytes();
        let result = self.db.get_cf(cf, key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        let mut vertex_opt = result.map(|bytes| deserialize_vertex(&bytes)).transpose()?;

        if let Some(ref mut vertex) = vertex_opt {
            let prop_cf = self.cf_handle("vertex_properties")?;
            let prefix = util_build_key(&[UtilComponent::Uuid(vertex.id.0)])?;
            let iter = self.db.iterator_cf(prop_cf, rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward));

            for item in iter {
                let (key_bytes, value_bytes) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
                let mut cursor = Cursor::new(&key_bytes);
                unsafe {
                    let owner_id = read_uuid(&mut cursor)?;
                    if owner_id != vertex.id.0 { break; }
                    let prop_name = read_identifier(&mut cursor)?;
                    let prop_value: models::PropertyValue = serde_json::from_slice(&value_bytes)
                        .map_err(|e| GraphError::DeserializationError(e.to_string()))?;
                    vertex.properties.insert(prop_name.0.0.to_string(), prop_value);
                }
            }
        }
        Ok(vertex_opt)
    }

    // Updated to be more efficient. Overwrites the existing vertex and its properties.
    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.create_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let mut batch = WriteBatch::default();
        
        // 1. Delete the vertex itself
        let cf = self.cf_handle("vertices")?;
        let key = id.as_bytes();
        batch.delete_cf(cf, key);
        
        // 2. Delete the vertex properties
        let prop_cf = self.cf_handle("vertex_properties")?;
        let prefix = util_build_key(&[UtilComponent::Uuid(*id)])?;
        let iter = self.db.iterator_cf(prop_cf, rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward));
        for item in iter {
            let (key_bytes, _) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            // A simple check to ensure we only delete properties for this vertex.
            if !key_bytes.starts_with(id.as_bytes()) {
                break;
            }
            batch.delete_cf(prop_cf, key_bytes);
        }
        
        // 3. Delete all edges connected to this vertex (both inbound and outbound)
        let edge_cf = self.cf_handle("edges")?;
        let iter = self.db.iterator_cf(edge_cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key_bytes, value_bytes) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            let edge = deserialize_edge(&value_bytes)?;
            if edge.outbound_id.0 == *id || edge.inbound_id.0 == *id {
                batch.delete_cf(edge_cf, key_bytes);
            }
        }
        
        self.db.write(batch).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let cf = self.cf_handle("vertices")?;
        let mut vertices = Vec::new();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (_key, value) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            let vertex = deserialize_vertex(&value)?;
            vertices.push(vertex);
        }
        Ok(vertices)
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let cf = self.cf_handle("edges")?;
        if self.get_vertex(&edge.outbound_id.into()).await?.is_none() || self.get_vertex(&edge.inbound_id.into()).await?.is_none() {
            return Err(GraphError::InvalidData("One or both vertices for the edge do not exist.".to_string()));
        }

        let key = create_edge_key(&edge.outbound_id, &edge.t, &edge.inbound_id)?;
        let value = serialize_edge(&edge)?;
        self.db.put_cf(cf, key, value)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let cf = self.cf_handle("edges")?;
        let key = create_edge_key(&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id))?;
        let result = self.db.get_cf(cf, key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        result.map(|bytes| deserialize_edge(&bytes)).transpose()
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let cf = self.cf_handle("edges")?;
        let key = create_edge_key(&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id))?;
        self.db.delete_cf(cf, key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let cf = self.cf_handle("edges")?;
        let mut edges = Vec::new();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (_key, value) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            let edge = deserialize_edge(&value)?;
            edges.push(edge);
        }
        Ok(edges)
    }

    async fn close(&self) -> GraphResult<()> {
        self.db.flush_wal(true)
            .map_err(|e| GraphError::StorageError(format!("Failed to flush WAL: {}", e)))?;
        info!("RocksdbStorage closed");
        Ok(())
    }
}