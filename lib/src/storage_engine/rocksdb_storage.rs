// lib/src/storage_engine/rocksdb_storage.rs
// Refactored: 2025-07-04 - Renamed to RocksdbGraphStorage, implemented GraphStorageEngine,
// and integrated RocksDB Column Family (CF) management.

#[cfg(feature = "with-rocksdb")]
use rocksdb::{DB, Options, WriteBatch, ColumnFamilyDescriptor, DBCompactionStyle};
#[cfg(feature = "with-rocksdb")]
use async_trait::async_trait;
#[cfg(feature = "with-rocksdb")]
use std::path::Path;
#[cfg(feature = "with-rocksdb")]
use std::sync::Arc;
#[cfg(feature = "with-rocksdb")]
use std::fmt::Debug; // Required for Debug trait bound

#[cfg(feature = "with-rocksdb")]
use crate::storage_engine::{GraphStorageEngine, StorageEngine, StorageConfig}; // Import traits and StorageConfig
#[cfg(feature = "with-rocksdb")]
use models::{Edge, Identifier, Json, Vertex};
#[cfg(feature = "with-rocksdb")]
use models::errors::{GraphError, GraphResult};
#[cfg(feature = "with-rocksdb")]
use serde_json::Value; // For query results
#[cfg(feature = "with-rocksdb")]
use uuid::Uuid;

// Import the new utility functions for serialization/deserialization and key building
#[cfg(feature = "with-rocksdb")]
use super::storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};
#[cfg(feature = "with-rocksdb")]
use crate::util::{build as util_build_key, Component as UtilComponent, read_uuid, read_identifier};
#[cfg(feature = "with-rocksdb")]
use std::io::Cursor;

#[cfg(feature = "with-rocksdb")]
// Define Column Family names for RocksDB
const CF_NAMES: &[&str] = &[
    "vertices",
    "edges",
    "vertex_properties",
    "edge_properties",
    // Add other CFs as needed for indexes, etc.
];

#[cfg(feature = "with-rocksdb")]
/// RocksDB-backed implementation of both `StorageEngine` and `GraphStorageEngine` traits.
/// This struct manages the underlying RocksDB database and provides methods for both
/// generic key-value operations and graph-specific operations using Column Families.
#[derive(Debug)]
pub struct RocksdbGraphStorage {
    db: Arc<DB>,
}

#[cfg(feature = "with-rocksdb")]
impl RocksdbGraphStorage {
    /// Creates a new `RocksdbGraphStorage` instance.
    pub fn new(config: &StorageConfig) -> GraphResult<Self> {
        let path = Path::new(&config.data_path);
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_max_open_files(config.max_open_files.unwrap_or(-1)); // Use config value or default

        // Recommended RocksDB tuning options (from rdb/datastore.rs)
        options.set_compaction_style(DBCompactionStyle::Level);
        options.set_write_buffer_size(67_108_864); // 64mb
        options.set_max_write_buffer_number(3);
        options.set_target_file_size_base(67_108_864); // 64mb
        options.set_level_zero_file_num_compaction_trigger(8);
        options.set_level_zero_slowdown_writes_trigger(17);
        options.set_level_zero_stop_writes_trigger(24);
        options.set_num_levels(4);
        options.set_max_bytes_for_level_base(536_870_912); // 512mb
        options.set_max_bytes_for_level_multiplier(8.0);

        // Prepare Column Family Descriptors
        let cf_descriptors: Vec<ColumnFamilyDescriptor> = CF_NAMES
            .iter()
            .map(|&name| ColumnFamilyDescriptor::new(name, Options::default()))
            .collect();

        let db = match DB::open_cf_descriptors(&options, path, cf_descriptors) {
            Ok(db) => db,
            Err(e) => {
                // If opening with CFs fails (e.g., first time creating), try opening without CFs
                // and then create them. This is a common pattern for RocksDB.
                let mut db = DB::open(&options, path)
                    .map_err(|e| GraphError::StorageError(format!("Failed to open RocksDB (initial): {}", e)))?;

                for cf_name in CF_NAMES {
                    db.create_cf(cf_name, &Options::default())
                        .map_err(|e| GraphError::StorageError(format!("Failed to create Column Family {}: {}", cf_name, e)))?;
                }
                db
            }
        };

        Ok(RocksdbGraphStorage { db: Arc::new(db) })
    }

    // Helper to get a CF handle
    fn cf_handle(&self, cf_name: &str) -> GraphResult<&rocksdb::ColumnFamily> {
        self.db.cf_handle(cf_name)
            .ok_or_else(|| GraphError::StorageError(format!("Column Family '{}' not found", cf_name)))
    }
}

#[cfg(feature = "with-rocksdb")]
#[async_trait]
impl StorageEngine for RocksdbGraphStorage {
    async fn connect(&self) -> GraphResult<()> {
        // RocksDB is "connected" when opened.
        Ok(())
    }

    async fn start(&self) -> GraphResult<()> {
        // RocksDB is "started" when opened.
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        // RocksDB handles flushing automatically on drop.
        // Explicit flush can be done if immediate persistence is needed before drop.
        self.db.flush_wal(true).map_err(|e| GraphError::Io(e.into()))?;
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "RocksDB"
    }

    fn is_running(&self) -> bool {
        // If the Arc<DB> is still valid, it's considered running.
        true
    }

    async fn insert(&self, key: &[u8], value: &[u8]) -> GraphResult<()> {
        // Generic insert uses the default CF
        self.db.put(key, value)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn retrieve(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        // Generic retrieve uses the default CF
        self.db.get(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    async fn delete(&self, key: &[u8]) -> GraphResult<()> {
        // Generic delete uses the default CF
        self.db.delete(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn flush(&self) -> GraphResult<()> {
        // RocksDB handles flushing automatically. Explicit flush_wal for durability.
        self.db.flush_wal(true).map_err(|e| GraphError::Io(e.into()))?;
        Ok(())
    }
}

#[cfg(feature = "with-rocksdb")]
#[async_trait]
impl GraphStorageEngine for RocksdbGraphStorage {
    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let cf = self.cf_handle("vertices")?;
        let key = vertex.id.0.as_bytes(); // Use the inner Uuid
        let value = serialize_vertex(&vertex)?;
        self.db.put_cf(cf, key, value)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        // Store properties in vertex_properties CF
        let prop_cf = self.cf_handle("vertex_properties")?;
        for (prop_name, prop_value) in vertex.properties {
            let prop_key = util_build_key(&[
                UtilComponent::Uuid(vertex.id.0),
                UtilComponent::Identifier(Identifier::new(&prop_name).map_err(|e| GraphError::InvalidData(format!("Invalid property name: {}", e)))?),
            ]);
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

        // Retrieve properties from vertex_properties CF
        if let Some(ref mut vertex) = vertex_opt {
            let prop_cf = self.cf_handle("vertex_properties")?;
            let prefix = util_build_key(&[UtilComponent::Uuid(vertex.id.0)]);
            let iter = self.db.iterator_cf(prop_cf, rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward));

            for item in iter {
                let (key_bytes, value_bytes) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
                let mut cursor = Cursor::new(&key_bytes);
                unsafe { // Safety: We control key format and expect valid UUID/Identifier
                    let owner_id = read_uuid(&mut cursor)?;
                    if owner_id != vertex.id.0 { break; } // Stop if we moved to another vertex's properties
                    let prop_name = read_identifier(&mut cursor)?;
                    let prop_value: models::PropertyValue = serde_json::from_slice(&value_bytes)
                        .map_err(|e| GraphError::DeserializationError(e.to_string()))?;
                    vertex.properties.insert(prop_name.0.0, prop_value);
                }
            }
        }
        Ok(vertex_opt)
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        // For RocksDB, update is effectively create, but we need to handle properties.
        // A full update would involve reading existing properties, comparing, and writing changes.
        // For simplicity here, we'll delete existing properties and then re-add.
        self.delete_vertex(&vertex.id.0).await?;
        self.create_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let cf = self.cf_handle("vertices")?;
        let key = id.as_bytes();
        self.db.delete_cf(cf, key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        // Delete associated properties from vertex_properties CF
        let prop_cf = self.cf_handle("vertex_properties")?;
        let prefix = util_build_key(&[UtilComponent::Uuid(*id)]);
        let iter = self.db.iterator_cf(prop_cf, rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward));
        let mut batch = WriteBatch::default();
        for item in iter {
            let (key_bytes, _) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            let mut cursor = Cursor::new(&key_bytes);
            unsafe { // Safety: We control key format and expect valid UUID
                let owner_id = read_uuid(&mut cursor)?;
                if owner_id != *id { break; }
            }
            batch.delete_cf(prop_cf, key_bytes);
        }
        self.db.write(batch)?;

        // Delete associated edges (both inbound and outbound)
        let edge_cf = self.cf_handle("edges")?;
        let mut batch = WriteBatch::default();
        // Iterate and delete edges where this vertex is outbound_id
        let prefix_out = create_edge_key(id, &Identifier::min(), &Uuid::min()); // Min Identifier and Uuid for range start
        let iter_out = self.db.iterator_cf(edge_cf, rocksdb::IteratorMode::From(&prefix_out, rocksdb::Direction::Forward));
        for item in iter_out {
            let (key_bytes, _) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            let mut cursor = Cursor::new(&key_bytes);
            unsafe {
                let current_out_id = read_uuid(&mut cursor)?;
                if current_out_id != *id { break; } // Moved past relevant edges
            }
            batch.delete_cf(edge_cf, key_bytes);
        }

        // Iterate and delete edges where this vertex is inbound_id (more complex without secondary index)
        // This would typically require a secondary index or a full scan.
        // For simplicity, this example only deletes edges where the vertex is the outbound_id.
        // A full solution would need an index on inbound_id or a more complex scan.
        // For now, we'll assume edges are deleted if their outbound_id is the deleted vertex.
        // To handle inbound_id deletion efficiently, a 'reversed_edges' CF would be needed,
        // mirroring the BTreeSet in InMemoryGraphStorage.

        self.db.write(batch)?;
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
        // Check if vertices exist (optional, but good for data integrity)
        if self.get_vertex(&edge.outbound_id.0).await?.is_none() || self.get_vertex(&edge.inbound_id.0).await?.is_none() {
            return Err(GraphError::InvalidData("One or both vertices for the edge do not exist.".to_string()));
        }

        let key = create_edge_key(&edge.outbound_id.0, &edge.t, &edge.inbound_id.0);
        let value = serialize_edge(&edge)?;
        self.db.put_cf(cf, key, value)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let cf = self.cf_handle("edges")?;
        let key = create_edge_key(outbound_id, edge_type, inbound_id);
        let result = self.db.get_cf(cf, key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        result.map(|bytes| deserialize_edge(&bytes)).transpose()
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await // For RocksDB, update is effectively create/overwrite
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let cf = self.cf_handle("edges")?;
        let key = create_edge_key(outbound_id, edge_type, inbound_id);
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

    async fn query(&self, query_string: &str) -> GraphResult<Value> {
        // Placeholder for actual query logic.
        println!("Executing query against RocksdbGraphStorage: {}", query_string);
        Ok(serde_json::json!({
            "status": "success",
            "query": query_string,
            "result": "RocksDB query execution placeholder"
        }))
    }
}

