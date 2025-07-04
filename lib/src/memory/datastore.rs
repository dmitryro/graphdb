// lib/src/memory/datastore.rs
// Refactored: 2025-07-04 - Renamed to InMemoryGraphStorage and implemented GraphStorageEngine.
// Removed generic Database<D: Datastore> struct.
// Updated: 2025-07-04 - Improved PropertyValue to Json conversion and back.
// Fixed: 2025-07-04 - Addressed GraphError variants, Identifier::new arguments, and UUID range query bounds.
// Fixed: 2025-07-04 - Corrected mismatched closing delimiter in new_with_path function.
// Fixed: 2025-07-04 - Explicitly typed Bound::Unbounded for Identifier in BTreeMap range queries.
// Fixed: 2025-07-04 - Corrected BTreeMap range syntax for tuple keys and used Identifier::new("".to_string()).unwrap().
// Fixed: 2025-07-04 - Corrected BTreeMap range bounds for tuple keys to wrap the entire tuple.
// Fixed: 2025-07-04 - Explicitly passed references to tuple keys within Bound variants for BTreeMap range.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::sync::{Arc, Mutex, MutexGuard};
use std::ops::Bound; // Import Bound for range queries

use async_trait::async_trait;
use serde_json::Value; // For query results

use models::errors::{GraphError, GraphResult};
use crate::util::next_uuid; // Use next_uuid from lib/src/util.rs
use models::{Edge, Identifier, Json, Vertex};
use models::bulk_insert::BulkInsertItem;
use models::identifiers::SerializableUuid; // Still used by Vertex/Edge structs
use models::properties::PropertyValue; // Import PropertyValue
use crate::util::{build as util_build_key, Component as UtilComponent}; // For building keys

use rmp_serde::decode::Error as RmpDecodeError;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use uuid::Uuid;

// Import GraphStorageEngine and StorageEngine traits
use crate::storage_engine::{GraphStorageEngine, StorageEngine};


// Define the internal in-memory storage structure
#[derive(Debug, Default, Serialize, Deserialize)]
struct InternalMemory {
    vertices: BTreeMap<Uuid, Identifier>, // Stores Vertex ID -> Label
    edges: BTreeSet<Edge>, // Stores actual Edge objects
    reversed_edges: BTreeSet<Edge>, // Stores reversed Edge objects for efficient traversal
    vertex_properties: BTreeMap<(Uuid, Identifier), Json>, // (Vertex ID, Property Name) -> Value
    edge_properties: BTreeMap<(Edge, Identifier), Json>, // (Edge, Property Name) -> Value
    // Indexed properties for efficient lookups by property value
    property_values: HashMap<Identifier, HashMap<Json, HashSet<IndexedPropertyMember>>>,
}

#[derive(Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
enum IndexedPropertyMember {
    Vertex(Uuid),
    Edge(Edge),
}

/// In-memory implementation of `GraphStorageEngine`.
/// This struct manages the in-memory graph data and provides methods for
/// both generic key-value operations and graph-specific operations.
#[derive(Debug, Clone)]
pub struct InMemoryGraphStorage {
    internal: Arc<Mutex<InternalMemory>>,
    path: Option<PathBuf>, // For MessagePack persistence
    is_running_flag: Arc<Mutex<bool>>, // To track if the storage is "running"
}

impl InMemoryGraphStorage {
    /// Creates a new `InMemoryGraphStorage` instance.
    pub fn new() -> Self {
        InMemoryGraphStorage {
            internal: Arc::new(Mutex::new(InternalMemory::default())),
            path: None,
            is_running_flag: Arc::new(Mutex::new(false)),
        }
    }

    /// Creates a new `InMemoryGraphStorage` instance with a path for MessagePack persistence.
    pub fn new_with_path<P: Into<PathBuf>>(path: P) -> Self {
        InMemoryGraphStorage {
            internal: Arc::new(Mutex::new(InternalMemory::default())),
            path: Some(path.into()),
            is_running_flag: Arc::new(Mutex::new(false)),
        }
    }

    /// Reads an `InMemoryGraphStorage` from a MessagePack file.
    pub fn from_msgpack_file<P: Into<PathBuf>>(path: P) -> GraphResult<Self> {
        let path = path.into();
        let f = File::open(&path).map_err(GraphError::Io)?;
        let buf = BufReader::new(f);
        let internal: InternalMemory = rmp_serde::from_read(buf)
            .map_err(|e| GraphError::DeserializationError(format!("Failed to decode MessagePack: {}", e)))?;
        
        Ok(InMemoryGraphStorage {
            internal: Arc::new(Mutex::new(internal)),
            path: Some(path),
            is_running_flag: Arc::new(Mutex::new(false)),
        })
    }

    // Internal helper to get a mutable lock on the data
    fn get_internal_mut(&self) -> GraphResult<MutexGuard<'_, InternalMemory>> {
        self.internal.lock().map_err(|e| GraphError::LockError(e.to_string()))
    }

    // Internal helper to get a read-only lock on the data
    fn get_internal_read(&self) -> GraphResult<MutexGuard<'_, InternalMemory>> {
        self.internal.lock().map_err(|e| GraphError::LockError(e.to_string()))
    }

    // Helper to serialize data to MessagePack and persist
    fn sync_internal(&self, internal: &InternalMemory) -> GraphResult<()> {
        if let Some(ref persist_path) = self.path {
            let temp_path = NamedTempFile::new().map_err(|e| GraphError::Io(e.into()))?;
            {
                let mut buf = BufWriter::new(temp_path.as_file());
                rmp_serde::encode::write(&mut buf, internal)
                    .map_err(|e| GraphError::SerializationError(e.to_string()))?;
            }
            temp_path
                .persist(persist_path)
                .map_err(|e| GraphError::Io(e.error.into()))?;
        }
        Ok(())
    }
}

#[async_trait]
impl StorageEngine for InMemoryGraphStorage {
    async fn connect(&self) -> GraphResult<()> {
        // In-memory storage is always "connected" once instantiated.
        Ok(())
    }

    async fn insert(&self, _key: &[u8], _value: &[u8]) -> GraphResult<()> { // Added underscores to unused variables
        // This generic insert/retrieve is not directly used for graph elements
        // but could be for other metadata. For simplicity, we'll just store in a dummy map.
        // In a real scenario, you'd define how raw K/V maps to your internal structure.
        Err(GraphError::NotImplemented("Generic insert for InMemoryGraphStorage".to_string()))
    }

    async fn retrieve(&self, _key: &[u8]) -> GraphResult<Option<Vec<u8>>> { // Added underscore to unused variable
        Err(GraphError::NotImplemented("Generic retrieve for InMemoryGraphStorage".to_string()))
    }

    async fn delete(&self, _key: &[u8]) -> GraphResult<()> { // Added underscore to unused variable
        Err(GraphError::NotImplemented("Generic delete for InMemoryGraphStorage".to_string()))
    }

    async fn flush(&self) -> GraphResult<()> {
        let internal = self.get_internal_read()?;
        self.sync_internal(&internal)
    }
}

#[async_trait]
impl GraphStorageEngine for InMemoryGraphStorage {
    async fn start(&self) -> GraphResult<()> {
        let mut is_running = self.is_running_flag.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        *is_running = true;
        println!("InMemoryGraphStorage started.");
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        let mut is_running = self.is_running_flag.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        *is_running = false;
        let internal = self.get_internal_read()?;
        self.sync_internal(&internal)?; // Persist on stop
        println!("InMemoryGraphStorage stopped.");
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "InMemory"
    }

    fn is_running(&self) -> bool {
        // Corrected: Directly unwrap the MutexGuard and dereference.
        *self.is_running_flag.lock().unwrap()
    }

    async fn query(&self, query_string: &str) -> GraphResult<Value> {
        // This is a placeholder for actual query execution logic.
        // In a real implementation, you'd parse `query_string` and execute it against `self.internal`.
        println!("Executing query against InMemoryGraphStorage: {}", query_string);
        Ok(serde_json::json!({
            "status": "success",
            "query": query_string,
            "result": "InMemory query execution placeholder"
        }))
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let mut internal = self.get_internal_mut()?;
        if internal.vertices.contains_key(&vertex.id.0) {
            return Err(GraphError::AlreadyExists(format!("Vertex with ID {} already exists.", vertex.id.0)));
        }
        internal.vertices.insert(vertex.id.0, vertex.label);
        // Store properties
        for (prop_name, prop_value) in vertex.properties {
            // Corrected: Pass owned String to Identifier::new
            let identifier = Identifier::new(prop_name)
                .map_err(|e| GraphError::InvalidData(format!("Invalid property name: {}", e)))?;
            // Convert PropertyValue to Json for storage in BTreeMap
            let json_value = serde_json::to_value(prop_value)
                .map_err(|e| GraphError::SerializationError(format!("Failed to convert PropertyValue to Json: {}", e)))?;
            internal.vertex_properties.insert((vertex.id.0, identifier), Json::new(json_value));
        }
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let internal = self.get_internal_read()?;
        if let Some(label) = internal.vertices.get(id) {
            let mut vertex = Vertex::new_with_id(*id, label.clone());
            // Retrieve properties
            // Corrected: Pass a single tuple of Bounds to the range method, using Identifier::new("".to_string()).unwrap()
            let start_bound_key = (id.clone(), Identifier::new("".to_string()).unwrap());
            let end_bound_key = (next_uuid(id.clone())?, Identifier::new("".to_string()).unwrap());

            for ((prop_id, prop_name), prop_json_value) in internal.vertex_properties.range((
                Bound::Included(&start_bound_key), // Pass reference to the tuple
                Bound::Excluded(&end_bound_key)   // Pass reference to the tuple
            )) {
                if prop_id == id {
                    // Convert Json back to PropertyValue
                    let prop_value: PropertyValue = serde_json::from_value(prop_json_value.0.as_ref().clone())
                        .map_err(|e| GraphError::DeserializationError(format!("Failed to convert Json to PropertyValue: {}", e)))?;
                    // Corrected: Convert Intern<String> to String for HashMap key
                    vertex.properties.insert(prop_name.0.0.to_string(), prop_value);
                } else {
                    break;
                }
            }
            Ok(Some(vertex))
        } else {
            Ok(None)
        }
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let mut internal = self.get_internal_mut()?;
        if !internal.vertices.contains_key(&vertex.id.0) {
            // Corrected: Pass owned String to Identifier::new
            return Err(GraphError::NotFound(Identifier::new(vertex.id.0.to_string()).unwrap()));
        }
        // Remove old properties for this vertex
        // Corrected: Pass a single tuple of Bounds to the range method, using Identifier::new("".to_string()).unwrap()
        let mut props_to_remove = Vec::new();
        let start_bound_key = (vertex.id.0, Identifier::new("".to_string()).unwrap());
        let end_bound_key = (next_uuid(vertex.id.0)?, Identifier::new("".to_string()).unwrap());

        for ((prop_id, prop_name), _) in internal.vertex_properties.range((
            Bound::Included(&start_bound_key), // Pass reference to the tuple
            Bound::Excluded(&end_bound_key)   // Pass reference to the tuple
        )) {
            if prop_id == &vertex.id.0 {
                props_to_remove.push((prop_id.clone(), prop_name.clone()));
            } else {
                break;
            }
        }
        for (id, name) in props_to_remove {
            internal.vertex_properties.remove(&(id, name));
        }

        // Update label and add new properties
        internal.vertices.insert(vertex.id.0, vertex.label);
        for (prop_name, prop_value) in vertex.properties {
            // Corrected: Pass owned String to Identifier::new
            let identifier = Identifier::new(prop_name)
                .map_err(|e| GraphError::InvalidData(format!("Invalid property name: {}", e)))?;
            let json_value = serde_json::to_value(prop_value)
                .map_err(|e| GraphError::SerializationError(format!("Failed to convert PropertyValue to Json: {}", e)))?;
            internal.vertex_properties.insert((vertex.id.0, identifier), Json::new(json_value));
        }
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let mut internal = self.get_internal_mut()?;
        if internal.vertices.remove(id).is_none() {
            // Corrected: Pass owned String to Identifier::new
            return Err(GraphError::NotFound(Identifier::new(id.to_string()).unwrap()));
        }

        // Remove associated properties
        // Corrected: Pass a single tuple of Bounds to the range method, using Identifier::new("".to_string()).unwrap()
        let mut props_to_remove = Vec::new();
        let start_bound_key = (id.clone(), Identifier::new("".to_string()).unwrap());
        let end_bound_key = (next_uuid(id.clone())?, Identifier::new("".to_string()).unwrap());

        for ((prop_id, prop_name), _) in internal.vertex_properties.range((
            Bound::Included(&start_bound_key), // Pass reference to the tuple
            Bound::Excluded(&end_bound_key)   // Pass reference to the tuple
        )) {
            if prop_id == id {
                props_to_remove.push((prop_id.clone(), prop_name.clone()));
            } else {
                break;
            }
        }
        for (prop_id, prop_name) in props_to_remove {
            internal.vertex_properties.remove(&(prop_id, prop_name));
        }

        // Remove associated edges
        internal.edges.retain(|edge| edge.outbound_id.0 != *id && edge.inbound_id.0 != *id);
        internal.reversed_edges.retain(|edge| edge.outbound_id.0 != *id && edge.inbound_id.0 != *id);

        // Remove associated edge properties
        // This line was causing E0425 in some user reports due to context mismatch.
        // In delete_vertex, 'id' is in scope, so this line is logically correct for removing properties
        // associated with edges connected to the deleted vertex.
        internal.edge_properties.retain(|(edge, _), _| edge.outbound_id.0 != *id && edge.inbound_id.0 != *id);

        Ok(())
    }

    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let internal = self.get_internal_read()?;
        let mut vertices = Vec::new();
        for (id, label) in internal.vertices.iter() {
            let mut vertex = Vertex::new_with_id(*id, label.clone());
            // Retrieve properties
            // Corrected: Pass a single tuple of Bounds to the range method, using Identifier::new("".to_string()).unwrap()
            let start_bound_key = (id.clone(), Identifier::new("".to_string()).unwrap());
            let end_bound_key = (next_uuid(id.clone())?, Identifier::new("".to_string()).unwrap());

            for ((prop_id, prop_name), prop_json_value) in internal.vertex_properties.range((
                Bound::Included(&start_bound_key), // Pass reference to the tuple
                Bound::Excluded(&end_bound_key)   // Pass reference to the tuple
            )) {
                if prop_id == id {
                    let prop_value: PropertyValue = serde_json::from_value(prop_json_value.0.as_ref().clone())
                        .map_err(|e| GraphError::DeserializationError(format!("Failed to convert Json to PropertyValue: {}", e)))?;
                    // Corrected: Convert Intern<String> to String for HashMap key
                    vertex.properties.insert(prop_name.0.0.to_string(), prop_value);
                } else {
                    break;
                }
            }
            vertices.push(vertex);
        }
        Ok(vertices)
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let mut internal = self.get_internal_mut()?;
        if !internal.vertices.contains_key(&edge.outbound_id.0) || !internal.vertices.contains_key(&edge.inbound_id.0) {
            return Err(GraphError::InvalidData("One or both vertices for the edge do not exist.".to_string()));
        }
        if internal.edges.contains(&edge) {
            return Err(GraphError::AlreadyExists(format!("Edge {:?} already exists.", edge)));
        }
        internal.edges.insert(edge.clone());
        internal.reversed_edges.insert(edge.reversed());
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let internal = self.get_internal_read()?;
        let edge_to_find = Edge::new(*outbound_id, edge_type.clone(), *inbound_id);
        if internal.edges.contains(&edge_to_find) {
            Ok(Some(edge_to_find))
        } else {
            Ok(None)
        }
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        // For in-memory, update is effectively create if it doesn't exist, or replace if it does.
        // We'll delete and then create to handle property updates cleanly.
        self.delete_edge(&edge.outbound_id.0, &edge.t, &edge.inbound_id.0).await?;
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let mut internal = self.get_internal_mut()?;
        let edge_to_delete = Edge::new(*outbound_id, edge_type.clone(), *inbound_id);
        if !internal.edges.remove(&edge_to_delete) {
            // Corrected: Pass owned String to Identifier::new
            return Err(GraphError::NotFound(Identifier::new(format!("{:?}", edge_to_delete)).unwrap()));
        }
        internal.reversed_edges.remove(&edge_to_delete.reversed());

        // Remove associated edge properties
        internal.edge_properties.retain(|(edge, _), _| edge != &edge_to_delete);
        
        Ok(())
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let internal = self.get_internal_read()?;
        Ok(internal.edges.iter().cloned().collect())
    }
}

