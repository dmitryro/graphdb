// lib/src/storage_engine/mysql_storage.rs
// Updated: 2025-08-08 - Implemented GraphStorageEngine trait for MySQLStorage
// Fixed: 2025-08-08 - Added Debug derive and used Mutex for Sync compliance
// Fixed: 2025-08-08 - Used correct StorageConfig import
// Fixed: 2025-08-08 - Implemented all required GraphStorageEngine methods
// Fixed: 2025-08-08 - Used GraphError::StorageError for errors
// Fixed: 2025-08-08 - Fixed connection string access and type issues
// NOTE: Assumes `store` table (key BLOB, value BLOB), `vertices` table (id VARCHAR(36), label TEXT, data JSON),
// and `edges` table (outbound_id VARCHAR(36), edge_type VARCHAR(255), inbound_id VARCHAR(36))

use crate::storage_engine::{GraphStorageEngine, StorageEngine, StorageConfig, StorageEngineType};
use async_trait::async_trait;
use mysql::prelude::*;
use mysql::{Conn, Opts};
use std::sync::Mutex;
use std::collections::HashMap;
use models::errors::GraphError;
use models::{Vertex, Edge, Identifier, PropertyValue};
use models::identifiers::SerializableUuid;
use models::properties::SerializableFloat;
use uuid::Uuid;
use serde_json::Value;

#[derive(Debug)]
pub struct MySQLStorage {
    client: Mutex<Conn>,
}

impl MySQLStorage {
    pub fn new(config: &StorageConfig) -> mysql::Result<Self> {
        // Get connection string from engine_specific_config
        let connection_string = config.engine_specific_config
            .as_ref()
            .ok_or_else(|| mysql::Error::UrlError(mysql::UrlError::InvalidValue(
                "connection_string".to_string(),
                "Connection string is required in engine_specific_config".to_string()
            )))?;
            
        let opts = Opts::from_url(connection_string)?;
        let client = Conn::new(opts)?;
        Ok(MySQLStorage {
            client: Mutex::new(client),
        })
    }
}

#[async_trait]
impl StorageEngine for MySQLStorage {
    async fn connect(&self) -> Result<(), GraphError> {
        Ok(())
    }

    async fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), GraphError> {
        let mut conn = self.client.lock()
            .map_err(|e| GraphError::StorageError(format!("Mutex lock failed: {}", e)))?;
        conn.exec_drop("INSERT INTO store (key, value) VALUES (?, ?)", (key, value))
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn retrieve(&self, key: &[u8]) -> Result<Option<Vec<u8>>, GraphError> {
        let mut conn = self.client.lock()
            .map_err(|e| GraphError::StorageError(format!("Mutex lock failed: {}", e)))?;
        let rows: Vec<(Vec<u8>,)> = conn
            .exec("SELECT value FROM store WHERE key = ?", (key,))
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(rows.into_iter().next().map(|row| row.0))
    }

    async fn delete(&self, key: &[u8]) -> Result<(), GraphError> {
        let mut conn = self.client.lock()
            .map_err(|e| GraphError::StorageError(format!("Mutex lock failed: {}", e)))?;
        conn.exec_drop("DELETE FROM store WHERE key = ?", (key,))
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn flush(&self) -> Result<(), GraphError> {
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for MySQLStorage {
    async fn start(&self) -> Result<(), GraphError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "mysql"
    }

    fn is_running(&self) -> bool {
        self.client.lock().map(|mut conn| conn.ping().is_ok()).unwrap_or(false)
    }

    async fn query(&self, query: &str) -> Result<Value, GraphError> {
        let mut conn = self.client.lock()
            .map_err(|e| GraphError::StorageError(format!("Mutex lock failed: {}", e)))?;
        let rows: Vec<String> = conn
            .query(query)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        serde_json::to_value(rows)
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        let mut conn = self.client.lock()
            .map_err(|e| GraphError::StorageError(format!("Mutex lock failed: {}", e)))?;
        let data = serde_json::to_string(&vertex.properties)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        conn.exec_drop(
            "INSERT INTO vertices (id, label, data) VALUES (?, ?, ?)",
            (vertex.id.0.to_string(), vertex.label.to_string(), data)
        ).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        let mut conn = self.client.lock()
            .map_err(|e| GraphError::StorageError(format!("Mutex lock failed: {}", e)))?;
        let rows: Vec<(String, String)> = conn
            .exec("SELECT label, data FROM vertices WHERE id = ?", (id.to_string(),))
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        rows.into_iter().next().map(|(label, data)| {
            let props_json: Value = serde_json::from_str(&data)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            
            // Convert JSON Value to HashMap<String, PropertyValue>
            let properties = if let Value::Object(map) = props_json {
                map.into_iter().map(|(k, v)| {
                    let prop_value = match v {
                        Value::String(s) => PropertyValue::String(s),
                        Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                PropertyValue::Integer(i)
                            } else if let Some(f) = n.as_f64() {
                                PropertyValue::Float(SerializableFloat(f))
                            } else {
                                PropertyValue::String(n.to_string())
                            }
                        },
                        Value::Bool(b) => PropertyValue::Boolean(b),
                        _ => PropertyValue::String(v.to_string()),
                    };
                    (k, prop_value)
                }).collect()
            } else {
                HashMap::new()
            };
            
            let identifier = Identifier::new(label)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            
            Ok(Vertex { 
                id: SerializableUuid(*id), 
                label: identifier, 
                properties 
            })
        }).transpose()
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        let mut conn = self.client.lock()
            .map_err(|e| GraphError::StorageError(format!("Mutex lock failed: {}", e)))?;
        let data = serde_json::to_string(&vertex.properties)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        conn.exec_drop(
            "UPDATE vertices SET label = ?, data = ? WHERE id = ?",
            (vertex.label.to_string(), data, vertex.id.0.to_string())
        ).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        let mut conn = self.client.lock()
            .map_err(|e| GraphError::StorageError(format!("Mutex lock failed: {}", e)))?;
        conn.exec_drop("DELETE FROM vertices WHERE id = ?", (id.to_string(),))
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let mut conn = self.client.lock()
            .map_err(|e| GraphError::StorageError(format!("Mutex lock failed: {}", e)))?;
        let rows: Vec<(String, String, String)> = conn
            .query("SELECT id, label, data FROM vertices")
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        rows.into_iter().map(|(id, label, data)| {
            let uuid = Uuid::parse_str(&id)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let props_json: Value = serde_json::from_str(&data)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
                
            // Convert JSON Value to HashMap<String, PropertyValue>
            let properties = if let Value::Object(map) = props_json {
                map.into_iter().map(|(k, v)| {
                    let prop_value = match v {
                        Value::String(s) => PropertyValue::String(s),
                        Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                PropertyValue::Integer(i)
                            } else if let Some(f) = n.as_f64() {
                                PropertyValue::Float(SerializableFloat(f))
                            } else {
                                PropertyValue::String(n.to_string())
                            }
                        },
                        Value::Bool(b) => PropertyValue::Boolean(b),
                        _ => PropertyValue::String(v.to_string()),
                    };
                    (k, prop_value)
                }).collect()
            } else {
                HashMap::new()
            };
            
            let identifier = Identifier::new(label)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
                
            Ok(Vertex { 
                id: SerializableUuid(uuid), 
                label: identifier, 
                properties 
            })
        }).collect()
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        let mut conn = self.client.lock()
            .map_err(|e| GraphError::StorageError(format!("Mutex lock failed: {}", e)))?;
        conn.exec_drop(
            "INSERT INTO edges (outbound_id, edge_type, inbound_id) VALUES (?, ?, ?)",
            (edge.outbound_id.0.to_string(), edge.t.to_string(), edge.inbound_id.0.to_string())
        ).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        let mut conn = self.client.lock()
            .map_err(|e| GraphError::StorageError(format!("Mutex lock failed: {}", e)))?;
        let rows: Vec<(u8,)> = conn
            .exec(
                "SELECT 1 FROM edges WHERE outbound_id = ? AND edge_type = ? AND inbound_id = ?",
                (outbound_id.to_string(), edge_type.to_string(), inbound_id.to_string())
            ).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(rows.into_iter().next().map(|_| Edge {
            outbound_id: SerializableUuid(*outbound_id),
            t: edge_type.clone(),
            inbound_id: SerializableUuid(*inbound_id),
        }))
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        let mut conn = self.client.lock()
            .map_err(|e| GraphError::StorageError(format!("Mutex lock failed: {}", e)))?;
        // Note: This implementation assumes we're updating the edge_type, but edges are identified by all three components
        // This may need to be reconsidered based on the actual use case
        conn.exec_drop(
            "UPDATE edges SET edge_type = ? WHERE outbound_id = ? AND inbound_id = ?",
            (edge.t.to_string(), edge.outbound_id.0.to_string(), edge.inbound_id.0.to_string())
        ).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        let mut conn = self.client.lock()
            .map_err(|e| GraphError::StorageError(format!("Mutex lock failed: {}", e)))?;
        conn.exec_drop(
            "DELETE FROM edges WHERE outbound_id = ? AND edge_type = ? AND inbound_id = ?",
            (outbound_id.to_string(), edge_type.to_string(), inbound_id.to_string())
        ).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let mut conn = self.client.lock()
            .map_err(|e| GraphError::StorageError(format!("Mutex lock failed: {}", e)))?;
        let rows: Vec<(String, String, String)> = conn
            .query("SELECT outbound_id, edge_type, inbound_id FROM edges")
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        rows.into_iter().map(|(outbound_id, edge_type, inbound_id)| {
            let outbound_uuid = Uuid::parse_str(&outbound_id)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let t = Identifier::new(edge_type)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let inbound_uuid = Uuid::parse_str(&inbound_id)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            Ok(Edge { 
                outbound_id: SerializableUuid(outbound_uuid), 
                t, 
                inbound_id: SerializableUuid(inbound_uuid) 
            })
        }).collect()
    }
}