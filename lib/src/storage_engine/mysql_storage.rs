// lib/src/storage_engine/mysql_storage.rs
// Updated: 2025-08-08 - Implemented GraphStorageEngine trait for MySQLStorage
// Fixed: 2025-08-08 - Added Debug derive and used Mutex for Sync compliance
// Fixed: 2025-08-08 - Used correct StorageConfig import
// Fixed: 2025-08-08 - Implemented all required GraphStorageEngine methods
// Fixed: 2025-08-08 - Used GraphError::StorageError for errors
// Fixed: 2025-08-08 - Fixed connection string access and type issues
// Fixed: 2025-08-08 - Corrected error handling and result collection in get_vertex and get_all_vertices
// Fixed: 2025-08-08 - Added missing .collect() calls
// Added: 2025-08-13 - Added close and as_any methods
// Updated: 2025-08-13 - Ensured all methods are async with async_trait
// Fixed: 2025-08-13 - Completed truncated retrieve method and removed erroneous characters
// NOTE: Assumes `store` table (key BLOB, value BLOB), `vertices` table (id VARCHAR(36), label TEXT, data JSON),
// and `edges` table (outbound_id VARCHAR(36), edge_type VARCHAR(255), inbound_id VARCHAR(36))

use async_trait::async_trait;
use crate::storage_engine::{GraphStorageEngine, StorageConfig, StorageEngine};
use models::errors::{GraphError, GraphResult};
use models::{Vertex, Edge, Identifier};
use models::identifiers::SerializableUuid;
use models::properties::SerializableFloat;
use serde_json::{Value, to_vec, from_value};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts};
use tokio::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug)]
pub struct MySQLStorage {
    client: Arc<Mutex<Conn>>,
}

impl MySQLStorage {
    pub async fn new(config: &StorageConfig) -> GraphResult<Self> {
        let connection_string = config.engine_specific_config
            .as_ref()
            .ok_or_else(|| GraphError::StorageError("Connection string is required in engine_specific_config".to_string()))?
            .as_str()
            .ok_or_else(|| GraphError::StorageError("Connection string must be a string".to_string()))?;
        let opts = Opts::from_url(connection_string)
            .map_err(|e| GraphError::StorageError(format!("Invalid MySQL connection string: {}", e)))?;
        let client = Conn::new(opts).await
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to MySQL: {}", e)))?;
        Ok(MySQLStorage {
            client: Arc::new(Mutex::new(client)),
        })
    }
}

#[async_trait]
impl StorageEngine for MySQLStorage {
    async fn connect(&self) -> GraphResult<()> {
        // Connection is established during initialization
        Ok(())
    }

    async fn insert(&self, key: &[u8], value: &[u8]) -> GraphResult<()> {
        let mut conn = self.client.lock().await;
        conn.exec_drop("INSERT INTO store (key, value) VALUES (?, ?)", (key, value))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn retrieve(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        let mut conn = self.client.lock().await;
        let rows: Vec<(Vec<u8>,)> = conn
            .exec("SELECT value FROM store WHERE key = ?", (key,))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(rows.into_iter().next().map(|row| row.0))
    }

    async fn delete(&self, key: &[u8]) -> GraphResult<()> {
        let mut conn = self.client.lock().await;
        conn.exec_drop("DELETE FROM store WHERE key = ?", (key,))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn flush(&self) -> GraphResult<()> {
        // MySQL doesn't require a flush operation in this context
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for MySQLStorage {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn start(&self) -> GraphResult<()> {
        // Connection is managed in new, so this is a no-op
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        // Connection is dropped with the struct
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "mysql"
    }

    async fn is_running(&self) -> bool {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(self.client.lock()).ping().is_ok()
    }

    async fn query(&self, query: &str) -> GraphResult<Value> {
        let mut conn = self.client.lock().await;
        let rows: Vec<mysql_async::Row> = conn
            .query(query)
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        let results: Vec<Value> = rows.into_iter()
            .map(|row| {
                row.columns().iter().enumerate().fold(serde_json::Map::new(), |mut map, (i, col)| {
                    let value: Value = row.get(i).map(|v| serde_json::to_value(v).unwrap_or(Value::Null)).unwrap_or(Value::Null);
                    map.insert(col.name_str().to_string(), value);
                    map
                })
            })
            .map(Value::Object)
            .collect();
        Ok(Value::Array(results))
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let mut conn = self.client.lock().await;
        let data = to_vec(&vertex.properties)
            .map_err(|e| GraphError::SerializationError(e.to_string()))?;
        conn.exec_drop(
            "INSERT INTO vertices (id, label, data) VALUES (?, ?, ?)",
            (vertex.id.0.to_string(), vertex.label.to_string(), data),
        ).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let mut conn = self.client.lock().await;
        let rows: Vec<(String, Vec<u8>)> = conn
            .exec("SELECT label, data FROM vertices WHERE id = ?", (id.to_string(),))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        rows.into_iter().next().map(|(label, data)| {
            let props_json: Value = from_value(serde_json::from_slice(&data)
                .map_err(|e| GraphError::DeserializationError(e.to_string()))?)
                .map_err(|e| GraphError::DeserializationError(e.to_string()))?;
            let properties = if let Value::Object(map) = props_json {
                map.into_iter().map(|(k, v)| {
                    let prop_value = from_value(v)
                        .map_err(|e| GraphError::DeserializationError(e.to_string()))?;
                    Ok((k, prop_value))
                }).collect::<GraphResult<HashMap<_, _>>>()?
            } else {
                HashMap::new()
            };
            let identifier = Identifier::new(label)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            Ok(Vertex {
                id: SerializableUuid(*id),
                label: identifier,
                properties,
            })
        }).transpose()
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let mut conn = self.client.lock().await;
        let data = to_vec(&vertex.properties)
            .map_err(|e| GraphError::SerializationError(e.to_string()))?;
        conn.exec_drop(
            "UPDATE vertices SET label = ?, data = ? WHERE id = ?",
            (vertex.label.to_string(), data, vertex.id.0.to_string()),
        ).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let mut conn = self.client.lock().await;
        conn.exec_drop("DELETE FROM vertices WHERE id = ?", (id.to_string(),))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let mut conn = self.client.lock().await;
        let rows: Vec<(String, String, Vec<u8>)> = conn
            .query("SELECT id, label, data FROM vertices")
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        rows.into_iter().map(|(id, label, data)| {
            let uuid = Uuid::parse_str(&id)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let props_json: Value = from_value(serde_json::from_slice(&data)
                .map_err(|e| GraphError::DeserializationError(e.to_string()))?)
                .map_err(|e| GraphError::DeserializationError(e.to_string()))?;
            let properties = if let Value::Object(map) = props_json {
                map.into_iter().map(|(k, v)| {
                    let prop_value = from_value(v)
                        .map_err(|e| GraphError::DeserializationError(e.to_string()))?;
                    Ok((k, prop_value))
                }).collect::<GraphResult<HashMap<_, _>>>()?
            } else {
                HashMap::new()
            };
            let identifier = Identifier::new(label)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            Ok(Vertex {
                id: SerializableUuid(uuid),
                label: identifier,
                properties,
            })
        }).collect()
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let mut conn = self.client.lock().await;
        conn.exec_drop(
            "INSERT INTO edges (outbound_id, edge_type, inbound_id) VALUES (?, ?, ?)",
            (edge.outbound_id.0.to_string(), edge.edge_type.to_string(), edge.inbound_id.0.to_string()),
        ).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let mut conn = self.client.lock().await;
        let rows: Vec<(u8,)> = conn
            .exec(
                "SELECT 1 FROM edges WHERE outbound_id = ? AND edge_type = ? AND inbound_id = ?",
                (outbound_id.to_string(), edge_type.to_string(), inbound_id.to_string()),
            ).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(rows.into_iter().next().map(|_| Edge {
            outbound_id: SerializableUuid(*outbound_id),
            edge_type: edge_type.clone(),
            inbound_id: SerializableUuid(*inbound_id),
        }))
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        let mut conn = self.client.lock().await;
        conn.exec_drop(
            "UPDATE edges SET edge_type = ? WHERE outbound_id = ? AND inbound_id = ?",
            (edge.edge_type.to_string(), edge.outbound_id.0.to_string(), edge.inbound_id.0.to_string()),
        ).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let mut conn = self.client.lock().await;
        conn.exec_drop(
            "DELETE FROM edges WHERE outbound_id = ? AND edge_type = ? AND inbound_id = ?",
            (outbound_id.to_string(), edge_type.to_string(), inbound_id.to_string()),
        ).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let mut conn = self.client.lock().await;
        let rows: Vec<(String, String, String)> = conn
            .query("SELECT outbound_id, edge_type, inbound_id FROM edges")
            .await
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
                inbound_id: SerializableUuid(inbound_uuid),
            })
        }).collect()
    }

    async fn close(&self) -> GraphResult<()> {
        // MySQL connection is dropped when the struct is dropped
        Ok(())
    }
}
