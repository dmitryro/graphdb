// lib/src/storage_engine/postgres_storage.rs
// Updated: 2025-08-08 - Implemented GraphStorageEngine trait for PostgresStorage
// Updated: 2025-08-13 - Made methods async, used tokio_postgres and tokio::sync::Mutex
// Added: 2025-08-13 - Aligned with GraphStorageEngine methods (create_vertex, etc.)
// Added: 2025-08-13 - Added close and as_any methods
// Fixed: 2025-08-13 - Used GraphError and &[u8] for key-value operations
// NOTE: Assumes a `vertices` table with columns `id` (UUID), `label` (TEXT), `data` (JSONB)
// NOTE: Assumes an `edges` table with columns `outbound_id` (UUID), `edge_type` (TEXT), `inbound_id` (UUID)
// NOTE: Assumes a `store` table with columns `key` (BYTEA), `value` (BYTEA) for key-value operations

use async_trait::async_trait;
use crate::storage_engine::{GraphStorageEngine, StorageConfig, StorageEngine};
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use models::identifiers::SerializableUuid;
use serde_json::{Value, to_vec, from_value};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;
use std::sync::Arc;
use std::collections::HashMap;

#[derive(Debug)]
pub struct PostgresStorage {
    client: Arc<Mutex<Client>>,
}

impl PostgresStorage {
    pub async fn new(config: &StorageConfig) -> GraphResult<Self> {
        let client = Client::connect(
            config.connection_string.as_ref()
                .ok_or_else(|| GraphError::StorageError("Postgres connection string is required".to_string()))?,
            NoTls,
        ).await
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to Postgres: {}", e)))?;
        Ok(PostgresStorage {
            client: Arc::new(Mutex::new(client)),
        })
    }
}

#[async_trait]
impl StorageEngine for PostgresStorage {
    async fn connect(&self) -> GraphResult<()> {
        // Connection is established during initialization
        Ok(())
    }

    async fn insert(&self, key: &[u8], value: &[u8]) -> GraphResult<()> {
        let client = self.client.lock().await;
        client
            .execute("INSERT INTO store (key, value) VALUES ($1, $2)", &[&key, &value])
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn retrieve(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        let client = self.client.lock().await;
        let rows = client
            .query("SELECT value FROM store WHERE key = $1", &[&key])
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(rows.into_iter().next().map(|row| row.get(0)))
    }

    async fn delete(&self, key: &[u8]) -> GraphResult<()> {
        let client = self.client.lock().await;
        client
            .execute("DELETE FROM store WHERE key = $1", &[&key])
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn flush(&self) -> GraphResult<()> {
        // Postgres doesn't require a flush operation
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for PostgresStorage {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn start(&self) -> GraphResult<()> {
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "postgresql"
    }

    async fn is_running(&self) -> bool {
        true // Postgres connection is always considered running
    }

    async fn query(&self, query_string: &str) -> GraphResult<Value> {
        let client = self.client.lock().await;
        let rows = client
            .query(query_string, &[])
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        let results: Vec<Value> = rows.into_iter()
            .map(|row| row.columns().iter().enumerate().fold(serde_json::Map::new(), |mut map, (i, col)| {
                let value: Value = match row.get(i) {
                    Some(v) => serde_json::to_value(v).unwrap_or(Value::Null),
                    None => Value::Null,
                };
                map.insert(col.name().to_string(), value);
                map
            }))
            .map(Value::Object)
            .collect();
        Ok(Value::Array(results))
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let client = self.client.lock().await;
        let data = to_vec(&vertex.properties)
            .map_err(|e| GraphError::SerializationError(e.to_string()))?;
        client
            .execute(
                "INSERT INTO vertices (id, label, data) VALUES ($1, $2, $3)",
                &[&vertex.id.0, &vertex.label.to_string(), &data],
            )
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let client = self.client.lock().await;
        let rows = client
            .query("SELECT label, data FROM vertices WHERE id = $1", &[&id])
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        rows.into_iter().next().map(|row| {
            let label: String = row.get(0);
            let data: Vec<u8> = row.get(1);
            let props_json: Value = from_slice(&data)
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
        let client = self.client.lock().await;
        let data = to_vec(&vertex.properties)
            .map_err(|e| GraphError::SerializationError(e.to_string()))?;
        client
            .execute(
                "UPDATE vertices SET label = $1, data = $2 WHERE id = $3",
                &[&vertex.label.to_string(), &data, &vertex.id.0],
            )
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let client = self.client.lock().await;
        client
            .execute("DELETE FROM vertices WHERE id = $1", &[&id])
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let client = self.client.lock().await;
        let rows = client
            .query("SELECT id, label, data FROM vertices", &[])
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        rows.into_iter().map(|row| {
            let id: Uuid = row.get(0);
            let label: String = row.get(1);
            let data: Vec<u8> = row.get(2);
            let props_json: Value = from_slice(&data)
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
                id: SerializableUuid(id),
                label: identifier,
                properties,
            })
        }).collect()
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let client = self.client.lock().await;
        client
            .execute(
                "INSERT INTO edges (outbound_id, edge_type, inbound_id) VALUES ($1, $2, $3)",
                &[&edge.outbound_id.0, &edge.edge_type.to_string(), &edge.inbound_id.0],
            )
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT 1 FROM edges WHERE outbound_id = $1 AND edge_type = $2 AND inbound_id = $3",
                &[&outbound_id, &edge_type.to_string(), &inbound_id],
            )
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(rows.into_iter().next().map(|_| Edge {
            outbound_id: SerializableUuid(*outbound_id),
            edge_type: edge_type.clone(),
            inbound_id: SerializableUuid(*inbound_id),
        }))
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        let client = self.client.lock().await;
        client
            .execute(
                "UPDATE edges SET edge_type = $1 WHERE outbound_id = $2 AND inbound_id = $3",
                &[&edge.edge_type.to_string(), &edge.outbound_id.0, &edge.inbound_id.0],
            )
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let client = self.client.lock().await;
        client
            .execute(
                "DELETE FROM edges WHERE outbound_id = $1 AND edge_type = $2 AND inbound_id = $3",
                &[&outbound_id, &edge_type.to_string(), &inbound_id],
            )
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let client = self.client.lock().await;
        let rows = client
            .query("SELECT outbound_id, edge_type, inbound_id FROM edges", &[])
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        rows.into_iter().map(|row| {
            let outbound_id: Uuid = row.get(0);
            let edge_type: String = row.get(1);
            let inbound_id: Uuid = row.get(2);
            let t = Identifier::new(edge_type)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            Ok(Edge {
                outbound_id: SerializableUuid(outbound_id),
                t,
                inbound_id: SerializableUuid(inbound_id),
            })
        }).collect()
    }

    async fn close(&self) -> GraphResult<()> {
        // Postgres connection is dropped when the struct is dropped
        Ok(())
    }
}
