// logging_service/src/log.rs
// Fixed: 2025-08-07 - Corrected PropertyValue conversion, fixed error handling, added UUID display conversion.
use async_trait::async_trait;
use chrono::Utc;
use lib::errors::GraphError as LibGraphError;
use lib::storage_engine::GraphStorageEngine;
use caching::Cache;
use models::Vertex;
use models::identifiers::{SerializableUuid, Identifier, SerializableInternString};
use models::properties::{PropertyValue, SerializableFloat};
use models::errors::GraphError as ModelsGraphError;
use serde_json::Value;
use anyhow::Result;
use slog::{Logger, o, info, Drain};
use std::sync::Arc;
use std::collections::HashMap;
use uuid::Uuid;

#[async_trait]
pub trait LogServiceTrait: Send + Sync {
    async fn log_query(&self, query: &str, user_id: &str, query_type: &str) -> Result<(), LibGraphError>;
}

pub struct LogService {
    logger: Logger,
    storage: Arc<dyn GraphStorageEngine>,
    cache: Cache,
}

// Helper function to convert serde_json::Value to PropertyValue
fn json_value_to_property_value(value: &Value) -> PropertyValue {
    match value {
        Value::String(s) => PropertyValue::String(s.clone()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropertyValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                PropertyValue::Float(SerializableFloat(f))
            } else {
                PropertyValue::String(n.to_string())
            }
        },
        Value::Bool(b) => PropertyValue::Boolean(*b),
        Value::Null => PropertyValue::String("null".to_string()),
        _ => PropertyValue::String(value.to_string()),
    }
}

#[async_trait]
impl LogServiceTrait for LogService {
    async fn log_query(&self, query: &str, user_id: &str, query_type: &str) -> Result<(), LibGraphError> {
        info!(self.logger, "Query executed"; "query" => query, "user_id" => user_id, "query_type" => query_type);
        
        let log_entry = serde_json::json!({
            "query": query,
            "user_id": user_id,
            "query_type": query_type,
            "timestamp": Utc::now().to_rfc3339(),
        });

        // Convert serde_json::Value to HashMap<String, PropertyValue>
        let properties: HashMap<String, PropertyValue> = log_entry.as_object()
            .ok_or_else(|| LibGraphError::SerializationError("Invalid log entry".to_string()))?
            .into_iter()
            .map(|(k, v)| (k.clone(), json_value_to_property_value(v)))
            .collect();

        let vertex = Vertex {
            id: SerializableUuid(Uuid::new_v4()),
            label: Identifier(SerializableInternString("QueryLog".to_string().into())),
            properties,
        };

        // Convert ModelsGraphError to LibGraphError using generic error conversion
        self.storage.create_vertex(vertex.clone()).await
            .map_err(|e| LibGraphError::from(anyhow::anyhow!("Failed to create vertex: {:?}", e)))?;

        // Convert SerializableUuid to string using the inner UUID
        self.cache.insert(vertex.id.0.to_string(), log_entry).await
            .map_err(|e| LibGraphError::from(e))?;
        
        Ok(())
    }
}

impl LogService {
    pub fn new(storage: Arc<dyn GraphStorageEngine>, cache: Cache) -> Result<Self, LibGraphError> {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let logger = Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")));
        
        Ok(LogService { logger, storage, cache })
    }
}