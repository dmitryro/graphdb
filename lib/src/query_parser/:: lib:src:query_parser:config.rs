// lib/src/query_parser/config.rs
// Created: 2025-08-09 - Key-value store for Cypher query execution

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use models::errors::GraphError;

#[derive(Clone)]
pub struct KeyValueStore {
    pub kvs: Arc<Mutex<BTreeMap<String, String>>>,
}

impl KeyValueStore {
    pub fn new() -> Self {
        KeyValueStore {
            kvs: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn set(&self, key: String, value: String) -> Result<(), GraphError> {
        let mut kvs = self.kvs.lock().map_err(|e| GraphError::StorageError(e.to_string()))?;
        kvs.insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, GraphError> {
        let kvs = self.kvs.lock().map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(kvs.get(key).cloned())
    }

    pub fn delete(&self, key: &str) -> Result<bool, GraphError> {
        let mut kvs = self.kvs.lock().map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(kvs.remove(key).is_some())
    }
}