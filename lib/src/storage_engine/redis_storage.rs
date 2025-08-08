// lib/src/storage_engine/redis_storage.rs
// Fixed: 2025-08-08 - Updated redis imports to match version 0.32.4
// Added: 2025-08-08 - Implemented GraphStorageEngine trait for RedisStorage
// NOTE: Uses Redis SET/GET/DEL for key-value operations and separate namespace (e.g., "node:") for graph operations

use crate::storage_engine::storage_engine::{GraphStorageEngine, StorageEngine, StorageConfig};
use redis::{Commands, Connection};
use std::cell::RefCell;
use std::rc::Rc;

pub struct RedisStorage {
    connection: Rc<RefCell<Connection>>,
}

impl RedisStorage {
    pub fn new(connection: Connection) -> redis::Result<Self> {
        Ok(RedisStorage {
            connection: Rc::new(RefCell::new(connection)),
        })
    }

    pub fn from_config(config: &StorageConfig) -> redis::Result<Self> {
        let client = redis::Client::open(config.connection_string.as_ref()
            .ok_or_else(|| redis::RedisError::from((redis::ErrorKind::ConfigError, "Connection string is required")))?)?;
        let connection = client.get_connection()?;
        Ok(RedisStorage {
            connection: Rc::new(RefCell::new(connection)),
        })
    }
}

impl StorageEngine for RedisStorage {
    fn connect(&self) -> Result<(), String> {
        // Connection is established during initialization
        Ok(())
    }

    fn insert(&self, key: &str, value: &str) -> Result<(), String> {
        let mut con = self.connection.borrow_mut();
        con.set(key, value).map_err(|e| e.to_string())
    }

    fn retrieve(&self, key: &str) -> Result<Option<String>, String> {
        let mut con = self.connection.borrow_mut();
        con.get(key).map_err(|e| e.to_string())
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        let mut con = self.connection.borrow_mut();
        con.del(key).map_err(|e| e.to_string())
    }
}

impl GraphStorageEngine for RedisStorage {
    fn insert_node(&self, id: &str, data: &str) -> Result<(), String> {
        let mut con = self.connection.borrow_mut();
        let node_key = format!("node:{}", id);
        con.set(&node_key, data).map_err(|e| e.to_string())
    }

    fn retrieve_node(&self, id: &str) -> Result<Option<String>, String> {
        let mut con = self.connection.borrow_mut();
        let node_key = format!("node:{}", id);
        con.get(&node_key).map_err(|e| e.to_string())
    }

    fn delete_node(&self, id: &str) -> Result<(), String> {
        let mut con = self.connection.borrow_mut();
        let node_key = format!("node:{}", id);
        con.del(&node_key).map_err(|e| e.to_string())
    }
}
