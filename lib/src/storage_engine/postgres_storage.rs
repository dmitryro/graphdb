// lib/src/storage_engine/postgres_storage.rs
// Updated: 2025-08-08 - Implemented GraphStorageEngine trait for PostgresStorage
// NOTE: Assumes a `nodes` table with columns `id` (TEXT) and `data` (TEXT) for graph operations
// NOTE: Assumes a `store` table with columns `key` (TEXT) and `value` (TEXT) for key-value operations

use crate::storage_engine::storage_engine::{GraphStorageEngine, StorageEngine, StorageConfig};
use postgres::{Client, NoTls};
use std::cell::RefCell;

pub struct PostgresStorage {
    client: RefCell<Client>,
}

impl PostgresStorage {
    pub fn new(config: &StorageConfig) -> postgres::Result<Self> {
        let client = Client::connect(config.connection_string.as_ref()
            .ok_or_else(|| postgres::error::Error::from(postgres::error::DbError {
                severity: "ERROR".to_string(),
                code: postgres::error::SqlState::UNDEFINED_OBJECT,
                message: "Connection string is required".to_string(),
                detail: None,
                hint: None,
                position: None,
                where_: None,
                schema: None,
                table: None,
                column: None,
                datatype: None,
                constraint: None,
                file: None,
                line: None,
                routine: None,
            })))?, NoTls)?;
        Ok(PostgresStorage {
            client: RefCell::new(client),
        })
    }
}

impl StorageEngine for PostgresStorage {
    fn connect(&self) -> Result<(), String> {
        // Connection is established during initialization
        Ok(())
    }

    fn insert(&self, key: &str, value: &str) -> Result<(), String> {
        self.client
            .borrow_mut()
            .execute("INSERT INTO store (key, value) VALUES ($1, $2)", &[&key, &value])
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn retrieve(&self, key: &str) -> Result<Option<String>, String> {
        let rows = self
            .client
            .borrow_mut()
            .query("SELECT value FROM store WHERE key = $1", &[&key]);
        match rows {
            Ok(rows) => {
                if let Some(row) = rows.iter().next() {
                    Ok(Some(row.get(0)))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(e.to_string()),
        }
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        self.client
            .borrow_mut()
            .execute("DELETE FROM store WHERE key = $1", &[&key])
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

impl GraphStorageEngine for PostgresStorage {
    fn insert_node(&self, id: &str, data: &str) -> Result<(), String> {
        self.client
            .borrow_mut()
            .execute("INSERT INTO nodes (id, data) VALUES ($1, $2)", &[&id, &data])
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn retrieve_node(&self, id: &str) -> Result<Option<String>, String> {
        let rows = self
            .client
            .borrow_mut()
            .query("SELECT data FROM nodes WHERE id = $1", &[&id]);
        match rows {
            Ok(rows) => {
                if let Some(row) = rows.iter().next() {
                    Ok(Some(row.get(0)))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(e.to_string()),
        }
    }

    fn delete_node(&self, id: &str) -> Result<(), String> {
        self.client
            .borrow_mut()
            .execute("DELETE FROM nodes WHERE id = $1", &[&id])
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}
