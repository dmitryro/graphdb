use crate::storage_engine::storage_engine::{StorageEngine, StorageConfig};
use postgres::{Client, NoTls};
use std::cell::RefCell;

pub struct PostgresStorage {
    client: RefCell<Client>,
}

impl PostgresStorage {
    pub fn new(config: &StorageConfig) -> Self {
        let client = Client::connect(&config.connection_string, NoTls)
            .expect("Failed to connect to PostgreSQL");
        PostgresStorage {
            client: RefCell::new(client),
        }
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

