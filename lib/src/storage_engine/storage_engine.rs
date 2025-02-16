// lib/src/storage_engine/storage_engine.rs

use serde::Deserialize;

#[derive(Deserialize)]
pub struct StorageConfig {
    pub connection_string: String,
}

pub trait StorageEngine {
    fn connect(&self) -> Result<(), String>;
    fn insert(&self, key: &str, value: &str) -> Result<(), String>;
    fn retrieve(&self, key: &str) -> Result<Option<String>, String>;
    fn delete(&self, key: &str) -> Result<(), String>;
}

