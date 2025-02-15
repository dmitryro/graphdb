// lib/src/storage_engine/config.rs

use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Debug)]
pub struct StorageConfig {
    pub engine: String,
    pub parameters: HashMap<String, String>,
}

impl StorageConfig {
    pub fn new(engine: String, parameters: HashMap<String, String>) -> Self {
        StorageConfig { engine, parameters }
    }
}

