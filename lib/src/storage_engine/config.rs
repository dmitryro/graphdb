// src/storage_engine/config.rs

use serde::Deserialize;
use std::fs::File;
use std::io::Read;

#[derive(Deserialize)]
pub struct StorageConfig {
    pub engine: String,
    pub connection_string: String,
}

impl StorageConfig {
    pub fn load_from_file(file_path: &str) -> Result<Self, String> {
        let mut file = File::open(file_path).map_err(|e| e.to_string())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(|e| e.to_string())?;
        toml::de::from_str(&contents).map_err(|e| e.to_string())
    }
}

