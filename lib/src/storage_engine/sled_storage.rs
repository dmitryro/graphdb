#[cfg(feature = "with_sled")]
use sled::{Db, IVec};

#[cfg(feature = "with_sled")]
pub struct SledStorage {
    db: Db,
}

#[cfg(feature = "with_sled")]
impl SledStorage {
    pub fn new(config: &StorageConfig) -> Self {
        let path = &config.connection_string;
        let db = sled::open(path).expect("Failed to open Sled database");
        SledStorage { db }
    }
}

#[cfg(feature = "with_sled")]
impl StorageEngine for SledStorage {
    fn connect(&self) -> Result<(), String> {
        // Connection is established during initialization
        Ok(())
    }

    fn insert(&self, key: &str, value: &str) -> Result<(), String> {
        self.db
            .insert(key, value.as_bytes())
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn retrieve(&self, key: &str) -> Result<Option<String>, String> {
        match self.db.get(key) {
            Ok(Some(ivec)) => Ok(Some(String::from_utf8(ivec.to_vec()).unwrap())),
            Ok(None) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        self.db
            .remove(key)
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

