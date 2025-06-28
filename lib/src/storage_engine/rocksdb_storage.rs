#[cfg(feature = "with_rocksdb")]
use rocksdb::{DB, Options};

#[cfg(feature = "with_rocksdb")]
pub struct RocksDBStorage {
    db: DB,
}

#[cfg(feature = "with_rocksdb")]
impl RocksDBStorage {
    pub fn new(config: &StorageConfig) -> Self {
        let path = &config.connection_string;
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, path).expect("Failed to open RocksDB");
        RocksDBStorage { db }
    }
}

#[cfg(feature = "with_rocksdb")]
impl StorageEngine for RocksDBStorage {
    fn connect(&self) -> Result<(), String> {
        // Connection is established during initialization
        Ok(())
    }

    fn insert(&self, key: &str, value: &str) -> Result<(), String> {
        self.db
            .put(key, value)
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn retrieve(&self, key: &str) -> Result<Option<String>, String> {
        match self.db.get(key) {
            Ok(Some(value)) => Ok(Some(String::from_utf8(value).unwrap())),
            Ok(None) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        self.db
            .delete(key)
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

