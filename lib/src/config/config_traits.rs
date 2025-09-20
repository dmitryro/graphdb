use models::errors::{GraphError, GraphResult};
use async_trait::async_trait;
use std::fmt::Debug;

#[async_trait]
pub trait AsyncStorageEngine: Send + Sync + Debug {
    /// Retrieves a value for a given key.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, GraphError>;

    /// Sets a value for a given key.
    async fn set(&self, key: &[u8], value: &[u8]) -> Result<(), GraphError>;

    /// Deletes a key-value pair.
    async fn delete(&self, key: &[u8]) -> Result<(), GraphError>;

    /// Persists all in-memory changes to disk.
    async fn flush(&self) -> Result<(), GraphError>;

    /// Closes the storage engine, flushing all data.
    async fn close(&self) -> Result<(), GraphError>;
}