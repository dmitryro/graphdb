// lib/src/storage_engine/sled_storage.rs
// Updated: 2025-06-30 - Made GraphStorageEngine trait public.

use anyhow::{Context, Result};
use async_trait::async_trait;
use sled::{Db, IVec};
use std::sync::Arc; // Needed for Arc<Self> in the trait definition
use tokio::sync::Mutex; // Assuming you might use this for internal state locking

// Function to open the Sled database
pub fn open_sled_db(path: &str) -> Result<Db> {
    sled::open(path).context("Failed to open Sled database")
}

// Define the GraphStorageEngine trait
#[async_trait]
pub trait GraphStorageEngine: Send + Sync + 'static { // <-- ADDED 'pub' here
    fn get_type(&self) -> &'static str;
    fn is_running(&self) -> bool;
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;

    // Placeholder for a generic query method
    async fn query(&self, query_string: &str) -> Result<serde_json::Value>;

    // Basic CRUD operations placeholders
    async fn insert_data(&self, key: &str, value: &str) -> Result<()>;
    async fn get_data(&self, key: &str) -> Result<Option<String>>;
    async fn delete_data(&self, key: &str) -> Result<()>;
}

// SledGraphStorage implementation
#[derive(Debug, Clone)]
pub struct SledGraphStorage {
    db: Db,
    // You might have specific trees for nodes, edges, properties if this were a full graph DB
    // For now, let's just use a generic 'graph_data' tree
    graph_data_tree: sled::Tree,
    is_started: Arc<Mutex<bool>>, // Track if the storage is "started"
}

impl SledGraphStorage {
    pub fn new(db: Db) -> Self {
        let graph_data_tree = db.open_tree("graph_data").expect("Failed to open graph_data tree");
        SledGraphStorage {
            db,
            graph_data_tree,
            is_started: Arc::new(Mutex::new(false)),
        }
    }
}

#[async_trait]
impl GraphStorageEngine for SledGraphStorage {
    fn get_type(&self) -> &'static str {
        "SledGraphStorage"
    }

    fn is_running(&self) -> bool {
        *self.is_started.blocking_lock() // Use blocking_lock for sync access in a non-async context here
    }

    async fn start(&self) -> Result<()> {
        let mut started = self.is_started.lock().await;
        if *started {
            println!("SledGraphStorage already started.");
            return Ok(());
        }
        *started = true;
        println!("SledGraphStorage started.");
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let mut started = self.is_started.lock().await;
        if !*started {
            println!("SledGraphStorage already stopped.");
            return Ok(());
        }
        // In a real Sled application, stopping might involve flushing or closing the DB
        // Sled DBs are generally managed at application lifecycle level.
        // For graceful shutdown, you might want to ensure all writes are flushed.
        self.db.flush_async().await.context("Failed to flush Sled DB on stop")?;
        *started = false;
        println!("SledGraphStorage stopped.");
        Ok(())
    }

    async fn query(&self, query_string: &str) -> Result<serde_json::Value> {
        // This is a very basic placeholder.
        // A real graph query engine would parse the query,
        // traverse the graph, and return structured results.
        println!("Executing SledGraphStorage query: {}", query_string);

        // Example: Simple echo for demonstration
        Ok(serde_json::json!({
            "status": "success",
            "query_executed": query_string,
            "result_type": "placeholder",
            "data": "Simulated query result from SledGraphStorage"
        }))
    }

    async fn insert_data(&self, key: &str, value: &str) -> Result<()> {
        self.graph_data_tree.insert(key, value)
            .context("Failed to insert data into Sled graph_data tree")?;
        Ok(())
    }

    async fn get_data(&self, key: &str) -> Result<Option<String>> {
        let ivec = self.graph_data_tree.get(key)
            .context("Failed to retrieve data from Sled graph_data tree")?;
        Ok(ivec.map(|i| String::from_utf8_lossy(&i).into_owned()))
    }

    async fn delete_data(&self, key: &str) -> Result<()> {
        self.graph_data_tree.remove(key)
            .context("Failed to delete data from Sled graph_data tree")?;
        Ok(())
    }
}
