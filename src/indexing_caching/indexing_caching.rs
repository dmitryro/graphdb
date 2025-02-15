// src/indexing_caching/indexing_caching.rs

use std::collections::HashMap;
use anyhow::{Result, Context};
use sled::Db;
use petgraph::graph::{DiGraph};

#[derive(Debug)]
pub struct IndexingCache {
    in_memory_cache: HashMap<String, String>,
    persistent_cache: Db,
    graph_index: DiGraph<String, String>,
}

impl IndexingCache {
    pub fn new(persistent_cache_path: &str) -> Result<Self> {
        let persistent_cache = sled::open(persistent_cache_path)
            .context("Failed to open persistent cache")?;
        
        let graph_index = DiGraph::new();

        Ok(IndexingCache {
            in_memory_cache: HashMap::new(),
            persistent_cache,
            graph_index,
        })
    }

    pub fn index_data_in_memory(&mut self, key: String, value: String) {
        self.in_memory_cache.insert(key, value);
    }

    pub fn index_data_persistent(&mut self, key: String, value: String) -> Result<()> {
        self.persistent_cache.insert(key.as_bytes(), value.as_bytes())
            .context("Failed to index data in persistent cache")?;
        Ok(())
    }

    pub fn get_data_from_memory(&self, key: &str) -> Option<&String> {
        self.in_memory_cache.get(key)
    }

    pub fn get_data_from_persistent(&self, key: &str) -> Result<Option<String>> {
        match self.persistent_cache.get(key.as_bytes())? {
            Some(data) => Ok(Some(String::from_utf8_lossy(&data).to_string())),
            None => Ok(None),
        }
    }

    pub fn index_in_graph(&mut self, node: String, edge: String) {
        let node_idx = self.graph_index.add_node(node);
        let edge_idx = self.graph_index.add_node(edge);
        self.graph_index.add_edge(node_idx, edge_idx, "relationship".to_string());
    }
}

// Add the following public functions

/// Index a new node with the given data.
pub fn index_node(node: &str, data: &str) {
    // Implement the logic to index the node with the provided data.
    // This is a placeholder implementation.
    println!("Indexing node '{}' with data '{}'", node, data);
}

/// Cache the state of a node.
pub fn cache_node_state(node: &str, state: &str) {
    // Implement the logic to cache the node's state.
    // This is a placeholder implementation.
    println!("Caching state '{}' for node '{}'", state, node);
}

/// Retrieve the cached state of a node.
pub fn get_cached_node_state(node: &str) -> Option<String> {
    // Implement the logic to retrieve the cached state of the node.
    // This is a placeholder implementation.
    println!("Retrieving cached state for node '{}'", node);
    Some("cached_state".to_string()) // Replace with actual retrieval logic.
}

