use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq, Eq)]  // Removed Hash derive
pub struct Edge {
    pub id: u64,
    pub start_node: u64,
    pub end_node: u64,
    pub properties: HashMap<String, String>,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
}

impl Edge {
    pub fn new(id: u64, start_node: u64, end_node: u64, properties: HashMap<String, String>, start_time: DateTime<Utc>) -> Self {
        Edge {
            id,
            start_node,
            end_node,
            properties,
            start_time,
            end_time: None,
        }
    }

    pub fn update(&mut self, properties: HashMap<String, String>, timestamp: DateTime<Utc>) {
        self.properties = properties;
        self.end_time = Some(timestamp);
    }
}

impl Hash for Edge {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state); // Only hash the `id`, not the properties.
    }
}

#[derive(Debug, Clone, PartialEq)] // Removed Hash derive
pub struct Node {
    pub id: u64,
    pub properties: HashMap<String, String>,
    pub timestamp: DateTime<Utc>,
}

impl Node {
    pub fn new(id: u64, properties: HashMap<String, String>, timestamp: DateTime<Utc>) -> Self {
        Node {
            id,
            properties,
            timestamp,
        }
    }

    pub fn update(&mut self, properties: HashMap<String, String>, timestamp: DateTime<Utc>) {
        self.properties = properties;
        self.timestamp = timestamp;
    }
}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state); // Only hash the `id`, not the properties.
    }
}

