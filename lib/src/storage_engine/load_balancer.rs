use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{Mutex as TokioMutex, RwLock};
use crate::config::{ReplicationStrategy, NodeHealth};
use models::errors::{GraphResult, GraphError};
pub use crate::config::config_structs::{ LoadBalancer };


impl LoadBalancer {
    /// Create a new LoadBalancer instance.
    pub fn new(replication_factor: usize) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            current_index: Arc::new(TokioMutex::new(0)),
            replication_factor,
        }
    }

    /// Add or update node health status.
    pub async fn update_node_health(&self, port: u16, is_healthy: bool, response_time_ms: u64) {
        let mut nodes = self.nodes.write().await;
        let node_health = nodes.entry(port).or_insert(NodeHealth {
            port,
            is_healthy: false,
            last_check: SystemTime::now(),
            response_time_ms: 0,
            error_count: 0,
        });
        
        node_health.is_healthy = is_healthy;
        node_health.last_check = SystemTime::now();
        node_health.response_time_ms = response_time_ms;
        
        if is_healthy {
            node_health.error_count = 0;
        } else {
            node_health.error_count += 1;
        }
        
        println!("===> LOAD BALANCER: Updated node {} health: healthy={}, response_time={}ms, errors={}", 
                 port, is_healthy, response_time_ms, node_health.error_count);
    }

    /// Get healthy nodes for read operations (round-robin).
    pub async fn get_read_node(&self) -> Option<u16> {
        let nodes = self.nodes.read().await;
        let healthy_nodes: Vec<u16> = nodes.values()
            .filter(|node| node.is_healthy)
            .map(|node| node.port)
            .collect();
        
        if healthy_nodes.is_empty() {
            return None;
        }
        
        let mut index = self.current_index.lock().await;
        let selected_port = healthy_nodes[*index % healthy_nodes.len()];
        *index = (*index + 1) % healthy_nodes.len();
        
        println!("===> LOAD BALANCER: Selected read node {}", selected_port);
        Some(selected_port)
    }

    /// Get nodes for write replication.
    pub async fn get_write_nodes(&self, strategy: ReplicationStrategy) -> Vec<u16> {
        let nodes = self.nodes.read().await;
        let mut healthy_nodes: Vec<(u16, u64)> = nodes.values()
            .filter(|node| node.is_healthy)
            .map(|node| (node.port, node.response_time_ms))
            .collect();
        
        // Sort by response time (fastest first)
        healthy_nodes.sort_by_key(|(_, response_time)| *response_time);
        
        let selected_nodes = match strategy {
            ReplicationStrategy::AllNodes => {
                healthy_nodes.iter().map(|(port, _)| *port).collect()
            },
            ReplicationStrategy::NNodes(n) => {
                healthy_nodes.iter()
                    .take(n.min(healthy_nodes.len()))
                    .map(|(port, _)| *port)
                    .collect()
            },
            ReplicationStrategy::Raft => {
                // For Raft, return all healthy nodes (Raft will handle consensus)
                healthy_nodes.iter().map(|(port, _)| *port).collect()
            }
        };
        
        println!("===> LOAD BALANCER: Selected write nodes {:?} for strategy {:?}", 
                 selected_nodes, strategy);
        selected_nodes
    }

    /// Get all healthy nodes.
    pub async fn get_healthy_nodes(&self) -> Vec<u16> {
        let nodes = self.nodes.read().await;
        nodes.values()
            .filter(|node| node.is_healthy)
            .map(|node| node.port)
            .collect()
    }
    
    /// Get the least loaded node for write operations.
    /// This method selects the healthiest node with the lowest response time.
    pub async fn get_least_loaded_node(&self) -> Option<u16> {
        let nodes = self.nodes.read().await;
        let mut healthy_nodes: Vec<&NodeHealth> = nodes.values()
            .filter(|node| node.is_healthy)
            .collect();

        if healthy_nodes.is_empty() {
            return None;
        }

        // Sort by response time (least loaded first)
        healthy_nodes.sort_by_key(|node| node.response_time_ms);
        let selected_port = healthy_nodes[0].port;

        println!("===> LOAD BALANCER: Selected least loaded node {}", selected_port);
        Some(selected_port)
    }

    /// Get all nodes, including unhealthy ones.
    pub async fn get_all_nodes(&self) -> Vec<u16> {
        let nodes = self.nodes.read().await;
        nodes.keys().cloned().collect()
    }

    /// Remove a node from the load balancer.
    pub async fn remove_node(&self, port: u16) {
        let mut nodes = self.nodes.write().await;
        if nodes.remove(&port).is_some() {
            println!("===> LOAD BALANCER: Removed node {}", port);
        } else {
            println!("===> LOAD BALANCER: Node {} not found, nothing to remove", port);
        }
    }

    /// Get a map of all node health statuses.
    pub async fn get_all_node_health(&self) -> HashMap<u16, NodeHealth> {
        let nodes = self.nodes.read().await;
        nodes.clone()
    }
}
