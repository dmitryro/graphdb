use crate::graph_engine::Graph;
use crate::graph_engine::Vertex;
use crate::graph_engine::Edge;
use uuid::Uuid;
use std::collections::{HashSet, VecDeque};
use std::str::FromStr; // Add this

impl Graph {
    /// Breadth-first traversal from a starting vertex id.
    pub fn bfs(&self, start: Uuid, max_depth: usize) -> Vec<&Vertex> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut results = Vec::new();

        queue.push_back((start, 0));
        visited.insert(start);

        while let Some((current_id, depth)) = queue.pop_front() {
            if depth > max_depth {
                break;
            }

            if let Some(vertex) = self.get_vertex(&current_id) {
                results.push(vertex);
            }

            if let Some(edge_ids) = self.get_edges_from(&current_id) {
                for edge_id in edge_ids {
                    if let Some(edge) = self.edges.get(edge_id) {
                        // ✅ CAST: Convert Identifier to Uuid
                        let next_id_str = edge.t.as_ref(); // &str
                        let next_id = match Uuid::from_str(next_id_str) {
                            Ok(id) => id,
                            Err(_) => continue, // Skip invalid UUIDs
                        };
                        
                        if !visited.contains(&next_id) {
                            visited.insert(next_id);
                            queue.push_back((next_id, depth + 1));
                        }
                    }
                }
            }
        }

        results
    }

    // Additional traversal or pattern matching methods here...
}