use crate::engine::graph::Graph;
use crate::engine::vertex::Vertex;
use crate::engine::edge::Edge;
use uuid::Uuid;
use std::collections::{HashSet, VecDeque};

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
                        let next_id = edge.to;
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

