// lib/src/durability/recovery.rs
use crate::durability::{DurabilityManager, GraphOp};
use crate::graph_engine::Graph;

pub fn recover(durability: &DurabilityManager) -> Graph {
    let mut graph = Graph::new();
    
    if let Some(snapshot) = durability.latest_snapshot() {
        println!("Loading snapshot from offset {}", snapshot.metadata.wal_offset);
        
        graph = durability
            .snapshot
            .load_graph(&snapshot)
            .unwrap_or_else(|_| Graph::new());
        
        // Handle Result from wal_entries_after
        match durability.wal_entries_after(snapshot.metadata.wal_offset) {
            Ok(wal_entries) => {
                for (_, op) in &wal_entries {
                    apply_op(&mut graph, op.clone());
                }
                println!("Replayed {} WAL entries", wal_entries.len());
            }
            Err(e) => {
                eprintln!("Failed to read WAL entries: {}", e);
            }
        }
    } else {
        println!("No snapshot found. Starting from empty graph.");
    }
    
    graph
}

fn apply_op(graph: &mut Graph, op: GraphOp) {
    match op {
        GraphOp::InsertVertex(v) => {
            graph.add_vertex(v);
        }
        GraphOp::InsertEdge(e) => {
            graph.add_edge(e);
        }
        GraphOp::DeleteVertex(id) => {
            let _ = graph.vertices.remove(&id);
        }
        GraphOp::DeleteEdge(id) => {
            let _ = graph.edges.remove(&id);
        }
        GraphOp::UpdateVertex(id, props) => {
            if let Some(v) = graph.vertices.get_mut(&id) {
                for (k, val) in props {
                    v.properties.insert(k, val);
                }
            }
        }
    }
}