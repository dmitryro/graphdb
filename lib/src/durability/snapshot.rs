// lib/src/durability/snapshot.rs
use crate::graph_engine::Graph;
use crate::graph_engine::{Vertex, Edge};
use serde::{Serialize, Deserialize};
use std::fs;
use std::path::{Path, PathBuf};
use chrono::Utc;
use log::{info, warn, error};

#[derive(Serialize, Deserialize, Debug)]
pub struct SnapshotMetadata {
    pub timestamp: i64,
    pub wal_offset: u64,
    pub graph_size: usize, // vertices + edges
}

#[derive(Debug)]
pub struct Snapshot {
    pub path: PathBuf,
    pub metadata: SnapshotMetadata,
}

pub struct SnapshotManager {
    dir: PathBuf,
}

impl SnapshotManager {
    pub fn open(dir: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    pub async fn create(&mut self, graph: &Graph, wal_offset: u64) -> Result<(), Box<dyn std::error::Error>> {
        let timestamp = Utc::now().timestamp();
        let metadata = SnapshotMetadata {
            timestamp,
            wal_offset,
            graph_size: graph.vertices.len() + graph.edges.len(),
        };

        let snapshot_id = format!("snapshot_{}", timestamp);
        let snap_dir = self.dir.join(&snapshot_id);
        fs::create_dir(&snap_dir)?;

        // Serialize data separately
        let vertices_data = serde_json::to_vec(
            &graph.vertices.values().collect::<Vec<_>>()
        )?;
        let edges_data = serde_json::to_vec(
            &graph.edges.values().collect::<Vec<_>>()
        )?;

        fs::write(snap_dir.join("vertices.json"), vertices_data)?;
        fs::write(snap_dir.join("edges.json"), edges_data)?;

        // ✅ Use new bincode API
        let metadata_bytes = bincode::serde::encode_to_vec(&metadata, bincode::config::standard())?;
        fs::write(snap_dir.join("meta.bin"), metadata_bytes)?;

        println!("Snapshot created: {} (WAL offset: {})", snapshot_id, wal_offset);
        Ok(())
    }

    pub fn latest(&self) -> Option<Snapshot> {
        let mut snapshots: Vec<_> = fs::read_dir(&self.dir)
            .ok()?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().is_dir())
            .filter_map(|entry| {
                let meta_path = entry.path().join("meta.bin");
                if meta_path.exists() {
                    let data = fs::read(&meta_path).ok()?;
                    
                    // ✅ Use new bincode API
                    let (meta, _): (SnapshotMetadata, _) = bincode::serde::decode_from_slice(
                        &data,
                        bincode::config::standard()
                    ).ok()?;
                    
                    Some((entry.path(), meta))
                } else {
                    None
                }
            })
            .collect();

        snapshots.sort_by_key(|&(_, ref meta)| std::cmp::Reverse(meta.timestamp));
        snapshots.into_iter().next().map(|(path, metadata)| Snapshot { path, metadata })
    }

    pub fn load_graph(&self, snapshot: &Snapshot) -> Result<Graph, Box<dyn std::error::Error>> {
        let vertices_path = snapshot.path.join("vertices.json");
        let edges_path = snapshot.path.join("edges.json");

        if !vertices_path.exists() || !edges_path.exists() {
            return Err("Snapshot files missing".into());
        }

        let vertices_data = fs::read(&vertices_path)?;
        let edges_data = fs::read(&edges_path)?;

        let vertices: Vec<Vertex> = serde_json::from_slice(&vertices_data)?;
        let edges: Vec<Edge> = serde_json::from_slice(&edges_data)?;

        // Recreate Graph from data
        let mut graph = Graph::new();
        for vertex in vertices {
            graph.add_vertex(vertex);
        }
        for edge in edges {
            graph.add_edge(edge);
        }

        Ok(graph)
    }
}