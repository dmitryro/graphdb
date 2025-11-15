// lib/src/durability/mod.rs
pub mod wal;
pub mod snapshot;
pub mod recovery;

use crate::graph_engine::{Vertex, Edge};
use uuid::Uuid;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GraphOp {
    InsertVertex(Vertex),
    UpdateVertex(Uuid, Vec<(String, crate::graph_engine::properties::PropertyValue)>),
    DeleteVertex(Uuid),
    InsertEdge(Edge),
    DeleteEdge(Uuid),
}

pub struct DurabilityManager {
    wal: wal::Wal,
    snapshot: snapshot::SnapshotManager,
}

impl DurabilityManager {
    pub fn open(path: impl Into<std::path::PathBuf>) -> Result<Self, Box<dyn std::error::Error>> {
        let path = path.into();
        let wal = wal::Wal::open(path.join("wal"))?;
        let snapshot = snapshot::SnapshotManager::open(path.join("snapshots"))?;
        Ok(Self { wal, snapshot })
    }

    pub fn append(&mut self, op: GraphOp) -> Result<u64, Box<dyn std::error::Error>> {
        self.wal.append(op)
    }

    pub async fn create_snapshot(&mut self, graph: &crate::graph_engine::Graph) -> Result<(), Box<dyn std::error::Error>> {
        self.snapshot.create(graph, self.wal.current_offset()).await
    }

    pub fn latest_snapshot(&self) -> Option<snapshot::Snapshot> {
        self.snapshot.latest()
    }

    pub fn wal_entries_after(&self, offset: u64) -> Result<Vec<(u64, GraphOp)>, Box<dyn std::error::Error>> {
        self.wal.entries_after(offset)
    }
}