// Graph Evolution

use crate::graph_evolution::change_tracker::{ChangeTracker, GraphChange};
use crate::graph_evolution::time_window::TimeWindow;
use crate::graph_evolution::models::{Node, Edge};
use std::sync::{Arc, Mutex};

/// Manages graph evolution.
pub struct GraphEvolution {
    tracker: Arc<Mutex<ChangeTracker>>,
}

impl GraphEvolution {
    /// Creates a new GraphEvolution instance.
    pub fn new() -> Self {
        Self {
            tracker: Arc::new(Mutex::new(ChangeTracker::new())),
        }
    }

    /// Records a node change.
    pub fn record_node_change(&self, node: &Node, change_type: &str) {
        let mut tracker = self.tracker.lock().unwrap();
        tracker.record_change(&node.id, change_type);
    }

    /// Records an edge change.
    pub fn record_edge_change(&self, edge: &Edge, change_type: &str) {
        let mut tracker = self.tracker.lock().unwrap();
        tracker.record_change(&edge.id, change_type);
    }

    /// Fetches changes within a time window.
    pub fn get_changes_within_window(&self, window: TimeWindow) -> Vec<GraphChange> {
        let tracker = self.tracker.lock().unwrap();
        tracker.get_changes_within_window(window)
    }
}

