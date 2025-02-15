use chrono::{DateTime, Utc};
use std::collections::{HashSet};
use crate::graph_evolution::models::{Edge, Node};

pub struct ChangeTracker {
    changes: Vec<Change>,
}

pub struct Change {
    pub timestamp: DateTime<Utc>,
    pub node: Option<Node>,
    pub edge: Option<Edge>,
}

impl ChangeTracker {
    pub fn new() -> Self {
        ChangeTracker {
            changes: Vec::new(),
        }
    }

    pub fn record_change(&mut self, timestamp: DateTime<Utc>, node: Option<Node>, edge: Option<Edge>) {
        let change = Change { timestamp, node, edge };
        self.changes.push(change);
    }

    // Replay changes up to a specific timestamp
    pub fn replay_changes_up_to_time(&self, timestamp: DateTime<Utc>) -> Option<(Vec<Node>, Vec<Edge>)> {
        let mut nodes = Vec::new();
        let mut edges = HashSet::new();

        for change in &self.changes {
            if change.timestamp <= timestamp {
                if let Some(node) = &change.node {
                    nodes.push(node.clone());
                }
                if let Some(edge) = &change.edge {
                    edges.insert(edge.clone());
                }
            }
        }

        Some((nodes, edges.into_iter().collect()))
    }
}

pub struct TimeWindow {
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
}

impl TimeWindow {
    pub fn new(start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> Self {
        TimeWindow { start_time, end_time }
    }
}

