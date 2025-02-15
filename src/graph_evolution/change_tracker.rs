use chrono::{DateTime, Utc};
use std::collections::VecDeque;

/// Represents a graph change.
#[derive(Debug, Clone)]
pub struct GraphChange {
    pub timestamp: DateTime<Utc>,
    pub entity_id: String,  // Node or Edge ID
    pub change_type: String, // "added", "removed", "updated"
}

/// Tracks graph changes over time.
pub struct ChangeTracker {
    changes: VecDeque<GraphChange>,
}

impl ChangeTracker {
    /// Creates a new change tracker.
    pub fn new() -> Self {
        Self {
            changes: VecDeque::new(),
        }
    }

    /// Records a new change.
    pub fn record_change(&mut self, entity_id: &str, change_type: &str) {
        let change = GraphChange {
            timestamp: Utc::now(),
            entity_id: entity_id.to_string(),
            change_type: change_type.to_string(),
        };
        self.changes.push_back(change);
    }

    /// Retrieves changes within a given time window.
    pub fn get_changes_within_window(&self, window: super::time_window::TimeWindow) -> Vec<GraphChange> {
        self.changes
            .iter()
            .filter(|change| window.contains(&change.timestamp))
            .cloned()
            .collect()
    }
}

