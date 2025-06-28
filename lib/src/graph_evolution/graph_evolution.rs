use chrono::{DateTime, Utc, TimeZone};  // Import TimeZone trait
use crate::graph_evolution::change_tracker::ChangeTracker;
use crate::graph_evolution::models::{Node, Edge};
use crate::graph_evolution::time_window::TimeWindow;

pub struct GraphEvolution {
    pub change_tracker: ChangeTracker,
}

impl GraphEvolution {
    pub fn new() -> Self {
        GraphEvolution {
            change_tracker: ChangeTracker::new(),
        }
    }

    // Replay changes up to the end time in the time window
    pub fn replay_changes(&self, time_window: &TimeWindow) -> Option<(Vec<Node>, Vec<Edge>)> {
        // Handling the LocalResult from timestamp_opt explicitly
        let end_time = match Utc.timestamp_opt(time_window.end_time as i64, 0) {
            chrono::LocalResult::Single(time) => time,
            _ => return None, // Handle invalid timestamp case
        };

        self.change_tracker.replay_changes_up_to_time(end_time)
    }

    // Method to add a change to the graph
    pub fn record_change(&mut self, timestamp: DateTime<Utc>, node: Option<Node>, edge: Option<Edge>) {
        self.change_tracker.record_change(timestamp, node, edge);
    }
}

