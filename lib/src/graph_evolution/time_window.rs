// src/graph_evolution/time_window.rs

#[derive(Debug, Clone)]
pub struct TimeWindow {
    pub start_time: u64,
    pub end_time: u64,
}

impl TimeWindow {
    pub fn new(start_time: u64, end_time: u64) -> Self {
        TimeWindow { start_time, end_time }
    }

    pub fn contains(&self, timestamp: u64) -> bool {
        timestamp >= self.start_time && timestamp <= self.end_time
    }
}

