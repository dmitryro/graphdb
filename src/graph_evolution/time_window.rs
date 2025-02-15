use chrono::{DateTime, Utc, Duration};

/// Represents a time window for querying changes.
#[derive(Debug, Clone)]
pub struct TimeWindow {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

impl TimeWindow {
    /// Creates a time window from a duration (e.g., last 10 minutes).
    pub fn from_duration(duration: Duration) -> Self {
        let end = Utc::now();
        let start = end - duration;
        Self { start, end }
    }

    /// Checks if a timestamp is within the window.
    pub fn contains(&self, timestamp: &DateTime<Utc>) -> bool {
        *timestamp >= self.start && *timestamp <= self.end
    }
}
