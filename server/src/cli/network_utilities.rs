use anyhow::{Result, Context, anyhow};

/// Validates a cluster range string (e.g., "8000-8002" or "8000").
/// Returns the original valid string if successful, or an error.
pub fn validate_cluster_range(range_str: &str) -> Result<String> {
    let parts: Vec<&str> = range_str.split('-').collect();
    if parts.len() == 2 {
        let start_port = parts[0].parse::<u16>().context("Invalid start port in cluster range")?;
        let end_port = parts[1].parse::<u16>().context("Invalid end port in cluster range")?;
        if start_port > end_port {
            anyhow::bail!("Start port cannot be greater than end port in cluster range.");
        }
        Ok(range_str.to_string())
    } else if parts.len() == 1 {
        parts[0].parse::<u16>().context("Invalid port in cluster range")?;
        Ok(range_str.to_string())
    } else {
        anyhow::bail!("Invalid cluster range format. Expected 'start-end' or 'port'.");
    }
}

