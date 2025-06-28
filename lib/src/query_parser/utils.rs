// src/query_parser/utils.rs

/// Checks if a query is empty (only whitespace).
pub fn is_empty(query: &str) -> bool {
    query.trim().is_empty()
}

