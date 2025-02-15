// src/query_parser/cypher_parser.rs

/// Checks if a query is likely a Cypher query.
pub fn is_cypher(query: &str) -> bool {
    let cypher_keywords = ["MATCH", "CREATE", "MERGE", "RETURN", "DETACH", "DELETE"];
    cypher_keywords.iter().any(|kw| query.trim().to_uppercase().starts_with(kw))
}

/// Parse a Cypher query (with simplified matching approach).
pub fn parse_cypher(query: &str) -> Result<String, String> {
    // Simple keyword-based check (you can replace with a real parser later)
    if is_cypher(query) {
        Ok(format!("Parsed Cypher query: {}", query)) // Replace with actual parsing if needed
    } else {
        Err("Not a valid Cypher query.".to_string())
    }
}

