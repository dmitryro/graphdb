// src/query_parser/graphql_parser.rs

use graphql_parser::parse_query; // Import the correct parse_query function from graphql-parser

/// Checks if a query is likely a GraphQL query.
pub fn is_graphql(query: &str) -> bool {
    let graphql_keywords = ["query", "mutation", "subscription"];
    graphql_keywords.iter().any(|kw| query.trim().to_lowercase().starts_with(kw))
}

/// Parse a GraphQL query.
pub fn parse_graphql(query: &str) -> Result<String, String> {
    // Use the correct parse_query function from graphql-parser crate
    match parse_query::<&str>(query) { // Explicitly specify type as &str
        Ok(doc) => Ok(format!("Parsed GraphQL query: {:?}", doc)), // Use debug formatting to print the document
        Err(err) => Err(format!("GraphQL parsing error: {}", err)),
    }
}

