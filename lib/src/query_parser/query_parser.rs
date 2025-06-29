use graphql_parser::parse_query;
use graphql_parser::query::Document; // Required for parse_query to compile
use std::fmt::Debug;

/// Represents the type of a parsed query: Cypher, SQL, or GraphQL.
#[derive(Debug)]
pub enum QueryType {
    Cypher,
    SQL,
    GraphQL,
}

/// Attempts to parse a query string into a known `QueryType`.
pub fn parse_query_from_string(query: &str) -> Result<QueryType, String> {
    // Try to parse as a GraphQL query first
    if parse_query::<&str>(query).is_ok() {
        return Ok(QueryType::GraphQL);
    }

    // Try to infer Cypher
    if query.trim_start().starts_with("MATCH") || query.contains("RETURN") {
        return Ok(QueryType::Cypher);
    }

    // Try to infer SQL
    if query.contains("SELECT") || query.contains("FROM") {
        return Ok(QueryType::SQL);
    }

    Err("Unrecognized query format".to_string())
}

