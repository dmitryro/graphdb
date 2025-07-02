// lib/src/query_parser/query_parser.rs
// Updated: 2025-07-02 - Re-introduced regex for Cypher and SQL query detection.

use graphql_parser::parse_query;
use graphql_parser::query::Document; // Required for parse_query to compile
use std::fmt::Debug;
use regex::Regex; // Import the regex crate
use once_cell::sync::Lazy; // For lazy static regex initialization

/// Represents the type of a parsed query: Cypher, SQL, or GraphQL.
#[derive(Debug)]
pub enum QueryType {
    Cypher,
    SQL,
    GraphQL,
}

// Lazily initialized regex for Cypher and SQL keywords
static CYPHER_REGEX: Lazy<Regex> = Lazy::new(|| {
    // Matches common Cypher keywords at the start of the query,
    // ignoring leading whitespace and case.
    Regex::new(r"^\s*(?i)(MATCH|CREATE|MERGE|RETURN|DETACH|DELETE|SET|REMOVE|WHERE|WITH|UNWIND|CALL|LOAD CSV)").unwrap()
});

static SQL_REGEX: Lazy<Regex> = Lazy::new(|| {
    // Matches common SQL keywords at the start of the query,
    // ignoring leading whitespace and case.
    Regex::new(r"^\s*(?i)(SELECT|INSERT INTO|UPDATE|DELETE FROM|CREATE TABLE|ALTER TABLE|DROP TABLE|TRUNCATE TABLE)").unwrap()
});


/// Attempts to parse a query string into a known `QueryType`.
pub fn parse_query_from_string(query: &str) -> Result<QueryType, String> {
    // Try to parse as a GraphQL query first
    // This is typically a more strict grammar, so it's a good first check.
    if parse_query::<&str>(query).is_ok() {
        return Ok(QueryType::GraphQL);
    }

    // Try to infer Cypher using regex
    if CYPHER_REGEX.is_match(query) {
        return Ok(QueryType::Cypher);
    }

    // Try to infer SQL using regex
    if SQL_REGEX.is_match(query) {
        return Ok(QueryType::SQL);
    }

    Err("Unrecognized query format".to_string())
}

