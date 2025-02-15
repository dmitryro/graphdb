// src/query_parser/sql_parser.rs

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Checks if a query is likely an SQL query.
pub fn is_sql(query: &str) -> bool {
    let sql_keywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"];
    sql_keywords.iter().any(|kw| query.trim().to_uppercase().starts_with(kw))
}

/// Parse an SQL query using sqlparser.
pub fn parse_sql(query: &str) -> Result<String, String> {
    let dialect = GenericDialect {}; // Use the generic dialect for SQL parsing
    match Parser::parse_sql(&dialect, query) {
        Ok(statements) => {
            // Return the parsed SQL query type
            Ok(format!("Parsed SQL query: {:?}", statements))
        }
        Err(err) => Err(format!("SQL parsing error: {}", err)),
    }
}

