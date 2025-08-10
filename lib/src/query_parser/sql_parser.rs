// src/query_parser/sql_parser.rs

use sqlparser::ast::{Statement, SetExpr, TableWithJoins, ObjectName, Query, CreateTable};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::Select;
use sqlparser::ast::SelectItem;
use std::collections::HashMap;

/// An enum to represent the structured, parsed SQL statement.
/// This provides a much more useful output than a simple string.
#[derive(Debug, PartialEq)]
pub enum ParsedStatement {
    Select {
        table_name: Option<String>,
        columns: Vec<String>,
    },
    CreateTable {
        table_name: String,
        columns: HashMap<String, String>,
    },
    Unknown(String),
    Unsupported,
    Empty,
}

/// Checks if a query is likely an SQL query.
/// This function uses a simple keyword check as a quick heuristic.
pub fn is_sql(query: &str) -> bool {
    let sql_keywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"];
    sql_keywords.iter().any(|kw| query.trim().to_uppercase().starts_with(kw))
}

/// Parse an SQL query using the `sqlparser` crate and return a structured
/// representation of the statement.
///
/// This implementation handles SELECT and CREATE TABLE statements and
/// provides a structured output for each.
pub fn parse_sql(query: &str) -> Result<ParsedStatement, String> {
    let dialect = GenericDialect {}; // Use the generic dialect for SQL parsing

    // The Parser can handle multiple statements, but we'll focus on the first one.
    let statements = match Parser::parse_sql(&dialect, query) {
        Ok(s) => s,
        Err(err) => return Err(format!("SQL parsing error: {}", err)),
    };

    let Some(statement) = statements.get(0) else {
        return Ok(ParsedStatement::Empty);
    };

    match statement {
        Statement::Query(query_box) => {
            // FIX: Use `&*query_box.body` to dereference the Box and then take a reference,
            // matching the expected type `SetExpr`.
            if let SetExpr::Select(select_box) = &*query_box.body {
                let select_statement: &Select = select_box;
                
                // Extract columns from the projection
                let columns = select_statement.projection.iter().map(|item| {
                    match item {
                        SelectItem::UnnamedExpr(expr) => expr.to_string(),
                        SelectItem::ExprWithAlias { expr, alias } => {
                            format!("{} AS {}", expr, alias)
                        },
                        SelectItem::Wildcard(_) => "*".to_string(),
                        _ => "UnknownColumn".to_string(),
                    }
                }).collect();

                // Extract table name from the FROM clause
                let table_name = if let Some(TableWithJoins { relation, .. }) = select_statement.from.get(0) {
                    Some(relation.to_string())
                } else {
                    None
                };

                Ok(ParsedStatement::Select { table_name, columns })
            } else {
                Ok(ParsedStatement::Unsupported)
            }
        },
        // FIX: The `CreateTable` variant is a tuple-like enum, containing a `CreateTable` struct.
        // We match on the struct itself and then access its fields.
        Statement::CreateTable(create_table) => {
            let table_name = create_table.name.to_string();
            let mut column_map = HashMap::new();
            for col in &create_table.columns {
                column_map.insert(col.name.to_string(), col.data_type.to_string());
            }
            Ok(ParsedStatement::CreateTable { table_name, columns: column_map })
        },
        _ => {
            // Handle other statements by just returning their type
            Ok(ParsedStatement::Unknown(format!("{:?}", statement)))
        }
    }
}