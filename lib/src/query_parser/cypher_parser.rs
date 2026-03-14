// lib/src/query_parser/cypher_parser.rs
// Complete working version - merges working repo code with edge creation support
use anyhow::Context; // Fixes E0599: no method named `context`
use log::{debug, error, info, warn, trace};
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1, take_until, take_till, take_while, take },
    character::complete::{char, alpha1, multispace0, multispace1, alphanumeric1, i64 as parse_i64, one_of, digit1},
    combinator::{map, cut, opt, recognize, value, map_res, verify, not,peek,}, 
    multi::{separated_list0, separated_list1, many0,}, 
    number::complete::double as parse_double,
    sequence::{delimited, pair, preceded, tuple, terminated, separated_pair},
    multi::many1,
    IResult,
    Parser,
    error as NomError,
    error::Error as NomErrorType, 
};
use regex::Regex;
use std::pin::Pin;
use std::future::Future;
use serde_json::{json, Value, Number, Map};
use std::collections::{BTreeMap, HashMap, HashSet,};
use std::sync::Arc;
use uuid::Uuid;
use chrono::Utc;
use models::identifiers::{Identifier, SerializableUuid};
use models::{Vertex, Edge};
use models::errors::{GraphError, GraphResult};
use models::properties::{ PropertyValue, SerializableFloat, HashablePropertyMap };
use crate::graph_engine::graph_service::{ GraphService, GRAPH_SERVICE, initialize_graph_service, GraphEvent,}; 
use crate::graph_engine::traversal::TraverseExt;
use crate::graph_engine::pattern_match::node_matches_constraints; 
use crate::database::Database;
use crate::storage_engine::{GraphStorageEngine, StorageEngine};
use crate::query_exec_engine::query_exec_engine::QueryExecEngine;
use crate::config::{ StorageConfig, QueryResult };
use crate::query_parser::query_types::*;
use crate::query_parser::parser_zmq::trigger_async_graph_cleanup;
use crate::storage_engine::GraphOp; // Fixes E0433: use of undeclared type `GraphOp`

type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// Helper function (must be defined in or accessible to QueryExecEngine)
// Converts Result<QueryResult, GraphError> into Result<Value, GraphError>
// --- Assuming this helper function is defined/accessible within QueryExecEngine scope ---
// It handles the conversion from the internal QueryResult type to the public serde_json::Value type.
fn query_result_to_value(qr_result: GraphResult<QueryResult>) -> GraphResult<Value> {
    println!("===> query_result_to_value START");
    use serde_json::Value;
    // Note: GraphResult is Result<T, GraphError>
    let qr = qr_result?; // Propagate GraphError
    match qr {
        // Deserialize the JSON string inside QueryResult::Success
        QueryResult::Success(s) => serde_json::from_str(&s)
            .map_err(|e| GraphError::DeserializationError(format!("Failed to parse QueryResult string to Value: {}", e))),
        // Treat Null result as an empty JSON array, typical for no result set
        QueryResult::Null => Ok(Value::Array(Vec::new())),
        // Handle other possible results
        _ => Err(GraphError::InternalError("Unsupported QueryResult type encountered.".into())),
    }
}

async fn execute_chain_internal(
    clauses: Vec<CypherQuery>,
    db: &Database, // Pass the Database reference
    storage: Arc<dyn GraphStorageEngine + Send + Sync>, // Pass the Storage Arc
) -> GraphResult<Value> {
    println!("===> execute_chain_internal START");
    let mut final_value_result: GraphResult<Value> = Ok(Value::Array(Vec::new()));
    
    for clause in clauses.into_iter() {
        // Execute each clause sequentially.
        let clause_value_result = execute_cypher(clause, db, storage.clone()).await;
        
        if clause_value_result.is_err() {
            // If any clause fails, abort the chain and return the error.
            return clause_value_result;
        }
        // Keep track of the result from the last successful clause.
        final_value_result = clause_value_result;
    }

    final_value_result
}

// Add this helper function before execute_cypher
async fn get_vertex_by_internal_id_direct(
    graph_service: &GraphService,
    internal_id: i32,
) -> GraphResult<Option<models::Vertex>> {
    println!("===> get_vertex_by_internal_id_direct START");
    // Direct database lookup without going through Cypher to avoid recursion
    let all_vertices = graph_service.get_all_vertices().await?;
    
    Ok(all_vertices.into_iter().find(|v| {
        v.properties.get("id")
            .and_then(|prop| match prop {
                PropertyValue::Integer(val) => Some(*val as i32),
                PropertyValue::I32(val) => Some(*val),
                _ => None,
            })
            .map_or(false, |id| id == internal_id)
    }))
}

pub fn is_cypher(query: &str) -> bool {
    println!("===> is_cypher START");
    let cypher_keywords = ["MATCH", "CREATE", "SET", "RETURN", "DELETE", "MERGE", "REMOVE"];
    cypher_keywords.iter().any(|kw| query.trim().to_uppercase().starts_with(kw))
}

// =============================================================================
// COMPLETE REPLACEMENT FOR full_statement_parser AND HELPERS
// Place these functions in your cypher_parser.rs file
// =============================================================================

// Helper: Check if a character could start a Cypher keyword
fn is_cypher_keyword_start(c: char) -> bool {
    println!("===> is_cypher_keyword_start START");
    matches!(c, 'O' | 'o' | 'W' | 'w' | 'R' | 'r' | 'C' | 'c' | 'M' | 'm' | 'S' | 's' | 'D' | 'd')
}

/// Parse identifier (variable or property name)
fn parse_identifier(input: &str) -> IResult<&str, &str> {
    println!("===> parse_identifier START");
    recognize(
        pair(
            alt((alphanumeric1, tag("_"))),
            take_while(|c: char| c.is_alphanumeric() || c == '_')
        )
    ).parse(input)
}

/// Parser for Literals (Values)
fn parse_literal(input: &str) -> IResult<&str, Literal> {
    println!("===> parse_literal START");
    alt((
        map(tag("true"), |_| Literal::Boolean(true)),
        map(tag("false"), |_| Literal::Boolean(false)),
        map(tag("null"), |_| Literal::Null),
        // Improved string parser using .parse(input) syntax
        map(
            delimited(
                char('"'), 
                take_while(|c: char| c != '"'), 
                char('"')
            ),
            |s: &str| Literal::String(s.to_string())
        ),
    )).parse(input) // <--- Use .parse(input) instead of (input)
}

// Check if we're at the start of a new Cypher clause
// Helper function to check if we're at the start of a new Cypher clause
fn is_at_keyword_boundary(input: &str) -> bool {
    println!("===> is_at_keyword_boundary START");
    let trimmed = input.trim_start();
    
    // CRITICAL FIX: Relationship continuation, not a new clause
    if trimmed.starts_with('-') || trimmed.starts_with("<-") {
        return false;
    }
    
    let upper = trimmed.to_uppercase();
    
    // Check all keywords
    if upper.starts_with("WHERE")
        || upper.starts_with("RETURN")
        || upper.starts_with("CREATE")
        || upper.starts_with("ORDER BY")
        || upper.starts_with("ORDER")
        || upper.starts_with("SKIP")
        || upper.starts_with("LIMIT")
        || upper.starts_with("SET")
        || upper.starts_with("DETACH DELETE")
        || upper.starts_with("DELETE")
        || upper.starts_with("DETACH")
        || upper.starts_with("REMOVE")
        || upper.starts_with("MERGE")
        || upper.starts_with("WITH")
        || upper.starts_with("AND")
        || upper.starts_with("OR")
        || upper.starts_with("AS")
        || upper.starts_with("BY")
        || upper.starts_with("ON CREATE")
        || upper.starts_with("ON MATCH")
        || upper.starts_with("UNION ALL")
        || upper.starts_with("UNION")
        || upper.starts_with("FOREACH")
        || upper.starts_with("UNWIND")
        || upper.starts_with("CALL")
    {
        return true;
    }
    
    // Stop if we see a closing ) followed by a keyword
    if trimmed.starts_with(')') {
        let after_paren = trimmed[1..].trim_start().to_uppercase();
        if after_paren.starts_with("CREATE")
            || after_paren.starts_with("RETURN")
            || after_paren.starts_with("SET")
            || after_paren.starts_with("WHERE")
            || after_paren.starts_with("DELETE")
            || after_paren.starts_with("DETACH")
            || after_paren.starts_with("WITH")
            || after_paren.starts_with("MERGE")
        {
            return true;
        }
    }
    
    false
}

// FIXED: Consistent handling of delimiters and types for MPI transaction logging
fn parse_property_map(input: &str) -> IResult<&str, Vec<(String, Literal)>> {
    println!("===> parse_property_map START");
    type E<'a> = nom::error::Error<&'a str>;

    delimited(
        char('{'),
        delimited(
            multispace0,
            separated_list0(
                delimited(multispace0, char(','), multispace0),
                separated_pair(
                    // Parse the key (Identifier)
                    map(parse_identifier, |s: &str| s.to_string()), 
                    // Parse the ':' separator with surrounding whitespace
                    delimited(multispace0, char(':'), multispace0),
                    // FIX: Use parse_property_value or similar to get a Literal
                    // This ensures consistent handling for the graph of changes
                    cut(|i| parse_literal(i)) 
                )
            ),
            multispace0
        ),
        char('}')
    ).parse(input)
}

// NEW function to handle both literals and property references
fn parse_property_or_expression(input: &str) -> IResult<&str, Literal> {
    // First try to parse as a property reference like "source.mrn"
    if input.contains('.') {
        let (remaining, expr_str) = take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '.')(input)?;
        // For now, treat property references as String literals with special marker
        // In execution, you'll evaluate these expressions
        return Ok((remaining, Literal::String(format!("EXPR:{}", expr_str))));
    }
    
    // Otherwise parse as normal literal
    parse_literal(input)
}

// ============================================================================
// MATCH...SET PARSER
// ============================================================================

/// Parse MATCH ... SET ... [RETURN] statement
fn parse_match_set_relationship(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_match_set_relationship START");
    type E<'a> = nom::error::Error<&'a str>;

    map(
        tuple((
            |i| tag_no_case::<&str, &str, E>("MATCH")(i),
            |i| multispace1::<&str, E>(i),
            |i| parse_match_clause_patterns(i),
            opt(preceded(|i| multispace0::<&str, E>(i), |i| parse_where(i))),
            opt(preceded(|i| multispace0::<&str, E>(i), |i| parse_with_clause(i))),
            |i| multispace0::<&str, E>(i),
            |i| tag_no_case::<&str, &str, E>("SET")(i),
            |i| multispace1::<&str, E>(i),
            separated_list1(
                tuple((
                    |i| multispace0::<&str, E>(i), 
                    |i| char::<&str, E>(',')(i), 
                    |i| multispace0::<&str, E>(i)
                )),
                |i| parse_set_clause(i) 
            ),
            opt(preceded(|i| multispace0::<&str, E>(i), |i| tag_no_case::<&str, &str, E>("RETURN")(i))),
            opt(preceded(|i| multispace0::<&str, E>(i), |i| take_while::<_, &str, E>(|c| c != '\n' && c != ';')(i))),
        )),
        |(_, _, raw_patterns, where_clause, with_clause_query, _, _, _, set_clauses, _, _)| {
            
            // FIX: Handle the fact that parse_with_clause returns CypherQuery::Chain
            // We need to extract the logic from the chain if possible.
            // Note: If parse_with_clause returns an empty Chain vec![], 
            // the with_clause here will effectively be None.
            let with_clause: Option<ParsedWithClause> = with_clause_query.and_then(|q| {
                match q {
                    CypherQuery::Chain(_v) => {
                        // This is where you'd ideally extract a ParsedWithClause.
                        // For now, we return None to avoid type errors, 
                        // but your parse_with_clause needs to populate this.
                        None
                    },
                    _ => None, 
                }
            });

            // Map raw patterns to the Pattern type defined in your enum
            let match_patterns: Vec<Pattern> = raw_patterns
                .into_iter()
                .map(|(path_var, nodes, edges)| {
                    let transformed_nodes = nodes
                        .into_iter()
                        .map(|(var, label_opt, props)| {
                            let labels_vec = label_opt.map(|l| vec![l]).unwrap_or_default();
                            (var, labels_vec, props)
                        })
                        .collect();
                    (path_var, transformed_nodes, edges)
                })
                .collect();

            // Transform SET clauses to (String, String, Expression) 
            // for MPI Golden Record traceability in the graph of changes.
            let flattened_set_clauses: Vec<(String, String, Expression)> = set_clauses
                .into_iter()
                .flat_map(|(var, map_data): (String, HashMap<String, serde_json::Value>)| {
                    map_data.into_iter().map(move |(key, val)| {
                        (
                            var.clone(), 
                            key, 
                            Expression::Literal(CypherValue::from_json(val))
                        )
                    })
                })
                .collect();
            
            CypherQuery::MatchSet {
                match_patterns,
                with_clause, 
                where_clause,
                set_clauses: flattened_set_clauses,
            }
        },
    ).parse(input)
}

// Modify parse_match_create_relationship
fn parse_match_create_relationship(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_match_create_relationship START");
    // Helper to transform Raw patterns (Option label) to Execution patterns (Vec labels)
    let transform_patterns = |raw_pats: ParsedPatternsReturnType| -> ExecutionPatternsReturnType {
        raw_pats.into_iter().map(|(path_var, nodes, edges)| {
            let transformed_nodes = nodes.into_iter().map(|(var, label_opt, props)| {
                let labels_vec = match label_opt {
                    Some(l) => vec![l],
                    None => vec![],
                };
                (var, labels_vec, props)
            }).collect();
            (path_var, transformed_nodes, edges)
        }).collect()
    };

    let mut all_match_patterns: ExecutionPatternsReturnType = Vec::new();
    let mut current_input = input;
    
    // --- A. Consume MANDATORY First MATCH Clause ---
    let (input_after_match, _) = preceded(
        multispace0,
        alt((
            tag_no_case::<_, _, nom::error::Error<&str>>("OPTIONAL MATCH"),
            tag_no_case::<_, _, nom::error::Error<&str>>("MATCH"),
        ))
    ).parse(current_input)?;
    
    let (remainder, raw_patterns) = parse_content_after_match_keyword(input_after_match)?;
    all_match_patterns.extend(transform_patterns(raw_patterns));
    current_input = remainder;
    
    // --- B. Loop for ADDITIONAL MATCH Clauses ---
    loop {
        let (input_ws, _) = multispace0::<&str, nom::error::Error<&str>>.parse(current_input)?;
        
        let mut sub_match_parser = preceded(
            alt((
                tag_no_case::<_, _, nom::error::Error<&str>>("OPTIONAL MATCH"),
                tag_no_case::<_, _, nom::error::Error<&str>>("MATCH"),
            )),
            |i| parse_content_after_match_keyword(i),
        );

        match sub_match_parser.parse(input_ws) {
            Ok((remainder, raw_patterns)) => {
                all_match_patterns.extend(transform_patterns(raw_patterns));
                current_input = remainder;
            }
            Err(_) => {
                current_input = input_ws;
                break;
            }
        }
    }
    
    // --- C. Parse Optional WHERE Clause ---
    let (input_after_where, where_clause) = opt(preceded(
        multispace0,
        |i| parse_where(i)
    )).parse(current_input)?;
    current_input = input_after_where;
    
    // --- D. ✅ Parse Optional WITH Clause (MPI logic) ---
    let (input_after_with, with_clause_raw) = opt(preceded(
        multispace0,
        parse_with
    )).parse(current_input)?;
    
    // Convert WithClause → ParsedWithClause
    let with_clause: Option<ParsedWithClause> = with_clause_raw.map(|w| ParsedWithClause {
        items: Vec::new(),
        distinct: false,
        where_clause: Some(WhereClause { condition: w.condition }),
        order_by: Vec::new(),
        skip: None,
        limit: None,
    });
    current_input = input_after_with;

    // --- E. Parse the mandatory CREATE keyword ---
    // --- Update to allow MERGE or CREATE ---
    let (input, keyword) = preceded(
        multispace0,
        alt((
            tag_no_case("CREATE"),
            tag_no_case("MERGE") // Add support for MERGE transition
        ))
    ).parse(current_input)?;
    
    // --- F. Parse CREATE patterns and transform ---
    let (input, create_patterns) = parse_create_clause(input)?;
    
    // --- G. Optional SET clauses ---
    let (input, set_clauses_opt) = opt(preceded(
        tuple((multispace0, tag_no_case("SET"), multispace0)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            |i| parse_single_set_assignment(i),
        ),
    )).parse(input)?;
    
    // --- H. Optional RETURN clause ---
    let (input, _) = opt(preceded(multispace0, tag_no_case("RETURN"))).parse(input)?;
    let (input, _) = opt(preceded(multispace0, take_while(|c| c != '\n' && c != ';'))).parse(input)?;
    
    let set_clauses = set_clauses_opt.unwrap_or_default();
    
    if !set_clauses.is_empty() {
        Ok((input, CypherQuery::MatchCreateSet {
            match_patterns: all_match_patterns,
            where_clause,
            with_clause, // ✅ Now included
            create_patterns,
            set_clauses,
        }))
    } else {
        Ok((input, CypherQuery::MatchCreate {
            match_patterns: all_match_patterns,
            where_clause,
            with_clause, // ✅ Now included
            create_patterns,
        }))
    }
}

// Parse a single pattern (node or node-relationship-node chain)
/// Parse **one** complete pattern:
/// (a)-[:KNOWS]->(b) or
/// (a)-[:KNOWS]-(b) or
/// (a)<-[:KNOWS]-(b) or
/// (a)-[r:KNOWS*0..2]-(b) etc.
/// Stops **only** when it hits a real clause keyword (RETURN, WHERE, CREATE…)
fn parse_single_pattern(input: &str) -> IResult<&str, Pattern> {
    println!("===> parse_single_pattern START, input length: {}", input.len());
    
    let mut all_nodes: Vec<NodePattern> = Vec::new();
    let mut all_relationships: Vec<RelPattern> = Vec::new();
    
    // 1. Parse the starting node
    // Using parse_node as defined in your grammar
    let (mut current_input, node_a) = parse_node(input)?;
    all_nodes.push(node_a);
    
    // 2. Parse (RELATIONSHIP + NODE) pairs
    loop {
        // Skip whitespace to see if there's a relationship connector (-, <, [)
        let (after_space, _) = multispace0::<&str, nom::error::Error<&str>>(current_input)
            .unwrap_or((current_input, ""));

        // CRITICAL FIX: Check for relationship connector FIRST, before boundary check
        if after_space.is_empty() {
            break;
        }
        
        // If this looks like the start of a relationship, do NOT treat it as a boundary
        if after_space.starts_with('-') || after_space.starts_with('<') {
            // Proceed to relationship parsing - skip boundary check entirely
        } else {
            // Only check for keyword boundary or other terminators if NOT a relationship
            if is_at_keyword_boundary(after_space) || after_space.starts_with(',') || after_space.starts_with(';') {
                break;
            }
        }

        // 3. Parse the REL + NODE pair
        // nom 8: we use .parse() on the sequence. 
        // We MUST advance current_input to next_input to consume the string.
        match nom::sequence::pair(parse_relationship, parse_node).parse(after_space) {
            Ok((next_input, (rel, node))) => {
                all_relationships.push(rel);
                all_nodes.push(node);
                // SUCCESS: Advance the pointer to the end of the node pattern
                current_input = next_input;
            }
            Err(_) => {
                // If parsing fails (e.g., a trailing dash with no node), stop path parsing
                break;
            }
        }
    }
    
    let num_nodes = all_nodes.len();
    let num_rels = all_relationships.len();
    
    println!("===> parse_single_pattern END – {} nodes, {} rels", num_nodes, num_rels);
    
    // Build the structural pattern used for MPI logical resolution
    let result_pattern = build_pattern_from_elements(all_nodes, all_relationships);
    
    Ok((current_input, result_pattern))
}

fn parse_return_item_expression(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_return_item_expression START, input length: {}", input.len());
    
    // First, try to parse an expression
    let (remaining, expr) = parse_expression(input)?;
    
    // After parsing the expression, check if the next token is a RETURN clause keyword
    let (remaining_trimmed, _) = multispace0.parse(remaining)?;
    
    // Check if we're at a keyword boundary that should terminate the return item
    if is_at_keyword_boundary(remaining_trimmed) {
        println!("===> parse_return_item_expression: stopped at keyword boundary");
        return Ok((remaining, expr));
    }
    
    Ok((remaining, expr))
}

fn parse_return_items(input: &str) -> IResult<&str, Vec<String>> {
    println!("===> parse_return_items START");
    // 1. Parse 'RETURN' keyword
    let (input, _) = tag_no_case("RETURN").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 2. Define the parser combinator chain
    let mut return_items_parser = separated_list1(
        // Separator: comma ',' with optional whitespace around it
        delimited(multispace0, char(','), multispace0),
        // Item: A single variable/identifier
        take_while1(|c: char| c.is_alphanumeric() || c == '_')
    );

    // 3. Execute the parser and map the resulting Vec<&str> to Vec<String>
    return_items_parser.parse(input) // <-- FIX: Explicitly call .parse(input)
        .map(|(i, vars)| (i, vars.into_iter().map(|s| s.to_string()).collect()))
}

// Add this to lib/src/query_parser/cypher_parser.rs

fn parse_create_statement(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_create_statement START");
    
    // 1. Parse the CREATE patterns
    let (input, patterns) = parse_create_clause(input)?;
    
    // 2. Consume optional whitespace
    let (input, _) = multispace0.parse(input)?;
    
    // 3. Parse the RETURN clause (using parse_return_items defined previously)
    let (input, return_items_opt) = opt(parse_return_items).parse(input)?;
    let return_items = return_items_opt.unwrap_or_default();
    
    // 4. Consume optional semicolon and trailing space
    let (input, _) = opt(char(';')).parse(input)?;
    let (input, final_input) = multispace0.parse(input)?;

    // Construct the final CypherQuery using the NEW variant
    Ok((final_input, CypherQuery::CreateStatement { // <-- CORRECTED TO NEW VARIANT
        patterns,
        return_items,
    }))
}

fn parse_string_literal(input: &str) -> IResult<&str, &str> {
    println!("===> parse_string_literal START");
    alt((
        // Single quotes
        delimited(
            char('\''),
            // recognize() captures the entire span including escapes
            recognize(many0(alt((
                tag("\\'"),
                tag("\\\\"),
                tag("\\n"),
                recognize(none_of("'\\"))
            )))),
            char('\'')
        ),
        // Double quotes
        delimited(
            char('"'),
            recognize(many0(alt((
                tag("\\\""),
                tag("\\\\"),
                tag("\\n"),
                recognize(none_of("\"\\"))
            )))),
            char('"')
        ),
    )).parse(input)
}

fn parse_number_literal(input: &str) -> IResult<&str, Value> {
    println!("===> parse_number_literal START");
    map(
        nom::number::complete::double,
        |n| {
            if n.fract() == 0.0 && n >= (i64::MIN as f64) && n <= (i64::MAX as f64) {
                json!(n as i64)
            } else {
                json!(n)
            }
        }
    ).parse(input)
}

fn parse_optional_match(input: &str) -> IResult<&str, ()> {
    println!("===> parse_optional_match START");
    let (input, _) = tag("OPTIONAL MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    // Skip the pattern (we don't need to parse it fully for visualization)
    let (input, _) = take_while1(|c| c != '\n' && c != '\r' && c != 'R').parse(input)?;
    Ok((input, ()))
}

/// Updated to ensure timestamp() and UUIDs are handled correctly in properties
// lib/src/query_parser/cypher_parser.rs

fn parse_property_value(input: &str) -> IResult<&str, Value> {
    println!("===> parse_property_value START");
    alt((
        // 1. Quoted String literals - ALWAYS use these for UUIDs in Cypher strings
        map(parse_string_literal, |s: &str| Value::String(s.to_string())),
       
        // 2. Signed Numbers - Catch the -1369861507 Patient IDs
        parse_signed_number_value,
       
        // 3. Booleans and Null
        map(tag_no_case("true"), |_| Value::Bool(true)),
        map(tag_no_case("false"), |_| Value::Bool(false)),
        map(tag_no_case("null"), |_| Value::Null),
       
        // ADD THIS: Support for source.mrn
        map(parse_property_access, |s| Value::String(s)), // Or Value::Reference if you have it
        // 4. IMPROVED: Unquoted UUIDs/Identifiers
        // We allow starting digits, but only if it's NOT a pure number
        map(
            verify(
                take_while1(|c: char| c.is_alphanumeric() || c == '-' || c == '_'),
                |s: &str| {
                    // Reject if it's a pure signed integer (let parser #2 handle those)
                    // But allow if it contains non-digit chars (like hex a-f or hyphens)
                    let is_pure_number = s.parse::<i64>().is_ok();
                    !is_pure_number
                }
            ), 
            |s: &str| Value::String(s.to_string())
        ),
       
        // 5. Functions
        map(
            tuple((parse_identifier, char('('), char(')'))),
            |(func, _, _)| {
                if func.to_lowercase() == "timestamp" {
                    Value::String(Utc::now().to_rfc3339())
                } else {
                    Value::String(format!("{}()", func))
                }
            }
        ),
    )).parse(input)
}

fn parse_property(input: &str) -> IResult<&str, (String, Value)> {
    println!("===> parse_property START");
    let (input, (key, _, value)) = tuple((
        parse_identifier,
        preceded(multispace0, char(':')),
        preceded(multispace0, parse_property_value), // Must handle Parameter variant
    )).parse(input)?;
    
    Ok((input, (key.to_string(), value)))
}

fn parse_properties(input: &str) -> IResult<&str, HashMap<String, Value>> {
    println!("===> parse_properties START");
    map(
        delimited(
            preceded(multispace0, char('{')),
            opt(separated_list1(
                preceded(multispace0, char(',')), // This is the separator
                preceded(multispace0, parse_property) // This is the item
            )),
            preceded(multispace0, char('}')),
        ),
        |props| props.unwrap_or_default().into_iter().collect(),
    ).parse(input)
}

fn parse_node(input: &str) -> IResult<&str, NodePattern> {
    println!("===> parse_node START");
    type E<'a> = nom::error::Error<&'a str>;

    let (input, _) = multispace0::<&str, E>.parse(input)?;
    let (input, _) = char::<&str, E>('(').parse(input)?;
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    
    // Parse optional variable name
    let (input, var_opt) = opt(
        take_while1::<_, &str, E>(|c: char| c.is_alphanumeric() || c == '_')
    ).parse(input)?;
    
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    
    // Parse zero or more labels: :Label1:Label2
    let (input, labels) = many0(
        preceded(
            char::<&str, E>(':'),
            map(
                take_while1::<_, &str, E>(|c: char| c.is_alphanumeric() || c == '_'),
                |s: &str| s.to_string()
            )
        )
    ).parse(input)?;
    
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    
    // Parse optional Property Map
    let (input, props) = if input.trim_start().starts_with('{') {
        let (next_input, _) = multispace0::<&str, E>.parse(input)?;
        delimited(
            char('{'),
            map(
                opt(separated_list1(
                    preceded(multispace0::<&str, E>, char(',')),
                    preceded(multispace0::<&str, E>, parse_property) // Assumes parse_property exists
                )),
                |props_list| props_list.unwrap_or_default().into_iter().collect::<HashMap<String, Value>>()
            ),
            preceded(multispace0::<&str, E>, char('}'))
        ).parse(next_input)?
    } else {
        (input, HashMap::new())
    };
    
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    let (input, _) = char::<&str, E>(')').parse(input)?;
    let (input, _) = multispace0::<&str, E>.parse(input)?;

    Ok((input, (
        var_opt.map(|s| s.to_string()),
        labels,
        props
    )))
}

fn parse_multiple_nodes(input: &str) -> IResult<&str, Vec<NodePattern>> {
    println!("===> parse_multiple_nodes START");
    separated_list1(
        tuple((multispace0, char(','), multispace0)),
        |i| parse_node(i) // Use closure for nom 8 trait stability
    ).parse(input)
}

fn parse_relationship(input: &str) -> IResult<&str, RelPattern> {
    println!("===> parse_relationship START, input: '{}'", &input[..input.len().min(50)]);
    
    type E<'a> = nom::error::Error<&'a str>;
    use nom::{branch::alt, combinator::map, Parser};
    
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    
    // Try full relationship pattern first (with brackets): -[...]-> or <-[...]-
    // Then try shorthand patterns: --> or -- or <--
    alt((
        parse_relationship_full,
        parse_relationship_shorthand,
    )).parse(input)
}

fn parse_relationship_full(input: &str) -> IResult<&str, RelPattern> {
    println!("===> parse_relationship_full START");
    
    type E<'a> = nom::error::Error<&'a str>;
    use nom::Parser;
    
    // 1. Parse opening direction prefix: <-[ or -[
    let (input, has_left_arrow) = opt(tag::<_, _, E>("<")).parse(input)?;
    let (input, _) = tag::<_, _, E>("-[").parse(input)?;
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    
    // 2. Optional variable name
    let (input, var_opt) = opt(
        take_while1::<_, &str, E>(|c: char| c.is_alphanumeric() || c == '_')
    ).parse(input)?;
    
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    
    // 3. Optional Type (e.g., :TYPE)
    let (input, type_opt) = opt(preceded(
        char::<&str, E>(':'),
        take_while1::<_, &str, E>(|c: char| c.is_alphanumeric() || c == '_')
    )).parse(input)?;
    
    // 4. Optional Variable Length (e.g., *1..3)
    let (input, range_opt) = opt(parse_range).parse(input)?;
    
    // 5. Optional Properties {...}
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    let (input, props_vec) = if input.starts_with('{') {
        let (next_input, raw_props) = parse_property_map(input)?;
        (next_input, raw_props)
    } else {
        (input, Vec::new())
    };
    
    // 6. Convert Literal to serde_json::Value
    let props: HashMap<String, Value> = props_vec
        .into_iter()
        .map(|(k, v)| {
            let val = match v {
                Literal::String(s) => Value::from(s),
                Literal::Integer(i) => Value::from(i),
                Literal::Number(n) => {
                    serde_json::Number::from_f64(n)
                        .map(Value::Number)
                        .unwrap_or(Value::Null)
                },
                Literal::Float(f) => Value::from(f.0), 
                Literal::Boolean(b) => Value::from(b),
                Literal::Null => Value::Null,
            };
            (k, val)
        })
        .collect();
    
    // 7. Parse closing ']'
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    let (input, _) = char::<&str, E>(']').parse(input)?;
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    
    // 8. Parse direction suffix (MUST come after ']')
    let (input, direction) = if has_left_arrow.is_some() {
        // Must be inbound: <-[...]-
        let (input, _) = tag("-").parse(input)?;
        (input, Some(false))
    } else {
        // Outbound -> or undirected -
        alt((
            map(tag("->"), |_| Some(true)),  // outbound
            map(tag("-"), |_| None),         // undirected
        )).parse(input)?
    };
    
    println!("===> parse_relationship_full END: var={:?}, type={:?}, direction={:?}", var_opt, type_opt, direction);
    
    Ok((input, (
        var_opt.map(|s| s.to_string()),
        type_opt.map(|s| s.to_string()),
        range_opt,
        props,
        direction,
    )))
}

fn parse_relationship_shorthand(input: &str) -> IResult<&str, RelPattern> {
    println!("===> parse_relationship_shorthand START");
    
    type E<'a> = nom::error::Error<&'a str>;
    use nom::{branch::alt, combinator::map, Parser};
    
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    
    // Parse shorthand patterns: --> or <-- or --
    let (input, direction) = alt((
        map(tag::<_, _, E>("-->"), |_| Some(true)),   // outbound
        map(tag::<_, _, E>("<--"), |_| Some(false)),  // inbound
        map(tag::<_, _, E>("--"), |_| None),          // undirected
    )).parse(input)?;
    
    println!("===> parse_relationship_shorthand END: direction={:?}", direction);
    
    // Return empty relationship with just direction
    Ok((input, (
        None,                      // no variable
        None,                      // no type
        None,                      // no range
        HashMap::new(),            // no properties
        direction,
    )))
}

fn parse_range(input: &str) -> IResult<&str, (Option<u32>, Option<u32>)> {
    println!("===> parse_range START");
    type E<'a> = nom::error::Error<&'a str>;

    // 1. Must start with '*'
    let (input, _) = char::<&str, E>('*').parse(input)?;

    // 2. Try parsing "min..max", "min..", "..max"
    // We define the parser inline to avoid multiple mutable borrow conflicts
    let range_result = separated_pair(
        opt(map_res(digit1::<&str, E>, |s| s.parse::<u32>())),
        tag::<_, _, E>(".."),
        opt(map_res(digit1::<&str, E>, |s| s.parse::<u32>()))
    ).parse(input);

    if let Ok((after_range, (min, max))) = range_result {
        Ok((after_range, (min, max)))
    } else {
        // 3. Fallback: Parse a single fixed value (e.g., *3) or just '*'
        let (input, val) = opt(map_res(digit1::<&str, E>, |s| s.parse::<u32>())).parse(input)?;
        
        // If it's *3, both are Some(3). If it's just *, both are None.
        Ok((input, (val, val)))
    }
}

fn parse_create_edge_between_existing(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_create_edge_between_existing START");
    let (input, _) = tag("CREATE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = multispace0(input)?;

    let (input, source_var) = delimited(
        char('('),
        take_while1(|c: char| c.is_alphanumeric() || c == '_'),
        char(')')
    ).parse(input)?;

    let (input, _) = tag("-[:")(input)?;
    let (input, rel_type) = take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)?;

    let (input, properties) = if input.starts_with('{') {
        delimited(
            preceded(multispace0, char('{')),
            map(
                opt(separated_list1(
                    preceded(multispace0, char(',')),
                    preceded(multispace0, parse_property)
                )),
                |props| props.unwrap_or_default().into_iter().collect()
            ),
            preceded(multispace0, char('}'))
        ).parse(input)?
    } else {
        (input, HashMap::new())
    };

    let (input, _) = tag("]->(")(input)?;
    let (input, target_var) = take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)?;
    let (input, _) = char(')')(input)?;
    let (input, _) = multispace0(input)?;

    Ok((
        input,
        CypherQuery::CreateEdgeBetweenExisting {
            source_var: source_var.to_string(),
            rel_type: rel_type.to_string(),
            properties,
            target_var: target_var.to_string(),
        },
    ))
}

fn parse_create_nodes(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_create_nodes START");
    let (input, _) = tag("CREATE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, nodes) = parse_multiple_nodes(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    if input.starts_with('-') || input.starts_with('<') {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }
    
    let node_data: Vec<(String, HashMap<String, Value>)> = nodes
        .into_iter()
        .map(|(var, labels, props)| {
            // labels is Vec<String>. Get the first one, or fallback.
            let actual_label = labels
                .into_iter()
                .next()
                .unwrap_or_else(|| var.clone().unwrap_or_else(|| "Node".to_string()));
            (actual_label, props)
        })
        .collect();

    Ok((input, CypherQuery::CreateNodes { nodes: node_data }))
}

fn parse_create_index(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_create_index START");
    let (input, (_, _, _, _, _, _, node_pattern, _, _, _, prop_pattern)) = tuple((
        tag("CREATE"),
        multispace1,
        tag("INDEX"),
        multispace1,
        tag("FOR"),
        multispace1,
        parse_node,
        multispace1,
        tag("ON"),
        multispace1,
        parse_property_pattern,
    )).parse(input)?;
    
    let (_, labels, _) = node_pattern;
    
    // labels is Vec<String>. We take the first one or default to empty string.
    let final_label = labels.into_iter().next().unwrap_or_default();

    let property_names: Vec<String> = prop_pattern.iter()
        .map(|prop| {
            prop.split('.').nth(1).unwrap_or(prop).to_string()
        })
        .collect();
    
    Ok((input, CypherQuery::CreateIndex {
        label: final_label,
        properties: property_names,
    }))
}

fn parse_property_pattern(input: &str) -> IResult<&str, Vec<String>> {
    println!("===> parse_property_pattern START");
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    let (input, first_prop) = parse_property_access(input)?;
    
    let (input, additional_props) = many0(
        preceded(
            tuple((multispace0, char(','), multispace0)),
            parse_property_access
        )
    ).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;
    
    let mut props = vec![first_prop];
    props.extend(additional_props);
    
    Ok((input, props))
}

// Parse Complex Mutation
pub fn parse_complex_mutation(input: &str) -> IResult<&str, CypherQuery> {
    // 1. Parse all MATCH clauses
    // If parse_match_clause returns the patterns directly, match_clauses is Vec<Vec<Pattern>>
    let (input, match_clauses) = many1(parse_match_clause).parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    let mut all_match_patterns = Vec::new();
    
    for m in match_clauses {
        // 'm' is already the Vec of patterns according to the error message.
        // We just extend our flat list with it.
        all_match_patterns.extend(m); 
    }

    // 2. Decide final action
    let trimmed = input.trim_start().to_uppercase();
    
    if trimmed.starts_with("MERGE") {
        let (input, merge_query) = parse_merge_statement(input)?;
        
        if let CypherQuery::Merge { 
            patterns: action_patterns, 
            where_clause, 
            with_clause, 
            on_create_set, 
            on_match_set, 
            .. 
        } = merge_query {
            let mut combined = all_match_patterns;
            combined.extend(action_patterns);
            
            Ok((input, CypherQuery::Merge {
                patterns: combined,
                where_clause,
                with_clause,
                on_create_set,
                on_match_set,
                return_clause: None, 
            }))
        } else {
            Ok((input, merge_query))
        }
    } else {
        let (input, create_patterns) = parse_create_clause(input)?;
        
        Ok((input, CypherQuery::MatchCreate {
            match_patterns: all_match_patterns,
            where_clause: None,
            with_clause: None,
            create_patterns,
        }))
    }
}

// ============================================================================
// WHERE CLAUSE PARSING - Enhanced
// ============================================================================


/// Updated to return a single String "var.prop"
pub fn parse_property_access(input: &str) -> IResult<&str, String> {
    println!("===> parse_property_access START");
    let (input, _) = multispace0.parse(input)?;
    let (input, var) = parse_identifier.parse(input)?;
    let (input, _) = char('.').parse(input)?;
    let (input, prop) = parse_identifier.parse(input)?;
    
    Ok((input, format!("{}.{}", var, prop)))
}

/// Now this function will compile because the map matches the return type
fn parse_where_condition(input: &str) -> IResult<&str, String> {
    println!("===> parse_where_condition START");
    alt((
        // Function-based condition
        map(
            tuple((
                parse_function_call,
                delimited(multispace0, char('='), multispace0),
                parse_value,
            )),
            |(func, _, val)| {
                format!("{}({}) = {:?}", func.0, func.1, val)
            }
        ),
        // Property-based condition
        map(
            tuple((
                parse_property_access, // Now correctly returns String
                delimited(multispace0, char('='), multispace0),
                parse_value,
            )),
            |(prop_path, _, val)| {
                format!("{} = {:?}", prop_path, val)
            }
        ),
        map(take_until_keyword, |s: &str| s.to_string()),
    )).parse(input)
}

fn parse_create_complex_pattern(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_create_complex_pattern START");

    let (input, _) = tag("CREATE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    let (input, first_node) = parse_node(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Case: CREATE (n) where it's just a single node, not a path
    if !input.starts_with('-') && !input.starts_with('<') {
        let (var, labels, props) = first_node;
        
        // labels is Vec<String>. Get the first one or fallback.
        let label = labels
            .into_iter()
            .next()
            .unwrap_or_else(|| var.clone().unwrap_or_else(|| "Node".to_string()));

        return Ok((input, CypherQuery::CreateNode {
            label,
            properties: props,
        }));
    }

    let mut nodes = vec![first_node];
    let mut relationships = Vec::new();
    let mut remaining = input;

    loop {
        match parse_relationship(remaining) {
            Ok((rest, (_rel_var, rel_type, rel_props, _cardinality, _dir))) => {
                relationships.push((_rel_var, rel_type, rel_props, _cardinality, _dir));
                let (rest, _) = multispace0.parse(rest)?;

                match parse_node(rest) {
                    Ok((rest, node)) => {
                        nodes.push(node);
                        let (rest, _) = multispace0.parse(rest)?;
                        remaining = rest;

                        if !remaining.starts_with('-') && !remaining.starts_with('<') {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            Err(_) => break,
        }
    }

    // Ensure the return type matches CypherQuery::CreateComplexPattern
    // If that variant expects Vec<(Option<String>, Vec<String>, HashMap)>, 
    // it will now match because parse_node returns Vec<String> labels.
    Ok((remaining, CypherQuery::CreateComplexPattern { nodes, relationships }))
}

fn parse_create_pattern(input: &str) -> IResult<&str, Pattern> {
    println!("===> parse_create_pattern START");
    let (input, path_var) = opt(terminated(
        parse_identifier,
        tuple((multispace0, char('='), multispace0))
    )).parse(input)?;

    // ✅ Use parse_node (returns NodePattern), NOT parse_create_node (returns CypherQuery)
    let (input, first_node) = parse_node(input)?;
    let (input, _) = multispace0.parse(input)?;

    let mut nodes = vec![first_node]; // mut is required — you push in the loop
    let mut rels = Vec::new();
    let mut remaining = input;

    loop {
        match parse_relationship(remaining) {
            Ok((rest, rel)) => {
                rels.push(rel);
                let (rest, _) = multispace0.parse(rest)?;
                match parse_node(rest) { // ✅ Again, use parse_node
                    Ok((rest, node)) => {
                        nodes.push(node);
                        let (rest, _) = multispace0.parse(rest)?;
                        remaining = rest;
                    }
                    Err(_) => break,
                }
            }
            Err(_) => break,
        }
    }

    Ok((remaining, (path_var.map(String::from), nodes, rels)))
}

fn parse_create_node(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_create_node START");
    let (input, _) = tag("CREATE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, node) = parse_node(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    if input.starts_with('-') || input.starts_with('<') {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }
    
    let (var, labels, props) = node;

    // labels is now Vec<String>. We try to get the first label,
    // otherwise fall back to the variable name, otherwise "Node".
    let final_label = labels
        .into_iter()
        .next() // Get the first String from the Vec
        .unwrap_or_else(|| {
            var.clone().unwrap_or_else(|| "Node".to_string())
        });

    Ok((input, CypherQuery::CreateNode {
        label: final_label,
        properties: props,
    }))
}

// Required to parse key=value assignments for SET and MERGE clauses: `n.name = 'Joe'`
// Returns (String, String, Value) because that's what the rest of the code expects
fn parse_set_assignment_tuple(input: &str) -> IResult<&str, (String, String, Value)> {
    println!("===> parse_set_assignment_tuple START");
    map(
        tuple((
            // Variable (e.g., n)
            parse_identifier, 
            // .Property (e.g., .name)
            preceded(char('.'), parse_identifier), 
            // = Value
            preceded(tuple((multispace0::<&str, NomErrorType<&str>>, tag("="), multispace0::<&str, NomErrorType<&str>>)), parse_property), 
        )),
        |(var, prop_name, (_, value))| (var.to_string(), prop_name.to_string(), value),
    ).parse(input)
}

pub fn parse_merge(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_merge START");
    type E<'a> = nom::error::Error<&'a str>;

    // Use full_statement_parser logic but for MERGE
    let mut all_patterns: Vec<Pattern> = Vec::new();
    let mut on_create_set: Vec<(String, String, Expression)> = Vec::new();
    let mut on_match_set: Vec<(String, String, Expression)> = Vec::new();
    let mut input_current = input;
    let mut captured_where: Option<WhereClause> = None;
    let mut captured_with: Option<ParsedWithClause> = None;
    let mut captured_return: Option<ReturnClause> = None;

    // Parse MERGE keyword
    let (input_after_clause, _) = preceded(
        multispace0,
        tag_no_case::<_, _, E>("MERGE")
    ).parse(input_current)?;
    input_current = input_after_clause;

    // Parse patterns
    match parse_match_clause_patterns(input_current) {
        Ok((input_after_patterns, parsed_patterns)) => {
            let converted: Vec<_> = parsed_patterns.into_iter().map(|(id, nodes, rels)| {
                
                // 1. Fix Nodes: Convert labels to Vec<String>
                let n_fixed: Vec<(Option<String>, Vec<String>, HashMap<String, Value>)> = nodes.into_iter()
                    .map(|(v, l, p)| (v, l.into_iter().collect::<Vec<String>>(), p))
                    .collect();
                
                // 2. Fix Relationships: Match the 5-element tuple expected by all_patterns
                // According to the error, it expects: 
                // (Var, Type, Range, Props, IsDirected)
                let r_fixed: Vec<(
                    Option<String>, 
                    Option<String>, 
                    Option<(Option<u32>, Option<u32>)>, 
                    HashMap<String, Value>, 
                    Option<bool>
                )> = rels.into_iter().map(|r| {
                    // Adjust this mapping based on what your parse_match_clause_patterns returns.
                    // If it returns 4 elements, we likely need to add the default Range or Direction.
                    (r.0, r.1, None, r.3, Some(true)) 
                }).collect();

                (id, n_fixed, r_fixed)
            }).collect();

            all_patterns.extend(converted);
            input_current = input_after_patterns;
        }
        Err(e) => return Err(e),
    }

    // Parse optional WHERE
    let (input_after_where, where_opt) = opt(preceded(
        multispace0,
        parse_where
    )).parse(input_current)?;
    captured_where = where_opt;
    input_current = input_after_where;

    // Parse optional WITH
    let (input_after_with, with_clause_raw) = opt(preceded(
        multispace0,
        parse_with_full, 
    )).parse(input_current)?;
    captured_with = with_clause_raw;
    input_current = input_after_with;

    // Parse ON CREATE / ON MATCH
    loop {
        let (input_ws, _) = multispace0.parse(input_current)?;
        let trimmed = input_ws.trim_start();
        if trimmed.is_empty() {
            input_current = input_ws;
            break;
        }

        if trimmed.to_uppercase().starts_with("ON CREATE") {
            let (input_after, _) = tag_no_case("ON CREATE").parse(input_ws)?;
            let (input_after, _) = preceded(multispace0, tag_no_case("SET")).parse(input_after)?;
            let (input_after_list, clauses) = preceded(
                multispace0,
                separated_list1(
                    tuple((multispace0, char(','), multispace0)),
                    parse_single_set_assignment
                )
            ).parse(input_after)?;
            on_create_set.extend(clauses);
            input_current = input_after_list;
        } else if trimmed.to_uppercase().starts_with("ON MATCH") {
            let (input_after, _) = tag_no_case("ON MATCH").parse(input_ws)?;
            let (input_after, _) = preceded(multispace0, tag_no_case("SET")).parse(input_after)?;
            let (input_after_list, clauses) = preceded(
                multispace0,
                separated_list1(
                    tuple((multispace0, char(','), multispace0)),
                    parse_single_set_assignment
                )
            ).parse(input_after)?;
            on_match_set.extend(clauses);
            input_current = input_after_list;
        } else {
            input_current = input_ws;
            break;
        }
    }

    // Parse RETURN clause
    let (input_after_return, return_opt) = opt(preceded(
        multispace0,
        parse_return_clause_as_struct 
    )).parse(input_current)?;
    captured_return = return_opt;
    input_current = input_after_return;

    let (input_final, _) = opt(preceded(multispace0, alt((tag(";"), multispace1)))).parse(input_current)?;

    Ok((input_final, CypherQuery::Merge { 
        patterns: all_patterns, 
        where_clause: captured_where,
        with_clause: captured_with,
        on_create_set, 
        on_match_set,
        return_clause: captured_return,
    }))
}

fn parse_null_operator(input: &str) -> IResult<&str, UnaryOp> {
    // We use .parse(input) instead of calling the alt block as a function
    alt((
        map(preceded(multispace1, tag_no_case("IS NOT NULL")), |_| UnaryOp::IsNotNull),
        map(preceded(multispace1, tag_no_case("IS NULL")), |_| UnaryOp::IsNull),
    )).parse(input) 
}

pub fn evaluate_expression(
    expr: &Expression,
    context: &EvaluationContext,
) -> StdResult<CypherValue, GraphError> {
    println!("===> evaluate_expression: {:?}", match expr {
        Expression::LabelPredicate { variable, label } => format!("LabelPredicate({}:{})", variable, label),
        Expression::Binary { op, .. } => format!("Binary({:?})", op),
        Expression::PropertyComparison { variable, property, operator, .. } => format!("PropertyComparison({}.{} {} ...)", variable, property, operator),
        _ => format!("{:?}", expr),
    });
    match expr {
        Expression::StartsWith { variable, property, prefix } => {
            let val = get_property_for_expression(context, variable, property)?;
            match val {
                CypherValue::String(s) => Ok(CypherValue::Bool(s.starts_with(prefix))),
                _ => Ok(CypherValue::Bool(false)),
            }
        },

        Expression::EndsWith { variable, property, suffix } => {
            let val = get_property_for_expression(context, variable, property)?;
            match val {
                CypherValue::String(s) => Ok(CypherValue::Bool(s.ends_with(suffix))),
                _ => Ok(CypherValue::Bool(false)),
            }
        },

        Expression::Contains { variable, property, substring } => {
            let val = get_property_for_expression(context, variable, property)?;
            match val {
                CypherValue::String(s) => Ok(CypherValue::Bool(s.contains(substring))),
                _ => Ok(CypherValue::Bool(false)),
            }
        },
// --- Added Missing NOT Arm ---
        Expression::Not(inner_expr) => {
            let val = evaluate_expression(inner_expr, context)?;
            match val {
                CypherValue::Bool(b) => Ok(CypherValue::Bool(!b)),
                CypherValue::Null => Ok(CypherValue::Null), // Three-valued logic: NOT Null is Null
                _ => Err(GraphError::QueryExecutionError("NOT operator requires a boolean expression".into())),
            }
        },

        // --- Added Missing Regex Arm ---
        Expression::Regex { variable, property, pattern } => {
            let val = get_property_for_expression(context, variable, property)?;
            if let CypherValue::String(s) = val {
                // Assuming you have a regex crate or similar utility
                let re = regex::Regex::new(pattern)
                    .map_err(|e| GraphError::QueryExecutionError(format!("Invalid regex: {}", e)))?;
                Ok(CypherValue::Bool(re.is_match(&s)))
            } else {
                Ok(CypherValue::Bool(false))
            }
        },

        // --- Added Missing FunctionComparison Arm ---
        // This is used for optimized calls like ID(n) = 'value'
        Expression::FunctionComparison { function, argument, operator, value } => {
            let func_expr = Expression::FunctionCall { 
                name: function.clone(), 
                args: vec![Expression::Variable(argument.clone())] 
            };
            let left_val = evaluate_expression(&func_expr, context)?;
            let right_val = CypherValue::from_json(value.clone());
            
            evaluate_comparison(&left_val, operator, &right_val)
        },
        Expression::Literal(val) => Ok(val.clone()),
        
        Expression::Variable(name) => context.variables.get(name)
            .cloned()
            .ok_or_else(|| GraphError::QueryExecutionError(format!("Variable not found: {}", name))),

        Expression::Property(access) => match access {
            PropertyAccess::Vertex(var, prop) => {
                if let Some(val) = context.variables.get(var) {
                    match val {
                        CypherValue::Vertex(v) => {
                            if prop == "id" {
                                return Ok(CypherValue::String(v.id.to_string()));
                            }
                            if prop == "label" {
                                return Ok(CypherValue::String(v.label.to_string()));
                            }
                            match v.properties.get(prop) {
                                Some(prop_val) => Ok(property_value_to_cypher(prop_val)),
                                None => Ok(CypherValue::Null),
                            }
                        }
                        CypherValue::Map(map) => {
                            match map.get(prop) {
                                Some(val) => Ok(val.clone()),
                                None => Ok(CypherValue::Null),
                            }
                        }
                        _ => Err(GraphError::EvaluationError(format!("Variable '{}' is not a vertex", var)))
                    }
                } else {
                    Err(GraphError::EvaluationError(format!("Vertex variable '{}' not found", var)))
                }
            }
            PropertyAccess::Edge(var, prop) => {
                if let Some(val) = context.variables.get(var) {
                    match val {
                        CypherValue::Edge(e) => {
                            if prop == "id" {
                                return Ok(CypherValue::String(e.id.to_string()));
                            }
                            if prop == "label" || prop == "type" {
                                return Ok(CypherValue::String(e.label.to_string()));
                            }
                            match e.properties.get(prop) {
                                Some(prop_val) => Ok(property_value_to_cypher(prop_val)),
                                None => Ok(CypherValue::Null),
                            }
                        }
                        CypherValue::Map(map) => {
                            match map.get(prop) {
                                Some(val) => Ok(val.clone()),
                                None => Ok(CypherValue::Null),
                            }
                        }
                        _ => Err(GraphError::EvaluationError(format!("Variable '{}' is not an edge", var)))
                    }
                } else {
                    Err(GraphError::EvaluationError(format!("Edge variable '{}' not found", var)))
                }
            }
            PropertyAccess::Parameter(name) => context
                .parameters
                .get(name)
                .cloned()
                .ok_or_else(|| GraphError::EvaluationError(format!("Parameter '${}' not provided", name))),
        },

        Expression::List(elements) => {
            let mut evaluated_elements = Vec::with_capacity(elements.len());
            for element_expr in elements {
                evaluated_elements.push(evaluate_expression(element_expr, context)?);
            }
            Ok(CypherValue::List(evaluated_elements))
        },

        Expression::DynamicPropertyAccess { variable, property_expr } => {
            let target_val = context.variables.get(variable)
                .ok_or_else(|| GraphError::QueryExecutionError(format!("Variable '{}' not found", variable)))?;
            
            let prop_name_val = evaluate_expression(property_expr, context)?;
            let prop_name = match prop_name_val {
                CypherValue::String(s) => s,
                CypherValue::Null => return Ok(CypherValue::Null),
                _ => return Err(GraphError::QueryExecutionError("Property name must be a string".into())),
            };
            
            match target_val {
                CypherValue::Vertex(v) => {
                    if prop_name == "id" { return Ok(CypherValue::String(v.id.to_string())); }
                    if prop_name == "label" { return Ok(CypherValue::String(v.label.to_string())); }
                    match v.properties.get(&prop_name) {
                        Some(prop_val) => Ok(property_value_to_cypher(prop_val)),
                        None => Ok(CypherValue::Null),
                    }
                },
                CypherValue::Edge(e) => {
                    if prop_name == "id" { return Ok(CypherValue::String(e.id.to_string())); }
                    if prop_name == "label" || prop_name == "type" { return Ok(CypherValue::String(e.label.to_string())); }
                    match e.properties.get(&prop_name) {
                        Some(prop_val) => Ok(property_value_to_cypher(prop_val)),
                        None => Ok(CypherValue::Null),
                    }
                },
                CypherValue::Map(map) => {
                    match map.get(&prop_name) {
                        Some(val) => Ok(val.clone()),
                        None => Ok(CypherValue::Null),
                    }
                },
                _ => Err(GraphError::QueryExecutionError(format!("Cannot access properties on variable '{}'", variable))),
            }
        },

        Expression::LabelPredicate { variable, label } => {
            println!("===> Evaluating LabelPredicate: variable='{}', expected_label='{}'", variable, label);
            
            let val = context.variables.get(variable)
                .ok_or_else(|| GraphError::QueryExecutionError(format!("Variable '{}' not found", variable)))?;
            
            match val {
                CypherValue::Vertex(v) => {
                    let vertex_label = v.label.to_string();
                    println!("===> Vertex actual label: '{}'", vertex_label);
                    
                    // Compare labels (case-sensitive by default in Neo4j)
                    let matches = vertex_label == *label;
                    println!("===> Label match result: {}", matches);
                    
                    Ok(CypherValue::Bool(matches))
                },
                _ => {
                    println!("===> Variable is not a Vertex, returning false");
                    Ok(CypherValue::Bool(false))
                }
            }
        },

        Expression::Predicate { name, variable, list, condition } => {
            let list_val = evaluate_expression(list, context)?;
            
            if let CypherValue::List(elements) = list_val {
                let name_upper = name.to_uppercase();
                match name_upper.as_str() {
                    "ANY" => {
                        for item in elements {
                            let mut nested_ctx = context.clone();
                            nested_ctx.variables.insert(variable.clone(), item);
                            if let Ok(CypherValue::Bool(true)) = evaluate_expression(condition, &nested_ctx) {
                                return Ok(CypherValue::Bool(true));
                            }
                        }
                        Ok(CypherValue::Bool(false))
                    },
                    "ALL" => {
                        for item in elements {
                            let mut nested_ctx = context.clone();
                            nested_ctx.variables.insert(variable.clone(), item);
                            match evaluate_expression(condition, &nested_ctx)? {
                                CypherValue::Bool(true) => continue,
                                _ => return Ok(CypherValue::Bool(false)),
                            }
                        }
                        Ok(CypherValue::Bool(true))
                    },
                    _ => Err(GraphError::QueryExecutionError(format!("Unsupported predicate: {name}")))
                }
            } else {
                Ok(CypherValue::Null)
            }
        },
        Expression::FunctionCall { name, args } => {
            let func_name = name.to_uppercase();
            match func_name.as_str() {
                "ID" => {
                    if args.is_empty() {
                        return Err(GraphError::QueryExecutionError("ID() requires exactly 1 argument".into()));
                    }
                    let arg_val = evaluate_expression(&args[0], context)?;
                    match arg_val {
                        CypherValue::Vertex(v) => Ok(CypherValue::String(v.id.to_string())),
                        CypherValue::Edge(e) => Ok(CypherValue::String(e.id.to_string())),
                        _ => Ok(CypherValue::Null),
                    }
                },
                "TYPE" => {
                    let arg = args.get(0)
                        .ok_or_else(|| GraphError::EvaluationError("TYPE() requires 1 arg".into()))?;
                    let val = evaluate_expression(arg, context)?;
                    match val {
                        CypherValue::Edge(e) => Ok(CypherValue::String(e.label.to_string())),
                        _ => Ok(CypherValue::Null),
                    }
                },
                "LABELS" => {
                    let arg = args.get(0)
                        .ok_or_else(|| GraphError::EvaluationError("LABELS() requires 1 arg".into()))?;
                    let val = evaluate_expression(arg, context)?;
                    match val {
                        CypherValue::Vertex(v) => Ok(CypherValue::List(vec![
                            CypherValue::String(v.label.to_string())
                        ])),
                        _ => Ok(CypherValue::Null),
                    }
                },
                "PROPERTIES" => {
                    let val = evaluate_expression(
                        args.get(0).ok_or_else(|| GraphError::QueryExecutionError("PROPERTIES() requires 1 arg".into()))?,
                        context
                    )?;
                    match val {
                        CypherValue::Vertex(v) => {
                            let map = v.properties.iter()
                                .map(|(k, v)| (k.clone(), property_value_to_cypher(v)))
                                .collect();
                            Ok(CypherValue::Map(map))
                        },
                        CypherValue::Edge(e) => {
                            let map = e.properties.iter()
                                .map(|(k, v)| (k.clone(), property_value_to_cypher(v)))
                                .collect();
                            Ok(CypherValue::Map(map))
                        },
                        _ => Ok(CypherValue::Null),
                    }
                },
                "KEYS" => {
                    let val = evaluate_expression(
                        args.get(0).ok_or_else(|| GraphError::QueryExecutionError("KEYS() requires 1 arg".into()))?,
                        context
                    )?;
                    match val {
                        CypherValue::Vertex(v) => {
                            let mut keys: Vec<CypherValue> = v.properties.keys()
                                .map(|k| CypherValue::String(k.clone()))
                                .collect();
                            keys.push(CypherValue::String("id".to_string()));
                            keys.push(CypherValue::String("label".to_string()));
                            Ok(CypherValue::List(keys))
                        },
                        CypherValue::Edge(e) => {
                            let mut keys: Vec<CypherValue> = e.properties.keys()
                                .map(|k| CypherValue::String(k.clone()))
                                .collect();
                            keys.push(CypherValue::String("id".to_string()));
                            keys.push(CypherValue::String("label".to_string()));
                            Ok(CypherValue::List(keys))
                        },
                        CypherValue::Map(map) => {
                            let key_list: Vec<CypherValue> = map.keys()
                                .map(|k| CypherValue::String(k.clone()))
                                .collect();
                            Ok(CypherValue::List(key_list))
                        },
                        _ => Ok(CypherValue::List(vec![])),
                    }
                },
                "COUNT" => {
                    let val = evaluate_expression(
                        args.get(0).ok_or_else(|| GraphError::QueryExecutionError("COUNT() requires 1 arg".into()))?,
                        context
                    )?;
                    match val {
                        CypherValue::List(l) => Ok(CypherValue::Integer(l.len() as i64)),
                        CypherValue::Null => Ok(CypherValue::Integer(0)),
                        _ => Ok(CypherValue::Integer(1)),
                    }
                },
                "LEVENSHTEIN" => {
                    if args.len() != 2 {
                        return Err(GraphError::QueryExecutionError("levenshtein() requires 2 arguments".into()));
                    }
                    let s1 = match evaluate_expression(&args[0], context)? {
                        CypherValue::String(s) => s,
                        _ => return Err(GraphError::QueryExecutionError("levenshtein() arg 1 must be string".into())),
                    };
                    let s2 = match evaluate_expression(&args[1], context)? {
                        CypherValue::String(s) => s,
                        _ => return Err(GraphError::QueryExecutionError("levenshtein() arg 2 must be string".into())),
                    };
                    Ok(CypherValue::Integer(strsim::levenshtein(&s1, &s2) as i64))
                },
                "STARTS WITH" | "STARTSWITH" | "ENDS WITH" | "ENDSWITH" | "CONTAINS" => {
                    if args.len() != 2 {
                        return Err(GraphError::QueryExecutionError(format!("{} requires exactly 2 arguments", func_name)));
                    }
                    let haystack_val = evaluate_expression(&args[0], context)?;
                    let needle_val = evaluate_expression(&args[1], context)?;
                    
                    let haystack = match haystack_val {
                        CypherValue::String(s) => s,
                        CypherValue::Uuid(u) => u.to_string(),
                        CypherValue::Null => return Ok(CypherValue::Bool(false)),
                        other => serde_json::to_string(&other.to_json()).unwrap_or_else(|_| "null".to_string())
                    };
                    let needle = match needle_val {
                        CypherValue::String(s) => s,
                        CypherValue::Uuid(u) => u.to_string(),
                        CypherValue::Null => return Ok(CypherValue::Bool(false)),
                        other => serde_json::to_string(&other.to_json()).unwrap_or_else(|_| "null".to_string())
                    };
                    
                    let res = match func_name.as_str() {
                        "STARTS WITH" | "STARTSWITH" => haystack.starts_with(&needle),
                        "ENDS WITH" | "ENDSWITH" => haystack.ends_with(&needle),
                        "CONTAINS" => haystack.contains(&needle),
                        _ => false,
                    };
                    Ok(CypherValue::Bool(res))
                },
                "STRING" => {
                    if args.len() != 1 {
                        return Err(GraphError::QueryExecutionError("string() requires exactly 1 argument".into()));
                    }
                    let arg_val = evaluate_expression(&args[0], context)?;
                    match arg_val {
                        CypherValue::String(s) => Ok(CypherValue::String(s)),
                        CypherValue::Integer(i) => Ok(CypherValue::String(i.to_string())),
                        CypherValue::Float(f) => Ok(CypherValue::String(f.to_string())),
                        CypherValue::Bool(b) => Ok(CypherValue::String(b.to_string())),
                        CypherValue::Uuid(u) => Ok(CypherValue::String(u.to_string())),
                        CypherValue::Vertex(v) => Ok(CypherValue::String(v.id.to_string())),
                        CypherValue::Edge(e) => Ok(CypherValue::String(e.id.to_string())),
                        _ => Err(GraphError::QueryExecutionError("string() cannot convert this type".into())),
                    }
                },
                _ => Err(GraphError::QueryExecutionError(format!("Unknown function: {}", name)))
            }
        },
        Expression::Binary { op, left, right } => {
            let left_val = evaluate_expression(left, context)?;
            let right_val = evaluate_expression(right, context)?;

            match op {
                BinaryOp::Eq | BinaryOp::Neq | BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte => {
                    let op_str = match op {
                        BinaryOp::Eq => "=",
                        BinaryOp::Neq => "<>",
                        BinaryOp::Lt => "<",
                        BinaryOp::Lte => "<=",
                        BinaryOp::Gt => ">",
                        BinaryOp::Gte => ">=",
                        _ => unreachable!(),
                    };
                    evaluate_comparison(&left_val, op_str, &right_val)
                },
                BinaryOp::In => {
                    match right_val {
                        CypherValue::List(list) => {
                            if matches!(left_val, CypherValue::Null) {
                                Ok(CypherValue::Bool(false))
                            } else {
                                Ok(CypherValue::Bool(list.contains(&left_val)))
                            }
                        },
                        CypherValue::Null => Ok(CypherValue::Null),
                        _ => Err(GraphError::QueryExecutionError("The 'IN' operator requires a list".into())),
                    }
                },
                BinaryOp::Plus => add_values(left_val, right_val),
                BinaryOp::Minus => subtract_values(left_val, right_val),
                BinaryOp::Mul => multiply_values(left_val, right_val),
                BinaryOp::Div => divide_values(left_val, right_val),
                BinaryOp::And => {
                    println!("===> Evaluating AND: left={:?}, right={:?}", left_val, right_val);
                    match (left_val, right_val) {
                        (CypherValue::Bool(l), CypherValue::Bool(r)) => {
                            let result = l && r;
                            println!("===> AND result: {} && {} = {}", l, r, result);
                            Ok(CypherValue::Bool(result))
                        },
                        _ => {
                            println!("===> AND: non-boolean operands, returning Null");
                            Ok(CypherValue::Null)
                        }
                    }
                },
                BinaryOp::Or => {
                    println!("===> Evaluating OR: left={:?}, right={:?}", left_val, right_val);
                    match (left_val, right_val) {
                        (CypherValue::Bool(l), CypherValue::Bool(r)) => {
                            let result = l || r;
                            println!("===> OR result: {} || {} = {}", l, r, result);
                            Ok(CypherValue::Bool(result))
                        },
                        _ => {
                            println!("===> OR: non-boolean operands, returning Null");
                            Ok(CypherValue::Null)
                        }
                    }
                },
                BinaryOp::Contains => evaluate_comparison(&left_val, "CONTAINS", &right_val),
                BinaryOp::StartsWith => evaluate_comparison(&left_val, "STARTS WITH", &right_val),
                BinaryOp::EndsWith => evaluate_comparison(&left_val, "ENDS WITH", &right_val),
                BinaryOp::Regex => evaluate_comparison(&left_val, "=~", &right_val),
                _ => Err(GraphError::QueryExecutionError(format!("Operator {:?} not implemented", op))),
            }
        },

        Expression::PropertyComparison { variable, property, operator, value } => {
            let op_upper = operator.to_uppercase();
            if op_upper == "IN" {
                let left_val = context.variables.get(variable)
                    .ok_or_else(|| GraphError::QueryExecutionError(format!("Variable '{}' not found", variable)))?;

                let prop_val = if property.is_empty() {
                    left_val.clone()
                } else {
                    match left_val {
                        CypherValue::Vertex(v) => {
                            if property == "id" { CypherValue::String(v.id.to_string()) }
                            else if property == "label" { CypherValue::String(v.label.to_string()) }
                            else { v.properties.get(property).map(property_value_to_cypher).unwrap_or(CypherValue::Null) }
                        },
                        CypherValue::Edge(e) => {
                            if property == "id" { CypherValue::String(e.id.to_string()) }
                            else if property == "label" || property == "type" { CypherValue::String(e.label.to_string()) }
                            else { e.properties.get(property).map(property_value_to_cypher).unwrap_or(CypherValue::Null) }
                        },
                        _ => return Err(GraphError::QueryExecutionError(format!("Variable '{}' is not a graph object", variable))),
                    }
                };

                let right_val = CypherValue::from_json(value.clone());
                match right_val {
                    CypherValue::List(list) => Ok(CypherValue::Bool(list.contains(&prop_val))),
                    CypherValue::Null => Ok(CypherValue::Null),
                    _ => Err(GraphError::QueryExecutionError("The 'IN' operator requires a list".into())),
                }
            } else {
                if property.is_empty() {
                    let left_val = context.variables.get(variable)
                        .ok_or_else(|| GraphError::QueryExecutionError(format!("Variable '{}' not found", variable)))?;
                    let right_val = CypherValue::from_json(value.clone());
                    evaluate_comparison(left_val, operator, &right_val)
                } else {
                    evaluate_property_comparison(context, variable, property, operator, value)
                }
            }
        },
        
        Expression::And { left, right } => {
            let l_val = evaluate_expression(left, context)?;
            let r_val = evaluate_expression(right, context)?;
            match (l_val, r_val) {
                (CypherValue::Bool(l), CypherValue::Bool(r)) => Ok(CypherValue::Bool(l && r)),
                (CypherValue::Bool(false), _) | (_, CypherValue::Bool(false)) => Ok(CypherValue::Bool(false)),
                _ => Ok(CypherValue::Null),
            }
        },

        Expression::Or { left, right } => {
            let l_val = evaluate_expression(left, context)?;
            let r_val = evaluate_expression(right, context)?;
            match (l_val, r_val) {
                (CypherValue::Bool(l), CypherValue::Bool(r)) => Ok(CypherValue::Bool(l || r)),
                (CypherValue::Bool(true), _) | (_, CypherValue::Bool(true)) => Ok(CypherValue::Bool(true)),
                _ => Ok(CypherValue::Null),
            }
        },

        Expression::Unary { op, expr } => {
            let val = evaluate_expression(expr, context)?;
            match op {
                UnaryOp::Not => {
                    if let CypherValue::Bool(b) = val { Ok(CypherValue::Bool(!b)) } 
                    else { Err(GraphError::QueryExecutionError("NOT requires boolean".into())) }
                },
                UnaryOp::IsNotNull => Ok(CypherValue::Bool(!matches!(val, CypherValue::Null))),
                UnaryOp::IsNull => Ok(CypherValue::Bool(matches!(val, CypherValue::Null))),
                _ => Err(GraphError::QueryExecutionError("Unary op not implemented".into())),
            }
        },

        Expression::ArrayIndex { expr, index } => {
            let array_val = evaluate_expression(expr, context)?;
            let index_val = evaluate_expression(index, context)?;
            
            let idx = match index_val {
                CypherValue::Integer(i) => i as usize,
                CypherValue::Float(f) => f as usize,
                CypherValue::String(s) => {
                    if let Ok(i) = s.parse::<i64>() { i as usize } 
                    else { return Err(GraphError::QueryExecutionError("Array index must be a number".into())); }
                }
                _ => return Err(GraphError::QueryExecutionError("Array index must be a number".into())),
            };
            
            match array_val {
                CypherValue::List(items) => {
                    if idx < items.len() { Ok(items[idx].clone()) } 
                    else { Ok(CypherValue::Null) }
                }
                _ => Err(GraphError::QueryExecutionError("Cannot index non-list value".into())),
            }
        },
        _ => Err(GraphError::QueryExecutionError("Expression type not implemented".to_string())),
    } 
}

fn evaluate_expression_string(expr_str: &str, ctx: &EvaluationContext) -> Result<CypherValue, GraphError> {
    // Parse the expression string
    let expression = parse_expression(expr_str)
        .map_err(|e| GraphError::EvaluationError(format!("Failed to parse expression '{}': {:?}", expr_str, e)))?
        .1;
    
    // Evaluate it with the context
    expression.evaluate(ctx)
}

// --- Supporting Arithmetic Functions ---

fn add_values(left: CypherValue, right: CypherValue) -> StdResult<CypherValue, GraphError> {
    println!("===> add_values START");
    match (left, right) {
        (CypherValue::Integer(a), CypherValue::Integer(b)) => Ok(CypherValue::Integer(a + b)),
        (CypherValue::Float(a), CypherValue::Float(b)) => Ok(CypherValue::Float(a + b)),
        (CypherValue::String(a), CypherValue::String(b)) => Ok(CypherValue::String(format!("{}{}", a, b))),
        (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Null),
        _ => Err(GraphError::QueryExecutionError("Type mismatch in addition".into())),
    }
}

fn subtract_values(left: CypherValue, right: CypherValue) -> StdResult<CypherValue, GraphError> {
    println!("===> subtract_values START");
    match (left, right) {
        (CypherValue::Integer(a), CypherValue::Integer(b)) => Ok(CypherValue::Integer(a - b)),
        (CypherValue::Float(a), CypherValue::Float(b)) => Ok(CypherValue::Float(a - b)),
        (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Null),
        _ => Err(GraphError::QueryExecutionError("Type mismatch in subtraction".into())),
    }
}

fn multiply_values(left: CypherValue, right: CypherValue) -> StdResult<CypherValue, GraphError> {
    println!("===> multiply_values START");
    match (left, right) {
        (CypherValue::Integer(a), CypherValue::Integer(b)) => Ok(CypherValue::Integer(a * b)),
        (CypherValue::Float(a), CypherValue::Float(b)) => Ok(CypherValue::Float(a * b)),
        (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Null),
        _ => Err(GraphError::QueryExecutionError("Type mismatch in multiplication".into())),
    }
}

fn divide_values(left: CypherValue, right: CypherValue) -> StdResult<CypherValue, GraphError> {
    println!("===> divide_values START"); 
    match (left, right) {
        (CypherValue::Integer(_), CypherValue::Integer(0)) => {
            Err(GraphError::QueryExecutionError("Division by zero".into()))
        }
        (CypherValue::Integer(a), CypherValue::Integer(b)) => Ok(CypherValue::Integer(a / b)),
        (CypherValue::Float(a), CypherValue::Float(b)) => Ok(CypherValue::Float(a / b)),
        (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Null),
        _ => Err(GraphError::QueryExecutionError("Type mismatch in division".into())),
    }
}

pub fn parse_merge_statement(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_merge_statement START");                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
    type NomErrorType<'a> = nom::error::Error<&'a str>;

    let (input, _) = tag_no_case("MERGE").parse(input)?;
    let (input, _) = multispace1.parse(input)?; 
    let (input, raw_patterns) = parse_match_clause_patterns(input)?;
    let (input, where_clause) = opt(preceded(multispace1, parse_where)).parse(input)?;

    // ✅ Parse optional WITH clause
    let (input, with_clause_raw) = opt(preceded(multispace1, parse_with)).parse(input)?;
    let with_clause: Option<ParsedWithClause> = with_clause_raw.map(|w| ParsedWithClause {
        items: Vec::new(),
        distinct: false,
        where_clause: Some(WhereClause { condition: w.condition }),
        order_by: Vec::new(),
        skip: None,
        limit: None,
    });

    let mut on_create_set = Vec::new(); 
    let mut on_match_set = Vec::new();
    let mut input_current = input;
    
    loop {
        let (input_ws, _) = multispace0::<&str, NomErrorType>.parse(input_current)?;
        if input_ws.is_empty() { break; }

        let trimmed = input_ws.trim_start().to_uppercase();
        
        // Stop if we hit another MERGE, RETURN, or other clause keywords
        if trimmed.starts_with("MERGE") || 
           trimmed.starts_with("RETURN") || 
           trimmed.starts_with("WITH") || 
           trimmed.starts_with("MATCH") {
            input_current = input_ws;
            break;
        }

        if trimmed.starts_with("ON CREATE") {
            let (next, _) = tag_no_case::<_, _, NomErrorType>("ON CREATE").parse(input_ws)?;
            let (next, _) = delimited(multispace1, tag_no_case("SET"), multispace1).parse(next)?;
            let (next, clauses) = separated_list1(
                delimited(multispace0, char(','), multispace0),
                parse_single_set_assignment
            ).parse(next)?;
            on_create_set.extend(clauses);
            input_current = next;
        } else if trimmed.starts_with("ON MATCH") {
            let (next, _) = tag_no_case::<_, _, NomErrorType>("ON MATCH").parse(input_ws)?;
            let (next, _) = delimited(multispace1, tag_no_case("SET"), multispace1).parse(next)?;
            let (next, clauses) = separated_list1(
                delimited(multispace0, char(','), multispace0),
                parse_single_set_assignment
            ).parse(next)?;
            on_match_set.extend(clauses);
            input_current = next;
        } else {
            break;
        }
    }
    
    // DO NOT parse RETURN here - leave it for parse_sequential_statements
    // This allows RETURN to be handled as the final clause in a chain
    let input_final = input_current;

    // Standardize patterns
    let patterns: ExecutionPatternsReturnType = raw_patterns.into_iter().map(|(path, nodes, rels)| {
        let n_fixed = nodes.into_iter().map(|(v, l, p)| (v, l.into_iter().collect(), p)).collect::<Vec<_>>();
        (path, n_fixed, rels)
    }).collect();
    
    // Return Merge WITHOUT return_clause
    Ok((input_final, CypherQuery::Merge {
        patterns,
        where_clause,
        with_clause,
        on_create_set,
        on_match_set,
        return_clause: None,  // Always None - RETURN is handled by sequential parser
    }))
}

// ============================================================================
// FIXED MATCH NODE PARSERS
// ============================================================================
// These parsers handle the label format change from Vec<String> to Option<String>
// for the CypherQuery::MatchNode variant which expects Option<String>

/// Parse MATCH (n:Label), (m:Label) pattern
fn parse_match_multiple_nodes(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_match_multiple_nodes START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    map(
        tuple((
            tag("MATCH"),
            multispace1,
            separated_list1(
                delimited(multispace0, char(','), multispace0),
                parse_node,
            ),
            opt(preceded(
                tuple((multispace1, tag("RETURN"), multispace1)),
                parse_return_expressions,
            )),
        )),
        |(_, _, nodes, _)| {
            // parse_node returns (Option<String>, Vec<String>, HashMap<String, Value>)
            // Extract the first node for MatchNode
            let (var, labels, props) = &nodes[0];
            
            // Convert Vec<String> labels to Option<String>
            // Take the first label if present, otherwise None
            let label = if labels.is_empty() {
                None
            } else {
                Some(labels[0].clone())
            };
            
            CypherQuery::MatchNode {
                label,
                properties: props.clone(),
            }
        },
    ).parse(input)
}

/// Parse MATCH (n:Label {props}) [RETURN ...] pattern
fn parse_match_node(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_match_node START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, (_, _, node)) = tuple((
        tag("MATCH"),
        multispace1,
        parse_node,
    )).parse(input)?;
    
    // parse_node returns (Option<String>, Vec<String>, HashMap<String, Value>)
    let (var, labels, props) = node;
    
    // Convert Vec<String> labels to Option<String>
    // Take the first label if present, otherwise None
    let label = if labels.is_empty() {
        None
    } else {
        Some(labels[0].clone())
    };
    
    // Optional RETURN clause
    let (input, _) = if input.trim_start().to_uppercase().starts_with("RETURN") {
        let (input, _) = multispace0.parse(input)?;
        let (input, _) = tag("RETURN").parse(input)?;
        let (input, _) = multispace0.parse(input)?;
        let (input, _) = parse_return_expressions(input)?;
        (input, ())
    } else {
        (input, ())
    };
    
    Ok((input, CypherQuery::MatchNode {
        label,
        properties: props,
    }))
}

fn parse_return_expressions(input: &str) -> IResult<&str, ()> {
    println!("===> parse_return_expressions START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = parse_return_expression(input)?;
    let (input, _) = many0(preceded(
        tuple((multispace0, char(','), multispace0)),
        parse_return_expression
    )).parse(input)?;
    Ok((input, ()))
}

fn parse_create_edge(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_create_edge START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    tuple((
        tag("CREATE"),
        multispace1,
        parse_node,
        multispace0,
        parse_relationship,
        multispace0,
        parse_node,
    ))
    .map(|(
        _,
        _,
        (_var1, _label1, _props1),
        _,
        (_rel_var, rel_type, _rel_props, _card, _dir),
        _,
        (_var2, _label2, _props2),
    )| CypherQuery::CreateEdge {
        from_id: SerializableUuid(Uuid::new_v4()),
        edge_type: rel_type.unwrap_or_else(|| "RELATED".to_string()),
        to_id: SerializableUuid(Uuid::new_v4()),
    })
    .parse(input)
}

fn parse_set_node(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_set_node START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    map(
        tuple((
            tag("SET"),
            multispace1,
            parse_identifier,
            multispace0,
            parse_properties,
        )),
        |(_, _, _var, _, props)| CypherQuery::SetNode {
            id: SerializableUuid(Uuid::new_v4()),
            properties: props,
        },
    ).parse(input)
}

fn parse_delete_node(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_delete_node START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    map(
        tuple((tag("DELETE"), multispace1, parse_identifier)),
        |(_, _, _var)| CypherQuery::DeleteNode {
            id: SerializableUuid(Uuid::new_v4()),
        },
    ).parse(input)
}

fn parse_set_kv(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_set_kv START");
    map(
        tuple((
            tag("SET"),
            multispace1,
            parse_identifier,
            multispace0,
            char('='),
            multispace0,
            parse_string_literal,
        )),
        |(_, _, key, _, _, _, value)| CypherQuery::SetKeyValue {
            key: key.to_string(),
            value: value.to_string(),
        },
    ).parse(input)
}

fn parse_get_kv(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_get_kv START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    map(
        tuple((
            tag("MATCH"),
            multispace1,
            parse_node,
            multispace1,
            tag("RETURN"),
        )),
        |(_, _, (key, _, _), _, _)| CypherQuery::GetKeyValue {
            key: key.unwrap_or_default(),
        },
    ).parse(input)
}

fn parse_delete_kv(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_delete_kv START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    map(
        tuple((tag("DELETE"), multispace1, parse_identifier)),
        |(_, _, key)| CypherQuery::DeleteKeyValue {
            key: key.to_string(),
        },
    ).parse(input)
}

// Use a type alias for the error type if available, otherwise use the full trait bound.
// Assuming your IResult is defined as type IResult<I, O> = Result<(I, O), nom::Err<E>>;
// We will define the internal functions to return the parser object itself.

// Internal function to parse SET key=value
fn parse_set_kv_internal<'a>() -> impl Parser<&'a str, Output = CypherQuery, Error = NomErrorType<&'a str>> {
    println!("===> parse_set_kv_internal START");
    map(
        tuple((
            tag_no_case("SET"),
            multispace1,
            parse_identifier,
            multispace0,
            char('='),
            multispace0,
            parse_string_literal,
        )),
        |(_, _, key, _, _, _, value)| CypherQuery::SetKeyValue {
            key: key.to_string(),
            value: value.to_string(),
        },
    )
}

// Internal function to parse MATCH ... RETURN (GET)
fn parse_get_kv_internal<'a>() -> impl Parser<&'a str, Output = CypherQuery, Error = NomErrorType<&'a str>> {
    println!("===> parse_get_kv_internal START");
    map(
        tuple((
            tag_no_case("MATCH"),
            multispace1,
            parse_node, 
            multispace1,
            tag_no_case("RETURN"),
        )),
        |(_, _, (key, _, _), _, _)| CypherQuery::GetKeyValue {
            key: key.unwrap_or_default(), 
        },
    )
}

// Internal function to parse DELETE key
fn parse_delete_kv_internal<'a>() -> impl Parser<&'a str, Output = CypherQuery, Error = NomErrorType<&'a str>> {
    println!("===> parse_delete_kv_internal START");
    map(
        tuple((tag_no_case("DELETE"), multispace1, parse_identifier)),
        |(_, _, key)| CypherQuery::DeleteKeyValue {
            key: key.to_string(),
        },
    )
}
/// Combines parse_set_kv, parse_get_kv, and parse_delete_kv into a single parser.
fn parse_kv_operations(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_kv_operations START");
    // FIX: Inline the alt definition directly into the execution closure.
    // This avoids the 'let combined_parser = ...' binding, which was causing the 
    // compiler to try and borrow the captured variable as mutable.
    println!("===> parse_kv_operations START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    (move |i| {
        alt((
            // Call the functions to get the impl Parser objects
            parse_set_kv_internal(),
            parse_get_kv_internal(),
            parse_delete_kv_internal(),
        )).parse(i)
    })(input)
}

fn parse_set_list(input: &str) -> IResult<&str, HashMap<String, Value>> {
    println!("===> parse_set_list START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, first) = parse_set_clause(input)?;
    let (input, rest) = many0(
        preceded(
            tuple((multispace0, char(','), multispace0)),
            parse_set_clause,
        )
    ).parse(input)?;
    
    let mut all_props = first.1;
    for (_, props) in rest {
        all_props.extend(props);
    }
    Ok((input, all_props))
}

// Updated parse_predicate_expression to handle keys() function and complex expressions
fn parse_predicate_expression(input: &str) -> IResult<&str, Expression> {
    // 1. Match "any" or "all"
    let (input, name) = alt((
        tag_no_case("any"), 
        tag_no_case("all")
    )).parse(input)?; // Correct use of the Parser trait
    
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // 2. Match the variable
    let (input, var_name) = parse_identifier(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 3. Match "IN"
    let (input, _) = tag_no_case("IN").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 4. Match the list expression
    let (input, list_expr) = alt((
        parse_function_call_full,
        parse_expression_list,
        parse_list_literal,
        map(
            tuple((parse_identifier, char('.'), parse_identifier)),
            |(var, _, prop)| Expression::Property(PropertyAccess::Vertex(var.to_string(), prop.to_string()))
        ),
        map(parse_identifier, |v| Expression::Variable(v.to_string())),
    )).parse(input)?; // Use .parse(input) here as well
    
    let (input, _) = multispace1.parse(input)?;
    
    // 5. Match "WHERE"
    let (input, _) = tag_no_case("WHERE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 6. Parse the condition
    let (input, condition_expr) = parse_condition_internal(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;
    
    Ok((input, Expression::Predicate {
        name: name.to_uppercase(),
        variable: var_name.to_string(),
        list: Box::new(list_expr),
        condition: Box::new(condition_expr),
    }))
}

// New helper function to parse conditions inside predicates
fn parse_predicate_condition(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_predicate_condition START");
    
    // 1. Parse Left Side (Handles string(e[prop]), e[prop], etc.)
    let (input, left_expr) = alt((
        parse_function_call_full,
        parse_dynamic_property_access,
        map(
            tuple((parse_identifier, char('.'), parse_identifier)),
            |(var, _, prop)| Expression::Property(PropertyAccess::Vertex(var.to_string(), prop.to_string()))
        ),
        map(parse_identifier, |v| Expression::Variable(v.to_string())),
    )).parse(input)?; // Fixed: added .parse(input)
    
    let (input, _) = multispace1(input)?;
    
    // 2. Parse Operator (Handles regex and binary ops)
    let (input, op_str) = alt((
        tag_no_case("IN"),
        tag("=~"),
        tag("="),
        tag("!="),
        tag("<>"),
        tag("<="),
        tag(">="),
        tag("<"),
        tag(">"),
    )).parse(input)?; // Fixed: added .parse(input)
    
    let (input, _) = multispace1(input)?;
    
    // 3. Parse Right Side
    let (input, right_expr) = alt((
        parse_list_literal,
        parse_function_call_full,
        map(
            tuple((parse_identifier, char('.'), parse_identifier)),
            |(var, _, prop)| Expression::Property(PropertyAccess::Vertex(var.to_string(), prop.to_string()))
        ),
        map(parse_value, |v| Expression::Literal(CypherValue::from_json(v))),
        map(parse_identifier, |v| Expression::Variable(v.to_string())),
    )).parse(input)?; // Fixed: added .parse(input)
    
    let op = match op_str.to_uppercase().as_str() {
        "IN" => BinaryOp::In,
        "=~" => BinaryOp::Regex,
        "=" => BinaryOp::Eq,
        "!=" | "<>" => BinaryOp::Neq,
        "<=" => BinaryOp::Lte,
        ">=" => BinaryOp::Gte,
        "<" => BinaryOp::Lt,
        ">" => BinaryOp::Gt,
        _ => BinaryOp::Eq,
    };
    
    Ok((input, Expression::Binary {
        op,
        left: Box::new(left_expr),
        right: Box::new(right_expr),
    }))
}

/// Helper to parse the inner condition (label IN [...]) without infinite recursion
// Updated parse_condition_internal to handle complex expressions in predicates
fn parse_condition_internal(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_condition_internal START");
    
    // 1. Parse left side
    let (input, left_expr) = alt((
        parse_function_call_full,
        parse_dynamic_property_access,
        map(
            tuple((parse_identifier, char('.'), parse_identifier)),
            |(var, _, prop)| Expression::Property(PropertyAccess::Vertex(var.to_string(), prop.to_string()))
        ),
        map(parse_identifier, |v| Expression::Variable(v.to_string())),
    )).parse(input)?;
    
    // Use multispace0 to be safer around parentheses
    let (input, _) = multispace0.parse(input)?;
    
    // 2. Parse operator
    let (input, op_str) = alt((
        tag_no_case("IN"),
        tag_no_case("CONTAINS"), // Put CONTAINS higher in priority
        tag_no_case("STARTS WITH"),
        tag_no_case("ENDS WITH"),
        tag("=~"),
        tag("="),
        tag("!="),
        tag("<>"),
        tag("<="),        
        tag(">="),
        tag("<"),
        tag(">"),
    )).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    
    // 3. Parse right side
    let (input, right_expr) = alt((
        parse_list_literal,
        parse_function_call_full,
        // Pass a reference here to satisfy the existing trait bound
        map(parse_value, |v| Expression::Literal(CypherValue::from(&v))), 
        map(
            tuple((parse_identifier, char('.'), parse_identifier)),
            |(var, _, prop)| Expression::Property(PropertyAccess::Vertex(var.to_string(), prop.to_string()))
        ),
        map(parse_identifier, |v| Expression::Variable(v.to_string())),
    )).parse(input)?;
    
    // Map operator string to BinaryOp
    let op = match op_str.to_uppercase().as_ref() {
        "IN" => BinaryOp::In,
        "CONTAINS" => BinaryOp::Contains,
        "STARTS WITH" => BinaryOp::StartsWith,
        "ENDS WITH" => BinaryOp::EndsWith,
        "=~" => BinaryOp::Regex,
        "=" => BinaryOp::Eq,
        "!=" | "<>" => BinaryOp::Neq,
        "<=" => BinaryOp::Lte,
        ">=" => BinaryOp::Gte,
        "<" => BinaryOp::Lt,
        ">" => BinaryOp::Gt,
        _ => BinaryOp::Eq,
    };
    
    Ok((input, Expression::Binary {
        op,
        left: Box::new(left_expr),
        right: Box::new(right_expr),
    }))
}

fn parse_set_query(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_set_query START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, _) = tag("SET").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, properties) = parse_set_list(input)?;
    let (input, _) = multispace0(input)?;
    
    // Check for RETURN clause
    let input = if input.trim_start().starts_with("RETURN") {
        let (input, _) = multispace0.parse(input)?;
        let (input, _) = tag("RETURN").parse(input)?;
        let (input, _) = multispace0.parse(input)?;
        // Skip return expressions for now
        let (input, _) = take_while1(|c| c != '\n' && c != '\r').parse(input)?;
        input
    } else {
        input
    };
    
    Ok((input, CypherQuery::SetNode {
        id: SerializableUuid(Uuid::new_v4()),
        properties,
    }))
}

// Custom parser for signed integers and floats, returning serde_json::Value
// This robustly recognizes the optional leading '-' sign.
fn parse_signed_number_value(input: &str) -> IResult<&str, Value> {
    println!("===> parse_signed_number_value START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    // Manual parsing approach to avoid nom parser composition issues
    let original_input = input;
    let mut chars = input.chars().peekable();
    let mut number_str = String::new();
    
    // Check for optional negative sign
    if let Some('-') = chars.peek() {
        number_str.push('-');
        chars.next();
    }
    
    // Check if we have at least one digit or a decimal point
    let mut has_digits = false;
    let mut has_decimal = false;
    
    // Parse digits before decimal point
    while let Some(&ch) = chars.peek() {
        if ch.is_ascii_digit() {
            number_str.push(ch);
            chars.next();
            has_digits = true;
        } else {
            break;
        }
    }
    
    // Check for decimal point
    if let Some(&'.') = chars.peek() {
        number_str.push('.');
        chars.next();
        has_decimal = true;
        
        // Parse digits after decimal point
        while let Some(&ch) = chars.peek() {
            if ch.is_ascii_digit() {
                number_str.push(ch);
                chars.next();
                has_digits = true;
            } else {
                break;
            }
        }
    }
    
    // Must have at least one digit
    if !has_digits {
        return Err(nom::Err::Error(NomError::Error {
            input: original_input,
            code: NomError::ErrorKind::Digit,
        }));
    }
    
    // Calculate remaining input
    let consumed = number_str.len();
    let remaining = &original_input[consumed..];
    
    // Attempt to parse the recognized string into the appropriate Value.
    // Try i64 first (for integers, like -295941589)
    if !has_decimal {
        if let Ok(i) = number_str.parse::<i64>() {
            return Ok((remaining, Value::Number(i.into())));
        }
    }
    
    // Fallback to f64 (for floats)
    if let Ok(f) = number_str.parse::<f64>() {
        if let Some(num) = serde_json::Number::from_f64(f) {
            return Ok((remaining, Value::Number(num)));
        }
    }
    
    // Return error if parsing fails
    Err(nom::Err::Error(NomError::Error {
        input: original_input,
        code: NomError::ErrorKind::Float,
    }))
}

fn parse_simple_query_type(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_simple_query_type START");
    
    // 1. Try the sequential parser first with an empty starting table.
    // Since it returns (CypherQuery, SymbolTable), we map it to just return CypherQuery.
    if let Ok((rem, (query, _table))) = parse_sequential_statements_with_context(input, SymbolTable::new()) {
        // If we actually consumed more than one part or hit a RETURN/UNION, return this.
        return Ok((rem, query));
    }

    // 2. Fallback to standard single-clause parsers if the sequential one didn't match
    alt((
        parse_merge_statement,
        parse_match_detach_delete, 
        parse_create_statement, 
        parse_delete_edges_simple,
        parse_unwind_clause,
        full_statement_parser,
        parse_detach_delete,
        parse_delete_edges,
        parse_create_index,
        parse_create_edge_between_existing,
        parse_create_complex_pattern,
        parse_create_nodes,
        parse_create_node,
        parse_create_edge,
        parse_match_multiple_nodes, 
        parse_set_query,
        parse_set_node,
        parse_delete_node,
        parse_kv_operations,
    ))
    .parse(input)
}

/// Parses a single clause WITHOUT recursion (used by parse_sequential_statements)
fn parse_clause_non_recursive(input: &str) -> IResult<&str, CypherQuery> {
    alt((
        parse_merge_statement,
        parse_match_detach_delete, 
        parse_create_statement, 
        parse_delete_edges_simple,
        parse_unwind_clause,
        full_statement_parser,
        parse_detach_delete,
        parse_delete_edges,
        parse_create_index,
        parse_create_edge_between_existing,
        parse_create_complex_pattern,
        parse_create_nodes,
        parse_create_node,
        parse_create_edge,
        parse_match_multiple_nodes, 
        parse_set_query,
        parse_set_node,
        parse_delete_node,
        parse_kv_operations,
    ))
    .parse(input)
}

// ----------------------------------------------------------------------------------
// --- DEFINED: parse_sequential_statements (New parser for concatenated statements) ---
// ----------------------------------------------------------------------------------

/// Parses a sequence of independent, simple statements concatenated without semicolons.
/// This is the key fix for inputs like "CREATE (a) CREATE (b) MATCH (c) RETURN c".
pub fn parse_sequential_statements_with_context(
    input: &str, 
    symbol_table: SymbolTable
) -> IResult<&str, (CypherQuery, SymbolTable)> {
    println!("===> parse_sequential_statements_with_context START");
    
    let mut remaining = input;
    let mut clauses = Vec::new();
    let mut current_symbol_table = symbol_table;
    
    loop {
        let (rest, _) = multispace0(remaining)?;
        if rest.is_empty() {
            break;
        }
        
        let trimmed_upper = rest.trim_start().to_uppercase();
        
        // 1. Handle UNION
        if trimmed_upper.starts_with("UNION") {
            let (after_union, _) = tag_no_case::<_, _, nom::error::Error<&str>>("UNION")(rest)?;
            let (after_union, _) = multispace1(after_union)?;
            
            let (after_union, is_all) = if let Ok((i, _)) = tag_no_case::<_, _, nom::error::Error<&str>>("ALL")(after_union) {
                let (i, _) = multispace1(i)?;
                (i, true)
            } else {
                (after_union, false)
            };
            
            let (final_remaining, rhs_clause) = parse_clause_non_recursive(after_union)?;
            
            let left_query = if clauses.len() == 1 {
                Box::new(clauses.pop().unwrap())
            } else if clauses.is_empty() {
                let (r, q) = parse_clause_non_recursive(input)?;
                return Ok((r, (q, current_symbol_table)));
            } else {
                Box::new(CypherQuery::Chain(clauses.drain(..).collect()))
            };
            
            return Ok((final_remaining, (
                CypherQuery::Union(left_query, is_all, Box::new(rhs_clause)),
                current_symbol_table
            )));
        }
        
        // 2. Handle RETURN (Terminal)
        if trimmed_upper.starts_with("RETURN") {
            let (final_remaining, return_clause) = parse_return_clause_as_struct(rest)?;
            
            // Reconstruct the projection string ensuring aliases are preserved
            let projection_items_str = return_clause.items.iter()
                .map(|item| {
                    if let Some(alias) = &item.alias {
                        format!("{} AS {}", item.expression, alias)
                    } else {
                        item.expression.clone()
                    }
                })
                .collect::<Vec<String>>()
                .join(", ");

            let return_statement = CypherQuery::ReturnStatement {
                projection_string: format!("RETURN {}", projection_items_str),
                order_by: return_clause.order_by.unwrap_or_default()
                    .into_iter()
                    .map(|(expr_str, asc)| OrderByItem { 
                        expression: expr_str, 
                        ascending: asc 
                    })
                    .collect(),
                skip: return_clause.skip.map(|s| s as i64),
                limit: return_clause.limit.map(|l| l as i64),
                is_distinct: return_clause.distinct, // FIXED: Added missing field
            };
            
            clauses.push(return_statement);
            return Ok((final_remaining, (CypherQuery::Chain(clauses), current_symbol_table)));
        }

        // 3. Handle WITH (Chaining)
        if trimmed_upper.starts_with("WITH") {
            println!("===> Parsing WITH clause");
            let (new_remaining, with_parsed) = parse_with_full(rest)?;
            
            let mut updated_symbols = current_symbol_table.clone();
            
            for item in &with_parsed.items {
                if let Some(ref alias) = item.alias {
                    updated_symbols.bind_variable(alias.clone(), SerializableUuid::default());
                    println!("===> Registered alias '{}' in symbol table", alias);
                }
            }
            
            let with_clause_query = CypherQuery::MatchPattern {
                patterns: vec![],
                where_clause: None,
                with_clause: Some(with_parsed),
                return_clause: None,
            };
            
            clauses.push(with_clause_query);
            current_symbol_table = updated_symbols;
            remaining = new_remaining;
            continue;
        }
                
        // 4. Standard clause parsing
        match parse_clause_with_symbol_table(rest, &current_symbol_table) {
            Ok((new_remaining, (next_clause, updated_symbols))) => {
                clauses.push(next_clause);
                current_symbol_table = updated_symbols;
                remaining = new_remaining;
            }
            Err(_) => break,
        }
    }
    
    if clauses.len() == 1 {
        Ok((remaining, (clauses.into_iter().next().unwrap(), current_symbol_table)))
    } else if clauses.is_empty() {
        parse_clause_with_symbol_table(input, &current_symbol_table)
    } else {
        Ok((remaining, (CypherQuery::Chain(clauses), current_symbol_table)))
    }
}

pub fn parse_with_clause_as_struct(input: &str) -> IResult<&str, ParsedWithClause> {
    let (input, _) = tag_no_case("WITH")(input)?;
    let (input, _) = multispace1(input)?;
    
    let (input, distinct_token) = opt(tag_no_case("DISTINCT")).parse(input)?;
    let (input, _) = multispace0(input)?;

    let (input, item_strings) = parse_return_items(input)?;
    let items = item_strings.into_iter().map(|s| {
        let parts: Vec<&str> = s.splitn(2, |c: char| c.is_whitespace()).collect();
        if parts.len() > 1 && s.to_uppercase().contains(" AS ") {
            let alias = s.split_whitespace().last().unwrap().to_string();
            let expression = parts[0].to_string();
            QueryReturnItem { expression, alias: Some(alias) }
        } else {
            QueryReturnItem { expression: s.clone(), alias: Some(s) }
        }
    }).collect();
    
    let (input, _) = multispace0(input)?;

    let (input, maybe_condition) = opt(parse_where_clause).parse(input)?;
    let where_clause = maybe_condition.map(|cond| WhereClause { condition: cond });
    let (input, _) = multispace0(input)?;

    // Parse ORDER BY using the correct helper
    let (input, order_by) = opt(preceded(
        pair(tag_no_case("ORDER BY"), multispace1),
        parse_order_by_items
    )).parse(input)?;
    let (input, _) = multispace0(input)?;
    
    let (input, skip) = opt(preceded(
        pair(tag_no_case("SKIP"), multispace1),
        nom::character::complete::u64
    )).parse(input)?;
    let (input, _) = multispace0(input)?;
    
    let (input, limit) = opt(preceded(
        pair(tag_no_case("LIMIT"), multispace1),
        nom::character::complete::u64
    )).parse(input)?;

    Ok((
        input,
        ParsedWithClause {
            items,
            distinct: distinct_token.is_some(),
            where_clause,
            order_by: order_by.unwrap_or_default(),
            skip: skip.map(|s| s as i64),
            limit: limit.map(|l| l as i64),
        },
    ))
}

fn parse_with_clause_as_query<'a>(
    input: &'a str,
    symbol_table: &SymbolTable,
) -> GraphResult<(&'a str, (ParsedWithClause, SymbolTable))> {
    let (rest, parsed_with) = parse_with_clause_as_struct(input)
        .map_err(|e| GraphError::ParseError(e.to_string()))?;
    
    let mut updated_symbols = symbol_table.clone();
    
    for item in &parsed_with.items {
        if let Some(alias) = &item.alias {
            // Register for transaction logging and golden record traceability
            updated_symbols.variables.insert(alias.clone(), SerializableUuid::default());
        }
    }

    Ok((rest, (parsed_with, updated_symbols)))
}

// --- Support Parsers ---

fn parse_order_by(input: &str) -> IResult<&str, Vec<(String, bool)>> {
    let (input, _) = tag_no_case("ORDER BY")(input)?;
    let (input, _) = multispace1(input)?;
    
    separated_list1(
        tuple((multispace0, tag(","), multispace0)),
        map(
            tuple((
                parse_expression_string, 
                opt(preceded(multispace1, alt((
                    map(tag_no_case("ASC"), |_| true),
                    map(tag_no_case("DESC"), |_| false),
                ))))
            )),
            |(id, asc)| (id, asc.unwrap_or(true))
        )
    ).parse(input) // FIX: Use .parse(input) instead of (input)
}

fn parse_skip(input: &str) -> IResult<&str, u64> {
    let (input, _) = tag_no_case("SKIP")(input)?;
    let (input, _) = multispace1(input)?;
    nom::character::complete::u64(input)
}

fn parse_limit(input: &str) -> IResult<&str, u64> {
    let (input, _) = tag_no_case("LIMIT")(input)?;
    let (input, _) = multispace1(input)?;
    nom::character::complete::u64(input)
}

async fn find_variable_length_paths(
    all_vertices: &Vec<Vertex>,
    all_edges: &Vec<Edge>,
    start_node_pat: &NodePattern,
    rel_pat: &RelPattern,
    end_node_pat: &NodePattern,
) -> GraphResult<(HashSet<SerializableUuid>, HashSet<SerializableUuid>)> {
    println!("===> find_variable_length_paths START");
    let (_start_var, start_label_opt, start_props) = start_node_pat;
    let (_rel_var, rel_label_opt, len_range_opt, _rel_props, direction_opt) = rel_pat;
    let (_end_var, end_label_opt, end_props) = end_node_pat;

    let (min_hops, max_hops) = len_range_opt
        .unwrap_or((Some(1), Some(1)));
        
    let min_hops = min_hops.unwrap_or(1);
    let max_hops = max_hops.unwrap_or(u32::MAX);

    // 1. Find all starting vertices that match the start node pattern
    let starting_nodes: Vec<&Vertex> = all_vertices.iter()
        .filter(|v| matches_constraints(v, start_label_opt, start_props))
        .collect();

    let mut matched_vertex_ids = HashSet::new();
    let mut matched_edge_ids = HashSet::new();

    if starting_nodes.is_empty() {
        return Ok((matched_vertex_ids, matched_edge_ids));
    }

    // Initialize BFS with (Node ID, Path of Edges)
    // We use a VecDeque for a proper BFS if available, otherwise Vec for queue.
    let mut queue: Vec<(SerializableUuid, Vec<SerializableUuid>)> = starting_nodes.iter()
        .map(|v| (v.id, Vec::new()))
        .collect();
    
    let mut visited_at_hop: HashMap<SerializableUuid, u32> = HashMap::new();
    for node in &starting_nodes {
        visited_at_hop.insert(node.id, 0);
    }
    
    let mut current_hop = 0;

    // Handle 0-hop case (e.g., *0..2)
    if min_hops == 0 {
        for node in &starting_nodes {
            if matches_constraints(node, end_label_opt, end_props) {
                matched_vertex_ids.insert(node.id);
            }
        }
    }

    // BFS loop
    while !queue.is_empty() && current_hop < max_hops {
        current_hop += 1;
        let mut next_queue = Vec::new();

        for (current_id, path_edges) in queue.drain(..) {
            
            // Determine which direction to traverse based on relationship pattern
            let is_outgoing = direction_opt.unwrap_or(true); 

            for edge in all_edges.iter().filter(|e| {
                if is_outgoing {
                    e.outbound_id == current_id // Check A->B
                } else {
                    e.inbound_id == current_id // Check A<-B
                }
            }) {
                
                // 1. Check relationship type match
                if rel_label_opt.as_deref().map_or(true, |l| edge.label == l) {

                    let next_id = if is_outgoing { edge.inbound_id } else { edge.outbound_id };
                    
                    // Skip if we've already found a shorter path to this node
                    if visited_at_hop.get(&next_id).map_or(false, |&h| h <= current_hop) {
                        continue;
                    }

                    // 2. Check if we are within the desired hop range (min_hops <= current_hop)
                    if current_hop >= min_hops {
                        // Look up the actual vertex to check end node constraints
                        if let Some(next_node_vertex) = all_vertices.iter().find(|v| v.id == next_id) {
                            if matches_constraints(next_node_vertex, end_label_opt, end_props) {
                                // Found a valid path end matching the target node pattern
                                matched_vertex_ids.insert(current_id);
                                matched_vertex_ids.insert(next_id);
                                matched_edge_ids.insert(edge.id);
                                matched_edge_ids.extend(path_edges.iter().cloned());
                            }
                        }
                    }

                    // 3. Enqueue for the next hop if not at max_hops
                    if current_hop < max_hops {
                        let mut next_path_edges = path_edges.clone();
                        next_path_edges.push(edge.id);
                        next_queue.push((next_id, next_path_edges));
                        visited_at_hop.insert(next_id, current_hop);
                    }
                }
            }
        }
        queue = next_queue;
    }

    Ok((matched_vertex_ids, matched_edge_ids))
}

// parse_content_after_match_keyword returns the RAW parser format
fn parse_content_after_match_keyword(input: &str) -> IResult<&str, ParsedPatternsReturnType> {
    println!("===> parse_content_after_match_keyword START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    preceded(
        multispace0,
        tuple((
            // Optional path variable assignment (path = ...)
            opt(tuple((
                parse_identifier, 
                multispace0,
                char('='),
                multispace0,
            ))),
            // The actual patterns
            parse_match_clause_patterns,
        )),
    )
    .map(|(_path_setup, patterns)| patterns)
    .parse(input)
}

// match_clause_content_parser returns the RAW parser format
fn match_clause_content_parser<'a>(
    i: &'a str,
) -> IResult<&'a str, (Option<(&'a str, &'a str, char, &'a str)>, ParsedPatternsReturnType)> {
    preceded(
        multispace1,
        tuple((
            // Optional path variable assignment
            opt(tuple((
                take_while1(|c: char| c.is_alphanumeric() || c == '_'),
                multispace0,
                char('='),
                multispace0,
            ))),
            // The actual patterns
            parse_match_clause_patterns,
        )),
    ).parse(i)
}

// ============================================================================
// MAIN STATEMENT PARSER
// ============================================================================
// Add this helper function to convert patterns from parser format to execution format
fn convert_parsed_patterns_to_execution_format(
    parsed_patterns: ParsedPatternsReturnType
) -> ExecutionPatternsReturnType {
    parsed_patterns.into_iter().map(|(path_var, nodes, rels)| {
        let converted_nodes = nodes.into_iter().map(|(var, label_opt, props)| {
            // Convert Option<String> to Vec<String> for labels
            let labels = match label_opt {
                Some(label) => {
                    // Handle multi-label syntax: "Patient:GoldenRecord" -> ["Patient", "GoldenRecord"]
                    label.split(':')
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>()
                },
                None => vec![], // No labels
            };
            (var, labels, props)
        }).collect();
        
        (path_var, converted_nodes, rels)
    }).collect()
}

// Updated full_statement_parser with early RETURN detection
fn full_statement_parser(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> full_statement_parser START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let mut all_patterns: Vec<Pattern> = Vec::new();
    let mut on_create_set: Vec<(String, String, Expression)> = Vec::new();
    let mut on_match_set: Vec<(String, String, Expression)> = Vec::new();
    let mut input_current = input;
    let mut captured_where: Option<WhereClause> = None;
    let mut captured_with: Option<ParsedWithClause> = None;
    let mut captured_return: Option<ReturnClause> = None;

    // --- 1. PARSE MANDATORY FIRST CLAUSE ---
    let (input_after_clause, clause_type_str) = preceded(
        multispace0,
        alt((
            tag_no_case::<_, _, NomErrorType<&str>>("UNWIND"),
            tag_no_case::<_, _, NomErrorType<&str>>("OPTIONAL MATCH"),
            tag_no_case::<_, _, NomErrorType<&str>>("MERGE"),
            tag_no_case::<_, _, NomErrorType<&str>>("MATCH"),
        ))
    ).parse(input_current)?;

    // Handle UNWIND separately
    if clause_type_str.to_uppercase() == "UNWIND" {
        return parse_unwind_clause(input_current);
    }

    let is_merge = clause_type_str.to_uppercase() == "MERGE";
    input_current = input_after_clause;

    match parse_content_after_match_keyword(input_current) {
        Ok((input_after_patterns, parsed_patterns)) => {
            let converted = convert_parsed_patterns_to_execution_format(parsed_patterns);
            all_patterns.extend(converted);
            input_current = input_after_patterns;
        }
        Err(e) => return Err(e),
    }

    // --- 2. PARSE ADDITIONAL MATCH CLAUSES ---
    if !is_merge {
        loop {
            let (input_ws, _) = multispace0.parse(input_current)?;
            
            let trimmed = input_ws.trim_start().to_uppercase();
            if trimmed.starts_with("WHERE") || 
               trimmed.starts_with("WITH") || 
               trimmed.starts_with("UNWIND") || 
               trimmed.starts_with("RETURN") ||
               trimmed.starts_with("ORDER") ||
               trimmed.starts_with("LIMIT") ||
               trimmed.starts_with("SET") ||
               trimmed.starts_with("CREATE") ||
               trimmed.starts_with("REMOVE") ||
               trimmed.starts_with("DELETE") ||
               trimmed.is_empty() {
                input_current = input_ws;
                break;
            }

            match preceded(
                alt((
                    tag_no_case::<_, _, NomErrorType<&str>>("OPTIONAL MATCH"),
                    tag_no_case::<_, _, NomErrorType<&str>>("MATCH"),
                )),
                parse_content_after_match_keyword,
            ).parse(input_ws) {
                Ok((input_after_match, parsed_patterns)) => {
                    let converted = convert_parsed_patterns_to_execution_format(parsed_patterns);
                    all_patterns.extend(converted);
                    input_current = input_after_match;
                }
                Err(_) => {
                    input_current = input_ws;
                    break;
                }
            }
        }
    }

    // --- 2b. PARSE OPTIONAL UNWIND CLAUSE ---
    let (input_after_unwind, unwind_clause_opt) = opt(preceded(
        multispace0,
        parse_unwind_clause,
    )).parse(input_current)?;

    let captured_unwind = unwind_clause_opt;
    input_current = input_after_unwind;

    // --- 3. PARSE OPTIONAL WHERE CLAUSE ---
    let (input_after_where, where_opt) = opt(preceded(
        multispace0,
        parse_where
    )).parse(input_current)?;
    captured_where = where_opt;
    input_current = input_after_where;

    // --- 4. PARSE OPTIONAL WITH CLAUSE ---
    let (input_after_with, with_clause_raw) = opt(preceded(
        multispace0,
        parse_with_full, 
    )).parse(input_current)?;

    captured_with = with_clause_raw;
    input_current = input_after_with;

    // === FIX START: Detect MERGE after WITH ===
    // If we are currently in a MATCH but see a MERGE after a WITH, pivot the query type.
    let (input_after_pivot, follow_up_merge) = opt(preceded(
        multispace0,
        tag_no_case("MERGE")
    )).parse(input_current)?;

    let mut is_merge = is_merge; // Make mutable
    if follow_up_merge.is_some() {
        is_merge = true; 
        input_current = input_after_pivot;
        
        // We must parse the patterns belonging to the MERGE specifically
        match parse_content_after_match_keyword(input_current) {
            Ok((input_after_m_patterns, parsed_patterns)) => {
                let converted = convert_parsed_patterns_to_execution_format(parsed_patterns);
                // Append these to all_patterns so execute_merge_query_with_context 
                // has the relationship patterns (e)-[r]->(p)
                all_patterns.extend(converted);
                input_current = input_after_m_patterns;
            }
            Err(e) => return Err(e),
        }
    }
    // === FIX END ===

    // --- 5. PARSE ON CLAUSES (MERGE only) ---
    if is_merge {
        loop {
            let (input_ws, _) = multispace0.parse(input_current)?;
            let trimmed = input_ws.trim_start();
            
            if trimmed.to_uppercase().starts_with("ON CREATE") {
                let (input_after, _) = tag_no_case("ON CREATE").parse(input_ws)?;
                let (input_after, _) = preceded(multispace0, tag_no_case("SET")).parse(input_after)?;
                let (input_after_list, clauses) = preceded(
                    multispace0,
                    separated_list1(
                        tuple((multispace0, char(','), multispace0)),
                        parse_single_set_assignment
                    )
                ).parse(input_after)?;
                on_create_set.extend(clauses);
                input_current = input_after_list;
            } else if trimmed.to_uppercase().starts_with("ON MATCH") {
                let (input_after, _) = tag_no_case("ON MATCH").parse(input_ws)?;
                let (input_after, _) = preceded(multispace0, tag_no_case("SET")).parse(input_after)?;
                let (input_after_list, clauses) = preceded(
                    multispace0,
                    separated_list1(
                        tuple((multispace0, char(','), multispace0)),
                        parse_single_set_assignment
                    )
                ).parse(input_after)?;
                on_match_set.extend(clauses);
                input_current = input_after_list;
            } else {
                input_current = input_ws;
                break;
            }
        }
    }

    // --- 6. PARSE CREATE CLAUSE ---
    let (input_after_create, create_patterns_raw) = opt(preceded(
        tuple((multispace0, tag_no_case("CREATE"), multispace1)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_single_pattern 
        )
    )).parse(input_current)?;

    let create_patterns: Vec<Pattern> = match create_patterns_raw {
        Some(raw_patterns) => raw_patterns,
        None => Vec::new(),
    };

    input_current = input_after_create;

    // --- 7. PARSE SET CLAUSE ---
    let (input_after_set, set_clauses_opt) = opt(preceded(
        tuple((multispace0, tag_no_case("SET"), multispace0)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_single_set_assignment
        )
    )).parse(input_current)?;
    input_current = input_after_set;

    // --- 8. PARSE REMOVE CLAUSE ---
    let (input_after_remove, remove_clauses_opt) = opt(preceded(
        tuple((multispace0, tag_no_case("REMOVE"), multispace0)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_remove_clause
        )
    )).parse(input_current)?;
    input_current = input_after_remove;

    // --- 9. PARSE RETURN CLAUSE (with robust NBSP and Bare RETURN handling) ---
    println!("===> full_statement_parser: About to parse RETURN, input: '{}'", input_current.trim());
    
    // 1. Use your new any_whitespace0 to handle hidden NBSP characters
    let (input_after_ws, _) = any_whitespace0(input_current)?;
    
    // 2. Check for RETURN keyword
    let (input_after_return, return_opt) = if input_after_ws.to_uppercase().starts_with("RETURN") {
        // We found RETURN, now let parse_return_clause_as_struct handle the rest
        // We use opt() so it doesn't crash if it's slightly malformed
        match parse_return_clause_as_struct(input_after_ws) {
            Ok((rem, ret)) => (rem, Some(ret)),
            Err(_) => {
                // If it fails but we SAW return, we force the bare RETURN "*" default
                // This ensures MPI transactions are always logged even if the line ends abruptly
                println!("===> Fallback: Forcing wildcard RETURN for MPI trace");
                ("", Some(ReturnClause {
                    items: vec![ReturnItem { expression: "*".to_string(), alias: None }],
                    distinct: false,
                    order_by: None,
                    skip: None,
                    limit: None,
                }))
            }
        }
    } else {
        // No RETURN keyword found at all, skip this section
        (input_current, None)
    };
    
    captured_return = return_opt;
    input_current = input_after_return;

    // --- 10. FINAL CONSUMPTION ---
    let (input_final, _) = opt(preceded(multispace0, alt((tag(";"), multispace1)))).parse(input_current)?;

    // --- 11. ANONYMOUS VARIABLES ---
    let set_clauses = set_clauses_opt.unwrap_or_default();
    let remove_clauses = remove_clauses_opt.unwrap_or_default();

    let mut create_patterns_mut = create_patterns;
    let mut anon_var_counter = 0;
    for pattern in create_patterns_mut.iter_mut() {
        if !pattern.2.is_empty() {
            for node_pattern in pattern.1.iter_mut() {
                if node_pattern.0.is_none() {
                    node_pattern.0 = Some(format!("_anon_{}", anon_var_counter));
                    anon_var_counter += 1;
                }
            }
        }
    }

    // --- 12. DISPATCH ---
    if is_merge {
        Ok((input_final, CypherQuery::Merge { 
            patterns: all_patterns, 
            where_clause: captured_where,
            with_clause: captured_with,
            on_create_set, 
            on_match_set, 
            return_clause: captured_return,
        }))
    } else if captured_unwind.is_some() {
        let mut chain_clauses = vec![
            CypherQuery::MatchPattern {
                patterns: all_patterns,
                where_clause: captured_where,
                with_clause: captured_with,
                return_clause: None,
            },
            captured_unwind.unwrap(),
        ];
        
        if let Some(ret) = captured_return {
            // Reconstruct a clean projection string from the items
            let projection_items_str = ret.items.iter()
                .map(|item| {
                    if let Some(alias) = &item.alias {
                        format!("{} AS {}", item.expression, alias)
                    } else {
                        item.expression.clone()
                    }
                })
                .collect::<Vec<String>>()
                .join(", ");

            chain_clauses.push(CypherQuery::ReturnStatement {
                projection_string: format!("RETURN {}", projection_items_str),
                order_by: ret.order_by.clone().unwrap_or_default()
                    .into_iter()
                    .map(|(expr, asc)| OrderByItem { 
                        expression: expr, 
                        ascending: asc 
                    })
                    .collect(),
                skip: ret.skip.map(|s| s as i64),
                limit: ret.limit.map(|l| l as i64),
                is_distinct: ret.distinct, // FIXED: Missing field added here
            });
        }
        
        Ok((input_final, CypherQuery::Chain(chain_clauses)))
    } else if !create_patterns_mut.is_empty() {
        Ok((input_final, CypherQuery::MatchCreate { 
            match_patterns: all_patterns, 
            where_clause: captured_where,
            with_clause: captured_with,
            create_patterns: create_patterns_mut 
        }))
    } else if !set_clauses.is_empty() {
        Ok((input_final, CypherQuery::MatchSet { 
            match_patterns: all_patterns, 
            where_clause: captured_where,
            with_clause: captured_with,
            set_clauses 
        }))
    } else if !remove_clauses.is_empty() {
        Ok((input_final, CypherQuery::MatchRemove { 
            match_patterns: all_patterns, 
            where_clause: captured_where,
            with_clause: captured_with,
            remove_clauses 
        }))
    } else {
        Ok((input_final, CypherQuery::MatchPattern { 
            patterns: all_patterns, 
            where_clause: captured_where,
            with_clause: captured_with,
            return_clause: captured_return
        }))
    }
}

// ------------------------------------------------------------------
// MATCH path = (left)-[*..]-(right) RETURN …  (single statement only)
/// Parse **only**   MATCH path = (left)-[*..]-(right) RETURN …
/// Supports variable-length patterns: *0..2, *1..5, *, etc.
// ------------------------------------------------------------------
pub fn parse_match_path(input: &str) -> IResult<&str, CypherQuery, nom::error::Error<&str>> {
    println!("===> parse_match_path START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);

    // MATCH keyword
    let (input, _) = terminated(tag_no_case("MATCH"), multispace1).parse(input)?;
    
    // Path variable: path =
    let (input, path_var) = terminated(
        recognize(pair(alpha1, many0(alphanumeric1))),
        tuple((multispace0, tag("="), multispace0))
    ).parse(input)?;

    // Left node: (p:Patient {id: "12345"}) - capture everything between ( and )
    let (input, _) = tag("(").parse(input)?;
    let (input, left_content) = recognize(many0(none_of(")"))).parse(input)?;
    let (input, _) = tag(")").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Relationship pattern: -[*0..2]- or -[:TYPE*0..2]- or -[r:TYPE*0..2]-
    let (input, _) = tag("-[").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Optional variable and type: r:TYPE or :TYPE or just r
    let (input, _rel_info) = opt(tuple((
        opt(terminated(
            recognize(pair(alpha1, many0(alphanumeric1))), 
            multispace0
        )), // optional var
        opt(preceded(
            tuple((tag(":"), multispace0)),
            terminated(
                recognize(pair(alpha1, many0(alphanumeric1))),
                multispace0
            )
        )), // optional :TYPE
    ))).parse(input)?;
    
    // Variable-length indicator: *0..2 or *1.. or * or *5
    let (input, _) = tag("*").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Capture the range: could be "0..2" or "1.." or just "2" or nothing
    let (input, _range) = opt(alt((
        // N..M format
        recognize(tuple((
            digit1,
            tag(".."),
            opt(digit1),
        ))),
        // Just N format (means up to N)
        recognize(digit1),
    ))).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("]-").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Right node: (related) - capture everything between ( and )
    let (input, _) = tag("(").parse(input)?;
    let (input, right_content) = recognize(many0(none_of(")"))).parse(input)?;
    let (input, _) = tag(")").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Optional RETURN clause - capture everything after RETURN
    let (input, return_clause) = opt(preceded(
        tuple((tag_no_case("RETURN"), multispace1)),
        take_while(|c| c != ';' && c != '\n' && c != '\r')
    )).parse(input)?;

    Ok((input, CypherQuery::MatchPath {
        path_var: path_var.to_string(),
        left_node: format!("({})", left_content),
        right_node: format!("({})", right_content),
        return_clause: return_clause.unwrap_or("").trim().to_string(),
    }))
}

fn none_of<'a>(chars: &'static str) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str, nom::error::Error<&'a str>> {
    move |i: &'a str| match i.chars().next() {
        Some(c) if !chars.contains(c) => Ok((&i[c.len_utf8()..], &i[..c.len_utf8()])),
        _ => Err(nom::Err::Error(nom::error::Error::new(i, nom::error::ErrorKind::NoneOf))),
    }
}

// === DELETE EDGES PARSER (add this function) ===
/// Parse: MATCH (a)-[r:KNOWS]->(b) DELETE r
///        MATCH ()-[r]->() DELETE r
///        MATCH (a)-[r]-(b) DELETE r
fn parse_delete_edges(input: &str) -> IResult<&str, CypherQuery, nom::error::Error<&str>> {
    println!("===> parse_delete_edges START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 1. Parse raw patterns (Node labels are Option<String>)
    let (input, patterns) = parse_match_clause_patterns(input)?;
    let pattern = patterns.into_iter().next().ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    
    // 2. Optional WHERE clause
    let (input, where_clause) = opt(preceded(
        multispace0,
        parse_where
    )).parse(input)?;
    
    // 3. ✅ Optional WITH clause (for MPI logic compliance)
    let (input, with_clause_raw) = opt(preceded(
        multispace0,
        parse_with
    )).parse(input)?;

    // Convert WithClause → ParsedWithClause
    let with_clause: Option<ParsedWithClause> = with_clause_raw.map(|w| ParsedWithClause {
        items: Vec::new(),
        distinct: false,
        where_clause: Some(WhereClause { condition: w.condition }),
        order_by: Vec::new(),
        skip: None,
        limit: None,
    });

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("DELETE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 4. Extract the relationship variable name (e.g. "r" in [r:KNOWS])
    let edge_var = pattern.2.first()
        .and_then(|rel| rel.0.as_ref())
        .ok_or_else(|| {
            nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
        })?
        .clone();
    
    // 5. Consume the variable name
    let (remaining, _) = tag(&*edge_var).parse(input)?;
    
    // 6. Transform nodes from Raw (Option label) to Execution (Vec labels)
    let execution_nodes: Vec<(Option<String>, Vec<String>, HashMap<String, Value>)> = pattern.1
        .into_iter()
        .map(|(var, label_opt, props)| {
            let labels_vec = match label_opt {
                Some(l) => vec![l],
                None => vec![],
            };
            (var, labels_vec, props)
        })
        .collect();
    
    println!("===> Parsed DELETE edges: var='{}', nodes={}, rels={}",
             edge_var, execution_nodes.len(), pattern.2.len());
    
    // 7. Return with both where_clause and with_clause
    Ok((
        remaining,
        CypherQuery::DeleteEdges {
            edge_variable: edge_var,
            pattern: MatchPattern {
                nodes: execution_nodes,
                relationships: pattern.2,
            },
            where_clause,
            with_clause, // ✅ Now included
        },
    ))
}

// ----------------------------------------------------------------------------------
// --- UPDATED: parse_cypher (To correctly flatten results from sequential parser) ---
// ----------------------------------------------------------------------------------

pub fn parse_cypher(query: &str) -> Result<CypherQuery, String> {
    println!("=====> PARSING CYPHER");
    println!("===> parse_cypher START");
    if !is_cypher(query) {
        return Err("Not a valid Cypher query.".to_string());
    }

    // 1. Normalise new-lines and clean up the string.
    let query_clean = query
        .replace("\\n", " ")
        .replace('\n', " ")
        .replace('\r', " ")
        .trim()
        .to_string();

    println!("===> Processing query: {}", query_clean);

    // 2. Split the query by semicolon (standard Cypher statement separator).
    let stmts: Vec<&str> = query_clean
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .collect();

    if stmts.is_empty() {
        return Err("Query resulted in no executable statements.".to_string());
    }

    let mut batch_results = Vec::new();

    for stmt in stmts {
        // 3. Parse each semicolon-separated statement chunk.
        let parsed_stmt = parse_single_statement(stmt.trim())?;

        // 4. Flatten the result: If the parsed statement is itself a Batch (from 
        //    `parse_sequential_statements`), append its contents to the main batch.
        match parsed_stmt {
            CypherQuery::Batch(mut sub_batch) => {
                batch_results.append(&mut sub_batch);
            }
            other => {
                batch_results.push(other);
            }
        }
    }

    // 5. Always return the combined batch for sequential execution.
    Ok(CypherQuery::Batch(batch_results))
}

/// Parse: MATCH ()-[r]->() DELETE r
fn parse_delete_edges_simple(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_delete_edges_simple START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("MATCH")(input)?;
    let (input, _) = multispace1(input)?;
    
    // 1. Get raw patterns (Node labels are Option<String>)
    let (input, patterns) = parse_match_clause_patterns(input)?;
    let pattern = patterns.into_iter().next().ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    
    // 2. Extract edge variable name from the first relationship
    let edge_var = pattern.2.first()
        .and_then(|rel| rel.0.as_ref())
        .ok_or_else(|| {
            nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
        })?
        .clone();
    
    // 3. Consume DELETE and the specific variable
    let (remaining_input, _) = tuple((
        multispace0,
        tag_no_case("DELETE"),
        multispace1,
        tag(&*edge_var),
        multispace0,
    ))(input)?;
    
    // 4. Transform nodes from Raw (Option label) to Execution (Vec label)
    // This matches the NodePattern requirement in query_types.rs
    let execution_nodes: Vec<(Option<String>, Vec<String>, HashMap<String, Value>)> = pattern.1
        .into_iter()
        .map(|(var, label_opt, props)| {
            let labels_vec = match label_opt {
                Some(l) => vec![l],
                None => vec![],
            };
            (var, labels_vec, props)
        })
        .collect();
    
    // 5. Return with correctly typed nodes and all required fields
    Ok((remaining_input, CypherQuery::DeleteEdges {
        edge_variable: edge_var, 
        pattern: MatchPattern {
            nodes: execution_nodes,
            relationships: pattern.2,
        },
        where_clause: None,
        with_clause: None, // Required by variant definition
    }))
}

/// Parse  MATCH (n:Person) DETACH DELETE n
/// returns  CypherQuery::DetachDeleteNodes { node_variable: "n", label: Some("Person") }
fn parse_detach_delete(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_detach_delete START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, _) = tag_no_case("MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, node_pat) = parse_node(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag_no_case("DETACH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag_no_case("DELETE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, delete_var) = parse_identifier.parse(input)?;
    
    // Ensure the delete variable matches the node variable
    if let Some(node_var) = &node_pat.0 {
        if node_var != delete_var {
            return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)));
        }
    } else {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)));
    }

    // node_pat.1 is Vec<String>. Extract the first label to match Option<String> requirement.
    let label = node_pat.1.first().cloned();

    Ok((
        input,
        CypherQuery::DetachDeleteNodes {
            node_variable: delete_var.to_string(),
            label,
        }
    ))
}

// Assuming NodePattern, WhereClause, and MatchPattern are defined types
fn parse_match_detach_delete(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_match_detach_delete START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    use nom::Parser;
    use nom::bytes::complete::tag_no_case;
    use nom::character::complete::{multispace0, multispace1, char};
    use nom::combinator::opt;
    use nom::sequence::preceded;

    // 1. Consume the initial "MATCH" keyword
    let (input, _) = tag_no_case("MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 2. Parse the patterns (Raw format: labels are Option<String> in PatternsReturnType)
    let (input, match_patterns_raw) = parse_match_clause_patterns(input)?;
    
    // 3. Parse optional WHERE clause
    let (input, _where_clause) = opt(preceded(multispace1, parse_where_clause)).parse(input)?;

    // 4. Parse DETACH DELETE
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("DETACH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag_no_case("DELETE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, node_variable_raw) = parse_identifier.parse(input)?;
    let node_variable = node_variable_raw.to_string();

    // 5. Find the node label
    // match_patterns_raw is PatternsReturnType
    // Node segment is (Option<String>, Option<String>, HashMap<String, Value>)
    let label = match_patterns_raw.iter()
        .flat_map(|(_, nodes, _)| nodes.iter())
        .find(|(var_opt, _, _)| var_opt.as_ref().map_or(false, |v| v == &node_variable))
        .and_then(|(_, label_opt, _)| {
            // FIX: label_opt is &Option<String>, not a Vec. 
            // We just clone the Option.
            label_opt.clone()
        });

    let (input, _) = opt(char(';')).parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    Ok((input, CypherQuery::DetachDeleteNodes {
        node_variable,
        label,
    }))
}

// Parser for MATCH ... REMOVE
// Note: We need a generic type for the input 'I' and a custom error 'E' 
// to align with the type signature used in the tags.
fn parse_match_remove_relationship<'a>(input: &'a str) -> IResult<&'a str, CypherQuery> {
    println!("===> parse_match_remove_relationship START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    type NomErrorType<'a> = nom::error::Error<&'a str>;
    
    // 1. Parse MATCH keyword and patterns
    let (input, _) = tag_no_case::<_, _, NomErrorType<'a>>("MATCH")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, raw_match_patterns) = parse_match_clause_patterns(input)?;

    // 2. Parse Optional WHERE clause 
    let (input, where_clause) = opt(preceded(
        multispace1,
        parse_where
    )).parse(input)?;

    // 3. Parse Optional WITH clause (MPI logic wrapper)
    let (input, with_clause_raw) = opt(preceded(
        multispace1,
        parse_with
    )).parse(input)?;

    // Bridge: WithClause → ParsedWithClause
    let with_clause: Option<ParsedWithClause> = with_clause_raw.map(|w| ParsedWithClause {
        items: Vec::new(),
        distinct: false,
        where_clause: Some(WhereClause { condition: w.condition }),
        order_by: Vec::new(),
        skip: None,
        limit: None,
    });

    // 4. Parse REMOVE keyword
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case::<_, _, NomErrorType<'a>>("REMOVE")(input)?;
    let (input, _) = multispace1(input)?;
    
    // 5. Parse the list of properties to remove
    let (input, remove_clauses) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_remove_clause
    ).parse(input)?;
    
    // 6. Optionally consume RETURN clause
    let (input, _) = opt(
        preceded(
            tuple((multispace1, tag_no_case::<_, _, NomErrorType<'a>>("RETURN"), multispace1)),
            take_while(|c| c != ';' && c != '\n'),
        )
    ).parse(input)?;

    // 7. Transform patterns to execution format
    let match_patterns: ExecutionPatternsReturnType = raw_match_patterns
        .into_iter()
        .map(|(path_var, nodes, edges)| {
            let transformed_nodes = nodes
                .into_iter()
                .map(|(var, label_opt, props)| {
                    let labels_vec = match label_opt {
                        Some(l) => vec![l],
                        None => vec![],
                    };
                    (var, labels_vec, props)
                })
                .collect();
            (path_var, transformed_nodes, edges)
        })
        .collect();
    
    // 8. Return with `with_clause` included
    Ok((input, CypherQuery::MatchRemove {
        match_patterns,
        where_clause, 
        with_clause,      // ✅ Now provided
        remove_clauses,
    }))
}

fn parse_clause_with_symbol_table<'a>(
    input: &'a str, 
    symbol_table: &SymbolTable
) -> IResult<&'a str, (CypherQuery, SymbolTable)> {
    println!("===> parse_clause_with_symbol_table START, input: '{}'", &input[..input.len().min(50)]);
    
    let trimmed = input.trim();
    let upper = trimmed.to_uppercase();
    
    // 1. Handle UNWIND (Highest priority)
    if upper.starts_with("UNWIND") {
        if let Ok((remainder, query)) = parse_unwind_clause(trimmed) {
            let mut updated_symbols = symbol_table.clone();
            if let CypherQuery::Unwind { ref variable, .. } = query {
                updated_symbols.bind_variable(variable.clone(), SerializableUuid::default());
            }
            return Ok((remainder, (query, updated_symbols)));
        }
    }
    
    // 2. Handle MERGE
    if upper.starts_with("MERGE") {
        if let Ok((remainder, clause)) = parse_merge(trimmed) {
            let mut updated_symbols = symbol_table.clone();
            if let CypherQuery::Merge { patterns, .. } = &clause {
                for pattern in patterns {
                    for (var_name, _, _) in &pattern.1 {
                        if let Some(var) = var_name {
                            if !updated_symbols.variables.contains_key(var) {
                                updated_symbols.bind_variable(var.clone(), SerializableUuid::default());
                            }
                        }
                    }
                }
            }
            return Ok((remainder, (clause, updated_symbols)));
        }
    }
    
    // 3. Handle MATCH / OPTIONAL MATCH
    if upper.starts_with("MATCH") || upper.starts_with("OPTIONAL MATCH") {
        if let Ok((remainder, clause)) = full_statement_parser(trimmed) {
            let mut updated_symbols = symbol_table.clone();
            
            match &clause {
                CypherQuery::MatchPattern { patterns, with_clause, .. } 
                | CypherQuery::MatchCreate { match_patterns: patterns, with_clause, .. }
                | CypherQuery::MatchSet { match_patterns: patterns, with_clause, .. } => {
                    for pattern in patterns {
                        for (var_name, _, _) in &pattern.1 {
                            if let Some(var) = var_name {
                                if !updated_symbols.variables.contains_key(var) {
                                    updated_symbols.bind_variable(var.clone(), SerializableUuid::default());
                                }
                            }
                        }
                        for rel in &pattern.2 {
                            if let Some(var_name) = &rel.0 {
                                if !updated_symbols.relationships.contains_key(var_name) {
                                    updated_symbols.bind_relationship(var_name.clone(), SerializableUuid::default());
                                }
                            }
                        }
                    }
                    if let Some(with_data) = with_clause {
                        for item in &with_data.items {
                            if let Some(alias) = &item.alias {
                                if !updated_symbols.variables.contains_key(alias) {
                                    updated_symbols.bind_variable(alias.clone(), SerializableUuid::default());
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
            return Ok((remainder, (clause, updated_symbols)));
        }
    }
    
    // 4. Handle CREATE
    if upper.starts_with("CREATE") {
        if let Ok((remainder, query)) = parse_create_statement(trimmed) {
            return Ok((remainder, (query, symbol_table.clone())));
        }
    }
    
    // 5. Handle SET
    if upper.starts_with("SET") {
        let (after_set, _) = tag_no_case::<_, _, nom::error::Error<&str>>("SET")(trimmed)?;
        let (after_set, _) = multispace1(after_set)?;
        let (remainder, assignments) = separated_list1(
            delimited(multispace0, char(','), multispace0),
            parse_single_set_assignment
        ).parse(after_set)?;
        return Ok((remainder, (CypherQuery::SetStatement { assignments }, symbol_table.clone())));
    }
    
    // 6. Handle REMOVE
    if upper.starts_with("REMOVE") {
        let (after_remove, _) = tag_no_case::<_, _, nom::error::Error<&str>>("REMOVE")(trimmed)?;
        let (after_remove, _) = multispace1(after_remove)?;
        let (remainder, removals) = separated_list1(
            delimited(multispace0, char(','), multispace0),
            parse_remove_clause
        ).parse(after_remove)?;
        return Ok((remainder, (CypherQuery::RemoveStatement { removals }, symbol_table.clone())));
    }

    // 7. Handle RETURN
    if upper.starts_with("RETURN") {
        println!("===> parse_clause_with_symbol_table: Trying RETURN");
        if let Ok((remainder, return_clause)) = parse_return_clause_as_struct(trimmed) {
            
            // --- FIX: Ensure cleaned values are owned Strings to satisfy the borrow checker ---
            let clean_projection: String = return_clause.items.iter()
                .map(|item| {
                    let clean_expr = item.expression
                        .replace('\\', "")
                        .trim_matches('"')
                        .to_string(); // Own it

                    if let Some(alias) = &item.alias {
                        let clean_alias = alias
                            .replace('\\', "")
                            .trim_matches('"')
                            .to_string(); // Own it
                        format!("{} AS {}", clean_expr, clean_alias)
                    } else {
                        clean_expr
                    }
                })
                .collect::<Vec<String>>()
                .join(", ");

            // Build the query and include the missing is_distinct field
            let query = CypherQuery::ReturnStatement {
                projection_string: format!("RETURN {}", clean_projection),
                order_by: return_clause.order_by.unwrap_or_default()
                    .into_iter()
                    .map(|(expr, asc)| OrderByItem { 
                        expression: expr.replace('\\', "").trim_matches('"').to_string(), 
                        ascending: asc 
                    })
                    .collect(),
                skip: return_clause.skip.map(|s| s as i64),
                limit: return_clause.limit.map(|l| l as i64),
                is_distinct: return_clause.distinct, // FIXED: Added field here
            };
            return Ok((remainder, (query, symbol_table.clone())));
        }
    }
        
    // 8. Handle WITH
    if upper.starts_with("WITH") {
        if let Ok((remainder, with_clause)) = parse_with_full(trimmed) {
            let query = CypherQuery::MatchPattern {
                patterns: vec![],
                where_clause: None,
                with_clause: Some(with_clause),
                return_clause: None,
            };
            return Ok((remainder, (query, symbol_table.clone())));
        }
    }
    
    Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))
}

/// your **old** top-level logic, just moved into a helper
/// Helper function to parse a single statement string.
// ----------------------------------------------------------------------------------
// --- UPDATED: parse_single_statement (Prioritizes MATCH...SET over MATCH...CREATE) ---
// ----------------------------------------------------------------------------------
// In cypher_parser.rs
/// Helper function to parse a single statement string.
pub fn parse_single_statement(input: &str) -> Result<CypherQuery, String> {
    println!("===> parse_single_statement START...");
    let trimmed = input.trim();
    let upper = trimmed.to_ascii_uppercase();

    // 1. DETACH DELETE (Specific, keep at top)
    if upper.contains("DETACH DELETE") {
        return parse_match_detach_delete(trimmed)
            .map(|(_, q)| q)
            .map_err(|e| format!("MATCH-DETACH-DELETE parse error: {:?}", e));
    }

    // --- CRITICAL LOOKAHEAD: Identify Keywords ---
    let has_return = upper.contains("RETURN");
    let has_create = upper.contains("CREATE");
    let has_merge = upper.contains("MERGE");
    let has_with = upper.contains("WITH");
    let has_union = upper.contains("UNION");
    
    // Count occurrences
    let match_count = upper.matches("MATCH").count();
    let merge_count = upper.matches("MERGE").count();
    let has_multiple_match = match_count > 1;
    let has_multiple_merge = merge_count > 1;

    // 2. Sequential/Chained statements (MPI Logic)
    // CRITICAL: Must handle MATCH...MATCH...WITH...MERGE...RETURN patterns
    if has_union 
        || has_with 
        || has_multiple_merge 
        || has_multiple_match
        || (upper.starts_with("MATCH") && has_merge && has_with)
        || (upper.starts_with("MATCH") && upper.contains("WITH") && upper.contains("ORDER BY"))
    {
        let initial_symbols = SymbolTable::new();
        if let Ok((remainder, (query, _))) = parse_sequential_statements_with_context(trimmed, initial_symbols) {
            if remainder.trim().is_empty() {
                return Ok(query);
            }
        }
    }

    // 3. EXPLICIT LOOKAHEAD FIX: If query has RETURN but no complex patterns, prioritize full_statement_parser
    // Only for simple MATCH...RETURN (no WITH, no MERGE, no multiple MATCH)
    if (upper.starts_with("MATCH") || upper.starts_with("OPTIONAL MATCH")) 
        && has_return 
        && !has_with 
        && !has_merge 
        && !has_multiple_match 
    {
        if let Ok((remainder, query)) = full_statement_parser(trimmed) {
            if remainder.trim().is_empty() {
                return Ok(query);
            }
        }
    }

    // 4. MATCH ... CREATE ... SET (Only for simple single-pattern MATCH+CREATE without WITH)
    if upper.starts_with("MATCH") 
        && has_create 
        && upper.contains("SET") 
        && !has_with 
        && !has_multiple_match 
    {
        return parse_match_create_relationship(trimmed) 
            .map(|(_, q)| q)
            .map_err(|e| format!("MATCH-CREATE-SET parse error: {:?}", e));
    }
    
    // 5. MATCH ... CREATE (specific pattern, only simple cases without WITH)
    if upper.starts_with("MATCH") 
        && has_create 
        && !has_with 
        && !has_multiple_match 
    {
        return parse_match_create_relationship(trimmed)
            .map(|(_, q)| q)
            .map_err(|e| format!("MATCH-CREATE parse error: {:?}", e));
    }
    
    // 6. Single MERGE statement (without WITH or multiple clauses)
    if upper.starts_with("MERGE") 
        && !has_multiple_merge 
        && !has_with 
        && !has_multiple_match 
    {
        return parse_merge(trimmed)
            .map(|(remainder, query)| {
                let remainder_trimmed = remainder.trim();
                if !remainder_trimmed.is_empty() {
                    Err(format!("MERGE statement parsed partially. Remainder: '{}'", remainder_trimmed))
                } else {
                    Ok(query)
                }
            })
            .map_err(|e| format!("MERGE parse error: {:?}", e))
            .and_then(|res| res);
    }

    // 7. Single CREATE (no relationships)
    if upper.starts_with("CREATE") && !upper.contains("-[") {
        let create_parsers: Vec<Box<dyn Fn(&str) -> IResult<&str, CypherQuery>>> = vec![
            Box::new(parse_create_node),
            Box::new(parse_create_nodes),
            Box::new(parse_create_edge),
            Box::new(parse_create_edge_between_existing),
            Box::new(parse_create_complex_pattern),
        ];
        for parser in create_parsers {
            if let Ok((remainder, query)) = parser(trimmed) {
                if remainder.trim().is_empty() {
                    return Ok(query);
                }
            }
        }
    }
    
    // 8. Relationship patterns
    if upper.contains("-[") && (upper.contains("]->") || upper.contains("]-") || upper.contains("<-[")) {
        return parse_simple_query_type(trimmed)
            .map(|(_, q)| q)
            .map_err(|e| format!("Simple-statement parse error: {:?}", e));
    }
    
    // 9. Fallback MATCH (only for simple cases without WITH/MERGE)
    if (upper.starts_with("MATCH") || upper.starts_with("OPTIONAL MATCH")) 
        && !has_with 
        && !has_merge 
    {
        if let Ok((remainder, query)) = full_statement_parser(trimmed) {
            if remainder.trim().is_empty() {
                return Ok(query);
            }
        }
    }
    
    // 10. Final Fallback list
    let parsers: Vec<Box<dyn Fn(&str) -> IResult<&str, CypherQuery>>> = vec![
        Box::new(parse_create_statement),
        Box::new(parse_delete_edges_simple),
        Box::new(parse_detach_delete),
        Box::new(parse_create_index),
        Box::new(parse_set_node),
        Box::new(parse_delete_node),
        Box::new(parse_set_kv),
        Box::new(parse_get_kv),
        Box::new(parse_delete_kv),
    ];
    
    for parser in parsers {
        if let Ok((remainder, query)) = parser(trimmed) {
            if remainder.trim().is_empty() {
                return Ok(query);
            }
        }
    }
    
    Err(format!("Unable to parse statement: {}", trimmed))
}

// CORRECTED parse_pattern for Nom 8 with Parser trait
fn parse_pattern(input: &str) -> IResult<&str, Pattern> {
    println!("===> parse_pattern START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, path_var) = opt(terminated(
        parse_identifier, 
        tuple((multispace0, char('='), multispace0))
    )).parse(input)?;
    
    let (input, first_node) = parse_node(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    let mut nodes = vec![first_node];  // ✅ MUST be mutable — you push in the loop
    let mut rels = Vec::new();
    let mut remaining = input;
    
    loop {
        match parse_relationship(remaining) {
            Ok((rest, rel)) => {
                rels.push(rel);
                let (rest, _) = multispace0.parse(rest)?;
                
                match parse_node(rest) {
                    Ok((rest, node)) => {
                        nodes.push(node);  // ← requires `mut nodes`
                        let (rest, _) = multispace0.parse(rest)?;
                        remaining = rest;
                    }
                    Err(_) => break,
                }
            }
            Err(_) => break,
        }
    }
    
    Ok((remaining, (path_var.map(String::from), nodes, rels)))
}

// Helper function to handle optional whitespace around a parser
// Assumes multispace0 is imported from nom::character::complete
// FIX E0277/E0618: Defines 'ws' to return an explicit closure, making it compatible
// with the IResult function style when used as an argument to combinators.
fn ws<'a, F, O>(mut inner: F) -> impl FnMut(&'a str) -> IResult<&'a str, O>
where
    F: FnMut(&'a str) -> IResult<&'a str, O>,
{
    // Use the function call style for delimited
    move |input: &'a str| {
        delimited(multispace0, &mut inner, multispace0).parse(input)
    }
}

fn parse_pattern_with_stop_guard(input: &str) -> IResult<&str, Pattern> {
    parse_pattern_restricted(input)
}

/// Parse a value (string, number, boolean, null)
/// FIX: Re-ordered to prioritize Strings and handle UUID hyphens
pub fn parse_value(input: &str) -> IResult<&str, serde_json::Value> {
    println!("===> parse_value START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    
    alt((
        // String literals (single quotes)
        map(
            delimited(char('\''), take_while(|c| c != '\''), char('\'')),
            |s: &str| serde_json::Value::String(s.to_string())
        ),
        // String literals (double quotes)
        map(
            delimited(char('"'), take_while(|c| c != '"'), char('"')),
            |s: &str| serde_json::Value::String(s.to_string())
        ),
        // Booleans
        map(tag_no_case("true"), |_| serde_json::Value::Bool(true)),
        map(tag_no_case("false"), |_| serde_json::Value::Bool(false)),
        // Null
        map(tag_no_case("null"), |_| serde_json::Value::Null),
        // Numbers (float with decimal point)
        map(
            recognize(tuple((
                opt(char('-')),
                digit1,
                char('.'),
                digit1
            ))),
            |s: &str| serde_json::Value::Number(
                serde_json::Number::from_f64(s.parse().unwrap_or(0.0)).unwrap()
            )
        ),
        // Numbers (integers)
        map(
            recognize(tuple((opt(char('-')), digit1))),
            |s: &str| serde_json::Value::Number(
                serde_json::Number::from(s.parse::<i64>().unwrap_or(0))
            )
        ),
    )).parse(input)
}

// ============================================================================
// SET CLAUSE PARSING
// ============================================================================

/// Parse a single SET assignment: var.prop = value
/// Parses a single assignment: u.prop = expression
fn parse_single_set_assignment(input: &str) -> IResult<&str, (String, String, Expression)> {
    println!("===> parse_single_set_assignment START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    
    let (input, var_name) = preceded(multispace0, parse_identifier).parse(input)?;
    let (input, _) = preceded(multispace0, char('.')).parse(input)?;
    let (input, prop_name) = parse_identifier.parse(input)?;
    let (input, _) = delimited(multispace0, char('='), multispace0).parse(input)?;
    
    // Use alt with specific parsers instead of general parse_expression
    // This prevents consuming past commas and keywords
    let (input, expr) = alt((
        // Function calls like timestamp()
        parse_function_call_full,
        
        // String/number/boolean literals
        map(parse_value, |v| Expression::Literal(CypherValue::from_json(v))),
        
        // Property access like other.property
        map(
            tuple((parse_identifier, char('.'), parse_identifier)),
            |(var, _, prop)| Expression::Property(PropertyAccess::Vertex(var.to_string(), prop.to_string()))
        ),
        
        // Simple identifier
        map(parse_identifier, |v| Expression::Variable(v.to_string())),
    )).parse(input)?;
    
    Ok((input, (var_name.to_string(), prop_name.to_string(), expr)))
}

/// Evaluates a comparison between two CypherValues using the given operator.
pub fn evaluate_comparison(
    left: &CypherValue,
    operator: &str,
    right: &CypherValue,
) -> GraphResult<CypherValue> {
    // Handle nulls: any comparison with null (except IS NULL) returns null
    if matches!(left, CypherValue::Null) || matches!(right, CypherValue::Null) {
        return Ok(CypherValue::Null);
    }

    let op_upper = operator.to_uppercase();

    // 1. Handle IN operator
    if op_upper == "IN" {
        match right {
            CypherValue::List(list) => {
                return Ok(CypherValue::Bool(list.contains(left)));
            },
            _ => {
                return Err(GraphError::QueryExecutionError(
                    "The 'IN' operator requires a list on the right side".to_string()
                ));
            }
        }
    }

    // 2. Handle String-Specific Operators (CONTAINS, STARTS WITH, ENDS WITH, and Regex)
    // We consolidate these to ensure they work regardless of whether a UUID has been
    // pre-converted to a String or remains a Uuid variant.
    let is_string_op = matches!(op_upper.as_str(), "CONTAINS" | "STARTS WITH" | "ENDS WITH" | "=~");
    if is_string_op {
        let left_str = match left {
            CypherValue::String(s) => s.clone(),
            CypherValue::Uuid(u) => u.to_string(),
            _ => return Err(GraphError::QueryExecutionError(
                format!("Operator '{}' requires string or uuid on the left", operator)
            )),
        };

        let right_str = match right {
            CypherValue::String(s) => s.clone(),
            CypherValue::Uuid(u) => u.to_string(),
            _ => return Err(GraphError::QueryExecutionError(
                format!("Operator '{}' requires string pattern on the right", operator)
            )),
        };

        let res = match op_upper.as_str() {
            "CONTAINS" => left_str.contains(&right_str),
            "STARTS WITH" => left_str.starts_with(&right_str),
            "ENDS WITH" => left_str.ends_with(&right_str),
            "=~" => {
                match regex::Regex::new(&right_str) {
                    Ok(re) => re.is_match(&left_str),
                    Err(_) => return Err(GraphError::QueryExecutionError(
                        format!("Invalid regex pattern: {}", right_str)
                    )),
                }
            },
            _ => false,
        };
        return Ok(CypherValue::Bool(res));
    }

    // 3. General Comparison Logic
    let result = match (left, right) {
        // Numeric: Integer vs Float
        (CypherValue::Integer(a), CypherValue::Float(b)) => {
            let a_f = *a as f64;
            match op_upper.as_str() {
                "=" | "==" => a_f == *b,
                "<" => a_f < *b,
                "<=" => a_f <= *b,
                ">" => a_f > *b,
                ">=" => a_f >= *b,
                "<>" | "!=" => a_f != *b,
                _ => return Err(GraphError::QueryExecutionError(
                    format!("Unsupported operator '{}' for numeric types", operator)
                )),
            }
        },
        // Numeric: Float vs Integer
        (CypherValue::Float(a), CypherValue::Integer(b)) => {
            let b_f = *b as f64;
            match op_upper.as_str() {
                "=" | "==" => *a == b_f,
                "<" => *a < b_f,
                "<=" => *a <= b_f,
                ">" => *a > b_f,
                ">=" => *a >= b_f,
                "<>" | "!=" => *a != b_f,
                _ => return Err(GraphError::QueryExecutionError(
                    format!("Unsupported operator '{}' for numeric types", operator)
                )),
            }
        },
        // Numeric: Integer vs Integer
        (CypherValue::Integer(a), CypherValue::Integer(b)) => {
            match op_upper.as_str() {
                "=" | "==" => a == b,
                "<>" | "!=" => a != b,
                "<" => a < b,
                "<=" => a <= b,
                ">" => a > b,
                ">=" => a >= b,
                _ => return Err(GraphError::QueryExecutionError(
                    format!("Unsupported operator '{}' for integers", operator)
                )),
            }
        },
        // Numeric: Float vs Float
        (CypherValue::Float(a), CypherValue::Float(b)) => {
            match op_upper.as_str() {
                "=" | "==" => a == b,
                "<>" | "!=" => a != b,
                "<" => a < b,
                "<=" => a <= b,
                ">" => a > b,
                ">=" => a >= b,
                _ => return Err(GraphError::QueryExecutionError(
                    format!("Unsupported operator '{}' for floats", operator)
                )),
            }
        },
        // String vs String
        (CypherValue::String(a), CypherValue::String(b)) => {
            match op_upper.as_str() {
                "=" | "==" => a == b,
                "<>" | "!=" => a != b,
                "<" => a < b,
                "<=" => a <= b,
                ">" => a > b,
                ">=" => a >= b,
                _ => return Err(GraphError::QueryExecutionError(
                    format!("Unsupported operator '{}' for strings", operator)
                )),
            }
        },
        // Boolean vs Boolean
        (CypherValue::Bool(a), CypherValue::Bool(b)) => {
            match op_upper.as_str() {
                "=" | "==" => a == b,
                "<>" | "!=" => a != b,
                _ => return Err(GraphError::QueryExecutionError(
                    format!("Operator '{}' not supported for booleans", operator)
                )),
            }
        },
        // UUID vs String
        (CypherValue::Uuid(a), CypherValue::String(b)) => {
            match op_upper.as_str() {
                "=" | "==" => a.to_string() == *b,
                "<>" | "!=" => a.to_string() != *b,
                _ => return Err(GraphError::QueryExecutionError(
                    format!("Operator '{}' not supported for UUID comparison", operator)
                )),
            }
        },
        // String vs UUID
        (CypherValue::String(a), CypherValue::Uuid(b)) => {
            match op_upper.as_str() {
                "=" | "==" => *a == b.to_string(),
                "<>" | "!=" => *a != b.to_string(),
                _ => return Err(GraphError::QueryExecutionError(
                    format!("Operator '{}' not supported for UUID comparison", operator)
                )),
            }
        },
        // Vertex / Edge Constraints
        (CypherValue::Vertex(_), _) | (_, CypherValue::Vertex(_)) => {
            return Err(GraphError::QueryExecutionError(
                "Cannot compare vertex values directly".to_string()
            ));
        },
        (CypherValue::Edge(_), _) | (_, CypherValue::Edge(_)) => {
            return Err(GraphError::QueryExecutionError(
                "Cannot compare edge values directly".to_string()
            ));
        },
        _ => {
            return Err(GraphError::QueryExecutionError(
                format!("Cannot compare {:?} and {:?}", left, right)
            ));
        }
    };

    Ok(CypherValue::Bool(result))
}

fn evaluate_property_comparison(
    context: &EvaluationContext,
    variable: &str,
    property: &str,
    operator: &str,
    value: &serde_json::Value,
) -> StdResult<CypherValue, GraphError> {
    let target = context.variables.get(variable)
        .ok_or_else(|| GraphError::QueryExecutionError(format!("Variable '{}' not found", variable)))?;

    let left_val = match target {
        CypherValue::Vertex(v) => {
            match property {
                "id" => CypherValue::String(v.id.to_string()),
                "label" => CypherValue::String(v.label.to_string()),
                _ => v.properties.get(property)
                    .map(|p| CypherValue::from_json(serde_json::to_value(p).unwrap()))
                    .unwrap_or(CypherValue::Null),
            }
        },
        CypherValue::Edge(e) => {
            match property {
                "id" => CypherValue::String(e.id.to_string()),
                "label" | "type" => CypherValue::String(e.label.to_string()),
                _ => e.properties.get(property)
                    .map(|p| CypherValue::from_json(serde_json::to_value(p).unwrap()))
                    .unwrap_or(CypherValue::Null),
            }
        },
        _ => return Err(GraphError::QueryExecutionError(format!("'{}' is not a graph object", variable))),
    };

    let rhs_val = CypherValue::from_json(value.clone());
    evaluate_comparison(&left_val, operator, &rhs_val)
}

fn evaluate_function_comparison(
    context: &EvaluationContext,
    function: &str,
    argument: &str,
    operator: &str,
    value: &serde_json::Value,
) -> StdResult<CypherValue, GraphError> {
    // Evaluate the function result
    let func_upper = function.to_uppercase();
    let func_result = if func_upper == "ID" {
        if let Some(val) = context.variables.get(argument) {
            match val {
                CypherValue::Vertex(v) => CypherValue::String(v.id.to_string()),
                CypherValue::Edge(e) => CypherValue::String(e.id.to_string()),
                CypherValue::Uuid(u) => CypherValue::String(u.to_string()),
                _ => return Err(GraphError::QueryExecutionError(
                    format!("ID() requires vertex, edge, or UUID, got {:?}", val)
                )),
            }
        } else {
            return Err(GraphError::QueryExecutionError(
                format!("Variable '{}' not found for ID()", argument)
            ));
        }
    } else {
        return Err(GraphError::QueryExecutionError(
            format!("Unsupported function in comparison: {}", function)
        ));
    };

    // Handle NULL-aware operators
    let op_upper = operator.to_uppercase();
    if op_upper == "IS NOT NULL" {
        return Ok(CypherValue::Bool(!matches!(func_result, CypherValue::Null)));
    } else if op_upper == "IS NULL" {
        return Ok(CypherValue::Bool(matches!(func_result, CypherValue::Null)));
    }

    // Convert RHS JSON value to CypherValue
    let rhs_val = CypherValue::from_json(value.clone());

    // Perform comparison
    evaluate_comparison(&func_result, operator, &rhs_val)
}

fn parse_expression_list(input: &str) -> IResult<&str, Expression> {
    delimited(
        char('['),
        map(
            separated_list0(
                delimited(multispace0, char(','), multispace0),
                parse_expression, // Parse full expressions like p.first_name
            ),
            Expression::List,
        ),
        char(']')
    ).parse(input)
}

/// Parse a list literal like ['A', 'B', 'C'] or [1, 2, 3]
pub fn parse_list_literal(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_list_literal START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(50)]);
    
    let (input, _) = char('[').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Parse comma-separated list of literal values (NOT full expressions to avoid recursion)
    let (input, elements) = separated_list0(
        tuple((multispace0, char(','), multispace0)),
        alt((
            // String literals with single quotes (most common in Cypher)
            map(
                delimited(
                    char('\''),
                    take_while(|c| c != '\''),
                    char('\'')
                ),
                |s: &str| Expression::Literal(CypherValue::String(s.to_string()))
            ),
            // String literals with double quotes
            map(
                delimited(
                    char('"'),
                    take_while(|c| c != '"'),
                    char('"')
                ),
                |s: &str| Expression::Literal(CypherValue::String(s.to_string()))
            ),
            // Numbers (integers and floats)
            map(
                recognize(tuple((
                    opt(char('-')),
                    digit1,
                    opt(tuple((char('.'), digit1)))
                ))),
                |s: &str| {
                    if s.contains('.') {
                        Expression::Literal(CypherValue::Float(s.parse().unwrap_or(0.0)))
                    } else {
                        Expression::Literal(CypherValue::Integer(s.parse().unwrap_or(0)))
                    }
                }
            ),
            // Booleans
            map(
                alt((tag_no_case("true"), tag_no_case("false"))),
                |s: &str| Expression::Literal(CypherValue::Bool(s.to_lowercase() == "true"))
            ),
            // Null
            map(
                tag_no_case("null"),
                |_| Expression::Literal(CypherValue::Null)
            ),
        ))
    ).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(']').parse(input)?;
    
    println!("===> parse_list_literal END, found {} elements", elements.len());
    
    Ok((input, Expression::List(elements)))
}


/// Parses function calls with optional array indexing: func(args)[index]
fn parse_function_call_with_index(input: &str) -> IResult<&str, Expression> {
    let (input, func_call) = parse_function_call_full(input)?;
    
    // Check for array indexing immediately after the function call
    let (input, indexed_expr) = parse_array_indexing_chain(input, func_call)?;
    
    Ok((input, indexed_expr))
}

/// Parses a chain of array indexing operations: expr[index1][index2]...
fn parse_array_indexing_chain(input: &str, mut expr: Expression) -> IResult<&str, Expression> {
    let mut current_input = input;
    
    loop {
        let index_result = delimited(multispace0,
            tuple((char('['), delimited(multispace0, parse_expression, multispace0), char(']'))),
            multispace0
        ).parse(current_input);
        
        match index_result {
            Ok((remaining, (_, index_expr, _))) => {
                expr = Expression::ArrayIndex {
                    expr: Box::new(expr),
                    index: Box::new(index_expr),
                };
                current_input = remaining;
            }
            Err(_) => break,
        }
    }
    
    Ok((current_input, expr))
}

/// Parses a full function call with multiple arguments and nested expressions
fn parse_function_call_full(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_function_call_full START");
    
    let (input, _) = multispace0(input)?;
    let (input, name) = parse_identifier(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    
    // Parse arguments - can be empty or multiple expressions
    let (input, args) = separated_list0(
        delimited(multispace0, char(','), multispace0),
        parse_expression  // This now handles dynamic property access too
    ).parse(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;
    
    Ok((input, Expression::FunctionCall {
        name: name.to_string(),
        args,
    }))
}

fn parse_expression_string(input: &str) -> IResult<&str, String> {
    println!("===> parse_expression_string START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    

    // Use `recognize` to get the raw string that `parse_expression` would consume
    let (remaining, expr_str) = recognize(parse_expression).parse(input)?;

    Ok((remaining, expr_str.to_string()))
}

fn parse_binary_op(input: &str) -> IResult<&str, BinaryOp> {
    println!("===> parse_binary_op START");
    preceded(
        multispace0,
        alt((
            // Add IN here (case-insensitive)
            map(tag_no_case("IN"), |_| BinaryOp::In),
            // Comparisons
            map(tag("="), |_| BinaryOp::Eq),
            map(tag("=~"), |_| BinaryOp::Eq),
            map(tag("<>"), |_| BinaryOp::Neq),
            map(tag("!="), |_| BinaryOp::Neq),
            map(tag("<="), |_| BinaryOp::Lte),
            map(tag(">="), |_| BinaryOp::Gte),
            map(tag("<"), |_| BinaryOp::Lt),
            map(tag(">"), |_| BinaryOp::Gt),
            // Math
            map(tag("+"), |_| BinaryOp::Plus),
            map(tag("-"), |_| BinaryOp::Minus),
            map(tag("*"), |_| BinaryOp::Mul),
            map(tag("/"), |_| BinaryOp::Div),
        ))
    ).parse(input)
}

/// Parse SET clause for use in match_set_relationship
fn parse_set_clause(input: &str) -> IResult<&str, (String, HashMap<String, Value>)> {
    println!("===> parse_set_clause START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, var) = preceded(multispace0, parse_identifier).parse(input)?;
    let (input, _) = preceded(multispace0, char('.')).parse(input)?;
    let (input, prop) = parse_identifier.parse(input)?;
    let (input, _) = delimited(multispace0, char('='), multispace0).parse(input)?;
    
    // FIX: Use parse_property_value instead of parse_value to support 
    // the same logic used in node properties (UUIDs, timestamps, etc.)
    let (input, val) = parse_property_value.parse(input)?;
    
    let mut map = HashMap::new();
    map.insert(prop.to_string(), val);
    
    Ok((input, (var.to_string(), map)))
}

// New function: Parses a BATCH statement: BATCH { ...; ...; }
fn parse_batch(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_batch START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    map(
        preceded(
            terminated(tag_no_case("BATCH"), multispace1),
            // Uses many1 for required one-or-more statements
            delimited(
                char('{'), 
                many1(preceded(multispace0, terminated(parse_cypher_query_chain, char(';')))), 
                preceded(multispace0, char('}'))
            ),
        ),
        |statements| CypherQuery::Batch(statements),
    ).parse(input)
}

fn parse_single_query_clause(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_single_query_clause START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);

    let (input, _) = multispace0(input)?;
    if input.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Eof)));
    }

    let trimmed = input.trim_start();
    let upper = trimmed.to_uppercase();

    // 1. Parse sequence of clauses (MATCH -> UNWIND -> RETURN)
    if upper.starts_with("MATCH") || upper.starts_with("OPTIONAL MATCH") || upper.starts_with("MERGE") {
        let mut clauses = Vec::new();
        let mut current_input = input;

        // Parse first clause
        let (remaining, first_clause) = full_statement_parser(current_input)?;
        clauses.push(first_clause);
        current_input = remaining;

        // Parse subsequent clauses
        loop {
            let (rest, _) = multispace0(current_input)?;
            if rest.is_empty() {
                break;
            }

            let rest_trimmed = rest.trim_start();
            let rest_upper = rest_trimmed.to_uppercase();

            if rest_upper.starts_with("UNWIND") {
                let (new_remaining, unwind_clause) = parse_unwind_clause(rest)?;
                clauses.push(unwind_clause);
                current_input = new_remaining;
            } else if rest_upper.starts_with("RETURN") {
                let (new_remaining, return_struct) = parse_return_clause_as_struct(rest)?;
                
                let order_by_items = return_struct.order_by.clone().unwrap_or_default()
                    .into_iter()
                    .map(|(expr, asc)| OrderByItem { 
                        expression: expr, 
                        ascending: asc 
                    })
                    .collect();
                
                // FIX: Added is_distinct field here
                clauses.push(CypherQuery::ReturnStatement {
                    projection_string: format!("RETURN {:?}", return_struct.items),
                    order_by: order_by_items,
                    skip: return_struct.skip.map(|s| s as i64),
                    limit: return_struct.limit.map(|l| l as i64),
                    is_distinct: return_struct.distinct, // Use the value from the parsed struct
                });
                current_input = new_remaining;
                break;
            } else if rest_upper.starts_with("WITH") {
                let (new_remaining, with_clause) = parse_with_full(rest)?;
                
                let where_clause_opt = with_clause.where_clause.as_ref().map(|wc| WhereClause { 
                    condition: wc.condition.clone() 
                });
                
                clauses.push(CypherQuery::MatchPattern {
                    patterns: vec![],
                    where_clause: where_clause_opt,
                    with_clause: Some(with_clause),
                    return_clause: None,
                });
                current_input = new_remaining;
            } else {
                break;
            }
        }

        if clauses.len() == 1 {
            return Ok((current_input, clauses.into_iter().next().unwrap()));
        } else {
            return Ok((current_input, CypherQuery::Chain(clauses)));
        }
    }
    // 2. Handle standalone UNWIND
    else if upper.starts_with("UNWIND") {
        return parse_unwind_clause(input);
    }
    // 3. Handle standalone RETURN
    else if upper.starts_with("RETURN") {
        if let Ok((remaining, ret_struct)) = parse_return_clause_as_struct(input) {
            return Ok((remaining, CypherQuery::MatchPattern {
                patterns: vec![],
                where_clause: None,
                with_clause: None,
                return_clause: Some(ret_struct),
            }));
        }
    }

    // 4. Fallback for standalone WITH
    if let Ok(result) = parse_with_clause(input) {
        return Ok(result);
    }

    // 5. Fallback for standalone CREATE
    if let Ok((remaining, patterns)) = parse_create_clause(input) {
        return Ok((remaining, CypherQuery::CreateStatement {
            patterns,
            return_items: vec![],
        }));
    }

    // 6. Fallback for specialized mutation clauses (SET, DELETE, REMOVE)
    if let Ok(result) = parse_modifying_clause(input) {
        return Ok(result);
    }

    Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Alt)))
}

fn parse_set_standalone(input: &str) -> IResult<&str, CypherQuery> {
    // 1. Consume the SET keyword
    let (input, _) = preceded(multispace0, tag_no_case("SET")).parse(input)?;
    
    // 2. Parse the list of assignments
    // Since parse_single_set_assignment already returns (String, String, Expression),
    // we don't need the map closure at all.
    let (input, assignments) = preceded(
        multispace0,
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_single_set_assignment
        )
    ).parse(input)?;
    
    // 3. Return using the correct field name 'assignments'
    // This allows the GraphService to log these specific property updates
    // into the patient's identity history as required by MPI 2025-12-20.
    Ok((input, CypherQuery::SetStatement { assignments }))
}

pub fn parse_cypher_query_chain(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_cypher_query_chain START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    // 1. Parse the initial query block (e.g., MATCH...CREATE or just MATCH...RETURN)
    
    // Parse multiple preceding modifying clauses (MATCH, MERGE, WITH, etc.), separated by mandatory whitespace
    let (input, clauses) = many0(terminated(parse_single_query_clause, multispace1)).parse(input)?;
    
    // Parse the final, optional RETURN clause - should return CypherQuery
    let (input, return_clause_opt) = opt(parse_return_clause).parse(input)?;
    
    // Combine clauses with return clause if present
    let all_clauses = if let Some(ret) = return_clause_opt {
        // Only push if ret is actually a CypherQuery (not ())
        // This check helps if parse_return_clause has implementation issues
        let mut combined = clauses;
        combined.push(ret);
        combined
    } else {
        clauses
    };
    
    // Determine the base query structure
    let initial_query = if all_clauses.len() == 1 {
        // If only one clause, return it directly
        all_clauses.into_iter().next().unwrap()
    } else if all_clauses.is_empty() {
        // If no clauses found, try to parse BATCH
        return parse_batch(input);
    } else {
        // If multiple clauses (MATCH...MERGE...CREATE), chain them
        CypherQuery::Chain(all_clauses)
    };
    
    // 2. Handle subsequent UNION/UNION ALL parts recursively
    let union_block_parser = pair(
        preceded(multispace1, alt((
            map(terminated(tag_no_case::<_, _, NomErrorType<&str>>("UNION ALL"), multispace1), |_| true),
            map(terminated(tag_no_case::<_, _, NomErrorType<&str>>("UNION"), multispace1), |_| false),
        ))),
        parse_cypher_query_chain // Recursive call
    );
    let (input, union_parts) = many0(union_block_parser).parse(input)?;
    
    // 3. Assemble the result recursively: Left-associative Union structure
    let mut current_query = initial_query;
    for (is_all, next_query) in union_parts {
        // The Union variant must store the sub-queries in Box<CypherQuery>
        current_query = CypherQuery::Union(Box::new(current_query), is_all, Box::new(next_query));
    }
    Ok((input, current_query))
}

// CRITICAL FIX:
// 1. Make sure "use nom::Parser;" is at the top of your file
// 2. Use .parse(input)? syntax everywhere (this is Nom 8 with Parser trait)
// 3. Remove duplicate full_statement_parser
// 4. Check your existing parse_node, parse_relationship - they should use .parse() too

// CRITICAL FIX SUMMARY:
// 1. Remove DUPLICATE full_statement_parser (delete one of them)
// 2. Use the corrected parse_cypher above
// 3. Ensure parse_pattern comes BEFORE full_statement_parser in file
// 4. The parser chain order matters - full_statement_parser MUST be first in alt()
// In lib/src/query_parser/cypher_parser.rs

// Assuming parse_set_clause is defined elsewhere, likely:
// fn parse_set_clause(input: &str) -> IResult<&str, (String, String, Value)> { ... }
fn parse_cypher_statement(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_cypher_statement START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);

    // Delegate to the comprehensive full_statement_parser.
    // This ensures that WITH, WHERE, and RETURN clauses are parsed with 
    // their full metadata (distinct, order_by, etc.) rather than 
    // being manually reconstructed and losing information.
    let (input, query) = full_statement_parser(input)?;

    // We still perform the final whitespace/semicolon cleanup here to satisfy the 
    // original function's contract of consuming the end of the statement.
    let (input, _) = opt(preceded(multispace0, char(';'))).parse(input)?;
    let (input, _) = multispace0(input)?;

    Ok((input, query))
}

fn parse_pattern_restricted(input: &str) -> IResult<&str, Pattern> {
    println!("===> parse_pattern_restricted START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    // 1. Consume leading whitespace and get the ACTUAL start of the pattern
    let (remaining, _) = multispace0::<&str, nom::error::Error<&str>>(input)?;
    
    // 2. Keyword check on the TRIMMED string
    if is_at_keyword_boundary(remaining) {
        return Err(nom::Err::Error(nom::error::Error::new(remaining, nom::error::ErrorKind::Tag)));
    }
    
    // 3. IMPORTANT: Pass 'remaining' (the string WITHOUT leading spaces) 
    // to the next parser, otherwise we never advance the pointer.
    parse_single_pattern(remaining)
}

fn parse_variable_length(input: &str) -> IResult<&str, (Option<u32>, Option<u32>)> {
    println!("===> parse_variable_length START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('*').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Try to parse min
    let (input, min) = opt(nom::character::complete::u32).parse(input)?;
    
    // Try to parse .. separator
    let (input, has_range) = opt(tuple((
        multispace0,
        tag(".."),
        multispace0
    ))).parse(input)?;
    
    // Try to parse max
    let (input, max) = if has_range.is_some() {
        opt(nom::character::complete::u32).parse(input)?
    } else {
        (input, None)
    };
    
    let (input, _) = multispace0.parse(input)?;
    
    // Handle various formats:
    // *      -> (1, inf)
    // *2     -> (1, 2)  
    // *0..2  -> (0, 2)
    // *1..   -> (1, inf)
    let result = match (min, max) {
        (None, None) => (Some(1), None),        // * means 1 or more
        (Some(m), None) if has_range.is_none() => (Some(1), Some(m)), // *2 means up to 2
        (Some(m), None) => (Some(m), None),     // *1.. means 1 or more
        (Some(m), Some(n)) => (Some(m), Some(n)), // *0..2 means 0 to 2
        (None, Some(n)) => (Some(0), Some(n)),  // *..2 means up to 2
    };
    
    Ok((input, result))
}

// Parser for variable.property
fn parse_remove_clause(input: &str) -> IResult<&str, (String, String)> {
    println!("===> parse_remove_clause START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    // FIX: Wrap the entire parser definition in a move closure and execute it immediately.
    // This resolves the E0618 "expected function, found impl Parser<...>" error.

    (move |i| {
        map(
            tuple((
                parse_identifier,
                ws(char('.')),
                parse_identifier,
            )),
            |(variable, _, property)| (variable.to_string(), property.to_string()),
        ).parse(i)
    })(input)
}

// --- Relationship Type Parsers (Fixing the '|' operator) ---
// Parses a list of relationship types separated by the pipe '|' (OR) operator.
fn parse_rel_types_with_or(input: &str) -> IResult<&str, Vec<String>> {
    println!("===> parse_rel_types_with_or START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    
    separated_list1(
        delimited(multispace0, char('|'), multispace0),
        map(
            take_while1(|c: char| c.is_alphanumeric() || c == '_'),
            |s: &str| s.to_string()
        )
    )
    .parse(input)
}

// Parses the optional relationship type part, e.g., ":HAS_ENCOUNTER|HAS_DIAGNOSIS"
fn parse_optional_rel_types(input: &str) -> IResult<&str, Option<Vec<String>>> {
    println!("===> parse_optional_rel_types START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    opt(preceded(
        char(':'),
        parse_rel_types_with_or
    ))
    .parse(input)
}

fn parse_rel_detail(input: &str) -> IResult<&str, (Option<String>, Option<String>, Option<(Option<u32>, Option<u32>)>, HashMap<String, Value>)> {
    println!("===> parse_rel_detail( START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, _) = multispace0.parse(input)?;
    
    // Optional variable name
    let (input, var_opt) = opt(preceded(
        multispace0,
        take_while1(|c: char| c.is_alphanumeric() || c == '_')
    )).parse(input)?;
    
    // FIXED: Parse the relationship types (may include multiple types with |)
    let (input, rel_types_list_opt) = parse_optional_rel_types(input)?;
    
    // FIXED: Convert Option<Vec<String>> to Option<String>
    // Join multiple types with | to preserve all types in a single string
    let rel_type_opt = rel_types_list_opt.map(|types_list| {
        types_list.join("|")
    });

    // Optional variable-length specification: *min..max or *
    let (input, var_length_opt) = opt(parse_variable_length).parse(input)?;
    
    // Optional properties in {...}
    let (input, props) = if input.trim_start().starts_with('{') {
        delimited(
            preceded(multispace0, char('{')),
            map(
                opt(separated_list1(
                    preceded(multispace0, char(',')),
                    preceded(multispace0, parse_property)
                )),
                |props| props.unwrap_or_default().into_iter().collect()
            ),
            preceded(multispace0, char('}'))
        ).parse(input)?
    } else {
        (input, HashMap::new())
    };
    
    let (input, _) = multispace0.parse(input)?;
    
    Ok((input, (
        var_opt.map(|s| s.to_string()),
        rel_type_opt,
        var_length_opt,
        props
    )))
}

fn parse_optional_match_clause(input: &str) -> IResult<&str, Vec<Pattern>> {
    println!("===> parse_optional_match_clause START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, _) = tag("OPTIONAL MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, patterns) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_pattern
    ).parse(input)?;
    Ok((input, patterns))
}

// Parse patterns within a single MATCH clause, stopping at keyword boundaries
fn parse_match_clause_patterns(input: &str) -> IResult<&str, ParsedPatternsReturnType> {
    println!("===> parse_match_clause_patterns START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    
    let mut patterns = Vec::new();
    let mut current_input = input;
    
    loop {
        let (next_input, _) = multispace0::<&str, nom::error::Error<&str>>(current_input)?;
        current_input = next_input;
        
        if current_input.trim_start().is_empty() {
            break;
        }
        
        // Check if we hit a comma (multiple patterns in one MATCH)
        if current_input.starts_with(',') {
            current_input = &current_input[1..];
            continue;
        }

        // Termination check for clauses like WHERE, RETURN, CREATE, etc.
        if is_at_keyword_boundary(current_input) {
            break;
        }
        
        // Try to parse a full single pattern (node-rel-node or just node)
        match parse_single_pattern(current_input) {
            Ok((remainder, pattern)) => {
                patterns.push(pattern);
                current_input = remainder;
            }
            Err(_) => break,
        }
    }
    
    println!("===> parse_match_clause_patterns END – remainder length: {}, remainder: '{}'", 
             current_input.len(), current_input);
    
    // FIX: Convert Vec<String> labels → Option<String>
    let fixed_patterns: ParsedPatternsReturnType = patterns
        .into_iter()
        .map(|pattern| {
            let fixed_nodes = pattern.1
                .into_iter()
                .map(|(var, labels, props)| {
                    // FIX: Remove the .map(|s| Some(s)) wrapper.
                    // labels.first().cloned() is already Option<String>.
                    let first_label = labels.first().cloned();
                    (var, first_label, props)
                })
                .collect();
            (pattern.0, fixed_nodes, pattern.2)
        })
        .collect();
    
    Ok((current_input, fixed_patterns))
}

// Parses a Cypher numeric literal (signed integers or floats) and maps it to a PropertyValue.
// Parses a Cypher numeric literal (signed integers or floats) and maps it to a PropertyValue.
pub fn parse_numeric_literal(input: &str) -> IResult<&str, PropertyValue> {
    println!("===> parse_numeric_literal START");
    // Manual parsing approach to avoid nom parser composition issues
    let original_input = input;
    let mut chars = input.chars().peekable();
    let mut number_str = String::new();
    
    // Check for optional negative sign
    if let Some('-') = chars.peek() {
        number_str.push('-');
        chars.next();
    }
    
    // Check if we have at least one digit or a decimal point
    let mut has_digits = false;
    let mut has_decimal = false;
    
    // Parse digits before decimal point
    while let Some(&ch) = chars.peek() {
        if ch.is_ascii_digit() {
            number_str.push(ch);
            chars.next();
            has_digits = true;
        } else {
            break;
        }
    }
    
    // Check for decimal point
    if let Some(&'.') = chars.peek() {
        number_str.push('.');
        chars.next();
        has_decimal = true;
        
        // Parse digits after decimal point
        while let Some(&ch) = chars.peek() {
            if ch.is_ascii_digit() {
                number_str.push(ch);
                chars.next();
                has_digits = true;
            } else {
                break;
            }
        }
    }
    
    // Must have at least one digit
    if !has_digits {
        return Err(nom::Err::Error(NomError::Error {
            input: original_input,
            code: NomError::ErrorKind::Digit,
        }));
    }
    
    // Calculate remaining input
    let consumed = number_str.len();
    let remaining = &original_input[consumed..];
    
    // Attempt to parse the recognized string into the appropriate PropertyValue.
    // Try i64 first (for integers, like -295941589)
    if !has_decimal {
        if let Ok(i) = number_str.parse::<i64>() {
            return Ok((remaining, PropertyValue::Integer(i)));
        }
    }
    
    // Fallback to f64 (for floats)
    if let Ok(f) = number_str.parse::<f64>() {
        return Ok((remaining, PropertyValue::Float(SerializableFloat(f))));
    }
    
    // Error case
    Err(nom::Err::Error(NomError::Error {
        input: original_input,
        code: NomError::ErrorKind::Float,
    }))
}

// --- Required Type Definitions (from query_types.rs) ---
// Note: You must ensure these types are in scope (e.g., via `use crate::query_parser::query_types::*`)
// use serde_json::Value; 
// use std::collections::HashMap;

// pub type NodePattern = (Option<String>, Option<String>, HashMap<String, Value>);
// pub type RelPattern = (Option<String>, Option<String>, Option<(Option<u32>, Option<u32>)>, HashMap<String, Value>, Option<bool>);
// pub type Pattern = (Option<String>, Vec<NodePattern>, Vec<RelPattern>);


/// Helper function to satisfy the compiler and build the final pattern structure.
/// 
/// Constructs the final `Pattern` tuple: `(Option<String>, Vec<NodePattern>, Vec<RelPattern>)`.
/// The first element is typically used for a path variable (e.g., `p` in MATCH p=(a)-->(b)`).
/// For simple patterns, this is set to `None`.
fn build_pattern_from_elements(nodes: Vec<NodePattern>, rels: Vec<RelPattern>) -> Pattern {
    // Path variable is None, followed by the collected node and relationship vectors.
    println!("===> build_pattern_from_elements START");
    (None, nodes, rels) 
}

fn parse_match_clause(input: &str) -> IResult<&str, Vec<Pattern>> {
    println!("===> parse_match_clause START");
    let (input, _) = tag("MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, patterns) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_pattern
    ).parse(input)?;
    Ok((input, patterns))
}

/// The entry point for parsing expressions within a WHERE clause.
fn parse_cypher_expression(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_cypher_expression START");
    parse_logical_expression(input)
}

/// Entry point for expression parsing.
/// This maintains the precedence: OR < AND < NOT < Comparisons < Atoms.
pub fn parse_expression(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_expression START, input length: {}", input.len());
    parse_logical_expression(input)
}

/// Level 1: OR (Lowest Precedence)
pub fn parse_logical_expression(input: &str) -> IResult<&str, Expression> {
    let (input, left) = parse_and_term(input)?;
    
    let (input, fold) = many0(
        tuple((
            multispace0,
            tag_no_case("OR"),
            multispace0,
            parse_and_term,
        ))
    ).parse(input)?;

    let res = fold.into_iter().fold(left, |acc, (_, _, _, next)| {
        Expression::Or {
            left: Box::new(acc),
            right: Box::new(next),
        }
    });
    Ok((input, res))
}

/// Level 2: AND
pub fn parse_and_term(input: &str) -> IResult<&str, Expression> {
    let (input, left) = parse_not_expression(input)?;
    
    let (input, fold) = many0(
        tuple((
            multispace0,
            tag_no_case("AND"),
            multispace0,
            parse_not_expression,
        ))
    ).parse(input)?;

    let res = fold.into_iter().fold(left, |acc, (_, _, _, next)| {
        Expression::And {
            left: Box::new(acc),
            right: Box::new(next),
        }
    });
    Ok((input, res))
}

/// Level 3: NOT (Unary Logical Negation)
pub fn parse_not_expression(input: &str) -> IResult<&str, Expression> {
    let (input, op_not) = opt(tuple((tag_no_case("NOT"), multispace0))).parse(input)?;
    
    let (input, expr) = parse_comparison_expression(input)?;
    
    if op_not.is_some() {
        Ok((input, Expression::Not(Box::new(expr))))
    } else {
        Ok((input, expr))
    }
}

/// Level 4: Comparisons and Label Predicates
/// Handles: =, <>, CONTAINS, STARTS WITH, ENDS WITH, and e:Label
pub fn parse_comparison_expression(input: &str) -> IResult<&str, Expression> {
    let (input, left) = parse_atom(input)?;
    let (input, _) = multispace0.parse(input)?;

    // 1. High Precedence Check: Label Predicate (e.g., e:IdentityEvent)
    // This must be checked immediately after an atom before binary operators.
    if let Ok((remaining, (_, label))) = tuple((char(':'), parse_identifier)).parse(input) {
        if let Expression::Variable(v) = &left {
            return Ok((remaining, Expression::LabelPredicate { 
                variable: v.clone(), 
                label: label.to_string() 
            }));
        }
    }

    // 2. Try to parse a Binary Operator
    let op_parse: IResult<&str, &str> = alt((
        tag_no_case("CONTAINS"),
        tag_no_case("STARTS WITH"),
        tag_no_case("ENDS WITH"),
        tag_no_case("IN"),
        tag("=~"),
        tag("="),
        tag("<>"),
        tag("!="),
        tag(">="),
        tag("<="),
        tag(">"),
        tag("<"),
    )).parse(input);

    if let Ok((input, op_str)) = op_parse {
        let (input, _) = multispace0.parse(input)?;
        let (input, right) = parse_atom(input)?;
        
        let op_upper = op_str.to_uppercase();
        
        let expr = match op_upper.as_str() {
            "=~" => {
                if let Expression::Property(PropertyAccess::Vertex(v, p)) = left {
                    if let Expression::Literal(CypherValue::String(pat)) = right {
                        Expression::Regex { variable: v, property: p, pattern: pat }
                    } else { return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))); }
                } else { return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))); }
            },
            "CONTAINS" => {
                if let Expression::Property(PropertyAccess::Vertex(v, p)) = left {
                    if let Expression::Literal(CypherValue::String(s)) = right {
                        Expression::Contains { variable: v, property: p, substring: s }
                    } else { return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))); }
                } else { return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))); }
            },
            "STARTS WITH" => {
                if let Expression::Property(PropertyAccess::Vertex(v, p)) = left {
                    if let Expression::Literal(CypherValue::String(s)) = right {
                        Expression::StartsWith { variable: v, property: p, prefix: s }
                    } else { return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))); }
                } else { return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))); }
            },
            "ENDS WITH" => {
                if let Expression::Property(PropertyAccess::Vertex(v, p)) = left {
                    if let Expression::Literal(CypherValue::String(s)) = right {
                        Expression::EndsWith { variable: v, property: p, suffix: s }
                    } else { return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))); }
                } else { return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))); }
            },
            _ => {
                let op = match op_upper.as_str() {
                    "=" => BinaryOp::Eq,
                    "IN" => BinaryOp::In,
                    "<>" | "!=" => BinaryOp::Neq,
                    ">" => BinaryOp::Gt,
                    "<" => BinaryOp::Lt,
                    ">=" => BinaryOp::Gte,
                    "<=" => BinaryOp::Lte,
                    _ => BinaryOp::Eq,
                };
                Expression::Binary { op, left: Box::new(left), right: Box::new(right) }
            }
        };
        Ok((input, expr))
    } else {
        // Fallback: return the atom or the label predicate
        Ok((input, left))
    }
}

/// Level 5: Atoms (Base elements)
/// Handles identifiers, literals, function calls, and grouping.
fn parse_atom(input: &str) -> IResult<&str, Expression> {
    let (mut current_input, mut left) = alt((
        delimited(char('('), delimited(multispace0, parse_expression, multispace0), char(')')),
        parse_predicate_expression,
        parse_function_call_with_index,
        parse_function_call_full,
        parse_dynamic_property_access,
        parse_list_literal,
        map(
            tuple((parse_identifier, char('.'), parse_identifier)),
            |(var, _, prop)| Expression::Property(PropertyAccess::Vertex(var.to_string(), prop.to_string()))
        ),
        map(parse_value, |v| Expression::Literal(CypherValue::from_json(v))),
        // FIX: Use verify to keep your boundary logic while satisfying lifetimes
        map(
            verify(parse_identifier, |id| !is_at_keyword_boundary(id)),
            |v| Expression::Variable(v.to_string())
        ),
    )).parse(input)?;

    // Keep your exact postfix logic below
    loop {
        let index_result = delimited(
            multispace0,
            tuple((char('['), delimited(multispace0, parse_expression, multispace0), char(']'))),
            multispace0
        ).parse(current_input);
        match index_result {
            Ok((after_index, (_, index_expr, _))) => {
                left = Expression::ArrayIndex { expr: Box::new(left), index: Box::new(index_expr) };
                current_input = after_index;
            }
            Err(_) => break,
        }
    }
    if let Ok((after_null, op)) = parse_null_operator(current_input) {
        left = match op {
            UnaryOp::IsNull => Expression::IsNull(Box::new(left)),
            UnaryOp::IsNotNull => Expression::IsNotNull(Box::new(left)),
            _ => Expression::Unary { op, expr: Box::new(left) },
        };
        current_input = after_null;
    }
    Ok((current_input, left))
}

pub fn parse_with(input: &str) -> IResult<&str, WithClause> {
    println!("===> parse_with START");
    
    // 1. Consume "WITH" keyword
    let (input, _) = tag_no_case("WITH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 2. Parse projection items (e.g., "p, p.age AS dist")
    // For now, skip/consume them since we're just tracking the WHERE condition
    let (input, _) = take_while1(|c: char| {
        let upper = c.to_ascii_uppercase();
        !(upper == 'W' && input.trim_start().to_uppercase().starts_with("WHERE"))
    }).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    
    // 3. Parse optional WHERE clause after WITH projections
    let (input, condition_opt) = opt(preceded(
        tuple((tag_no_case("WHERE"), multispace1)),
        parse_logical_expression
    )).parse(input)?;
    
    // 4. If no WHERE, create a trivial true condition
    let condition = condition_opt.unwrap_or_else(|| {
        Expression::Literal(CypherValue::Bool(true))
    });
    
    Ok((input, WithClause { condition }))
}

pub fn parse_with_full(input: &str) -> IResult<&str, ParsedWithClause> {
    println!("===> parse_with_full START");
    
    // 1. Consume "WITH"
    let (input, _) = tag_no_case("WITH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 2. DISTINCT
    let (input, distinct) = opt(terminated(tag_no_case("DISTINCT"), multispace1)).parse(input)?;
    let is_distinct = distinct.is_some();
    
    // 3. Split at clause boundaries (WHERE, MERGE, MATCH, RETURN, WITH, CREATE, DELETE, SET, REMOVE, ORDER BY, SKIP, LIMIT)
    let (projection_str, rest_after_projections) = {
        let mut depth = 0;
        let mut in_string = false;
        let mut quote_char = ' ';
        let mut found_boundary = None;
        
        for (i, c) in input.char_indices() {
            match c {
                '"' | '\'' => {
                    if !in_string {
                        in_string = true;
                        quote_char = c;
                    } else if c == quote_char {
                        in_string = false;
                    }
                }
                '(' if !in_string => depth += 1,
                ')' if !in_string => depth -= 1,
                _ => {}
            }
            
            // Check for clause keywords at depth 0, outside strings
            if !in_string && depth == 0 {
                let remaining = &input[i..];
                let trimmed = remaining.trim_start();
                let upper = trimmed.to_uppercase();
                
                // List of clause keywords that should terminate the WITH projection
                let clause_keywords = [
                    "WHERE ", "WHERE", "MERGE ", "MERGE", "MATCH ", "MATCH", 
                    "RETURN ", "RETURN", "WITH ", "WITH", "CREATE ", "CREATE",
                    "DELETE ", "DELETE", "SET ", "SET", "REMOVE ", "REMOVE",
                    "ORDER BY ", "ORDER BY", "SKIP ", "SKIP", "LIMIT ", "LIMIT",
                    "UNION ", "UNION"
                ];
                
                for keyword in &clause_keywords {
                    if upper.starts_with(keyword) {
                        // Make sure previous character is whitespace or we're at start
                        let prev_is_boundary = i == 0 || input.chars().nth(i-1).map_or(true, |c| c.is_whitespace());
                        if prev_is_boundary {
                            found_boundary = Some(i);
                            println!("===> Found clause boundary '{}' at position {}", keyword, i);
                            break;
                        }
                    }
                }
                if found_boundary.is_some() {
                    break;
                }
            }
        }
        
        if let Some(pos) = found_boundary {
            (&input[..pos], &input[pos..])
        } else {
            println!("===> No clause boundary found, taking all as projection");
            (input, "")
        }
    };
    
    println!("===> Projection string: '{}'", projection_str.trim());
    println!("===> Rest after projections: '{}'", rest_after_projections.trim());
    
    // 4. Parse projections
    let items = if !projection_str.is_empty() {
        parse_projection_items(projection_str.trim())
            .unwrap_or_else(|e| {
                println!("===> Projection parse error: {}", e);
                Vec::new()
            })
    } else {
        Vec::new()
    };
    
    // 5. Parse WHERE clause if present
    let (mut remaining_input, where_clause) = {
        let (i, _) = multispace0.parse(rest_after_projections)?;
        let trimmed_upper = i.trim_start().to_uppercase();
        
        if trimmed_upper.starts_with("WHERE ") || trimmed_upper == "WHERE" {
            println!("===> Parsing WHERE clause");
            let (i, _) = tag_no_case("WHERE").parse(i)?;
            let (i, _) = multispace1.parse(i)?;
            
            // Parse the condition
            let (i, condition) = parse_expression.parse(i)?;
            println!("===> Parsed WHERE condition: {:?}", condition);
            
            (i, Some(WhereClause { condition }))
        } else {
            println!("===> No WHERE clause to parse");
            (i, None)
        }
    };
    
    println!("===> WITH where_clause present: {}", where_clause.is_some());
    
    // 6. Consume ORDER BY / SKIP / LIMIT from remaining_input
    let (mut remaining_input, _) = multispace0.parse(remaining_input)?;
    
    let (i, order_by_items) = opt(preceded(
        pair(tag_no_case("ORDER BY"), multispace1),
        parse_order_by_items
    )).parse(remaining_input)?;
    remaining_input = i;
    
    let (mut remaining_input, _) = multispace0.parse(remaining_input)?;
    
    let (i, skip_val) = opt(preceded(
        preceded(multispace0, tag_no_case("SKIP")),
        preceded(multispace1, map_res(digit1, |s: &str| s.parse::<i64>()))
    )).parse(remaining_input)?;
    remaining_input = i;
    
    let (mut remaining_input, _) = multispace0.parse(remaining_input)?;
    
    let (i, limit_val) = opt(preceded(
        preceded(multispace0, tag_no_case("LIMIT")),
        preceded(multispace1, map_res(digit1, |s: &str| s.parse::<i64>()))
    )).parse(remaining_input)?;
    remaining_input = i;
    
    let (remaining_input, _) = multispace0.parse(remaining_input)?;
    
    Ok((remaining_input, ParsedWithClause {
        items,
        distinct: is_distinct,
        where_clause,
        order_by: order_by_items.unwrap_or_default(),
        skip: skip_val,
        limit: limit_val,
    }))
}

/// Find keyword position ensuring word boundaries
fn find_keyword_position(haystack: &str, needle: &str) -> Option<usize> {
    let mut pos = 0;
    while let Some(found) = haystack[pos..].find(needle) {
        let absolute_pos = pos + found;
        
        // Check if it's at a word boundary (preceded by whitespace or start)
        let is_start = absolute_pos == 0 || 
                       haystack.chars().nth(absolute_pos - 1).map_or(false, char::is_whitespace);
        
        // Check if it's at a word boundary (followed by whitespace or end)
        let end_pos = absolute_pos + needle.len();
        let is_end = end_pos >= haystack.len() ||
                     haystack.chars().nth(end_pos).map_or(false, char::is_whitespace);
        
        if is_start && is_end {
            return Some(absolute_pos);
        }
        
        pos = absolute_pos + 1;
    }
    None
}

/// Find a keyword with proper word boundaries
fn find_keyword_boundary(haystack: &str, needle: &str) -> Option<usize> {
    let mut start = 0;
    
    while let Some(pos) = haystack[start..].find(needle) {
        let absolute_pos = start + pos;
        
        // Check word boundary before
        let before_ok = absolute_pos == 0 || 
                        haystack.chars().nth(absolute_pos - 1)
                                .map_or(false, |c| c.is_whitespace() || c == ',');
        
        // Check word boundary after
        let after_pos = absolute_pos + needle.len();
        let after_ok = after_pos >= haystack.len() ||
                       haystack.chars().nth(after_pos)
                               .map_or(false, |c| c.is_whitespace() || c == ',');
        
        if before_ok && after_ok {
            return Some(absolute_pos);
        }
        
        start = absolute_pos + 1;
    }
    
    None
}

/// Extract just the identifier part, stopping at keywords or extra whitespace
fn extract_alias_identifier(text: &str) -> &str {
    // Keywords that should terminate the alias
    const KEYWORDS: &[&str] = &["WHERE", "ORDER", "SKIP", "LIMIT", "RETURN", "WITH"];
    
    let upper = text.to_uppercase();
    let mut end_pos = text.len();
    
    // Find the first keyword
    for keyword in KEYWORDS {
        if let Some(pos) = find_keyword_boundary(&upper, keyword) {
            if pos < end_pos {
                end_pos = pos;
            }
        }
    }
    
    // Take up to the keyword and trim
    let candidate = text[..end_pos].trim();
    
    // Take only the first word (identifiers don't have spaces)
    if let Some(space_pos) = candidate.find(char::is_whitespace) {
        candidate[..space_pos].trim()
    } else {
        candidate
    }
}

/// Parse projection items from a string like "p, levenshtein(...) AS dist"
/// Parse projection items from a string like "p, levenshtein(...) AS dist"
fn parse_projection_items(input: &str) -> Result<Vec<QueryReturnItem>, String> {
    let mut items = Vec::new();
    let mut current = input;
    
    while !current.is_empty() {
        let mut depth = 0;
        let mut in_string = false;
        let mut quote_char = ' ';
        let mut found_comma = None;
        let mut found_clause = None;
        
        // Find the next comma OR clause keyword at depth 0
        for (i, c) in current.char_indices() {
            match c {
                '"' | '\'' => {
                    if !in_string {
                        in_string = true;
                        quote_char = c;
                    } else if c == quote_char {
                        in_string = false;
                    }
                }
                '(' => {
                    if !in_string { depth += 1; }
                }
                ')' => {
                    if !in_string { depth -= 1; }
                }
                _ => {}
            }

            if !in_string && depth == 0 {
                let remaining = &current[i..];
                let upper = remaining.to_uppercase();
                // Stop at clause keywords (not inside expressions)
                if upper.starts_with(" WHERE") ||
                   upper.starts_with(" RETURN") ||
                   upper.starts_with(" ORDER") ||
                   upper.starts_with(" SKIP") ||
                   upper.starts_with(" LIMIT") {
                    found_clause = Some(i);
                    break;
                }
            }

            if !in_string && depth == 0 && c == ',' {
                found_comma = Some(i);
                break;
            }
        }
        
        // Split at clause boundary first, then comma
        let (item_str, rest) = if let Some(pos) = found_clause {
            (&current[..pos], &current[pos..]) // Stop BEFORE clause
        } else if let Some(pos) = found_comma {
            (&current[..pos], &current[pos+1..])
        } else {
            (current, "")
        };
        
        let trimmed_item = item_str.trim();
        if !trimmed_item.is_empty() {
            let (expr, alias) = parse_single_projection_item(trimmed_item)?;
            items.push(QueryReturnItem {
                expression: expr.to_string(),
                alias: alias.map(|s| s.to_string()),
            });
        }

        // If we stopped at a clause, don't continue parsing
        if found_clause.is_some() {
            break;
        }
        
        current = rest.trim();
    }
    
    Ok(items)
}

/// Parse a single projection item: "expr" or "expr AS alias"
fn parse_single_projection_item(item: &str) -> Result<(&str, Option<&str>), String> {
    // Find " AS " at depth 0 (outside parentheses and strings)
    let as_pos = find_as_at_depth_zero(item)?;
    
    if let Some(pos) = as_pos {
        let expr = item[..pos].trim();
        let after_as = item[pos + 3..].trim(); // +3 for "AS " (note: " AS " is 4 chars, but we found at space)
        
        // Extract just the identifier, stopping at any keyword or whitespace
        let alias = extract_alias_identifier(after_as);
        
        return Ok((expr, Some(alias)));
    }
    
    Ok((item.trim(), None))
}

/// Find the position of " AS " at depth 0 (outside parentheses and strings)
fn find_as_at_depth_zero(text: &str) -> Result<Option<usize>, String> {
    let upper = text.to_uppercase();
    let mut depth = 0;
    let mut in_string = false;
    let mut quote_char = ' ';
    
    let mut i = 0;
    while i < text.len() {
        let c = text.chars().nth(i).unwrap();
        
        match c {
            '"' | '\'' => {
                if !in_string {
                    in_string = true;
                    quote_char = c;
                } else if c == quote_char {
                    in_string = false;
                }
            }
            '(' => {
                if !in_string { depth += 1; }
            }
            ')' => {
                if !in_string { depth -= 1; }
            }
            _ => {}
        }
        
        // Check for " AS " at this position when at depth 0
        if !in_string && depth == 0 && i + 4 <= upper.len() {
            let slice = &upper[i..];
            
            // Check for " AS " (space before and after)
            if slice.starts_with(" AS ") {
                return Ok(Some(i));
            }
        }
        
        i += 1;
    }
    
    Ok(None)
}

/// Extract an identifier, stopping at any keyword
fn extract_identifier_before_keyword<'a>(text: &'a str, keywords: &[&str]) -> &'a str {
    let upper = text.to_uppercase();
    
    // Find the first keyword position
    let mut min_pos = text.len();
    
    for keyword in keywords {
        // Look for keyword as a whole word
        if let Some(pos) = find_keyword_position(&upper, keyword) {
            if pos < min_pos {
                min_pos = pos;
            }
        }
    }
    
    // Take everything up to the keyword
    let result = text[..min_pos].trim();
    
    // If there's whitespace in the middle (shouldn't happen for valid identifiers),
    // take only the first word
    if let Some(space_pos) = result.find(char::is_whitespace) {
        result[..space_pos].trim()
    } else {
        result
    }
}

// Helper function to convert Expression to CypherExpression
fn convert_expression_to_cypher(expr: Expression) -> CypherExpression {
    match expr {
        Expression::Property(PropertyAccess::Vertex(var, prop)) => {
            CypherExpression::PropertyLookup { var, prop }
        },
        Expression::Literal(v) => CypherExpression::Literal(cypher_value_to_value(v)),
        Expression::Variable(v) => CypherExpression::Variable(v),
        
        // --- FIXED: Use UnaryOp Enum instead of String ---
        Expression::Not(inner_expr) => {
            CypherExpression::UnaryOp {
                op: UnaryOp::Not, 
                expr: Box::new(convert_expression_to_cypher(*inner_expr)),
            }
        },
        Expression::IsNull(inner_expr) => {
            CypherExpression::UnaryOp {
                op: UnaryOp::IsNull,
                expr: Box::new(convert_expression_to_cypher(*inner_expr)),
            }
        },
        Expression::IsNotNull(inner_expr) => {
            CypherExpression::UnaryOp {
                op: UnaryOp::IsNotNull,
                expr: Box::new(convert_expression_to_cypher(*inner_expr)),
            }
        },
        Expression::Unary { op, expr: inner_expr } => {
            CypherExpression::UnaryOp {
                op, // op is already a UnaryOp enum here
                expr: Box::new(convert_expression_to_cypher(*inner_expr)),
            }
        },

        Expression::FunctionCall { name, args } => {
            CypherExpression::FunctionCall { 
                name, 
                args: args.into_iter().map(convert_expression_to_cypher).collect() 
            }
        },
        Expression::And { left, right } => {
            CypherExpression::BinaryOp {
                left: Box::new(convert_expression_to_cypher(*left)),
                op: "AND".to_string(), // BinaryOp variant uses String in your definition
                right: Box::new(convert_expression_to_cypher(*right)),
            }
        },
        Expression::Or { left, right } => {
            CypherExpression::BinaryOp {
                left: Box::new(convert_expression_to_cypher(*left)),
                op: "OR".to_string(),
                right: Box::new(convert_expression_to_cypher(*right)),
            }
        },
        _ => {
            // Log the actual variant for debugging your MPI event graph
            CypherExpression::Variable("unknown".to_string())
        },
    }
}


// Helper function to convert CypherValue to Value
fn cypher_value_to_value(cv: CypherValue) -> Value {
    println!("===> cypher_value_to_value START");
    match cv {
        CypherValue::Null => Value::Null,
        
        CypherValue::Bool(b) => Value::Bool(b),
        
        CypherValue::Integer(i) => json!(i),
        
        CypherValue::Float(f) => {
            // FIX: Use 'f' directly. It is a primitive f64, not a wrapper.
            if let Some(n) = Number::from_f64(f) {
                Value::Number(n)
            } else {
                // JSON spec does not support NaN or Infinity; 
                // we log as Null to avoid breaking the event graph serialization.
                Value::Null 
            }
        },
        
        CypherValue::String(s) => Value::String(s),
        
        CypherValue::Uuid(uuid) => Value::String(uuid.to_string()),
        
        // Ensure Patient/Encounter nodes are correctly serialized for MPI auditing
        CypherValue::Vertex(v) => json!(v),
        CypherValue::Edge(e) => json!(e),
        
        // Recursive conversion for Maps (e.g., patient property bags)
        CypherValue::Map(m) => {
            let mut map = Map::new();
            for (k, v) in m {
                map.insert(k, cypher_value_to_value(v));
            }
            Value::Object(map)
        },
        
        // Recursive conversion for Lists (e.g., list of previous IDs)
        CypherValue::List(l) => {
            let list = l.into_iter()
                .map(cypher_value_to_value)
                .collect::<Vec<Value>>();
            Value::Array(list)
        },
    }
}

pub fn expression_to_json_value(expr: Expression) -> Result<serde_json::Value, String> {
    match expr {
        Expression::List(elements) => {
            let mut arr = Vec::new();
            for e in elements {
                // Ensure the list only contains literals for the comparison
                if let Expression::Literal(cv) = e {
                    arr.push(cv.to_json());
                } else {
                    return Err("List in comparison must contain literals".to_string());
                }
            }
            Ok(serde_json::Value::Array(arr))
        },
        Expression::Literal(cv) => Ok(cv.to_json()),
        _ => Err("Expected literal or list for comparison value".to_string()),
    }
}

pub fn parse_comparison_expr(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_comparison_expr START");
    
    // --- BRANCH A: Function Calls (e.g., labels(e) IN [...]) ---
    if let Ok((remaining, (func_name, arg))) = parse_function_call(input) {
        let (remaining, _) = multispace0.parse(remaining)?;
        let (remaining, op) = parse_comparison_op(remaining)?;
        let (remaining, _) = multispace0.parse(remaining)?;
        
        let op_upper = op.to_uppercase();
        let (remaining, val) = if op_upper.contains("NULL") {
            (remaining, serde_json::Value::Null)
        } else if op_upper == "IN" {
            let (rem, list_expr) = parse_list_literal(remaining)?;
            let json_val = expression_to_json_value(list_expr)
                .map_err(|_| nom::Err::Failure(nom::error::Error::new(remaining, nom::error::ErrorKind::Tag)))?;
            (rem, json_val)
        } else {
            parse_value(remaining)?
        };
        
        return Ok((remaining, Expression::FunctionComparison {
            function: func_name,
            argument: arg,
            operator: op.to_string(),
            value: val,
        }));
    }

    // --- BRANCH B: Property Access (e.g., e.source_uuid = "...") ---
    let (input, full_path) = parse_property_access(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, op) = parse_comparison_op(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    let op_upper = op.to_uppercase();
    let (input, val) = if op_upper.contains("NULL") {
        (input, serde_json::Value::Null)
    } else if op_upper == "IN" {
        let (rem, list_expr) = parse_list_literal(input)?;
        let json_val = expression_to_json_value(list_expr)
            .map_err(|_| nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))?;
        (rem, json_val)
    } else {
        parse_value(input)?
    };

    let (var, prop) = full_path.split_once('.')
        .map(|(v, p)| (v.to_string(), p.to_string()))
        .unwrap_or_else(|| (full_path.clone(), String::new()));

    Ok((input, Expression::PropertyComparison {
        variable: var,
        property: prop,
        operator: op.to_string(),
        value: val,
    }))
}

// Return Expression, not CypherExpression
pub fn parse_property_or_terminal(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_property_or_terminal START");
    // Try property lookup: n.name
    if let Ok((i, var)) = parse_identifier(input) {
        if let Ok((i2, _)) = char::<&str, nom::error::Error<&str>>('.').parse(i) {
            let (i3, prop) = parse_identifier(i2)?;
            return Ok((i3, Expression::Property(PropertyAccess::Vertex(
                var.to_string(),
                prop.to_string(),
            ))));
        }
    }
    
    // Fall back to terminal expression
    parse_terminal_expression(input)
}

pub fn parse_terminal_expression(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_terminal_expression START");
    alt((
        // 1. Parenthesized: (expr)
        parse_parenthesized_expression,

        // 2. Function call: ID(n) — but note: standalone functions aren't in your Expression enum
        // So skip this unless you add FunctionCall variant
        // (Keep only if you extend Expression)

        // 3. Property: n.name
        map(
            pair(parse_identifier, preceded(char('.'), parse_identifier)),
            |(var, prop)| Expression::Property(PropertyAccess::Vertex(
                var.to_string(),
                prop.to_string(),
            )),
        ),

        // 4. Literal
        map(parse_literal_value, |v| Expression::Literal(CypherValue::from_json(v))),

        // 5. Variable
        map(parse_identifier, |s| Expression::Variable(s.to_string())),
    ))
    .parse(input) // ✅ Use .parse(input) for reliability in Nom 8
}

// Support for $mrn, $id, etc.
fn parse_parameter(input: &str) -> IResult<&str, String> {
    println!("===> parse_parameter START");
    let (input, _) = char('$')(input)?;
    let (input, name) = parse_identifier(input)?;
    Ok((input, format!("${}", name)))
}

fn parse_parenthesized_expression(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_parenthesized_expression START");
    delimited(
        char('('),
        preceded(multispace0, parse_logical_expression),
        preceded(multispace0, char(')')),
    )
    .parse(input) // ✅ Use .parse(input) for Nom 8
}

fn parse_literal_value(input: &str) -> IResult<&str, serde_json::Value> {
    println!("===> parse_literal_value START");
    alt((
        map(parse_double, |f| serde_json::json!(f)),
        map(parse_i64, |i| serde_json::json!(i)), 
        map(delimited(char('"'), take_until("\""), char('"')), |s: &str| serde_json::json!(s)),
        map(delimited(char('\''), take_until("'"), char('\'')), |s: &str| serde_json::json!(s)),
    )).parse(input)
}

/// Parse WHERE clause - returns WhereClause struct with condition field
pub fn parse_where(input: &str) -> IResult<&str, WhereClause> {
    println!("===> parse_where START");
    let (input, _) = tag_no_case("WHERE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // Parse using the precedence-aware logical tree
    let (input, condition) = parse_logical_expression(input)?;
    
    Ok((input, WhereClause { condition }))
}

/// Parse function call (e.g., ID(n))
fn parse_function_call(input: &str) -> IResult<&str, (String, String)> {
    println!("===> parse_function_call START");
    let (input, _) = multispace0.parse(input)?;
    let (input, func_name) = parse_identifier.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, arg) = parse_identifier.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;
    
    Ok((input, (func_name.to_string(), arg.to_string())))
}

/// Parse comparison operators including IN and =~ (regex)
pub fn parse_comparison_op(input: &str) -> IResult<&str, &str> {
    println!("===> parse_comparison_op START");
    alt((
        tag_no_case("IN"),           // Must come before other operators
        tag_no_case("IS NOT NULL"),
        tag_no_case("IS NULL"),
        tag_no_case("STARTS WITH"),
        tag_no_case("ENDS WITH"),
        tag_no_case("CONTAINS"),
        tag("=~"),                  
        tag(">="),
        tag("<="),
        tag("<>"),
        tag("!="),
        tag("="),
        tag(">"),
        tag("<"),
    )).parse(input)
}

/// Parse a single WHERE condition/expression (label check, property, function, or parenthesized)
pub fn parse_where_expression(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_where_expression START, input: '{}'", &input[..input.len().min(50)]);
    
    // 1. Try to parse as a predicate expression with enhanced list_expr support
    if let Ok((remaining, expr)) = parse_predicate_expression(input) {
        return Ok((remaining, expr));
    }

    // 1b. If that fails, try a manual predicate parse that allows property access in list_expr
    if input.trim_start().to_uppercase().starts_with("ANY(") || input.trim_start().to_uppercase().starts_with("ALL(") {
        let (input, name) = alt((tag_no_case("any"), tag_no_case("all"))).parse(input)?;
        let (input, _) = multispace0.parse(input)?;
        let (input, _) = char('(').parse(input)?;
        let (input, _) = multispace0.parse(input)?;
        
        let (input, var_name) = parse_identifier(input)?;
        let (input, _) = multispace1.parse(input)?;
        let (input, _) = tag_no_case("IN").parse(input)?;
        let (input, _) = multispace1.parse(input)?;
        
        // Enhanced list_expr parsing: allow property access
        let (input, list_expr) = alt((
            parse_function_call_full,
            parse_list_literal,
            map(
                tuple((parse_identifier, char('.'), parse_identifier)),
                |(var, _, prop)| Expression::Property(PropertyAccess::Vertex(var.to_string(), prop.to_string()))
            ),
            map(parse_identifier, |v| Expression::Variable(v.to_string())),
        )).parse(input)?;
        
        let (input, _) = multispace1.parse(input)?;
        let (input, _) = tag_no_case("WHERE").parse(input)?;
        let (input, _) = multispace1.parse(input)?;
        
        // Enhanced condition parsing: allow property access on RHS
        let (input, left_var) = parse_identifier(input)?;
        let (input, _) = multispace1.parse(input)?;
        let (input, op_str) = alt((tag_no_case("IN"), tag("=~"), tag("="), tag("<>"), tag("!="))).parse(input)?;
        let (input, _) = multispace1.parse(input)?;
        
        let (input, right) = alt((
            parse_list_literal,
            parse_function_call_full,
            map(
                tuple((parse_identifier, char('.'), parse_identifier)),
                |(var, _, prop)| Expression::Property(PropertyAccess::Vertex(var.to_string(), prop.to_string()))
            ),
            map(parse_value, |v| Expression::Literal(CypherValue::from_json(v))),
            map(parse_identifier, |v| Expression::Variable(v.to_string())),
        )).parse(input)?;

        let op = match op_str.to_uppercase().as_str() {
            "IN" => BinaryOp::In,
            "=" => BinaryOp::Eq,
            "=~" => BinaryOp::Eq, // Note: regex in predicates
            "<>" | "!=" => BinaryOp::Neq,
            _ => BinaryOp::Eq,
        };

        let condition = Expression::Binary {
            op,
            left: Box::new(Expression::Variable(left_var.to_string())),
            right: Box::new(right),
        };
        
        let (input, _) = multispace0.parse(input)?;
        let (input, _) = char(')').parse(input)?;
        
        return Ok((input, Expression::Predicate {
            name: name.to_uppercase(),
            variable: var_name.to_string(),
            list: Box::new(list_expr),
            condition: Box::new(condition),
        }));
    }

    // 2. Parenthesized expressions (Recursion handles complex OR/AND inside)
    if let Ok((remaining, expr)) = parse_parenthesized_expression(input) {
        return Ok((remaining, expr));
    }

    // 3. Label Predicate check: e:IdentityEvent (with optional parentheses)
    // Try with parentheses first: (e:IdentityEvent)
    if let Ok((remaining, _)) = char::<&str, nom::error::Error<&str>>('(').parse(input) {
        if let Ok((remaining2, (var, _, label, _))) = tuple((
            parse_identifier,
            char(':'),
            parse_identifier,
            char(')'),
        )).parse(remaining) {
            return Ok((remaining2, Expression::LabelPredicate {
                variable: var.to_string(),
                label: label.to_string(),
            }));
        }
    }
    
    // Try without parentheses: e:IdentityEvent
    if let Ok((remaining, (var, _, label))) = tuple((
        parse_identifier,
        char(':'),
        parse_identifier,
    )).parse(input) {
        println!("===> Found label predicate: {}:{}", var, label);
        return Ok((remaining, Expression::LabelPredicate {
            variable: var.to_string(),
            label: label.to_string(),
        }));
    }

    // 4. Function calls with array indexing like labels(e)[0] = 'Value'
    if let Ok((remaining, expr)) = parse_function_call_with_index(input) {
        let (remaining, _) = multispace0.parse(remaining)?;
        let (remaining, op) = parse_comparison_op(remaining)?;
        let (remaining, _) = multispace0.parse(remaining)?;
        
        let op_upper = op.to_uppercase();
        let (remaining, val) = if op_upper.contains("NULL") {
            (remaining, Value::Null)
        } else if op_upper == "IN" {
            let (rem, list_expr) = parse_list_literal(remaining)?;
            let json_val = match list_expr {
                Expression::List(elements) => {
                    let mut arr = Vec::new();
                    for elem in elements {
                        if let Expression::Literal(cv) = elem {
                            arr.push(cv.to_json());
                        }
                    }
                    Value::Array(arr)
                }
                _ => Value::Null,
            };
            (rem, json_val)
        } else {
            parse_value(remaining)?
        };
        
        return Ok((remaining, Expression::Binary {
            op: match op_upper.as_str() {
                "=" => BinaryOp::Eq,
                "!=" | "<>" => BinaryOp::Neq,
                ">" => BinaryOp::Gt,
                "<" => BinaryOp::Lt,
                ">=" => BinaryOp::Gte,
                "<=" => BinaryOp::Lte,
                "IN" => BinaryOp::In,
                _ => BinaryOp::Eq,
            },
            left: Box::new(expr),
            right: Box::new(Expression::Literal(CypherValue::from_json(val))),
        }));
    }

    // 5. Function calls (e.g., labels(e) IN ['A', 'B'])
    if let Ok((remaining, (func_name, arg))) = parse_function_call(input) {
        let (remaining, _) = multispace0.parse(remaining)?;
        let (remaining, op) = parse_comparison_op(remaining)?;
        let (remaining, _) = multispace0.parse(remaining)?;
        
        let op_upper = op.to_uppercase();
        let (remaining, val) = if op_upper.contains("NULL") {
            (remaining, Value::Null)
        } else if op_upper == "IN" {
            let (rem, list_expr) = parse_list_literal(remaining)?;
            let json_val = match list_expr {
                Expression::List(elements) => {
                    let mut arr = Vec::new();
                    for elem in elements {
                        if let Expression::Literal(cv) = elem {
                            arr.push(cv.to_json());
                        }
                    }
                    Value::Array(arr)
                }
                _ => Value::Null,
            };
            (rem, json_val)
        } else {
            parse_value(remaining)?
        };
        
        return Ok((remaining, Expression::FunctionComparison {
            function: func_name,
            argument: arg,
            operator: op.to_string(),
            value: val,
        }));
    }
    
    // 6. Property access (e.g., n.name = "Alice" or e.source_uuid IN ['a', 'b'])
    let (input, full_path) = parse_property_access(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // 7. Parse operator - CRITICAL: Check for =~ BEFORE calling parse_comparison_op
    let (input, op_str) = {
        let trimmed = input.trim_start();
        
        // Check =~ FIRST (before other operators)
        if trimmed.starts_with("=~") {
            println!("===> Found =~ operator");
            let (i, _) = tag("=~").parse(trimmed)?;
            let (i, _) = multispace0.parse(i)?;
            (i, "=~")
        } else if trimmed.to_uppercase().starts_with("STARTS WITH") {
            let (i, _) = tag_no_case("STARTS").parse(trimmed)?;
            let (i, _) = multispace1.parse(i)?;
            let (i, _) = tag_no_case("WITH").parse(i)?;
            let (i, _) = multispace0.parse(i)?;
            (i, "STARTS WITH")
        } else if trimmed.to_uppercase().starts_with("ENDS WITH") {
            let (i, _) = tag_no_case("ENDS").parse(trimmed)?;
            let (i, _) = multispace1.parse(i)?;
            let (i, _) = tag_no_case("WITH").parse(i)?;
            let (i, _) = multispace0.parse(i)?;
            (i, "ENDS WITH")
        } else if trimmed.to_uppercase().starts_with("CONTAINS") {
            let (i, _) = tag_no_case("CONTAINS").parse(trimmed)?;
            let (i, _) = multispace0.parse(i)?;
            (i, "CONTAINS")
        } else {
            let (i, op) = parse_comparison_op(trimmed)?;
            let (i, _) = multispace0.parse(i)?;
            (i, op)
        }
    };
    
    println!("===> Parsed operator: '{}', remaining: '{}'", op_str, &input[..input.len().min(30)]);
    
    // 8. Parse value (handle IN operator with lists)
    let op_upper = op_str.to_uppercase();
    let (input, val) = if op_upper.contains("NULL") {
        (input, Value::Null)
    } else if op_upper == "IN" {
        let (rem, list_expr) = parse_list_literal(input)?;
        let json_val = match list_expr {
            Expression::List(elements) => {
                let mut arr = Vec::new();
                for elem in elements {
                    if let Expression::Literal(cv) = elem {
                        arr.push(cv.to_json());
                    }
                }
                Value::Array(arr)
            }
            _ => Value::Null,
        };
        (rem, json_val)
    } else {
        let result = parse_value(input)?;
        println!("===> Parsed value successfully: {:?}", result.1);
        result
    };
    
    println!("===> After value parse, remaining: '{}'", &input[..input.len().min(30)]);
    
    // 9. Split property path
    let (var, prop) = full_path.split_once('.')
        .map(|(v, p)| (v.to_string(), p.to_string()))
        .unwrap_or_else(|| (full_path.clone(), String::new()));

    // 10. Handle regex operator =~
    if op_str == "=~" {
        if let Value::String(pattern) = val {
            println!("===> Creating Regex expression for pattern: '{}'", pattern);
            return Ok((input, Expression::Regex {
                variable: var,
                property: prop,
                pattern,
            }));
        } else {
            println!("===> ERROR: =~ operator requires string value");
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag
            )));
        }
    }

    // 11. Handle STARTS WITH
    if op_str == "STARTS WITH" {
        if let Value::String(prefix) = val {
            return Ok((input, Expression::StartsWith {
                variable: var,
                property: prop,
                prefix,
            }));
        }
    }

    // 12. Standard property comparison
    Ok((input, Expression::PropertyComparison {
        variable: var,
        property: prop,
        operator: op_str.to_string(),
        value: val,
    }))
}

/// The WHERE clause parser itself - updated for legacy compatibility with AND support
fn parse_where_clause_content(input: &str) -> IResult<&str, String> {
    println!("===> parse_where_clause_content START");
    let (input, _) = tag_no_case("WHERE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // ✅ Call the new precedence-aware logical parser
    let (input, final_expr) = parse_logical_expression(input)?;

    // Return debug format for legacy string-based return types
    Ok((input, format!("{:?}", final_expr)))
}

fn take_until_keyword(input: &str) -> IResult<&str, &str> {
    println!("===> take_until_keyword START");
    let keywords = [
        "SET", "RETURN", "CREATE", "DELETE", "WITH", 
        "REMOVE", "MATCH", "MERGE", "FOREACH", "UNWIND",
        "ON CREATE", "ON MATCH", "ORDER BY", "LIMIT", "SKIP"
    ];
    
    let upper_input = input.to_uppercase();
    let mut earliest_idx = input.len();
    
    for kw in keywords {
        // Check if keyword appears at the start (after optional whitespace)
        let trimmed = upper_input.trim_start();
        if trimmed.starts_with(kw) {
            if let Some(pos) = input.find(|c: char| !c.is_whitespace()) {
                if pos < earliest_idx {
                    earliest_idx = pos;
                }
            }
            continue;
        }
        
        // Look for keyword preceded by whitespace
        for separator in &[" ", "\n", "\t", "\r"] {
            let search_pattern = format!("{}{}", separator, kw);
            if let Some(idx) = upper_input.find(&search_pattern) {
                if idx < earliest_idx {
                    earliest_idx = idx;
                }
            }
        }
    }
    
    let taken = &input[..earliest_idx];
    let remainder = &input[earliest_idx..];
    
    Ok((remainder, taken.trim()))
}

/// Parse a raw node pattern string
///     ( [var][:Label] [{props}] )
/// into (label, properties) exactly like the old logic.
fn parse_node_pattern(input: &str) -> GraphResult<(Option<String>, HashMap<String, Value>)> {
    type E<'a> = nom::error::Error<&'a str>;
    println!("===> parse_node_pattern START"); 
    // 1. Identify the "Inside" of the parentheses first to avoid infinite recursion
    let (remaining, inner) = delimited(
        tag::<_, _, E>("("),
        take_until(")"),
        tag::<_, _, E>(")")
    ).parse(input)
     .map_err(|_| GraphError::ValidationError("Malformed node: missing brackets".into()))?;

    // 2. Parse the Variable Name (everything up to the first colon or space or end)
    let (after_var, _var_name) = take_until::<_, _, E>(":").parse(inner)
        .unwrap_or(("", inner)); // If no colon, the whole thing is the var name

    // 3. Parse Multiple Labels: :User:Admin:Employee
    // We use many1 because we know we are starting at a colon
    let mut labels_parser = many1(preceded(
        tag::<_, _, E>(":"),
        take_while1::<_, _, E>(|c: char| c.is_alphanumeric() || c == '_')
    ));

    // Try to parse labels from the point after the variable
    let (after_labels, labels) = opt(labels_parser).parse(after_var)
        .map_err(|_| GraphError::ValidationError("Invalid label format".into()))?;

    // 4. Parse Properties: { ... }
    // We look for the curly brace in whatever is left
    let mut props_parser = opt(preceded(
        multispace0::<_, E>,
        delimited(
            tag::<_, _, E>("{"),
            take_until::<_, _, E>("}"),
            tag::<_, _, E>("}")
        )
    ));

    let (_, props_str) = props_parser.parse(after_labels)
        .map_err(|_| GraphError::ValidationError("Malformed properties".into()))?;

    // 5. Finalize data
    let primary_label = labels.and_then(|l_vec| l_vec.first().map(|s| s.to_string()));
    
    let properties = if let Some(p_str) = props_str {
        let json_fix = p_str.replace("'", "\"");
        serde_json::from_str::<HashMap<String, Value>>(&format!("{{{}}}", json_fix))
            .unwrap_or_default()
    } else {
        HashMap::new()
    };

    Ok((primary_label, properties))
}

// =============================================================================
// INSTRUCTIONS FOR INTEGRATION:
// =============================================================================
// 1. Find your existing full_statement_parser function in cypher_parser.rs
// 2. Delete it completely
// 3. Copy all 5 functions above (is_cypher_keyword_start through full_statement_parser)
// 4. Paste them into your file where the old full_statement_parser was
// 5. Make sure parse_return_clause exists and looks like this:
// Helper to consume RETURN ... [ORDER BY ...]
fn parse_return_clause(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_return_clause START"); 
    
    // 1. Consume 'RETURN' keyword
    let (input, _) = preceded(
        multispace0::<&str, NomErrorType<&str>>,
        tag_no_case("RETURN"),
    ).parse(input)?;
    
    // 2. Consume whitespace
    let (input, _) = multispace0.parse(input)?;

    // --- NEW: Handle DISTINCT keyword ---
    let (input, distinct_token) = opt(pair(tag_no_case("DISTINCT"), multispace1)).parse(input)?;
    let is_distinct = distinct_token.is_some();
    // ------------------------------------
    
    // Check if remaining input is empty or incomplete
    let trimmed = input.trim_start();
    let is_incomplete = trimmed.is_empty() || 
        trimmed.to_uppercase().starts_with("ORDER") ||
        trimmed.to_uppercase().starts_with("LIMIT") ||
        trimmed.to_uppercase().starts_with("SKIP") ||
        trimmed.starts_with(';');
    
    if is_incomplete {
        println!("===> ERROR: Incomplete RETURN statement in parse_return_clause");
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge
        )));
    }
    
    // Capture raw projection string
    // Note: take_while1 here will stop at ORDER, SKIP, or LIMIT keywords
    let (input_after_proj, projection_items_str) = take_while1(|c: char| {
        let upper = c.to_ascii_uppercase();
        !(upper == 'O' || upper == 'S' || upper == 'L' || c == ';' || c == '\n' || c == '\r')
    }).parse(input)?;
    
    let proj_trimmed = projection_items_str.trim();
    if proj_trimmed.is_empty() {
        println!("===> ERROR: Empty projection string after RETURN");
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge
        )));
    }
    
    // 3. Parse ORDER BY clause
    let (input, order_by) = opt(preceded(
        tuple((multispace0, tag_no_case::<_, _, NomErrorType<&str>>("ORDER BY"), multispace1)),
        parse_order_by_items,
    )).parse(input_after_proj)?;
    let order_by = order_by.unwrap_or_default();
    
    // 4. Parse SKIP clause
    let (input, skip) = opt(preceded(
        tuple((multispace0, tag_no_case::<_, _, NomErrorType<&str>>("SKIP"), multispace1)),
        map_res(take_while1(|c: char| c.is_ascii_digit()), |s: &str| s.parse::<i64>())
    )).parse(input)?;
    
    // 5. Parse LIMIT clause
    let (input, limit) = opt(preceded(
        tuple((multispace0, tag_no_case::<_, _, NomErrorType<&str>>("LIMIT"), multispace1)),
        map_res(take_while1(|c: char| c.is_ascii_digit()), |s: &str| s.parse::<i64>())
    )).parse(input)?;
    
    // 6. Build query - FIX: Added is_distinct field
    let return_query = CypherQuery::ReturnStatement { 
        projection_string: proj_trimmed.to_string(),
        order_by,
        skip,
        limit,
        is_distinct, // Map the local variable here
    };
    
    Ok((input, return_query))
}

fn apply_projection_aliases(projection_string: &str, obj: &mut serde_json::Map<String, serde_json::Value>) {
    // 1. Clean the RETURN prefix
    let clean_projection = projection_string.trim_start_matches("RETURN ").trim();
    
    // 2. Regex to split commas only if NOT inside parentheses
    // This handles: RETURN coalesce(p.name, 'N/A') AS name, p.id
    let split_re = Regex::new(r",\s*(?![^()]*\))").unwrap();
    let items: Vec<&str> = split_re.split(clean_projection).map(|s| s.trim()).collect();

    // 3. Regex for the " AS " alias pattern (case-insensitive)
    let alias_re = Regex::new(r"(?i)\s+AS\s+").unwrap();

    for item in items {
        if let Some(mat) = alias_re.find(item) {
            let expr = item[..mat.start()].trim();
            let alias = item[mat.end()..].trim();
            
            // If the object has the raw expression key, move it to the alias key
            if let Some(val) = obj.remove(expr) {
                obj.insert(alias.to_string(), val);
            }
        } 
        // AUTO-FLATTEN: If "p.mrn" exists but no alias was provided, move to "mrn"
        else if item.contains('.') {
            if let Some(val) = obj.remove(item) {
                let flat_key = item.split('.').last().unwrap_or(item);
                obj.insert(flat_key.to_string(), val);
            }
        }
    }
}

// Conceptual Helper method on NodePattern, Vertex (or similar)
// You must implement this in an appropriate file.

fn matches_constraints(
    vertex: &Vertex, 
    labels: &Vec<String>, 
    properties: &HashMap<String, Value>
) -> bool {
    println!("===> matches_constraints START"); 
    // Label matching: vertex must have ALL specified labels
    let label_matches = if labels.is_empty() {
        true // No label constraint
    } else {
        let vertex_label = vertex.label.as_ref();
        
        // Check if vertex has all required labels
        labels.iter().all(|required_label| {
            // Support both single and multi-label formats
            // Vertex label might be "Patient" or "Patient:GoldenRecord"
            if vertex_label == required_label {
                true
            } else if vertex_label.contains(':') {
                // Split vertex's multi-label and check if required label is present
                vertex_label.split(':').any(|l| l == required_label)
            } else {
                false
            }
        })
    };
    
    if !label_matches {
        return false;
    }
    
    // Property matching: all specified properties must match
    properties.iter().all(|(key, expected_val)| {
        vertex.properties.get(key).map_or(false, |actual_val| {
            // Convert expected Value to PropertyValue for comparison
            match to_property_value(expected_val.clone()) {
                Ok(expected_pv) => actual_val == &expected_pv,
                Err(_) => false,
            }
        })
    })
}

fn parse_create_clause(input: &str) -> IResult<&str, Vec<Pattern>> {
    println!("===> parse_create_clause START"); 
    separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_create_pattern, // ✅ Use CREATE-specific parser
    )
    .parse(input)
}

// In your expression parsing logic
/// Parses dynamic property access like n[prop] where prop is an expression
fn parse_dynamic_property_access(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_dynamic_property_access START");
    
    let (input, var_name) = parse_identifier(input)?;
    let (input, _) = char('[')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, prop_expr) = parse_expression(input)?; // Recursively parse the property expression
    let (input, _) = multispace0(input)?;
    let (input, _) = char(']')(input)?;
    
    Ok((input, Expression::DynamicPropertyAccess {
        variable: var_name.to_string(),
        property_expr: Box::new(prop_expr),
    }))
}

/// Parses a UNION [ALL] clause and returns the right-hand side query
fn parse_union_clause(input: &str) -> IResult<&str, (bool, Box<CypherQuery>)> {
    let (input, _) = tag_no_case("UNION")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Check for ALL keyword - specify error type explicitly
    let (input, is_all) = if let Ok((i, _)) = tag_no_case::<_, _, nom::error::Error<&str>>("ALL")(input) {
        let (i, _) = multispace1(i)?;
        (i, true)
    } else {
        (input, false)
    };
    
    // Parse the RHS as a single query clause (not sequential)
    let (input, rhs_query) = parse_simple_query_type(input)?;
    
    Ok((input, (is_all, Box::new(rhs_query))))
}

/// Parses an UNWIND clause: UNWIND expression AS variable
fn parse_unwind_clause(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_unwind_clause START, input: '{}'", &input[..input.len().min(50)]);
    
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("UNWIND")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse the expression to unwind (e.g., [1,2,3] or keys(e))
    let (input, expression) = parse_expression(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("AS")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse the variable name to bind each element to
    let (input, variable) = parse_identifier(input)?;
    
    println!("===> parse_unwind_clause END: variable='{}'", variable);
    
    Ok((input, CypherQuery::Unwind {
        expression,
        variable: variable.to_string(),
    }))
}

fn parse_where_clause(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("WHERE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    // Call the OR level (the bottom of the precedence chain)
    parse_logical_expression(input) 
}

// Fix parse_return_clause_as_struct to handle empty/incomplete RETURN
// A more robust whitespace parser that handles NBSP (0xA0) and other Unicode spaces
fn any_whitespace0(input: &str) -> IResult<&str, &str> {
    take_while(|c: char| c.is_whitespace())(input)
}

// Fixed parse_return_clause_as_struct with proper bare RETURN handling
pub fn parse_return_clause_as_struct(input: &str) -> IResult<&str, ReturnClause> {
    println!("=========> parse_return_clause_as_struct START");
    println!("=========> Input: '{}'", input.trim());
    
    // 1. Consume ANY kind of whitespace (including NBSP)
    let (input, _) = any_whitespace0(input)?;
    
    println!("=========> parse_return_clause_as_struct STEP 1");
    // 2. Consume 'RETURN' keyword
    let (input, _) = tag_no_case("RETURN")(input)?;
    
    println!("=========> parse_return_clause_as_struct STEP 2");
    // 3. Consume trailing whitespace
    let (input, _) = any_whitespace0(input)?;
    
    println!("=========> parse_return_clause_as_struct STEP 3");
    let trimmed = input.trim();
    
    println!("=========> parse_return_clause_as_struct STEP 4 - checking if bare RETURN");
    println!("===> Remaining input after RETURN: '{}'", trimmed);
    
    let is_bare_return = trimmed.is_empty() || 
        trimmed == ";" ||
        trimmed.to_uppercase().starts_with("ORDER") ||
        trimmed.to_uppercase().starts_with("LIMIT") ||
        trimmed.to_uppercase().starts_with("SKIP");
    
    if is_bare_return {
        println!("===> ERROR: Bare RETURN detected - this is not allowed!");
        // Return a Failure error to prevent backtracking
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge
        )));
    }
    
    println!("=========> parse_return_clause_as_struct STEP 5");
    // 4. Handle DISTINCT
    let (input, distinct_opt) = opt(terminated(tag_no_case("DISTINCT"), multispace1)).parse(input)?;
    let is_distinct = distinct_opt.is_some();
    
    println!("=========> parse_return_clause_as_struct STEP 6");
    // 5. Parse Projection Items
    let (input, items_raw) = separated_list1(
        delimited(multispace0, char(','), multispace0),
        parse_single_return_item
    ).parse(input)?;
    
    println!("=========> parse_return_clause_as_struct STEP 7");
    // 6. Parse ORDER BY
    let (input, order_by_raw) = opt(preceded(
        tuple((multispace0, tag_no_case("ORDER BY"), multispace1)),
        parse_order_by_items,
    )).parse(input)?;
    
    let order_by = order_by_raw.map(|items| {
        items.into_iter().map(|item| {
            (item.expression, !item.ascending) 
        }).collect::<Vec<(String, bool)>>()
    });
    
    println!("=========> parse_return_clause_as_struct STEP 8");
    // 7. SKIP & LIMIT
    let (input, skip) = opt(preceded(
        tuple((multispace0, tag_no_case("SKIP"), multispace1)),
        map_res(take_while1(|c: char| c.is_ascii_digit()), |s: &str| s.parse::<usize>())
    )).parse(input)?;
    
    let (input, limit) = opt(preceded(
        tuple((multispace0, tag_no_case("LIMIT"), multispace1)),
        map_res(take_while1(|c: char| c.is_ascii_digit()), |s: &str| s.parse::<usize>())
    )).parse(input)?;
    
    println!("=========> parse_return_clause_as_struct COMPLETE");
    Ok((input, ReturnClause {
        items: items_raw,
        distinct: is_distinct,
        order_by,
        skip,
        limit,
    }))
}

fn parse_single_return_item(input: &str) -> IResult<&str, ReturnItem> {
    println!("===> parse_single_return_item START, input: '{}'", &input[..input.len().min(50)]);
    
    // 1. Calculate the slice that actually belongs to this expression
    let mut end_pos = 0;
    let bytes = input.as_bytes();
    while end_pos < bytes.len() {
        let current_slice = &input[end_pos..];
        
        // Break on comma (next item)
        if bytes[end_pos] == b',' { break; }
        
        // Break if we hit a keyword boundary (ORDER BY, LIMIT, etc.)
        if bytes[end_pos].is_ascii_whitespace() && is_at_keyword_boundary(current_slice.trim_start()) {
            break;
        }
        end_pos += 1;
    }

    let (remaining_after_expr, expr_raw) = take(end_pos)(input)?;
    
    // 2. Parse the expression from that specific slice only
    // This prevents greedy consumption of keywords
    let (_, expr_struct) = parse_expression(expr_raw.trim_end())?;
    
    // 3. Handle optional AS alias from the REMAINING input
    let (remaining, _) = multispace0.parse(remaining_after_expr)?;
    let (remaining, alias_opt) = opt(preceded(
        tuple((tag_no_case("AS"), multispace1)),
        parse_identifier
    )).parse(remaining)?;
    
    let (remaining, _) = multispace0.parse(remaining)?;
    
    println!("===> parse_single_return_item parsed: expr='{}', alias={:?}", expr_raw.trim(), alias_opt);
    
    Ok((remaining, ReturnItem {
        expression: expr_raw.trim().to_string(),
        alias: alias_opt.map(|s| s.to_string()),
    }))
}

fn parse_return_expression(input: &str) -> IResult<&str, String> {
    // Capture everything until comma or end of line and convert to String
    println!("===> parse_return_expression START"); 
    map(
        take_while1(|c| c != ',' && c != '\n' && c != '\r'),
        |s: &str| s.to_string(),
    ).parse(input)
}

fn extract_main_entity(nodes: &Vec<NodePattern>) -> Option<NodePattern> {
    println!("===> extract_main_entity START"); 
    nodes.first().cloned()
}

fn parse_match_node_original(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_match_node_original START");  
    let (input, (_, _, node)) = tuple((
        tag("MATCH"),
        multispace1,
        parse_node,
    )).parse(input)?;
    
    // node is (Option<String>, Vec<String>, HashMap<String, Value>)
    let (_var, labels, props) = node;

    // Convert Vec<String> to Option<String> 
    // This takes the first label if it exists, otherwise None
    let label = labels.first().cloned();

    Ok((input, CypherQuery::MatchNode {
        label,
        properties: props,
    }))
}

pub async fn execute_cypher_from_string(
    query: &str,
    storage: Arc<dyn GraphStorageEngine + Send + Sync>,
) -> GraphResult<Value> {
    let parsed = parse_cypher(query).map_err(|e| GraphError::StorageError(e))?;
    let db = Arc::new(Database {
        storage: storage.clone(),
        config: StorageConfig::default(),
    });
    println!("===> execute_cypher_from_string START");  
    execute_cypher(parsed, &db, storage).await
}

// ==============================================================================
// FIX #2: Add helper function for variable-length path expansion
// ==============================================================================

fn expand_variable_paths(
    all_edges: &[Edge],
    matched_vertices: &std::collections::HashSet<SerializableUuid>,
    matched_edges: &mut std::collections::HashSet<SerializableUuid>,
    min_hops: u32,
    max_hops: u32,
) {
    println!("===> expand_variable_paths START");  
    // BFS to find all paths within hop range
    let mut visited = std::collections::HashSet::new();
    let mut current_level = matched_vertices.clone();
    
    for hop in 1..=max_hops {
        let mut next_level = std::collections::HashSet::new();
        
        for vertex_id in &current_level {
            for edge in all_edges {
                let (from_id, to_id) = if edge.outbound_id == *vertex_id {
                    (edge.outbound_id, edge.inbound_id)
                } else if edge.inbound_id == *vertex_id {
                    (edge.inbound_id, edge.outbound_id)
                } else {
                    continue;
                };
                
                if !visited.contains(&edge.id) {
                    visited.insert(edge.id);
                    
                    // Include edge if we're at or past min_hops
                    if hop >= min_hops {
                        matched_edges.insert(edge.id);
                    }
                    
                    next_level.insert(to_id);
                }
            }
        }
        
        if next_level.is_empty() {
            break;
        }
        
        current_level = next_level;
    }
}

// exec_cypher_pattern - helper
// Assume GraphService is defined and available in scope
// exec_cypher_pattern - helper
// Assume GraphService is defined and available in scope
async fn exec_cypher_pattern(
    patterns: Vec<(Option<String>, Vec<(Option<String>, Option<String>, HashMap<String, Value>)>, Vec<(Option<String>, Option<String>, Option<(Option<u32>, Option<u32>)>, HashMap<String, Value>, Option<bool>)>)>,
    graph_service: &GraphService, // <--- UPDATED DEPENDENCY
) -> GraphResult<(Vec<Vertex>, Vec<Edge>)> {
    println!("===> exec_cypher_pattern START");  
    // *** DELEGATION FIX: Retrieve all vertices and edges via GraphService ***
    let all_vertices = graph_service.get_all_vertices().await?;
    let all_edges = graph_service.get_all_edges().await?;
    
    println!("===> Database contains {} vertices and {} edges", all_vertices.len(), all_edges.len());

    // Context: MPI Event Logging [2025-12-20]
    // Log query execution into the graph of events for traceability.
    
    let mut matched_vertex_ids = std::collections::HashSet::new();
    let mut matched_edge_ids = std::collections::HashSet::new();
    let mut var_bindings: HashMap<String, HashSet<SerializableUuid>> = HashMap::new();

    for (pattern_idx, pattern) in patterns.iter().enumerate() {
        let nodes = &pattern.1;
        let rels = &pattern.2;
        let mut pattern_vertex_ids = std::collections::HashSet::new();
        let mut vars_bound_in_this_pattern = std::collections::HashSet::new();
        
        // Step 1: Match all nodes in this pattern
        for (var_name, label_constraint, prop_constraints) in nodes {
            println!("===> Matching node: var={:?}, label={:?}, props={:?}", 
                     var_name, label_constraint, prop_constraints.keys().collect::<Vec<_>>());
            
            // Check if variable is already bound from a previous pattern
            if let Some(var) = var_name {
                if let Some(bound_ids) = var_bindings.get(var) {
                    if label_constraint.is_some() || !prop_constraints.is_empty() {
                        let required_props: HashMap<String, PropertyValue> = prop_constraints
                            .iter()
                            .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                            .collect::<GraphResult<_>>()?;

                        for vertex_id in bound_ids {
                            if let Some(v) = all_vertices.iter().find(|v| &v.id == vertex_id) {
                                let label_ok = label_constraint.as_ref().map_or(true, |ql| {
                                    let vl = v.label.as_ref();
                                    vl == ql || vl.starts_with(&format!("{}:", ql))
                                });
                                
                                let props_ok = required_props.iter().all(|(k, expected)| {
                                    v.properties.get(k).map_or(false, |actual| actual == expected)
                                });
                                
                                if label_ok && props_ok {
                                    pattern_vertex_ids.insert(*vertex_id);
                                    matched_vertex_ids.insert(*vertex_id);
                                }
                            }
                        }
                    } else {
                        pattern_vertex_ids.extend(bound_ids);
                        matched_vertex_ids.extend(bound_ids);
                    }
                    continue;
                }
            }
            
            // Variable not bound yet, match against all vertices
            let required_props: HashMap<String, PropertyValue> = prop_constraints
                .iter()
                .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                .collect::<GraphResult<_>>()?;

            let mut matched_for_this_var = HashSet::new();
            
            for v in &all_vertices {
                let label_ok = label_constraint.as_ref().map_or(true, |ql| {
                    let vl = v.label.as_ref();
                    vl == ql || vl.starts_with(&format!("{}:", ql))
                });
                
                let props_ok = required_props.iter().all(|(k, expected)| {
                    v.properties.get(k).map_or(false, |actual| actual == expected)
                });
                
                if label_ok && props_ok {
                    pattern_vertex_ids.insert(v.id);
                    matched_vertex_ids.insert(v.id);
                    matched_for_this_var.insert(v.id);
                }
            }
            
            if let Some(var) = var_name {
                if !var_bindings.contains_key(var) {
                    vars_bound_in_this_pattern.insert(var.clone());
                }
                var_bindings.insert(var.clone(), matched_for_this_var);
            }
        }
        
        // Step 2: Match relationships in this pattern
        if !rels.is_empty() {
            for (rel_idx, rel) in rels.iter().enumerate() {
                // Preserving tuple layout: added is_optional context to the flag
                let (rel_var, rel_type, rel_range, _rel_props, is_optional) = rel;
                let optional = is_optional.unwrap_or(false);

                if let Some((min_hops, max_hops)) = rel_range {
                    let min = min_hops.unwrap_or(1);
                    let max = max_hops.unwrap_or(5);
                    for edge in &all_edges {
                        let type_matches = rel_type.as_ref().map_or(true, |rt| {
                            edge.edge_type.as_ref() == rt || edge.label == *rt
                        });
                        if type_matches {
                            matched_edge_ids.insert(edge.id);
                            matched_vertex_ids.insert(edge.outbound_id);
                            matched_vertex_ids.insert(edge.inbound_id);
                        }
                    }
                } else {
                    let start_node_pattern = &nodes[rel_idx];
                    let end_node_pattern = &nodes[rel_idx + 1];
                    let start_var_name = start_node_pattern.0.as_ref();
                    let end_var_name = end_node_pattern.0.as_ref();

                    let start_node_id_set: HashSet<SerializableUuid> = start_var_name
                        .and_then(|var| var_bindings.get(var).cloned())
                        .unwrap_or_else(|| pattern_vertex_ids.clone());

                    let end_node_id_set: HashSet<SerializableUuid> = if let Some(var) = end_var_name {
                        if let Some(bound_ids) = var_bindings.get(var) {
                            if vars_bound_in_this_pattern.contains(var) {
                                let mut matching_end_nodes = HashSet::new();
                                for v in &all_vertices {
                                    // ... existing constraint check logic ...
                                    matching_end_nodes.insert(v.id);
                                }
                                matching_end_nodes
                            } else { bound_ids.clone() }
                        } else { pattern_vertex_ids.clone() }
                    } else { pattern_vertex_ids.clone() };

                    let mut found_any_edge = false;
                    for edge in &all_edges {
                        let type_matches = rel_type.as_ref().map_or(true, |rt| {
                            edge.edge_type.as_ref() == rt || edge.label == *rt
                        });
                        if !type_matches { continue; }

                        let connects_outbound = start_node_id_set.contains(&edge.outbound_id) && end_node_id_set.contains(&edge.inbound_id);
                        let connects_inbound = start_node_id_set.contains(&edge.inbound_id) && end_node_id_set.contains(&edge.outbound_id);

                        if connects_outbound || connects_inbound {
                            found_any_edge = true;
                            matched_edge_ids.insert(edge.id);
                            matched_vertex_ids.insert(edge.outbound_id);
                            matched_vertex_ids.insert(edge.inbound_id);
                            
                            if let Some(end_var) = end_var_name {
                                let connected_id = if connects_outbound { edge.inbound_id } else { edge.outbound_id };
                                var_bindings.entry(end_var.clone()).or_insert_with(HashSet::new).insert(connected_id);
                            }
                        }
                    }
                    // If not optional and no edges found, logic would usually break the path,
                    // but we preserve the specific var_bindings to allow OPTIONAL MATCH to return p.
                }
            }
        }
    }

    // Context: Patient Logging [2025-12-20]
    // Every transaction must log the patient's golden record into the graph of changes.

    let final_vertices: Vec<Vertex> = all_vertices.into_iter().filter(|v| matched_vertex_ids.contains(&v.id)).collect();
    let final_edges: Vec<Edge> = all_edges.into_iter().filter(|e| matched_edge_ids.contains(&e.id)).collect();

    Ok((final_vertices, final_edges))
}

pub fn parse_modifying_clause(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_modifying_clause START");  
    
    // 1. CREATE Clause
    if let Ok((remaining, raw_patterns)) = preceded(
        terminated(tag_no_case("CREATE"), multispace1),
        parse_match_clause_patterns
    ).parse(input) {
        let execution_patterns: ExecutionPatternsReturnType = raw_patterns
            .into_iter()
            .map(|(id, nodes, edges)| {
                let transformed_nodes = nodes
                    .into_iter()
                    .map(|(var, label_opt, props)| {
                        let labels_vec = match label_opt {
                            Some(l) => vec![l],
                            None => vec![],
                        };
                        (var, labels_vec, props)
                    })
                    .collect();
                (id, transformed_nodes, edges)
            })
            .collect();

        return Ok((remaining, CypherQuery::CreateStatement { 
            patterns: execution_patterns, 
            return_items: vec![] 
        }));
    }
    
    // 2. SET Clause - FIXED to handle Expression types
    if let Ok(result) = preceded(
        terminated(tag_no_case("SET"), multispace1),
        map(
            separated_list1(
                tuple((multispace0, char(','), multispace0)),
                parse_single_set_assignment
            ), 
            |assignments| CypherQuery::SetStatement { assignments }
        )
    ).parse(input) {
        return Ok(result);
    }
    
    // 3. DELETE Clause
    let detach_check = opt(terminated(tag_no_case::<_, _, NomErrorType<&str>>("DETACH"), multispace1)).parse(input);
    if let Ok((after_detach, detach_opt)) = detach_check {
        let is_detach = detach_opt.is_some();
        
        if let Ok((after_delete, _)) = terminated(tag_no_case::<_, _, NomErrorType<&str>>("DELETE"), multispace1).parse(after_detach) {
            if let Ok((final_input, variables)) = separated_list1(
                tuple((multispace0::<&str, NomErrorType<&str>>, char::<_, NomErrorType<&str>>(','), multispace0::<&str, NomErrorType<&str>>)),
                nom::bytes::complete::take_while1(|c: char| !c.is_whitespace() && c != ',')
            ).parse(after_delete) {
                let vars: Vec<String> = variables.into_iter().map(String::from).collect();
                return Ok((final_input, CypherQuery::DeleteStatement { 
                    variables: vars,
                    detach: is_detach,
                }));
            }
        }
    }
    
    // 4. REMOVE Clause
    if let Ok(result) = preceded(
        terminated(tag_no_case("REMOVE"), multispace1),
        map(
            separated_list1(
                tuple((multispace0::<&str, NomErrorType<&str>>, char::<_, NomErrorType<&str>>(','), multispace0::<&str, NomErrorType<&str>>)),
                parse_remove_clause
            ), 
            |removals| CypherQuery::RemoveStatement { removals }
        )
    ).parse(input) {
        return Ok(result);
    }
    
    Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Alt)))
}

pub fn parse_with_clause(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_with_clause START");
    
    let (input, _) = tag_no_case("WITH")(input)?;
    let (input, _) = multispace1(input)?;

    // 1. Check for optional DISTINCT
    let (input, distinct_token) = opt(pair(tag_no_case("DISTINCT"), multispace1)).parse(input)?;
    let is_distinct = distinct_token.is_some();

    // 2. Parse the items (e.g., "p AS target_patient, e.id")
    let (input, item_strings) = parse_return_items(input)?;
    
    // 3. Parse optional trailing clauses
    let (input, _) = multispace0(input)?;
    let (input, where_clause) = opt(parse_where_clause).parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, order_by) = opt(parse_order_by_items).parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, skip) = opt(preceded(pair(tag_no_case("SKIP"), multispace1), nom::character::complete::u64)).parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, limit) = opt(preceded(pair(tag_no_case("LIMIT"), multispace1), nom::character::complete::u64)).parse(input)?;

    // 4. Map strings into QueryReturnItem structs
    let items: Vec<QueryReturnItem> = item_strings.into_iter().map(|s| {
        if s.to_uppercase().contains(" AS ") {
            let parts: Vec<&str> = s.splitn(2, |c: char| c.is_whitespace()).collect();
            let expr = parts[0].to_string();
            let alias = s.split_whitespace().last().map(|a| a.to_string());
            QueryReturnItem { expression: expr, alias }
        } else {
            QueryReturnItem { 
                expression: s.clone(), 
                alias: Some(s) 
            }
        }
    }).collect();

    // 5. Construct the ParsedWithClause
    let with_data = ParsedWithClause {
        items,
        distinct: is_distinct, 
        where_clause: where_clause.map(|cond| WhereClause { condition: cond }),
        order_by: order_by.unwrap_or_default(),
        skip: skip.map(|s| s as i64),
        limit: limit.map(|l| l as i64),
    };

    // 6. Wrap in a Chain
    // FIX: Added is_distinct field to the ReturnStatement initializer
    Ok((input, CypherQuery::Chain(vec![
        CypherQuery::ReturnStatement {
            projection_string: "WITH".to_string(),
            order_by: with_data.order_by.clone(),
            skip: with_data.skip,
            limit: with_data.limit,
            is_distinct: with_data.distinct, // Field added here
        }
    ])))
}

// Must be defined in scope or imported:
// use models::properties::PropertyValue; 
// use models::errors::{GraphError, GraphResult}; 
// use serde_json::Value; 

/// Resolves a Cypher variable to a stored Vertex ID by matching its label and properties.
/// This assumes 'to_property_value' is a defined helper to convert serde_json::Value to PropertyValue.
// =============================================================================
// CORE FUNCTION 1: resolve_var (Database Query Logic)
// =============================================================================

/// Resolves a node variable's ID by checking the storage engine for a matching 
/// vertex based on its label and properties.
async fn resolve_var(
    graph_service: &GraphService, 
    var: &str,
    label: &Option<String>,
    properties: &HashMap<String, serde_json::Value>,
    where_clause: &Option<WhereClause>,
    with_clause: &Option<WithClause>,
) -> GraphResult<Option<SerializableUuid>> {
    println!("===>        resolve_var: Looking for var='{}', label={:?}", var, label);
    
    let mut query_props: HashMap<String, PropertyValue> = HashMap::new();
    
    // 1. Process inline properties from the MATCH pattern
    for (k, v) in properties.iter() {
        query_props.insert(k.clone(), to_property_value(v.clone())?);
    }
    
    // 2. Extract filters from WHERE clause
    let where_filters = extract_filters_for_var(where_clause, var);
    for (k, v) in where_filters {
        query_props.insert(k, v);
    }
    
    // 3. Extract filters from WITH clause (Critical for MPI context)
    let with_filters = extract_filters_for_var_from_with(with_clause, var);
    for (k, v) in with_filters {
        query_props.insert(k, v);
    }
    
    println!("===>        Searching vertices with merged constraints: {:?}", query_props);
    
    println!("========================== USING DIRECT DB ACCESS TO GET ALL VERTICES ========================");
    let all_vertices = graph_service.get_all_vertices().await?;
    
    let matched_vertex = all_vertices.into_iter().find(|v| {
        // Match Label
        let matches_label = label.as_ref().map_or(true, |query_label| {
            v.label.as_ref() == query_label.as_str()
        });
        
        if !matches_label { return false; }
        
        // Match merged Properties with Coercive Equality
        query_props.iter().all(|(k, expected_val)| {
            v.properties.get(k).map_or(false, |actual_val| {
                // Try direct match (fast path)
                if actual_val == expected_val {
                    return true;
                }
                
                // MPI Coercion: Handle cases where query is Integer but storage is String
                match (expected_val, actual_val) {
                    (PropertyValue::Integer(ev), PropertyValue::String(av)) => {
                        av == &ev.to_string()
                    },
                    (PropertyValue::String(ev), PropertyValue::Integer(av)) => {
                        ev == &av.to_string()
                    },
                    (PropertyValue::Uuid(ev), PropertyValue::String(av)) => {
                        av == &ev.to_string()
                    },
                    (PropertyValue::String(ev), PropertyValue::Uuid(av)) => {
                        ev == &av.to_string()
                    },
                    (PropertyValue::Integer(ev), PropertyValue::Float(av)) => {
                        (*ev as f64 - av.0).abs() < f64::EPSILON
                    },
                    (PropertyValue::Float(ev), PropertyValue::Integer(av)) => {
                        (ev.0 - *av as f64).abs() < f64::EPSILON
                    },
                    _ => false,
                }
            })
        })
    });
    
    match matched_vertex {
        Some(v) => {
            println!("===>        resolve_var: SUCCESS - Returning {}", v.id.0);
            Ok(Some(v.id))
        },
        None => {
            println!("===>        resolve_var: No match found for '{}' - returning None (empty result)", var);
            Ok(None)
        }
    }
}


fn extract_filters_from_expression(
    expr: &Expression,
    var_name: &str,
    filters: &mut HashMap<String, PropertyValue>,
) {
    println!("===> extract_filters_from_expression START");
    match expr {
        // --- Added NOT Variant ---
        Expression::Not(inner_expr) => {
            // Usually, we recurse. Note: If this is for an index lookup, 
            // you might need a separate 'excluded_filters' map, 
            // as 'NOT property = value' cannot be a direct equality filter.
            extract_filters_from_expression(inner_expr, var_name, filters);
        }

        Expression::And { left, right } => {
            extract_filters_from_expression(left, var_name, filters);
            extract_filters_from_expression(right, var_name, filters);
        }
        Expression::Or { left, right } => {
            // Caution: In many query optimizers, OR filters cannot be 
            // merged into a single HashMap because they aren't intersection-based.
            extract_filters_from_expression(left, var_name, filters);
            extract_filters_from_expression(right, var_name, filters);
        }
        Expression::Binary { op: BinaryOp::Eq, left, right } => {
            if let Expression::Property(PropertyAccess::Vertex(v, prop)) = left.as_ref() {
                if v == var_name {
                    if let Expression::Literal(cypher_val) = right.as_ref() {
                        let prop_val = PropertyValue::from(cypher_val.clone());
                        filters.insert(prop.clone(), prop_val);
                    }
                }
            }
            if let Expression::Property(PropertyAccess::Vertex(v, prop)) = right.as_ref() {
                if v == var_name {
                    if let Expression::Literal(cypher_val) = left.as_ref() {
                        let prop_val = PropertyValue::from(cypher_val.clone());
                        filters.insert(prop.clone(), prop_val);
                    }
                }
            }
        }
        Expression::FunctionComparison {
            function,
            argument,
            operator,
            value,
        } => {
            if function.to_uppercase() == "ID" && argument == var_name && operator == "=" {
                if let Ok(prop_val) = to_property_value(value.clone()) {
                    filters.insert("id".to_string(), prop_val);
                }
            }
        }
        Expression::PropertyComparison {
            variable,
            property,
            operator,
            value,
        } => {
            if variable == var_name && operator == "=" {
                if let Ok(prop_val) = to_property_value(value.clone()) {
                    filters.insert(property.clone(), prop_val);
                }
            }
        }
        Expression::StartsWith {
            variable,
            property,
            prefix,
        } => {
            if variable == var_name {
                if let Ok(prop_val) = to_property_value(Value::String(prefix.clone())) {
                    filters.insert(property.clone(), prop_val);
                }
            }
        }
        _ => {}
    }
}

fn extract_filters_for_var_from_with(
    with_clause: &Option<WithClause>,
    var_name: &str,
) -> HashMap<String, PropertyValue> {
    println!("===> extract_filters_for_var_from_with START");
    let mut filters = HashMap::new();
    
    if let Some(wc) = with_clause {
        // Reuse extract_filters_for_var logic by wrapping WITH condition in a dummy WhereClause
        let condition_filters = extract_filters_for_var(&Some(WhereClause { 
            condition: wc.condition.clone() 
        }), var_name);
        
        filters.extend(condition_filters);
    }
    
    filters
}

fn extract_filters_for_var(
    where_clause: &Option<WhereClause>,
    var_name: &str,
) -> HashMap<String, PropertyValue> {
    println!("===> extract_filters_for_var START for variable: {}", var_name);
    let mut filters = HashMap::new();
    
    if let Some(WhereClause { condition }) = where_clause {
        let mut queue = vec![condition];
        
        while let Some(expr) = queue.pop() {
            match expr {
                Expression::And { left, right } => {
                    queue.push(left);
                    queue.push(right);
                }
                Expression::Or { left, right } => {
                    queue.push(left);
                    queue.push(right);
                }
                Expression::LabelPredicate { variable, label } => {
                    if variable == var_name {
                        filters.insert("label".to_string(), PropertyValue::String(label.clone()));
                    }
                }
                Expression::PropertyComparison {
                    variable,
                    property,
                    operator,
                    value,
                } => {
                    if variable == var_name && (operator == "=" || operator == "==") {
                        if let Ok(prop_val) = to_property_value(value.clone()) {
                            filters.insert(property.clone(), prop_val);
                        }
                    }
                }
                Expression::Binary {
                    left,
                    op: BinaryOp::Eq,
                    right,
                } => {
                    if let Expression::Property(PropertyAccess::Vertex(var, prop)) = left.as_ref() {
                        if var == var_name {
                            if let Expression::Literal(cypher_val) = right.as_ref() {
                                filters.insert(prop.clone(), PropertyValue::from(cypher_val.clone()));
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    
    filters
}


// A higher-level function to resolve all node patterns in a MATCH clause.
// It iterates over all node patterns and uses `resolve_var` to find their IDs,
// populating the variable-to-ID map for subsequent edge matching.
// =============================================================================
// CORE FUNCTION 2: resolve_match_patterns
// =============================================================================

/// A higher-level function to resolve all node patterns in a MATCH clause.
/// It iterates over all node patterns and uses `resolve_var` to find their IDs,
/// ensuring each variable is bound to an existing vertex.
async fn resolve_match_patterns(
    graph_service: &GraphService, 
    match_patterns: Vec<Pattern>,
    where_clause: &Option<WhereClause>,
    with_clause: &Option<WithClause>,
) -> GraphResult<HashMap<String, SerializableUuid>> {
    let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();
    
    println!("===> resolve_match_patterns: Received {} patterns", match_patterns.len());
    
    for (pattern_idx, pat) in match_patterns.iter().enumerate() {
        println!(
            "===> Processing pattern {}: path_var={:?}, {} nodes, {} rels", 
            pattern_idx, pat.0, pat.1.len(), pat.2.len()
        );
        
        if !pat.2.is_empty() {
            return Err(GraphError::NotImplemented(format!(
                "Full graph pattern matching with relationships (Pattern {}) is not yet implemented.", 
                pattern_idx
            )));
        }
        
        for (node_idx, (var_opt, labels_vec, properties)) in pat.1.iter().enumerate() {
            if let Some(v_ref) = var_opt.as_ref() {
                let var_name = v_ref.to_string();
                
                if !var_to_id.contains_key(&var_name) {
                    println!("===>    Calling resolve_var for '{}' with WHERE and WITH context", var_name);
                    
                    let first_label_opt = labels_vec.first().cloned();
                    
                    let id_opt = resolve_var(
                        graph_service, 
                        v_ref, 
                        &first_label_opt, 
                        properties,
                        where_clause,
                        with_clause,
                    ).await?;
                    
                    // Handle Option - if node not found, return empty result
                    match id_opt {
                        Some(id) => {
                            println!("===>    SUCCESS: '{}' resolved to {}", var_name, id.0);
                            var_to_id.insert(var_name, id);
                        }
                        None => {
                            println!("===>    Node '{}' not found - MATCH returns empty result", var_name);
                            // Return empty HashMap to signal no matches found
                            return Ok(HashMap::new());
                        }
                    }
                }
            }
        }
    }
    
    Ok(var_to_id)
}

// Complete execute_cypher function with proper MatchNode implementation
// This replaces the execute_cypher function in cypher_parser.rs

// =================================================================
// THE COMPLETED EXECUTION FUNCTION
// =================================================================

// Complete execute_cypher function with proper MatchNode implementation
// This replaces the execute_cypher function in cypher_parser.rs
// FIXED execute_cypher with correct relationship matching logic
// Helper function to execute pattern matching logic
// This extracts the shared logic to avoid recursion
pub async fn execute_cypher(
    query: CypherQuery,
    _db: &Database,
    storage: Arc<dyn GraphStorageEngine + Send + Sync>,
) -> GraphResult<Value> {
    // This public function remains simple and non-recursive
    println!("===> execute_cypher START");
    execute_cypher_sync_wrapper(query, _db, storage).await
}

//    Define a non-async helper that contains the original logic
//    (using a different name, or an internal private function)
fn execute_cypher_sync_wrapper<'a>( // 1. Introduce lifetime parameter 'a
    query: CypherQuery,
    _db: &'a Database, // 2. Apply lifetime 'a to the reference
    storage: Arc<dyn GraphStorageEngine + Send + Sync>,
) -> BoxedFuture<'a, GraphResult<Value>> { // 3. Apply lifetime 'a to the return type
    println!("===> execute_cypher_sync_wrapper START");


    // Box the async block that contains the original logic
    Box::pin(async move {
        // NOTE: The entire original body of the old execute_cypher function goes here.
        {
            let global_context = get_global_context().await;
            let mut ctx = global_context.write().await;
            ctx.vertex_bindings.clear();
            ctx.edge_bindings.clear();
            ctx.current_vertices.clear();
            ctx.current_edges.clear();
        }

        // Inside this async block, when you hit the recursive call, 
        // you must use the same boxing pattern:
        let graph_service = initialize_graph_service(storage.clone()).await?;

        match query {
            // ----------  NEW  ----------
            CypherQuery::Batch(stmts) => {
                use futures::future::try_join_all;
                // Assuming `json!` is available in scope.

                let tasks = stmts.into_iter().map(|stmt| {
                    // Clone the immutable references needed for the parallel async block
                    let db_ref = _db; 
                    let stg = storage.clone();
                    // Execute each statement in the batch using the main executor.
                    async move { execute_cypher(stmt, db_ref, stg).await }
                });

                // Execute all statements in parallel (using try_join_all)
                let results = try_join_all(tasks).await?;
                
                // Return the collected results wrapped as a single JSON object.
                Ok(json!({ "results": results }))
            },
            // ============================================================================
            // UNWIND - Return flat structure
            // ============================================================================
            CypherQuery::Unwind { expression, variable } => {
                println!("===> EXECUTING Unwind with variable '{}'", variable);
                
                // 1. Setup context (In a full implementation, this should include 
                // variables matched in previous clauses like 'e' from MATCH)
                let ctx = EvaluationContext {
                    variables: HashMap::new(), 
                    parameters: HashMap::new(),
                };

                // 2. Evaluate the expression (e.g., keys(e))
                let result = expression.evaluate(&ctx)?;
                
                // 3. Ensure we have a list to unwind
                let list = if let CypherValue::List(items) = result {
                    items
                } else {
                    return Err(GraphError::QueryExecutionError(
                        "UNWIND requires a list expression".to_string()
                    ));
                };
                
                // 4. Transform the list items into a flat 'rows' structure
                let rows: Vec<serde_json::Value> = list
                    .into_iter()
                    .map(|item| {
                        // Each row is an object where the key is the 'AS' variable
                        json!({
                            variable.clone(): item
                        })
                    })
                    .collect();
                
                // 5. Return the flat structure with the populated rows
                Ok(json!({
                    "rows": rows,
                    "vertices": [],
                    "edges": [],
                    "stats": {
                        "vertices_matched": 0,
                        "edges_matched": 0,
                        "rows_generated": rows.len()
                    }
                }))
            }
            // --- UPDATED MATCH PATTERN ---
            // Handles pure MATCH ... RETURN, relying on a full pattern matcher.
            // --- UPDATED MATCH PATTERN BRANCH ---
            // --- FIXED: MatchPattern with variable binding and WHERE evaluation ---
            // ============================================================================
            // MATCH PATTERN - Return UNWRAPPED (something else wraps it)
            // ============================================================================
            CypherQuery::MatchPattern { patterns, where_clause, with_clause, return_clause } => {
                println!("===> EXECUTING MatchPattern with {} patterns", patterns.len());

                let transformed_patterns: Vec<_> = patterns.iter().map(|(id, nodes, edges)| {
                    let transformed_nodes: Vec<_> = nodes.iter().map(|(var, labels, props)| {
                        (var.clone(), labels.first().cloned(), props.clone())
                    }).collect();
                    (id.clone(), transformed_nodes, edges.clone())
                }).collect();

                let (mut final_vertices, final_edges) = exec_cypher_pattern(transformed_patterns, &graph_service).await?;

                let var_name = patterns.get(0)
                    .and_then(|p| p.1.get(0))
                    .and_then(|n| n.0.as_ref())
                    .map(|s| s.as_str())
                    .unwrap_or("n");

                let edge_var_name = patterns.get(0)
                    .and_then(|p| p.2.get(0))
                    .and_then(|edge_tuple| edge_tuple.0.as_ref()) 
                    .map(|s| s.as_str())
                    .unwrap_or("r");

                // WHERE clause filtering
                if let Some(wc) = where_clause {
                    let original_count = final_vertices.len();
                    let mut filtered = Vec::new();

                    for (idx, v) in final_vertices.iter().enumerate() {
                        let mut ctx = EvaluationContext::from_vertex(var_name, v);
                        if let Some(edge) = final_edges.get(idx) {
                            let edge_json = serde_json::to_value(edge).unwrap_or(serde_json::Value::Null);
                            ctx.variables.insert(edge_var_name.to_string(), CypherValue::from_json(edge_json));
                        }

                        let passes = match wc.condition.evaluate(&ctx) {
                            Ok(CypherValue::Bool(true)) => true,
                            Ok(CypherValue::Bool(false)) | Ok(CypherValue::Null) => false,
                            _ => {
                                if let Expression::PropertyComparison { variable, property, operator, value } = &wc.condition {
                                    let left_val_opt = if variable == var_name {
                                        if property.is_empty() { Some(CypherValue::Vertex(v.clone())) }
                                        else { v.properties.get(property).map(|pv| CypherValue::from_json(serde_json::to_value(pv).unwrap_or(serde_json::Value::Null))) }
                                    } else if variable == edge_var_name {
                                        final_edges.get(idx).and_then(|e| {
                                            if property.is_empty() { Some(CypherValue::from_json(serde_json::to_value(e).unwrap_or(serde_json::Value::Null))) }
                                            else { e.properties.get(property).map(|pv| CypherValue::from_json(serde_json::to_value(pv).unwrap_or(serde_json::Value::Null))) }
                                        })
                                    } else { None };

                                    if let Some(left_val) = left_val_opt {
                                        compare_values(&left_val, operator, &CypherValue::from_json(value.clone()))
                                    } else { false }
                                } else { false }
                            }
                        };
                        if passes { filtered.push(v.clone()); }
                    }
                    final_vertices = filtered;
                    println!("===> After initial MATCH WHERE: {} -> {} vertices", original_count, final_vertices.len());
                }

                // WITH clause logic
                if let Some(with) = with_clause {
                    let original_count = final_vertices.len();
                    let mut filtered = Vec::new();

                    for (idx, v) in final_vertices.iter().enumerate() {
                        let mut ctx = EvaluationContext::from_vertex(var_name, v);
                        if let Some(edge) = final_edges.get(idx) {
                            let edge_json = serde_json::to_value(edge).unwrap_or(serde_json::Value::Null);
                            ctx.variables.insert(edge_var_name.to_string(), CypherValue::from_json(edge_json));
                        }

                        let mut projection_success = true;
                        for item in &with.items {
                            if item.alias.is_none() && item.expression.trim() == var_name { continue; }
                            match parse_expression(&item.expression) {
                                Ok((_, expr)) => {
                                    match expr.evaluate(&ctx) {
                                        Ok(val) => { if let Some(alias) = &item.alias { ctx.variables.insert(alias.clone(), val); } }
                                        Err(_) => { projection_success = false; break; }
                                    }
                                }
                                Err(_) => { projection_success = false; break; }
                            }
                        }

                        if !projection_success { continue; }

                        let passes = if let Some(wwc) = &with.where_clause {
                            match wwc.condition.evaluate(&ctx) {
                                Ok(CypherValue::Bool(result)) => result,
                                _ => {
                                    if let Expression::PropertyComparison { variable, property, operator, value } = &wwc.condition {
                                        if property.is_empty() {
                                            if let Some(var_val) = ctx.variables.get(variable) {
                                                compare_values(var_val, operator, &CypherValue::from_json(value.clone()))
                                            } else { false }
                                        } else { false }
                                    } else { false }
                                }
                            }
                        } else { true };

                        if passes { filtered.push(v.clone()); }
                    }
                    final_vertices = filtered;
                    println!("===> After WITH WHERE: {} -> {} vertices", original_count, final_vertices.len());
                }

                // Apply RETURN LIMIT/SKIP
                let mut final_v = final_vertices;
                let mut final_e = final_edges.clone();

                if let Some(ret) = return_clause {
                    if let Some(skip) = ret.skip {
                        final_v = final_v.into_iter().skip(skip).collect();
                        final_e = final_e.into_iter().skip(skip).collect();
                    }
                    if let Some(limit) = ret.limit {
                        final_v.truncate(limit);
                        final_e.truncate(limit);
                    }
                }

                // DO NOT WRAP - Return flat structure
                Ok(json!({
                    "vertices": final_v,
                    "edges": final_e,
                    "stats": {
                        "vertices_matched": final_v.len(),
                        "edges_matched": final_e.len()
                    }
                }))
            }
            CypherQuery::MatchSet { match_patterns, where_clause, with_clause, set_clauses } => {
                println!("===> EXECUTING MatchSet with {} patterns", match_patterns.len());
                
                let transformed_patterns: Vec<_> = match_patterns.iter().map(|(id, nodes, edges)| {
                    let transformed_nodes: Vec<_> = nodes.iter().map(|(var, labels, props)| {
                        (var.clone(), labels.first().cloned(), props.clone())
                    }).collect();
                    (id.clone(), transformed_nodes, edges.clone())
                }).collect();

                let (mut matched_vertices, _) = exec_cypher_pattern(transformed_patterns, &graph_service).await?;
                
                let var_name = match_patterns.get(0)
                    .and_then(|p| p.1.get(0))
                    .and_then(|node| node.0.as_ref())
                    .map(|s| s.as_str())
                    .unwrap_or("p");

                if let Some(wc) = where_clause {
                    matched_vertices.retain(|v| {
                        let ctx = EvaluationContext::from_vertex(var_name, v);
                        wc.condition.evaluate(&ctx).map(|val| val.as_bool()).unwrap_or(false)
                    });
                }

                if let Some(pwic) = with_clause {
                    if let Some(inner_wc) = &pwic.where_clause {
                        matched_vertices.retain(|v| {
                            let ctx = EvaluationContext::from_vertex(var_name, v);
                            inner_wc.condition.evaluate(&ctx).map(|val| val.as_bool()).unwrap_or(false)
                        });
                    }
                }

                if matched_vertices.is_empty() {
                    return Ok(json!({ 
                        "vertices": [], 
                        "stats": { "vertices_updated": 0 } 
                    }));
                }
                
                let mut updated_vertices = Vec::new();
                for mut vertex in matched_vertices {
                    for (set_var, prop_name, expression) in &set_clauses {
                        let current_var = set_var.as_str();
                        let ctx = EvaluationContext::from_vertex(current_var, &vertex);
                        let evaluated_val = expression.evaluate(&ctx)?;
                        let json_val = evaluated_val.to_json();
                        let prop_value = to_property_value(json_val)?;
                        vertex.properties.insert(prop_name.clone(), prop_value);
                        vertex.updated_at = models::BincodeDateTime(chrono::Utc::now());
                    }
                    graph_service.update_vertex(vertex.clone()).await?;
                    updated_vertices.push(vertex);
                }
                
                Ok(json!({ 
                    "vertices": updated_vertices,
                    "stats": { "vertices_updated": updated_vertices.len() }
                }))
            }
            
            CypherQuery::MatchCreate {
                match_patterns,
                where_clause,
                with_clause,
                create_patterns,
            } => {
                println!("===> EXECUTING MatchCreate");
                
                let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();
                let mut created_vertices = Vec::new();
                let mut created_edges = Vec::new();

                let compat_with_clause: Option<WithClause> = with_clause.as_ref().and_then(|pw| {
                    pw.where_clause.as_ref().map(|wc| WithClause {
                        condition: wc.condition.clone(),
                    })
                });

                // FIX: Clone match_patterns because resolve_match_patterns takes ownership (Vec<Pattern>)
                // This allows the original match_patterns to be used for the is_empty() check below.
                let matched_bindings = resolve_match_patterns(
                    &*graph_service,
                    match_patterns.clone(),
                    &where_clause,
                    &compat_with_clause,
                ).await?;

                // Logic: Proceed if we found matches OR if there were no match patterns to begin with (pure CREATE)
                let mut should_proceed = !matched_bindings.is_empty() || match_patterns.is_empty();
                let ctx = EvaluationContext::from_uuid_bindings(&matched_bindings);

                if should_proceed {
                    if let Some(wc) = &where_clause {
                        if !wc.condition.evaluate(&ctx).map(|v| v.as_bool()).unwrap_or(false) {
                            should_proceed = false;
                        }
                    }
                }

                if should_proceed {
                    if let Some(pwic) = &with_clause {
                        if let Some(inner_wc) = &pwic.where_clause {
                            if !inner_wc.condition.evaluate(&ctx).map(|v| v.as_bool()).unwrap_or(false) {
                                should_proceed = false;
                            }
                        }
                    }
                }

                if should_proceed {
                    var_to_id.extend(matched_bindings);

                    for (pat_idx, pat) in create_patterns.iter().enumerate() {
                        for (node_idx, (var_opt, labels_vec, properties)) in pat.1.iter().enumerate() {
                            let v_final = var_opt.clone().unwrap_or_else(|| {
                                format!("__anon_pat{}_n{}", pat_idx, node_idx)
                            });

                            if !var_to_id.contains_key(&v_final) {
                                let props: HashMap<String, PropertyValue> = properties
                                    .iter()
                                    .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                                    .collect::<GraphResult<HashMap<String, PropertyValue>>>()?;

                                let final_label = labels_vec.first().cloned().unwrap_or_else(|| "Node".to_string());
                                let new_id = SerializableUuid(Uuid::new_v4());
                                let vertex = Vertex {
                                    id: new_id,
                                    label: Identifier::new(final_label)?,
                                    properties: props, 
                                    created_at: Utc::now().into(),
                                    updated_at: Utc::now().into(),
                                };
                                graph_service.create_vertex(vertex.clone()).await?;
                                var_to_id.insert(v_final.clone(), new_id);
                                created_vertices.push(vertex);
                            }
                        }

                        for (i, rel_tuple) in pat.2.iter().enumerate() {
                            let from_var = pat.1[i].0.clone().unwrap_or_else(|| {
                                format!("__anon_pat{}_n{}", pat_idx, i)
                            });
                            let to_var = pat.1[i + 1].0.clone().unwrap_or_else(|| {
                                format!("__anon_pat{}_n{}", pat_idx, i + 1)
                            });

                            let from_id = *var_to_id.get(&from_var)
                                .ok_or_else(|| GraphError::ValidationError(from_var))?;
                            let to_id = *var_to_id.get(&to_var)
                                .ok_or_else(|| GraphError::ValidationError(to_var))?;

                            // FIX: Underscored unused variables (_rel_var, _len_range) to silence compiler warnings
                            let (_rel_var, rel_label_opt, _len_range, rel_properties, direction_opt) = rel_tuple;
                            let (outbound_id, inbound_id) = match direction_opt {
                                Some(false) => (to_id, from_id),
                                _ => (from_id, to_id),
                            };

                            let edge_type_str = rel_label_opt.clone().unwrap_or_else(|| "RELATED".to_string());
                            let rel_props: BTreeMap<String, PropertyValue> = rel_properties
                                .iter()
                                .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                                .collect::<GraphResult<BTreeMap<String, PropertyValue>>>()?;

                            let edge = Edge {
                                id: SerializableUuid(Uuid::new_v4()),
                                outbound_id,
                                inbound_id,
                                edge_type: Identifier::new(edge_type_str.clone())?,
                                label: edge_type_str,
                                properties: rel_props,
                            };

                            graph_service.create_edge(edge.clone()).await?;
                            created_edges.push(edge);
                        }
                    }
                }

                Ok(json!({
                    "status": "success",
                    "created_vertices": created_vertices,
                    "created_edges": created_edges,
                    "stats": {
                        "vertices_created": created_vertices.len(),
                        "relationships_created": created_edges.len()
                    }
                }))
            }

            CypherQuery::MatchRemove {
                match_patterns,
                where_clause,
                with_clause,
                remove_clauses,
            } => {
                let transformed_patterns: Vec<_> = match_patterns.iter().map(|(id, nodes, edges)| {
                    let transformed_nodes: Vec<_> = nodes.iter().map(|(var, labels, props)| {
                        (var.clone(), labels.first().cloned(), props.clone())
                    }).collect();
                    (id.clone(), transformed_nodes, edges.clone())
                }).collect();

                let (mut matched_vertices, _) = exec_cypher_pattern(transformed_patterns, &graph_service).await?;
                
                let var_name = match_patterns.get(0)
                    .and_then(|p| p.1.get(0))
                    .and_then(|node| node.0.as_ref())
                    .map(|s| s.as_str())
                    .unwrap_or("p");

                if let Some(wc) = where_clause {
                    matched_vertices.retain(|v| {
                        let ctx = EvaluationContext::from_vertex(var_name, v);
                        wc.condition.evaluate(&ctx).map(|val| val.as_bool()).unwrap_or(false)
                    });
                }

                if let Some(pwic) = with_clause {
                    if let Some(inner_wc) = &pwic.where_clause {
                        matched_vertices.retain(|v| {
                            let ctx = EvaluationContext::from_vertex(var_name, v);
                            inner_wc.condition.evaluate(&ctx).map(|val| val.as_bool()).unwrap_or(false)
                        });
                    }
                }

                let mut updated_vertices = Vec::new();
                for mut vertex in matched_vertices {
                    for (_var, prop_name) in &remove_clauses {
                        vertex.properties.remove(prop_name.as_str());
                        vertex.updated_at = Utc::now().into();
                    }
                    graph_service.update_vertex(vertex.clone()).await?;
                    updated_vertices.push(vertex);
                }

                Ok(json!({
                    "vertices": updated_vertices,
                    "stats": { "vertices_updated": updated_vertices.len() }
                }))
            }

            CypherQuery::MatchCreateSet {
                match_patterns,
                where_clause,
                with_clause,
                create_patterns,
                set_clauses,
            } => {
                println!("===> EXECUTING MatchCreateSet");
                let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();
                let mut created_vertices: Vec<Vertex> = Vec::new();
                let mut updated_vertices: Vec<Vertex> = Vec::new();

                let compat_with = with_clause.as_ref().and_then(|pw| {
                    pw.where_clause.as_ref().map(|wc| WithClause {
                        condition: wc.condition.clone(),
                    })
                });

                let matched_bindings = resolve_match_patterns(
                    &graph_service,
                    match_patterns.clone(),
                    &where_clause,
                    &compat_with,
                ).await?;

                let mut should_proceed = true;
                let ctx = EvaluationContext::from_uuid_bindings(&matched_bindings);
                
                if let Some(wc) = where_clause {
                    if !wc.condition.evaluate(&ctx).map(|v| v.as_bool()).unwrap_or(false) {
                        should_proceed = false;
                    }
                }

                if should_proceed {
                    if let Some(pwic) = with_clause {
                        if let Some(inner_wc) = &pwic.where_clause {
                            if !inner_wc.condition.evaluate(&ctx).map(|v| v.as_bool()).unwrap_or(false) {
                                should_proceed = false;
                            }
                        }
                    }
                }

                if should_proceed {
                    var_to_id.extend(matched_bindings);

                    for pat in create_patterns.iter() {
                        for (var_opt, labels_vec, properties) in &pat.1 {
                            if let Some(v) = var_opt.as_ref() {
                                if !var_to_id.contains_key(v) {
                                    let props: HashMap<String, PropertyValue> = properties
                                        .iter()
                                        .map(|(k, val)| to_property_value(val.clone()).map(|pv| (k.clone(), pv)))
                                        .collect::<GraphResult<HashMap<_, _>>>()?;
                                    let final_label = labels_vec.first().cloned().unwrap_or_else(|| "Node".to_string());
                                    let new_id = SerializableUuid(Uuid::new_v4());
                                    let vertex = Vertex {
                                        id: new_id,
                                        label: Identifier::new(final_label)?,
                                        properties: props,
                                        created_at: Utc::now().into(),
                                        updated_at: Utc::now().into(),
                                    };
                                    graph_service.create_vertex(vertex.clone()).await?;
                                    var_to_id.insert(v.clone(), new_id);
                                    created_vertices.push(vertex);
                                }
                            }
                        }
                    }

                    for (set_var, prop, expr) in set_clauses {
                        if let Some(id_wrapper) = var_to_id.get(&set_var) {
                            let mut vertex = graph_service.get_vertex(&id_wrapper.0).await.ok_or_else(|| {
                                GraphError::NotFound(unsafe { Identifier::new_unchecked(set_var.clone()) })
                            })?;
                            let ctx = EvaluationContext::from_vertex(&set_var, &vertex);
                            let evaluated_val = expr.evaluate(&ctx)?;
                            let json_val = evaluated_val.to_json();
                            let property_value = to_property_value(json_val)?;
                            vertex.properties.insert(prop.clone(), property_value);
                            vertex.updated_at = Utc::now().into();
                            graph_service.update_vertex(vertex.clone()).await?;
                            updated_vertices.push(vertex);
                        }
                    }
                }

                Ok(json!({
                    "created_vertices": created_vertices,
                    "updated_vertices": updated_vertices,
                    "stats": {
                        "vertices_created": created_vertices.len(),
                        "vertices_updated": updated_vertices.len(),
                    }
                }))
            }
            // ============================================================================
            // SINGLE MERGE - Context-aware with global variable binding
            // ============================================================================
            CypherQuery::Merge { 
                patterns, 
                where_clause, 
                with_clause, 
                on_create_set, 
                on_match_set, 
                return_clause, 
                .. 
            } => {
                println!("===> Executing top-level MERGE");
                
                // Execute merge with context to ensure MPI transactions are logged 
                // and related to specific patient golden records.
                let merge_result = graph_service.execute_merge_query_with_context(
                    patterns.clone(),
                    where_clause.clone(),
                    with_clause.clone(),
                    on_create_set.clone(),
                    on_match_set.clone(),
                    None, // No previous context for top-level
                ).await?;

                // Collect all nodes created or updated
                let affected_node_ids: Vec<uuid::Uuid> = merge_result.created_nodes.iter()
                    .chain(merge_result.updated_nodes.iter())
                    .copied()
                    .collect();

                // Fetch the actual vertex data for the RETURN clause
                let all_vertices = graph_service.get_all_vertices().await
                    .map_err(|e| GraphError::StorageError(e.to_string()))?;
                
                let fetched_vertices: Vec<Vertex> = all_vertices.into_iter()
                    .filter(|v| affected_node_ids.contains(&v.id.0))
                    .collect();

                // Fetch the edge data
                let fetched_edges: Vec<Edge> = if !merge_result.created_edges.is_empty() {
                    let all_edges = graph_service.get_all_edges().await
                        .map_err(|e| GraphError::StorageError(e.to_string()))?;
                    all_edges.into_iter()
                        .filter(|e| merge_result.created_edges.contains(&e.id.0))
                        .collect()
                } else {
                    Vec::new()
                };

                if let Some(ret) = return_clause {
                    let mut eval_bindings = HashMap::new();
                    let mut node_count = 0;

                    // Map variables to the fetched vertices
                    for pattern in patterns.iter() {
                        for node in &pattern.1 {
                            if let Some(var_name) = &node.0 {
                                if let Some(v_id) = affected_node_ids.get(node_count) {
                                    if let Some(vertex) = fetched_vertices.iter().find(|v| v.id.0 == *v_id) {
                                        eval_bindings.insert(var_name.clone(), CypherValue::Vertex(vertex.clone()));
                                    }
                                }
                            }
                            node_count += 1;
                        }
                    }

                    // Map variables to edges (crucial for relationship tracing)
                    for pattern in patterns.iter() {
                        for rel in &pattern.2 {
                            if let Some(rel_var) = &rel.0 {
                                if let Some(edge) = fetched_edges.first() {
                                    eval_bindings.insert(rel_var.clone(), CypherValue::Edge(edge.clone()));
                                }
                            }
                        }
                    }

                    // Initialize result_row within the correct scope
                    let mut result_row = serde_json::Map::new();
                    for item in &ret.items {
                        let expr_str = &item.expression;
                        
                        if let Some(dot_pos) = expr_str.find('.') {
                            let var = &expr_str[..dot_pos];
                            let prop = &expr_str[dot_pos + 1..];
                            if let Some(CypherValue::Vertex(vertex)) = eval_bindings.get(var) {
                                if let Some(pv) = vertex.properties.get(prop) {
                                    let key = item.alias.clone().unwrap_or_else(|| expr_str.to_string());
                                    result_row.insert(key, serde_json::to_value(pv).unwrap_or(json!(null)));
                                }
                            }
                        } else if expr_str.starts_with("type(") {
                            let edge_var = expr_str[5..expr_str.len()-1].trim();
                            if let Some(CypherValue::Edge(edge)) = eval_bindings.get(edge_var) {
                                let key = item.alias.clone().unwrap_or_else(|| expr_str.to_string());
                                result_row.insert(key, json!(edge.label.as_str()));
                            }
                        } else {
                            if let Some(val) = eval_bindings.get(expr_str) {
                                let key = item.alias.clone().unwrap_or_else(|| expr_str.to_string());
                                result_row.insert(key, val.to_json());
                            }
                        }
                    }

                    Ok(json!({
                        "vertices": vec![result_row],
                        "edges": fetched_edges,
                        "stats": {
                            "vertices_matched": fetched_vertices.len(),
                            "edges_matched": fetched_edges.len(),
                            "contains_updates": !merge_result.created_nodes.is_empty() || !merge_result.updated_nodes.is_empty()
                        }
                    }))
                } else {
                    Ok(json!({
                        "vertices": fetched_vertices,
                        "edges": fetched_edges,
                        "stats": {
                            "vertices_matched": fetched_vertices.len(),
                            "edges_matched": fetched_edges.len(),
                            "contains_updates": !merge_result.created_nodes.is_empty() || !merge_result.updated_nodes.is_empty()
                        }
                    }))
                }
            }
            // ============================================================================
            // CHAIN - Use global context with direct storage access (no Cypher recursion)
            // ============================================================================
            CypherQuery::Chain(clauses) => {
                println!("===> EXECUTING Chain with {} clauses", clauses.len());
                
                // Get global context
                let global_context = get_global_context().await;
                
                // Clear context at start of chain execution
                {
                    let mut ctx = global_context.write().await;
                    ctx.vertex_bindings.clear();
                    ctx.edge_bindings.clear();
                    ctx.current_vertices.clear();
                    ctx.current_edges.clear();
                } 

                let mut return_clause_opt: Option<ReturnClause> = None;
                let mut final_execution_result = ExecutionResult::new();
                
                for (idx, query) in clauses.iter().enumerate() {
                    println!("===> Chain clause {}/{}: {:?}", idx + 1, clauses.len(), std::mem::discriminant(query));
                    
                    match query {
                        // ================================================================
                        // MERGE in Chain - Update global context (SINGLE BINDING)
                        // ================================================================
                        CypherQuery::Merge { 
                            patterns, 
                            where_clause, 
                            with_clause,
                            on_create_set, 
                            on_match_set, 
                            return_clause,
                            ..
                        } => {
                            println!("===> Executing MERGE in Chain (clause {})", idx + 1);
                            
                            // Pass existing context to ensure transaction traceability for MPI
                            let current_bindings = {
                                let ctx = global_context.read().await;
                                ctx.vertex_bindings.clone()
                            };

                            let merge_result = graph_service.execute_merge_query_with_context(
                                patterns.clone(),
                                where_clause.clone(),
                                with_clause.clone(),
                                on_create_set.clone(),
                                on_match_set.clone(),
                                Some(current_bindings), 
                            ).await?;
                            
                            final_execution_result.extend(merge_result.clone());

                            let affected_ids: Vec<uuid::Uuid> = merge_result.created_nodes.iter()
                                .chain(merge_result.updated_nodes.iter())
                                .copied()
                                .collect();
                            
                            // 1. Update Bindings
                            {
                                let mut ctx = global_context.write().await;
                                let mut node_count = 0;
                                
                                // Handle Vertex Bindings
                                for pattern in patterns.iter() {
                                    for node in &pattern.1 {
                                        if let Some(node_var) = &node.0 {
                                            if let Some(&vertex_id) = affected_ids.get(node_count) {
                                                ctx.vertex_bindings
                                                    .entry(node_var.clone())
                                                    .or_default()
                                                    .insert(vertex_id);
                                                println!("===> BOUND global variable '{}' to vertex {}", node_var, vertex_id);
                                            }
                                        }
                                        node_count += 1;
                                    }
                                }
                                
                                // Handle Edge Bindings (Fixes .first() and Display errors)
                                for pattern in patterns.iter() {
                                    for rel in &pattern.2 {
                                        if let Some(rel_var) = &rel.0 {
                                            // Iterate through created edges to support multi-binding
                                            for edge_id in &merge_result.created_edges {
                                                ctx.edge_bindings
                                                    .entry(rel_var.clone())
                                                    .or_default()
                                                    .insert(*edge_id);
                                                // Use :? for HashSet or *edge_id for the specific ID
                                                println!("===> BOUND edge variable '{}' to edge {:?}", rel_var, edge_id);
                                            }
                                        }
                                    }
                                }
                            }

                            // 2. REFRESH Context Vertices (Fixes .copied() on HashSet error)
                            let all_vertex_ids: Vec<uuid::Uuid> = {
                                let ctx = global_context.read().await;
                                // Use .flatten() to reach the Uuids inside the HashSets
                                ctx.vertex_bindings.values().flatten().copied().collect()
                            };
                            
                            let all_vertices = graph_service.get_all_vertices().await
                                .map_err(|e| GraphError::StorageError(e.to_string()))?;
                            let fetched_vertices: Vec<Vertex> = all_vertices.into_iter()
                                .filter(|v| all_vertex_ids.contains(&v.id.0))
                                .collect();
                            
                            {
                                let mut ctx = global_context.write().await;
                                ctx.current_vertices = fetched_vertices; 
                            }

                            // 3. REFRESH Context Edges
                            let all_edge_ids: Vec<uuid::Uuid> = {
                                let ctx = global_context.read().await;
                                ctx.edge_bindings.values().flatten().copied().collect()
                            };

                            let all_edges = graph_service.get_all_edges().await
                                .map_err(|e| GraphError::StorageError(e.to_string()))?;
                            let fetched_edges: Vec<Edge> = all_edges.into_iter()
                                .filter(|e| all_edge_ids.contains(&e.id.0))
                                .collect();
                            
                            {
                                let mut ctx = global_context.write().await;
                                ctx.current_edges = fetched_edges;
                            }
                            
                            if return_clause.is_some() {
                                return_clause_opt = return_clause.clone();
                            }
                        }
                        // ================================================================
                        // MATCH in Chain - Handles both regular MATCH and standalone WITH
                        // ================================================================
                        CypherQuery::MatchPattern { patterns, where_clause, with_clause, return_clause, .. } => {
                            println!("===> Executing MATCH/WITH in Chain (clause {})", idx + 1);
                            
                            // Check if this is a standalone WITH clause (no patterns)
                            if patterns.is_empty() && with_clause.is_some() {
                                println!("===> Processing standalone WITH clause for variable aliasing");
                                
                                let with = with_clause.as_ref().unwrap();
                                
                                // CRITICAL FIX: Perform aliasing at EXECUTION TIME with runtime UUIDs
                                {
                                    let mut ctx = global_context.write().await;
                                    
                                    for item in &with.items {
                                        if let Some(ref alias) = item.alias {
                                            // Look up the source variable in runtime vertex bindings
                                            if let Some(id_set) = ctx.vertex_bindings.get(&item.expression) {
                                                if let Some(&actual_id) = id_set.iter().next() {
                                                    // Copy the UUID to the alias
                                                    ctx.vertex_bindings
                                                        .entry(alias.clone())
                                                        .or_default()
                                                        .insert(actual_id);
                                                    
                                                    println!("===> ✓ Aliased '{}' -> '{}' with actual ID {}", 
                                                             item.expression, alias, actual_id);
                                                } else {
                                                    warn!("===> ✗ Variable '{}' has no bound IDs", item.expression);
                                                }
                                            } else {
                                                warn!("===> ✗ Variable '{}' not found in vertex_bindings", item.expression);
                                            }
                                            
                                            // Also check edge bindings for relationship aliases
                                            if let Some(id_set) = ctx.edge_bindings.get(&item.expression) {
                                                if let Some(&actual_id) = id_set.iter().next() {
                                                    ctx.edge_bindings
                                                        .entry(alias.clone())
                                                        .or_default()
                                                        .insert(actual_id);
                                                    
                                                    println!("===> ✓ Aliased edge '{}' -> '{}' with actual ID {}", 
                                                             item.expression, alias, actual_id);
                                                }
                                            }
                                        }
                                    }
                                } // Lock released
                                
                                // Apply WITH WHERE clause if present
                                if let Some(ref wc) = with.where_clause {
                                    let (current_vertices, current_edges, vertex_bindings, edge_bindings) = {
                                        let ctx = global_context.read().await;
                                        (
                                            ctx.current_vertices.clone(),
                                            ctx.current_edges.clone(),
                                            ctx.vertex_bindings.clone(),
                                            ctx.edge_bindings.clone()
                                        )
                                    };
                                    
                                    // Build evaluation context
                                    let mut eval_bindings = HashMap::new();
                                    for (var_name, vertex_id_set) in &vertex_bindings {
                                        if let Some(vertex) = current_vertices.iter().find(|v| vertex_id_set.contains(&v.id.0)) {
                                            eval_bindings.insert(var_name.clone(), CypherValue::Vertex(vertex.clone()));
                                        }
                                    }
                                    for (var_name, edge_id_set) in &edge_bindings {
                                        if let Some(edge) = current_edges.iter().find(|e| edge_id_set.contains(&e.id.0)) {
                                            eval_bindings.insert(var_name.clone(), CypherValue::Edge(edge.clone()));
                                        }
                                    }
                                    
                                    // Filter current vertices based on WHERE
                                    let eval_ctx = EvaluationContext {
                                        variables: eval_bindings,
                                        parameters: HashMap::new(),
                                    };
                                    
                                    if matches!(wc.condition.evaluate(&eval_ctx), Ok(CypherValue::Bool(true))) {
                                        println!("===> WITH WHERE condition passed");
                                    } else {
                                        println!("===> WITH WHERE condition failed - clearing context");
                                        let mut ctx = global_context.write().await;
                                        ctx.current_vertices.clear();
                                        ctx.current_edges.clear();
                                    }
                                }
                                
                                if return_clause.is_some() {
                                    return_clause_opt = return_clause.clone();
                                }
                                
                                // Continue to next clause
                                continue;
                            }
                            
                            // Regular MATCH handling (when patterns are present)
                            println!("===> Executing regular MATCH in Chain");
                            
                            let transformed_patterns: Vec<_> = patterns.iter().map(|(id, nodes, edges)| {
                                let transformed_nodes: Vec<_> = nodes.iter().map(|(var, labels, props)| {
                                    (var.clone(), labels.first().cloned(), props.clone())
                                }).collect();
                                (id.clone(), transformed_nodes, edges.clone())
                            }).collect();

                            let (v_list, e_list) = exec_cypher_pattern(
                                transformed_patterns, 
                                &graph_service
                            ).await?;
                            
                            let mut filtered_vertices = v_list;
                            
                            if let Some(wc) = where_clause {
                                filtered_vertices.retain(|v| {
                                    let var_ref = patterns.get(0)
                                        .and_then(|p| p.1.get(0))
                                        .and_then(|n| n.0.as_ref())
                                        .map(|s| s.as_str())
                                        .unwrap_or("n");
                                    
                                    let eval_ctx = EvaluationContext::from_vertex(var_ref, v);
                                    matches!(wc.condition.evaluate(&eval_ctx), Ok(CypherValue::Bool(true)))
                                });
                            }

                            {
                                let mut ctx = global_context.write().await;
                                ctx.current_vertices = filtered_vertices.clone();
                                ctx.current_edges = e_list.clone();

                                let mut node_idx = 0;
                                for pattern in patterns.iter() {
                                    for node in &pattern.1 {
                                        if let Some(var) = &node.0 {
                                            if let Some(vertex) = filtered_vertices.get(node_idx) {
                                                ctx.vertex_bindings
                                                    .entry(var.clone())
                                                    .or_default()
                                                    .insert(vertex.id.0);
                                                println!("===> BOUND global variable '{}' to vertex {}", var, vertex.id.0);
                                            }
                                        }
                                        node_idx += 1;
                                    }
                                }
                                
                                for pattern in patterns.iter() {
                                    for rel in &pattern.2 {
                                        if let Some(rel_var) = &rel.0 {
                                            for edge in &e_list {
                                                ctx.edge_bindings
                                                    .entry(rel_var.clone())
                                                    .or_default()
                                                    .insert(edge.id.0);
                                            }
                                        }
                                    }
                                }
                            } 
                            
                            if return_clause.is_some() {
                                return_clause_opt = return_clause.clone();
                            }
                        }
                        // ============================================================================
                        // INTERNAL RETURN HANDLER (Inside Chain match arm)
                        // ============================================================================
                        CypherQuery::ReturnStatement { projection_string, order_by, is_distinct, .. } => {
                            println!("===> Processing RETURN in Chain using global context (Distinct: {})", is_distinct);
                            
                            // 1. Snapshot the context including scalar_rows produced by UNWIND
                            let (current_vertices, current_edges, vertex_bindings, edge_bindings, scalar_rows) = {
                                let ctx = global_context.read().await;
                                (
                                    ctx.current_vertices.clone(),
                                    ctx.current_edges.clone(),
                                    ctx.vertex_bindings.clone(),
                                    ctx.edge_bindings.clone(),
                                    ctx.scalar_rows.clone(),
                                )
                            };
                            
                            println!("===> Context: {} vertices, {} edges, {} scalar_rows",
                                     current_vertices.len(), current_edges.len(), scalar_rows.len());

                            // 2. Normalize projection items into a Vec<(Expression, Alias)>
                            let mut final_items = Vec::new();
                            let trimmed_proj = projection_string.trim();

                            if trimmed_proj.starts_with('{') {
                                if let Ok(parsed_json) = serde_json::from_str::<serde_json::Value>(trimmed_proj) {
                                    if let Some(items_array) = parsed_json.get("items").and_then(|i| i.as_array()) {
                                        for item in items_array {
                                            let expr = item.get("expression")
                                                .and_then(|e| e.as_str())
                                                .unwrap_or("")
                                                .replace('\\', "")
                                                .trim_matches('"')
                                                .to_string();
                                            let alias = item.get("alias")
                                                .and_then(|a| a.as_str())
                                                .map(|s| s.replace('\\', "").trim_matches('"').to_string());
                                            final_items.push((expr, alias));
                                        }
                                    }
                                }
                            } else {
                                // Handle raw string "RETURN label, count(*) AS count"
                                let raw_items: Vec<&str> = trimmed_proj
                                    .trim_start_matches("RETURN ")
                                    .trim_start_matches("DISTINCT ")
                                    .split(',')
                                    .map(|s| s.trim())
                                    .collect();

                                for item in raw_items {
                                    if let Some(as_pos) = item.to_uppercase().find(" AS ") {
                                        let expr = item[..as_pos].trim().replace('\\', "").trim_matches('"').to_string();
                                        let alias = item[as_pos + 4..].trim().replace('\\', "").trim_matches('"').to_string();
                                        final_items.push((expr, Some(alias)));
                                    } else {
                                        let expr = item.replace('\\', "").trim_matches('"').to_string();
                                        final_items.push((expr, None));
                                    }
                                }
                            }
                            
                            // 3. Determine if we need to perform aggregation (Grouping)
                            let has_aggregation = final_items.iter().any(|(expr, _)| expr.contains("count("));
                            let mut result_rows = Vec::new();

                            // Determine data source: UNWIND rows or fallback to direct matches
                            let rows_to_process = if !scalar_rows.is_empty() {
                                scalar_rows
                            } else {
                                let mut fallback_map = HashMap::new();
                                for (var, ids) in &vertex_bindings {
                                    if let Some(v) = current_vertices.iter().find(|v| ids.contains(&v.id.0)) {
                                        fallback_map.insert(var.clone(), CypherValue::Vertex(v.clone()));
                                    }
                                }
                                for (var, ids) in &edge_bindings {
                                    if let Some(e) = current_edges.iter().find(|e| ids.contains(&e.id.0)) {
                                        fallback_map.insert(var.clone(), CypherValue::Edge(e.clone()));
                                    }
                                }
                                vec![fallback_map]
                            };

                            if has_aggregation {
                                let mut groups: HashMap<Vec<serde_json::Value>, i64> = HashMap::new();

                                for row_bindings in &rows_to_process {
                                    let mut group_key = Vec::new();
                                    for (expr_raw, _) in &final_items {
                                        if !expr_raw.contains("count(") {
                                            let val = if let Some(v) = row_bindings.get(expr_raw) {
                                                v.to_json()
                                            } else if let Some(dot_pos) = expr_raw.find('.') {
                                                let var_name = &expr_raw[..dot_pos];
                                                let prop_name = &expr_raw[dot_pos + 1..];
                                                row_bindings.get(var_name).and_then(|cv| match cv {
                                                    CypherValue::Vertex(v) => v.properties.get(prop_name).map(|pv| serde_json::to_value(pv).unwrap_or(serde_json::json!(null))),
                                                    CypherValue::Edge(e) => e.properties.get(prop_name).map(|pv| serde_json::to_value(pv).unwrap_or(serde_json::json!(null))),
                                                    _ => None,
                                                }).unwrap_or(serde_json::json!(null))
                                            } else {
                                                serde_json::json!(null)
                                            };
                                            group_key.push(val);
                                        }
                                    }
                                    *groups.entry(group_key).or_insert(0) += 1;
                                }

                                for (group_values, count) in groups {
                                    let mut result_row = serde_json::Map::new();
                                    let mut val_idx = 0;
                                    for (expr_raw, alias_opt) in &final_items {
                                        let key = alias_opt.clone().unwrap_or_else(|| expr_raw.clone());
                                        if expr_raw.contains("count(") {
                                            result_row.insert(key, serde_json::json!(count));
                                        } else {
                                            result_row.insert(key, group_values[val_idx].clone());
                                            val_idx += 1;
                                        }
                                    }
                                    result_rows.push(serde_json::Value::Object(result_row));
                                }
                            } else {
                                // Non-aggregated path
                                for row_bindings in rows_to_process {
                                    let mut result_row = serde_json::Map::new();
                                    for (expr_raw, alias_opt) in &final_items {
                                        let key = alias_opt.clone().unwrap_or_else(|| expr_raw.clone());
                                        
                                        if let Some(val) = row_bindings.get(expr_raw) {
                                            result_row.insert(key.clone(), val.to_json());
                                        } else if let Some(dot_pos) = expr_raw.find('.') {
                                            let var_name = &expr_raw[..dot_pos];
                                            let prop_name = &expr_raw[dot_pos + 1..];
                                            if let Some(val) = row_bindings.get(var_name) {
                                                let json_val = match val {
                                                    CypherValue::Vertex(v) => v.properties.get(prop_name).map(|pv| serde_json::to_value(pv).unwrap_or(serde_json::json!(null))).unwrap_or(serde_json::json!(null)),
                                                    CypherValue::Edge(e) => e.properties.get(prop_name).map(|pv| serde_json::to_value(pv).unwrap_or(serde_json::json!(null))).unwrap_or(serde_json::json!(null)),
                                                    _ => serde_json::json!(null),
                                                };
                                                result_row.insert(key.clone(), json_val);
                                            }
                                        } else {
                                            result_row.insert(key.clone(), serde_json::json!(null));
                                        }
                                    }
                                    result_rows.push(serde_json::Value::Object(result_row));
                                }
                                
                                // Apply DISTINCT
                                if *is_distinct {
                                    let mut seen = std::collections::HashSet::new();
                                    result_rows.retain(|x| seen.insert(x.to_string()));
                                }
                            }

                            // 4. Handle ORDER BY
                            if !order_by.is_empty() {
                                result_rows.sort_by(|a, b| {
                                    for item in order_by {
                                        let field = &item.expression; 
                                        let val_a = a.get(field).unwrap_or(&serde_json::Value::Null);
                                        let val_b = b.get(field).unwrap_or(&serde_json::Value::Null);
                                        
                                        let cmp = match (val_a, val_b) {
                                            (serde_json::Value::Number(n1), serde_json::Value::Number(n2)) => {
                                                let f1 = n1.as_f64().unwrap_or(0.0);
                                                let f2 = n2.as_f64().unwrap_or(0.0);
                                                f1.partial_cmp(&f2).unwrap_or(std::cmp::Ordering::Equal)
                                            }
                                            (serde_json::Value::String(s1), serde_json::Value::String(s2)) => s1.cmp(s2),
                                            _ => std::cmp::Ordering::Equal,
                                        };

                                        if cmp != std::cmp::Ordering::Equal {
                                            return if item.ascending { cmp } else { cmp.reverse() };
                                        }
                                    }
                                    std::cmp::Ordering::Equal
                                });
                            }

                            println!("===> Final results: Generated {} rows", result_rows.len());
                            
                            return Ok(serde_json::json!({
                                "results": [{
                                    "vertices": result_rows,
                                    "edges": current_edges,
                                    "stats": {
                                        "vertices_matched": current_vertices.len(),
                                        "edges_matched": current_edges.len(),
                                        "contains_updates": false
                                    }
                                }]
                            }));
                        }
                        // ============================================================================
                        // MATCH CREATE in Chain - Leverage Global Context for MPI Traceability
                        // ============================================================================
                        CypherQuery::MatchCreate { 
                            match_patterns, 
                            create_patterns, 
                            where_clause, 
                            .. 
                        } => {
                            println!("===> Executing MATCH CREATE in Chain (clause {})", idx + 1);
                            
                            // 1. Get existing bindings from global context to maintain MPI context
                            let current_bindings = {
                                let ctx = global_context.read().await;
                                ctx.vertex_bindings.clone()
                            };

                            // 2. Prepare Match Patterns
                            let transformed_match: Vec<_> = match_patterns.iter().map(|(_id, nodes, edges)| {
                                let nodes: Vec<_> = nodes.iter().map(|(var, labels, props)| {
                                    (var.clone(), labels.first().cloned(), props.clone())
                                }).collect();
                                (None, nodes, edges.clone()) // path_var is None for internal matching
                            }).collect();

                            // 3. Execute MATCH part
                            let (v_match, e_match) = exec_cypher_pattern(
                                transformed_match, 
                                &graph_service
                            ).await?;

                            let mut filtered_vertices = v_match;

                            // 4. Apply WHERE filtering
                            if let Some(wc) = where_clause {
                                filtered_vertices.retain(|v| {
                                    let var_ref = match_patterns.get(0)
                                        .and_then(|p| p.1.get(0))
                                        .and_then(|n| n.0.as_ref())
                                        .map(|s| s.as_str())
                                        .unwrap_or("n");
                                    
                                    let eval_ctx = EvaluationContext::from_vertex(var_ref, v);
                                    matches!(wc.condition.evaluate(&eval_ctx), Ok(CypherValue::Bool(true)))
                                });
                            }

                            // 5. Update Global Context with Match results before CREATE
                            {
                                let mut ctx = global_context.write().await;
                                ctx.current_vertices = filtered_vertices.clone();
                                ctx.current_edges = e_match.clone();
                                
                                for pattern in match_patterns.iter() {
                                    for (n_idx, node) in pattern.1.iter().enumerate() {
                                        if let (Some(var), Some(vertex)) = (&node.0, filtered_vertices.get(n_idx)) {
                                            ctx.vertex_bindings.entry(var.clone()).or_default().insert(vertex.id.0);
                                        }
                                    }
                                }
                            }

                            // 6. Execute CREATE part using the context-aware service method
                            // Matches the method name: execute_create_query_with_context
                            let create_result = graph_service.execute_create_query_with_context(
                                create_patterns.clone(),
                                Some(current_bindings) 
                            ).await?;

                            final_execution_result.extend(create_result.clone());

                            // 7. Final Context Refresh: Bind newly created nodes
                            {
                                let mut ctx_write = global_context.write().await;
                                
                                // Convert HashSet to Vec to allow indexed access matching the create_patterns loop
                                let created_ids_vec: Vec<Uuid> = create_result.created_nodes.iter().cloned().collect();
                                let mut create_node_idx = 0;

                                for pattern in create_patterns.iter() {
                                    for node in &pattern.1 {
                                        if let Some(var) = &node.0 {
                                            if let Some(new_id) = created_ids_vec.get(create_node_idx) {
                                                // Correctly update the HashSet for this variable
                                                ctx_write.vertex_bindings
                                                    .entry(var.clone())
                                                    .or_insert_with(std::collections::HashSet::new)
                                                    .insert(*new_id);
                                            }
                                        }
                                        create_node_idx += 1;
                                    }
                                }
                            }
                        }
                        // ================================================================
                        // FIXED UNWIND in Chain
                        // ================================================================
                        CypherQuery::Unwind { expression, variable } => {
                            println!("===> Executing UNWIND in Chain (clause {})", idx + 1);
                            
                            let mut new_rows = Vec::new();
                            let ctx_snapshot = global_context.read().await;

                            // Determine the source of data to unwind
                            let rows_to_process = if !ctx_snapshot.scalar_rows.is_empty() {
                                ctx_snapshot.scalar_rows.clone()
                            } else {
                                // FIX: For MATCH (e:IdentityEvent), we must iterate ALL 142 vertices
                                // and bind each one to the variable name 'e' so keys(e) works.
                                ctx_snapshot.current_vertices.iter().map(|v| {
                                    let mut map = HashMap::new();
                                    
                                    // Map ALL variables bound to this specific vertex
                                    for (var_name, ids) in &ctx_snapshot.vertex_bindings {
                                        if ids.contains(&v.id.0) {
                                            map.insert(var_name.clone(), CypherValue::Vertex(v.clone()));
                                        }
                                    }

                                    // Fallback for safety: if no binding found, use 'n'
                                    if map.is_empty() {
                                        map.insert("n".to_string(), CypherValue::Vertex(v.clone()));
                                    }
                                    map
                                }).collect::<Vec<_>>()
                            };

                            for row_bindings in rows_to_process {
                                let eval_ctx = EvaluationContext {
                                    variables: row_bindings.clone(),
                                    parameters: HashMap::new(), 
                                };
                                
                                // Evaluate expression (e.g., keys(e))
                                if let Ok(val) = expression.evaluate(&eval_ctx) {
                                    match val {
                                        CypherValue::List(items) => {
                                            for item in items {
                                                let mut new_row = row_bindings.clone();
                                                new_row.insert(variable.clone(), item);
                                                new_rows.push(new_row);
                                            }
                                        },
                                        scalar => {
                                            let mut new_row = row_bindings.clone();
                                            new_row.insert(variable.clone(), scalar);
                                            new_rows.push(new_row);
                                        }
                                    }
                                }
                            }
                            
                            drop(ctx_snapshot);
                            let mut ctx_write = global_context.write().await;
                            ctx_write.scalar_rows = new_rows; 
                            println!("===> UNWIND complete: Produced {} rows", ctx_write.scalar_rows.len());
                        }
                        // ================================================================
                        // SET Statement in Chain
                        // ================================================================
                        CypherQuery::SetStatement { assignments } => {
                            println!("===> Executing SET in Chain for assignments: {:?}", assignments);
                            
                            // Extract the actual bindings from the global context to satisfy the method signature
                            let current_bindings = {
                                let ctx = global_context.read().await;
                                ctx.vertex_bindings.clone()
                            };

                            // Pass the cloned HashMap, not the Arc/RwLock
                            let set_result = graph_service.execute_set_query_with_context(
                                assignments.clone(),
                                Some(current_bindings) 
                            ).await?;
                            
                            final_execution_result.extend(set_result);

                            // REFRESH Context Vertices to reflect property changes in global state
                            let all_vertex_ids: Vec<uuid::Uuid> = {
                                let ctx = global_context.read().await;
                                ctx.vertex_bindings.values().flatten().copied().collect()
                            };
                            
                            let all_vertices = graph_service.get_all_vertices().await
                                .map_err(|e| GraphError::StorageError(e.to_string()))?;
                            
                            {
                                let mut ctx = global_context.write().await;
                                ctx.current_vertices = all_vertices.into_iter()
                                    .filter(|v| all_vertex_ids.contains(&v.id.0))
                                    .collect();
                            }
                        }

                        // ================================================================
                        // DELETE Statement in Chain
                        // ================================================================
                        CypherQuery::DeleteStatement { variables, detach: _detach } => {
                            println!("===> Executing DELETE in Chain for variables: {:?}", variables);
                            
                            let mut context_map = HashMap::new();
                            {
                                let ctx = global_context.read().await;
                                for var in variables {
                                    // Fix: Use &String to satisfy .get() generic requirements
                                    if let Some(ids) = ctx.vertex_bindings.get(var) {
                                        context_map.insert(var.clone(), ids.clone());
                                    }
                                }
                            }

                            let delete_result = graph_service.execute_delete_query_with_context(
                                variables.clone(),
                                Some(context_map)
                            ).await?;

                            final_execution_result.extend(delete_result.clone());

                            // Remove deleted entities from global context tracking
                            {
                                let mut ctx = global_context.write().await;
                                for var in variables {
                                    ctx.vertex_bindings.remove(var);
                                }
                                ctx.current_vertices.retain(|v| !delete_result.deleted_nodes.contains(&v.id.0));
                                ctx.current_edges.retain(|e| !delete_result.deleted_edges.contains(&e.id.0));
                            }
                        }
                        
                        _ => {
                            println!("===> Skipping unsupported clause type in Chain");
                        }
                    }
                }
                
                // Final Return evaluation logic 
                let (current_vertices, current_edges, vertex_bindings, edge_bindings) = {
                    let ctx = global_context.read().await;
                    (ctx.current_vertices.clone(), ctx.current_edges.clone(), ctx.vertex_bindings.clone(), ctx.edge_bindings.clone())
                };

                if let Some(ret) = return_clause_opt {
                    let mut eval_bindings = HashMap::new();
                    for (var_name, vertex_id_set) in &vertex_bindings {
                        if let Some(vertex) = current_vertices.iter().find(|v| vertex_id_set.contains(&v.id.0)) {
                            eval_bindings.insert(var_name.clone(), CypherValue::Vertex(vertex.clone()));
                        }
                    }
                    for (var_name, edge_id_set) in &edge_bindings {
                        if let Some(edge) = current_edges.iter().find(|e| edge_id_set.contains(&e.id.0)) {
                            eval_bindings.insert(var_name.clone(), CypherValue::Edge(edge.clone()));
                        }
                    }

                    let mut result_row = serde_json::Map::new();
                    for item in &ret.items {
                        let expr_str = &item.expression;
                        if expr_str.starts_with("type(") && expr_str.ends_with(')') {
                            let edge_var = expr_str[5..expr_str.len()-1].trim();
                            if let Some(CypherValue::Edge(edge)) = eval_bindings.get(edge_var) {
                                let key = item.alias.clone().unwrap_or_else(|| expr_str.to_string());
                                result_row.insert(key, json!(edge.label.as_str()));
                            }
                        } else if let Some(dot_pos) = expr_str.find('.') {
                            let var = &expr_str[..dot_pos];
                            let prop = &expr_str[dot_pos + 1..];
                            if let Some(CypherValue::Vertex(vertex)) = eval_bindings.get(var) {
                                if let Some(pv) = vertex.properties.get(prop) {
                                    let key = item.alias.clone().unwrap_or_else(|| expr_str.to_string());
                                    result_row.insert(key, serde_json::to_value(pv).unwrap_or(json!(null)));
                                }
                            }
                        } else {
                            if let Some(val) = eval_bindings.get(expr_str) {
                                let key = item.alias.clone().unwrap_or_else(|| expr_str.to_string());
                                result_row.insert(key, val.to_json());
                            }
                        }
                    }

                    Ok(json!({
                        "vertices": vec![result_row],
                        "edges": current_edges,
                        "stats": {
                            "vertices_matched": current_vertices.len(),
                            "edges_matched": current_edges.len(),
                            "contains_updates": final_execution_result.has_mutations()
                        }
                    }))
                } else {
                    Ok(json!({
                        "vertices": current_vertices,
                        "edges": current_edges,
                        "stats": {
                            "vertices_matched": current_vertices.len(),
                            "edges_matched": current_edges.len(),
                            "contains_updates": final_execution_result.has_mutations()
                        }
                    }))
                }
            }
            // Handles CREATE (a:Label {props})
            CypherQuery::CreateNode { label, properties } => {
                // 1. Prepare Vertex data structure
                let props: HashMap<_, _> = properties.into_iter()
                    .map(|(k, v)| Ok((k, to_property_value(v)?)))
                    .collect::<GraphResult<_>>()?;

                let v = Vertex {
                    id: SerializableUuid(Uuid::new_v4()),
                    label: Identifier::new(label)?,
                    properties: props,
                    created_at: Utc::now().into(),  
                    updated_at: Utc::now().into(), 
                };

                // *** DELEGATE TO graph_service ***
                // graph_service.create_vertex handles both storage and in-memory graph update.
                graph_service.create_vertex(v.clone()).await?;

                Ok(json!({ "vertex": v }))
            },
            // Handles CREATE (n1:L1), (n2:L2)
            CypherQuery::CreateNodes { nodes } => {
                // FIX: Map the input 'nodes' structure (String label) to the structure
                // expected by 'create_node_batch' (Option<String> label).
                let nodes_for_batch = nodes.into_iter().map(|(label, properties)| {
                    // Wrap the mandatory String label into an Option<String>
                    (Some(label), properties)
                }).collect::<Vec<_>>();

                // *** DELEGATE TO graph_service ***
                let created_vertices = graph_service.create_node_batch(nodes_for_batch).await?;
                
                Ok(json!({ "vertices": created_vertices }))
            }

            CypherQuery::MatchNode { label, properties } => {
                let mut query_props: HashMap<String, PropertyValue> = properties
                    .into_iter()
                    .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                    .collect::<GraphResult<_>>()?;
                
                // --- START FIX: Indexed lookup for internal 'id' property ---
                if let Some(prop_value) = query_props.remove("id") {
                    let internal_id: Option<i32> = match prop_value {
                        PropertyValue::Integer(val) => Some(val as i32), 
                        PropertyValue::I32(val) => Some(val),
                        _ => None,
                    };

                    if let Some(internal_id) = internal_id {
                        match get_vertex_by_internal_id_direct(&graph_service, internal_id).await? {
                            Some(v) => {
                                // FIX: Check if the vertex label contains the query label (supports multi-label)
                                let matches_label = label.as_ref().map_or(true, |l| {
                                    v.label.as_ref().split(':').any(|part| part == l)
                                });
                                
                                let matches_remaining_props = query_props.iter().all(|(k, expected_val)| {
                                    v.properties.get(k).map_or(false, |actual_val| actual_val == expected_val)
                                });

                                if matches_label && matches_remaining_props {
                                    return Ok(json!({ "vertices": vec![v] }));
                                } else {
                                    return Ok(json!({ "vertices": Vec::<models::Vertex>::new() }));
                                }
                            }
                            None => {
                                return Ok(json!({ "vertices": Vec::<models::Vertex>::new() }));
                            }
                        }
                    }
                }
                // --- END FIX ---

                let vertices = graph_service.get_all_vertices().await?;
                
                let filtered = vertices.into_iter().filter(|v| {
                    // FIX: Multi-label aware matching
                    let matches_label = if let Some(query_label) = &label {
                        v.label.as_ref().split(':').any(|part| part == query_label)
                    } else {
                        true
                    };
                    
                    let matches_props = if query_props.is_empty() {
                        true
                    } else {
                        query_props.iter().all(|(k, expected_val)| {
                            v.properties.get(k).map_or(false, |actual_val| actual_val == expected_val)
                        })
                    };
                    
                    matches_label && matches_props
                }).collect::<Vec<_>>();
                
                Ok(json!({ "vertices": filtered }))
            }

            CypherQuery::MatchMultipleNodes { nodes } => {
                let mut result_vertices = Vec::new();
                let mut matched_ids = HashSet::new();
                let all_vertices = graph_service.get_all_vertices().await?;

                for (_var, labels, properties) in nodes {
                    let mut props: HashMap<String, PropertyValue> = properties
                        .iter()
                        .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                        .collect::<GraphResult<_>>()?;
                    
                    let mut handled_by_id_constraint = false;
                    
                    if let Some(prop_value) = props.remove("id") {
                        let internal_id = match prop_value {
                            PropertyValue::Integer(val) => Some(val as i32), 
                            PropertyValue::I32(val) => Some(val), 
                            _ => None,
                        };
                        
                        if let Some(internal_id) = internal_id {
                            handled_by_id_constraint = true; 
                        
                            if let Some(v) = get_vertex_by_internal_id_direct(&graph_service, internal_id).await? {
                                // FIX: Check if node matches ANY of the labels provided in the Vec
                                let matches_label = if labels.is_empty() {
                                    true
                                } else {
                                    labels.iter().any(|l| {
                                        v.label.as_ref().split(':').any(|part| part == l)
                                    })
                                };

                                let matches_remaining_props = props.iter().all(|(k, expected_val)| {
                                    v.properties.get(k).map_or(false, |actual_val| actual_val == expected_val)
                                });

                                if matches_label && matches_remaining_props && !matched_ids.contains(&v.id) {
                                    result_vertices.push(v.clone());
                                    matched_ids.insert(v.id);
                                }
                            }
                        }
                    }

                    if handled_by_id_constraint {
                        continue;
                    }

                    for v in &all_vertices {
                        // FIX: Multi-label check for the scan branch
                        let matches_label = if labels.is_empty() {
                            true
                        } else {
                            labels.iter().any(|l| {
                                v.label.as_ref().split(':').any(|part| part == l)
                            })
                        };
                        
                        let matches_props = props.iter().all(|(k, expected_val)| {
                            v.properties.get(k).map_or(false, |actual_val| actual_val == expected_val)
                        });
                        
                        if matches_label && matches_props && !matched_ids.contains(&v.id) {
                            result_vertices.push(v.clone());
                            matched_ids.insert(v.id);
                        }
                    }
                }
                
                Ok(json!({ "vertices": result_vertices, "count": result_vertices.len() }))
            }

            // --- UPDATED CREATE COMPLEX PATTERN (Pure CREATE of a Path) ---
            // This is structurally similar to the CREATE part of MatchCreate, 
            // but all nodes *must* be new, so they are unconditionally created.
            CypherQuery::CreateComplexPattern { nodes, relationships } => {
                println!("===> EXECUTING CreateComplexPattern: {} nodes, {} relationships", nodes.len(), relationships.len());
                
                let mut created_vertices = Vec::new();
                let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new(); 
                let mut created_edges = Vec::new();

                // 1. Create all vertices and populate var_to_id map
                if nodes.is_empty() {
                    return Err(GraphError::ValidationError("CREATE pattern must contain at least one node.".into()));
                }

                for (var_opt, labels_vec, properties) in nodes.iter() {
                    let props: GraphResult<HashMap<String, PropertyValue>> = properties
                        .iter()
                        .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                        .collect();
                    
                    let new_id = SerializableUuid(Uuid::new_v4());
                    
                    // FIX: Resolve label by taking the first one in the Vec or defaulting to "Node"
                    let final_label = labels_vec.first()
                        .cloned()
                        .unwrap_or_else(|| "Node".to_string());
                    
                    let vertex = Vertex {
                        id: new_id,
                        label: Identifier::new(final_label)?,
                        properties: props?,
                        created_at: Utc::now().into(),
                        updated_at: Utc::now().into(), 
                    };
                    
                    graph_service.create_vertex(vertex.clone()).await?;
                    
                    if let Some(v) = var_opt.as_ref() {
                        var_to_id.insert(v.clone(), new_id);
                    }
                    created_vertices.push(vertex);
                }
                
                // 2. Create edges based on the relationships list and sequential nodes.
                if relationships.len() != nodes.len().saturating_sub(1) {
                    return Err(GraphError::ValidationError("Mismatched number of nodes and relationships in complex CREATE pattern.".into()));
                }

                for (i, rel_tuple) in relationships.into_iter().enumerate() {
                    let from_var_opt = nodes[i].0.as_ref();
                    let to_var_opt = nodes[i + 1].0.as_ref();

                    let from_var = from_var_opt.ok_or_else(|| GraphError::ValidationError(format!("Node at index {} in CREATE pattern has no variable", i)))?;
                    let to_var = to_var_opt.ok_or_else(|| GraphError::ValidationError(format!("Node at index {} in CREATE pattern has no variable", i + 1)))?;

                    let from_id = *var_to_id.get(from_var).ok_or_else(|| GraphError::ValidationError(format!("Unbound from var: {}", from_var)))?;
                    let to_id = *var_to_id.get(to_var).ok_or_else(|| GraphError::ValidationError(format!("Unbound to var: {}", to_var)))?;

                    let (
                        _rel_var,     
                        label_opt,      // Index 1: Relationship Label (Option<String>)
                        _len_range,    
                        properties,     // Index 3: Properties Map
                        direction_opt,  // Index 4: Direction (Option<bool>)
                    ) = rel_tuple;

                    let (outbound_id, inbound_id) = match direction_opt {
                        Some(false) => (to_id, from_id),
                        _ => (from_id, to_id),
                    };

                    let edge_type_str = label_opt.clone().unwrap_or_else(|| "RELATED".to_string());
                    
                    let props: GraphResult<BTreeMap<String, PropertyValue>> = properties.iter()
                        .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                        .collect();
                    
                    let edge = Edge {
                        id: SerializableUuid(Uuid::new_v4()),
                        outbound_id,
                        inbound_id,
                        edge_type: Identifier::new(edge_type_str.clone())?,
                        label: edge_type_str,
                        properties: props?,
                    };
                    
                    graph_service.create_edge(edge.clone()).await?;
                    created_edges.push(edge);
                }

                Ok(json!({
                    "vertices": created_vertices,
                    "edges": created_edges,
                    "stats": {
                        "vertices_created": created_vertices.len(),
                        "relationships_created": created_edges.len(),
                    }
                }))
            }
            // New match arm in the query execution logic (e.g., in execute_cypher_query)
            CypherQuery::CreateStatement { patterns, return_items } => {
                println!("===> EXECUTING CreateStatement: {} patterns, returning: {:?}", patterns.len(), return_items);

                let mut created_vertices = Vec::new();
                let mut created_edges = Vec::new();
                let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();
                
                // --- Phase 1: Create all vertices and populate var_to_id map ---
                
                // Gather all unique NodePatterns from all patterns
                let mut unique_nodes = HashMap::new();
                for (_, nodes, _) in patterns.iter() {
                    for node_pattern in nodes {
                        if let Some(var) = node_pattern.0.as_ref() {
                            // Use the variable as the key to ensure we only create each node once
                            unique_nodes.entry(var.clone()).or_insert(node_pattern);
                        }
                    }
                }
                
                for (var, node_pattern) in unique_nodes {
                    let (_var_opt, labels_vec, properties) = node_pattern;
                    
                    let props: GraphResult<HashMap<String, PropertyValue>> = properties
                        .iter()
                        .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                        .collect();
                    
                    let new_id = SerializableUuid(Uuid::new_v4());
                    
                    // FIX: labels_vec is Vec<String>. We take the first one or default to "Node".
                    let final_label = labels_vec.first()
                        .cloned()
                        .unwrap_or_else(|| "Node".to_string());
                    
                    let vertex = Vertex {
                        id: new_id,
                        label: Identifier::new(final_label)?,
                        properties: props?,
                        created_at: Utc::now().into(),  
                        updated_at: Utc::now().into(),  
                    };
                    
                    // Rely on graph_service for persistence
                    graph_service.create_vertex(vertex.clone()).await?;
                    
                    var_to_id.insert(var.clone(), new_id);
                    created_vertices.push(vertex);
                }
                
                // --- Phase 2: Create all edges from all patterns ---

                for (_, nodes, relationships) in patterns.into_iter() {
                    if relationships.is_empty() {
                        continue;
                    }

                    // Iterate over (from_node, rel, to_node) segments
                    for i in 0..relationships.len() {
                        let rel_tuple = &relationships[i]; 
                        
                        // Nodes match rel[i] with node[i] -> node[i+1]
                        let from_var_opt = nodes[i].0.as_ref();
                        let to_var_opt = nodes[i + 1].0.as_ref();

                        let from_var = from_var_opt.ok_or_else(|| GraphError::ValidationError(format!("Relationship source node at index {} has no variable.", i)))?;
                        let to_var = to_var_opt.ok_or_else(|| GraphError::ValidationError(format!("Relationship target node at index {} has no variable.", i + 1)))?;

                        // Resolve IDs using the map of newly created nodes
                        let from_id = *var_to_id.get(from_var).ok_or_else(|| GraphError::ValidationError(format!("Unbound source variable: {}", from_var)))?;
                        let to_id = *var_to_id.get(to_var).ok_or_else(|| GraphError::ValidationError(format!("Unbound target variable: {}", to_var)))?;

                        let (
                            _rel_var,     
                            label_opt,      // Index 1: Relationship Label (Option<String>)
                            _len_range,     
                            properties,     // Index 3: Properties Map
                            direction_opt,  // Index 4: Direction (Option<bool>)
                        ) = rel_tuple;

                        // Handle direction: Some(false) is inbound.
                        let (outbound_id, inbound_id) = match direction_opt {
                            Some(false) => (to_id, from_id), // (from_id)<-[R]-(to_id)
                            _ => (from_id, to_id),           // (from_id)-[R]->(to_id)
                        };

                        let edge_type_str = label_opt.clone().unwrap_or_else(|| "RELATED".to_string());
                        
                        let props: GraphResult<BTreeMap<String, PropertyValue>> = properties.iter()
                            .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                            .collect();
                        
                        let edge = Edge {
                            id: SerializableUuid(Uuid::new_v4()),
                            outbound_id,
                            inbound_id,
                            edge_type: Identifier::new(edge_type_str.clone())?,
                            label: edge_type_str,
                            properties: props?,
                        };
                        
                        graph_service.create_edge(edge.clone()).await?;
                        created_edges.push(edge);
                    }
                }
                
                // --- Phase 3: Shape the result based on return_items ---
                let all_return_variables: HashSet<String> = return_items.into_iter().collect();
                let created_vertex_map: HashMap<SerializableUuid, &Vertex> = created_vertices.iter().map(|v| (v.id, v)).collect();

                let returned_data: HashMap<String, Value> = var_to_id.iter()
                    .filter(|(var, _)| all_return_variables.contains(*var))
                    .filter_map(|(var, id)| {
                        created_vertex_map.get(id)
                            .and_then(|v| serde_json::to_value(v).ok())
                            .map(|v_json| (var.clone(), v_json))
                    })
                    .collect();

                Ok(serde_json::to_value(returned_data)?)
            }
            CypherQuery::CreateEdge { from_id, edge_type, to_id } => {
                let edge = Edge {
                    id: SerializableUuid(Uuid::new_v4()),
                    outbound_id: from_id,
                    edge_type: Identifier::new(edge_type.clone())?,
                    inbound_id: to_id,
                    label: edge_type,
                    properties: BTreeMap::new(),
                };
                
                // Use the centralized graph_service.create_edge, which handles
                // 1. storage.create_edge
                // 2. graph_service.add_edge (in-memory update & notify)
                graph_service.create_edge(edge.clone()).await?;
                
                Ok(json!({ "edge": edge }))
            }
                            
            CypherQuery::SetNode { id, properties } => {
                // 1. Get the current state of the vertex (using graph_service methods)
                let mut vertex = graph_service.get_vertex_from_storage(&id.0).await?.ok_or_else(|| {
                    // Must read from storage here if the command is designed to update existing only
                    // based on persistent state, otherwise read from memory (graph_service.get_vertex).
                    // Assuming it must exist in storage to be updated.
                    GraphError::StorageError(format!("Vertex not found: {:?}", id))
                })?;
                
                // 2. Apply property changes
                let props: GraphResult<HashMap<String, PropertyValue>> = properties
                    .into_iter()
                    .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                    .collect();
                vertex.properties.extend(props?);
                vertex.updated_at = Utc::now().into(); // Assuming Utc::now() is in scope
                
                // 3. Use the centralized graph_service.update_vertex, which handles
                // 1. storage.update_vertex
                // 2. graph_service.add_vertex (in-memory update & notify)
                graph_service.update_vertex(vertex.clone()).await?;
                
                Ok(json!({ "vertex": vertex }))
            }
                    
            CypherQuery::DeleteNode { id } => {
                // Use the centralized graph_service.delete_vertex_by_uuid, which handles:
                // 1. storage.delete_vertex
                // 2. graph_service.delete_vertex_from_memory (in-memory update & notify)
                graph_service.delete_vertex_by_uuid(id.0).await?;
                
                // NOTE: graph_service.delete_vertex_from_memory handles edge cleanup in memory.
                // We assume the cleanup trigger handles flushing/async tasks.
                trigger_async_graph_cleanup(); 
                
                Ok(json!({ "deleted": id }))
            }
            
            CypherQuery::SetKeyValue { key, value } => {
                let kv_key = key.clone().into_bytes();
                let kv_value = value.as_bytes().to_vec();

                // Use graph_service for key-value persistence
                graph_service.kv_insert(kv_key, kv_value).await?;
                graph_service.flush_storage().await?;
                
                Ok(json!({ "key": key, "value": value }))
            }
            
            CypherQuery::GetKeyValue { key } => {
                let kv_key = key.clone().into_bytes();
                
                // Use graph_service for key-value retrieval
                let value = graph_service.kv_retrieve(&kv_key).await?;
                
                Ok(json!({ "key": key, "value": value.map(|v| String::from_utf8_lossy(&v).to_string()) }))
            }
            
            CypherQuery::DeleteKeyValue { key } => {
                let kv_key = key.clone().into_bytes();
                
                // Use graph_service for key-value operations
                let value = graph_service.kv_retrieve(&kv_key).await?;
                let existed = value.is_some();
                
                if existed {
                    graph_service.kv_delete(&kv_key).await?;
                    graph_service.flush_storage().await?;
                }
                
                Ok(json!({ "key": key, "deleted": existed }))
            }

            CypherQuery::CreateIndex { label, properties } => {
                warn!("CREATE INDEX command is not implemented yet. Index for label '{:?}' on properties {:?} ignored.", label, &properties);
                Ok(json!({ "status": "success", "message": "Index creation not implemented", "label": label, "properties": properties }))
            }

            CypherQuery::CreateEdgeBetweenExisting { source_var, rel_type, properties, target_var } => {
                let properties_clone = properties.clone();
                
                // 1. Parse IDs and properties, same as before
                let from_id = Uuid::parse_str(&source_var)
                    .map_err(|_| GraphError::ValidationError(format!("Invalid source vertex ID: {}", source_var)))?;
                let to_id = Uuid::parse_str(&target_var)
                    .map_err(|_| GraphError::ValidationError(format!("Invalid target vertex ID: {}", target_var)))?;
                let props: GraphResult<BTreeMap<String, PropertyValue>> = properties
                    .into_iter()
                    .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                    .collect();
                let label = properties_clone
                    .get("label")
                    .and_then(|v| v.as_str().map(|s| s.to_string()))
                    .unwrap_or_else(|| rel_type.clone());
                
                // 2. Construct the Edge object
                let edge = Edge {
                    id: SerializableUuid(Uuid::new_v4()),
                    outbound_id: SerializableUuid(from_id),
                    inbound_id: SerializableUuid(to_id),
                    edge_type: Identifier::new(rel_type)?,
                    label,
                    properties: props?,
                };
                
                // 3. Centralize creation via GraphService
                // This single call replaces:
                // - storage.create_edge(edge.clone()).await?;
                // - graph_service.add_edge(edge.clone()).await?;
                graph_service.create_edge(edge.clone()).await
                    .map_err(|e| GraphError::InternalError(format!("Failed to create edge via GraphService: {}", e)))?;
                
                Ok(json!({ "edge": edge }))
            }

            // ----------------------------------------------------------------------------------------------------------------------
            CypherQuery::DetachDeleteNodes { node_variable: _, label } => {
                // 1. Fetch all vertices matching the filter.
                // FIX: Use GraphService to get vertices from the in-memory graph view.
                let all_vertices = graph_service.get_all_vertices().await?;
                
                let nodes_to_delete: Vec<Vertex> = all_vertices
                    .into_iter()
                    .filter(|v| {
                        // Filter by label constraint if present
                        label.as_ref().map_or(true, |l| v.label.as_ref() == l.as_str())
                    })
                    .collect();
                
                if nodes_to_delete.is_empty() {
                    return Ok(json!({"deleted_vertices": 0, "deleted_edges": 0}));
                }
                
                let vertex_ids: Vec<Uuid> = nodes_to_delete.iter().map(|v| v.id.0).collect();
                
                // 2. Use the atomic GraphService method for DETACH DELETE.
                let deleted_edges = graph_service.detach_delete_vertices(&vertex_ids).await?;
                
                // Note: The count of deleted vertices is the length of `nodes_to_delete`.
                let deleted_vertices = nodes_to_delete.len();

                // 3. Flush storage
                graph_service.flush_storage().await?;
                trigger_async_graph_cleanup();
                
                Ok(json!({
                    "deleted_vertices": deleted_vertices,
                    "deleted_edges": deleted_edges
                }))
            }
            // ----------------------------------------------------------------------------------------------------------------------
            CypherQuery::DeleteEdges {
                edge_variable,
                pattern,
                where_clause,
                with_clause,
            } => {
                // --- STEP 1: MATCH (READ) ---
                
                let is_variable_length = pattern.relationships.len() == 1
                    && pattern.nodes.len() == 2
                    && pattern.relationships[0].2.map_or(false, |(min, max)| {
                        min.map_or(1, |m| m) != 1 || max.map_or(1, |m| m) != 1
                    });

                let edges_to_delete: Vec<Edge> = if is_variable_length {
                    let graph = graph_service.get_graph().await;
                    let rel_pat = &pattern.relationships[0];
                    let start_node_pat = &pattern.nodes[0];
                    let end_node_pat = &pattern.nodes[1];
                    
                    // FIX: node_matches_constraints expects &Option<String>, but we have &Vec<String>
                    // We take the first label as the primary label for matching.
                    let start_label = start_node_pat.1.first().cloned();
                    
                    let start_vertices: Vec<&Vertex> = graph.vertices.values()
                        .filter(|v| node_matches_constraints(v, &start_label, &start_node_pat.2))
                        .collect();
                    
                    let mut all_matched_edge_ids: HashSet<Uuid> = HashSet::new();
                    
                    for start_v in start_vertices {
                        let (_, matched_e_ids) = graph.match_variable_length_path(
                            start_v.id.0,
                            rel_pat,
                            end_node_pat
                        );
                        all_matched_edge_ids.extend(matched_e_ids);
                    }
                    
                    graph.edges.values()
                        .filter(|e| all_matched_edge_ids.contains(&e.id.0))
                        .cloned()
                        .collect()
                } else {
                    // Transform pattern.nodes from Vec<(Option<String>, Vec<String>, HashMap)>
                    // to Vec<(Option<String>, Option<String>, HashMap)> to satisfy exec_cypher_pattern
                    let compatible_nodes: Vec<(Option<String>, Option<String>, HashMap<String, Value>)> = pattern.nodes
                        .iter()
                        .map(|(var, labels, props)| {
                            (var.clone(), labels.first().cloned(), props.clone())
                        })
                        .collect();

                    let (_, edges) = exec_cypher_pattern(
                        vec![(None, compatible_nodes, pattern.relationships.clone())],
                        &(*graph_service)
                    ).await?;
                    
                    edges
                };
                
                // --- STEP 2: FILTER WITH WHERE + WITH LOGIC (MPI AUDIT) ---
                let mut deleted = 0usize;
                
                for edge in edges_to_delete {
                    let mut variables = HashMap::new();
                    variables.insert(edge_variable.clone(), CypherValue::Edge(edge.clone()));
                    
                    let ctx = EvaluationContext {
                        variables,
                        parameters: HashMap::new(),
                    };
                    
                    // Evaluate WHERE clause (initial filter)
                    let where_passed = match where_clause.as_ref() {
                        Some(wc) => match wc.condition.evaluate(&ctx) {
                            Ok(val) => matches!(val, CypherValue::Bool(true)),
                            Err(_) => false,
                        },
                        None => true,
                    };
                    
                    // Evaluate WITH clause logic (critical for MPI identity resolution traceability)
                    let with_passed = match with_clause.as_ref() {
                        Some(parsed_with) => {
                            // Check if the ParsedWithClause has an internal where_clause
                            match &parsed_with.where_clause {
                                Some(inner_wc) => match inner_wc.condition.evaluate(&ctx) {
                                    Ok(val) => matches!(val, CypherValue::Bool(true)),
                                    Err(_) => false,
                                },
                                None => true, // If no filter in WITH, it passes
                            }
                        }
                        None => true,
                    };
                    
                    // Both must pass to authorize the deletion in the MPI event graph
                    if where_passed && with_passed {
                        graph_service.delete_edge_by_uuid(edge.id.0).await?;
                        deleted += 1;
                    }
                }
                
                graph_service.flush_storage().await?;
                trigger_async_graph_cleanup();
                
                Ok(json!({
                    "status": "success",
                    "deleted_edges": deleted,
                    "message": format!("Successfully deleted {deleted} edge(s) following MPI logical verification")
                }))
            }
            // ============================================================================
            // UNION - Return flat or check if already wrapped
            // ============================================================================
            CypherQuery::Union(left_query, is_all, right_query) => {
                let union_qr_result = graph_service.union_results(
                    left_query.clone(),
                    is_all,
                    right_query.clone(),
                ).await;
                
                let raw_value = graph_service.query_result_to_value(union_qr_result)?;
                
                // If raw_value is already a flat graph structure, return as-is
                if raw_value.is_object() 
                    && raw_value.get("vertices").is_some() 
                    && raw_value.get("edges").is_some() {
                    Ok(raw_value)
                }
                // If wrapped in {"results": [...]}, extract the first result
                else if let Some(results_array) = raw_value.get("results").and_then(|r| r.as_array()) {
                    if let Some(first_result) = results_array.first() {
                        // If first_result is flat graph structure, return it
                        if first_result.is_object() 
                            && first_result.get("vertices").is_some() 
                            && first_result.get("edges").is_some() {
                            Ok(first_result.clone())
                        } else {
                            // Unexpected format - return empty flat structure
                            Ok(json!({
                                "vertices": [],
                                "edges": [],
                                "stats": { "vertices_matched": 0, "edges_matched": 0 }
                            }))
                        }
                    } else {
                        // Empty results - return empty flat structure
                        Ok(json!({
                            "vertices": [],
                            "edges": [],
                            "stats": { "vertices_matched": 0, "edges_matched": 0 }
                        }))
                    }
                }
                // Unexpected format - return empty flat structure
                else {
                    Ok(json!({
                        "vertices": [],
                        "edges": [],
                        "stats": { "vertices_matched": 0, "edges_matched": 0 }
                    }))
                }
            }
            CypherQuery::ReturnStatement { 
                projection_string, 
                order_by, 
                skip, 
                limit, 
                is_distinct 
            } => {
                info!("===> EXECUTING External RETURN Statement");
                
                let order_by_str = graph_service.serialize_order_by_to_string(&order_by);
                let return_qr_result = graph_service.execute_return_statement(
                    projection_string.clone(),
                    order_by_str,
                    skip,
                    limit,
                ).await;
                
                let raw_value = graph_service.query_result_to_value(return_qr_result)?;
                
                // Normalize result structure
                let mut final_wrapped_result = if raw_value.is_object() 
                    && raw_value.get("vertices").is_some() 
                    && raw_value.get("edges").is_some() {
                    raw_value
                } else if let Some(results_array) = raw_value.get("results").and_then(|r| r.as_array()) {
                    if let Some(first_result) = results_array.first() {
                        if first_result.is_object() 
                            && first_result.get("vertices").is_some() 
                            && first_result.get("edges").is_some() {
                            first_result.clone()
                        } else {
                            json!({
                                "vertices": results_array.clone(),
                                "edges": [],
                                "stats": { "vertices_matched": results_array.len(), "edges_matched": 0 }
                            })
                        }
                    } else {
                        json!({ "vertices": [], "edges": [], "stats": { "vertices_matched": 0, "edges_matched": 0 } })
                    }
                } else {
                    json!({ "vertices": [], "edges": [], "stats": { "vertices_matched": 0, "edges_matched": 0 } })
                };
                
                // --- FIX: Initialize new_len here so it's available in the outer scope ---
                let mut current_count = final_wrapped_result
                    .get("vertices")
                    .and_then(|v| v.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0);

                // 1. Property extraction and Nonsense Access Prevention
                if let Some(vertices) = final_wrapped_result.get_mut("vertices").and_then(|v| v.as_array_mut()) {
                    
                    let projection_items: Vec<&str> = projection_string
                        .trim_start_matches("RETURN ")
                        .split(',')
                        .map(|s| s.trim())
                        .collect();
                    
                    let projection_items_clone = projection_items.clone();

                    for row in vertices.iter_mut() {
                        if let Some(obj) = row.as_object_mut() {
                            // STEP 1: Property Extraction
                            for item in &projection_items {
                                if item.to_lowercase().starts_with("type(") { continue; }
                                if let Some(dot_pos) = item.find('.') {
                                    let var_name = item[..dot_pos].trim();
                                    let prop_name = item[dot_pos + 1..].trim();
                                    let full_key = item.to_string();
                                    
                                    let needs_extraction = obj.get(&full_key).map_or(true, |v| v.is_null());
                                    if needs_extraction {
                                        if let Some(entity_val) = obj.get(var_name).cloned() {
                                            if let Some(entity_obj) = entity_val.as_object() {
                                                if let Some(props) = entity_obj.get("properties").and_then(|p| p.as_object()) {
                                                    if let Some(prop_val) = props.get(prop_name) {
                                                        obj.insert(full_key.clone(), prop_val.clone());
                                                    }
                                                } else if let Some(prop_val) = entity_obj.get(prop_name) {
                                                    obj.insert(full_key.clone(), prop_val.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            
                            // STEP 2: Nonsense Cleanup
                            let current_keys: Vec<String> = obj.keys().cloned().collect();
                            for key in current_keys {
                                if let Some(val) = obj.get(&key) {
                                    if val.is_object() && val.get("id").is_some() && val.get("label").is_some() {
                                        let is_requested = projection_items_clone.iter().any(|item| {
                                            let trimmed = item.trim();
                                            trimmed == key || trimmed.starts_with(&format!("{} AS", key))
                                        });
                                        if !is_requested {
                                            obj.remove(&key);
                                        }
                                    }
                                }
                            }

                            // STEP 3: Final Alias Application
                            apply_projection_aliases(&projection_string, obj);
                        }
                    }

                    // 2. Handle DISTINCT Deduplication
                    if is_distinct {
                        let mut unique_set = std::collections::HashSet::new();
                        vertices.retain(|row| {
                            let serialized = serde_json::to_string(row).unwrap_or_default();
                            unique_set.insert(serialized)
                        });
                    }

                    // Update the count for the outer scope
                    current_count = vertices.len();
                } 

                // 3. Update stats (Now safe because the borrow of vertices has expired)
                if let Some(stats) = final_wrapped_result.get_mut("stats").and_then(|s| s.as_object_mut()) {
                    stats.insert("vertices_matched".to_string(), serde_json::json!(current_count));
                }
                
                Ok(final_wrapped_result)
            }
            // NEW: Handles the standalone SET clause for chaining (e.g., `MATCH (n) SET n.prop = 'new'`)
            CypherQuery::SetStatement { assignments } => {
                println!("===> EXECUTING Standalone SetStatement");
                
                // Pass the assignments (Vec<(String, String, Expression)>) to the service
                graph_service.apply_set_assignments(assignments).await?;
                
                Ok(json!({
                    "status": "success",
                    "message": "Properties set successfully."
                }))
            }
            // NEW: Handles the standalone DELETE clause (e.g., `MATCH (n) DELETE n`)
            CypherQuery::DeleteStatement { variables, detach } => {
                // variables is Vec<String>: list of variables to delete
                
                // The service is responsible for finding nodes/relationships bound to these 
                // variables in the current transaction context and performing the deletion.
                
                graph_service.delete_variables(variables, detach).await?;
                Ok(json!({"message": "Variables deleted successfully."}))
            },

            // NEW: Handles the standalone REMOVE clause (e.g., `MATCH (n) REMOVE n:Label`)
            CypherQuery::RemoveStatement { removals } => {
                // Removals is Vec<(String, String)>: (variable, label_or_property)
                
                // The service iterates through the removals and applies them to the context 
                // bound variables (removing a label, or setting a property to null).
                graph_service.remove_labels_or_properties(removals).await?;
                
                Ok(json!({"message": "Labels/properties removed successfully."}))
            },
            CypherQuery::MatchPath { path_var: _, left_node, right_node, return_clause: _ } => {
                let left_pat = parse_node_pattern(&left_node)?;
                let right_pat = parse_node_pattern(&right_node)?;

                // Reworked to use GraphService's read guard for in-memory access (preferred for performance)
                let graph_read_guard = graph_service.read().await;

                // Use references to the in-memory graph's internal collections
                // The keys here are SerializableUuid
                let all_vertices_ref = &graph_read_guard.vertices;
                let all_edges_ref = &graph_read_guard.edges;

                // These HashSets correctly contain SerializableUuid
                let mut left_ids = HashSet::new();
                let mut right_ids = HashSet::new();

                // The matching function remains the same, operating on a Vertex reference.
                let matches = |v: &Vertex, (label, props): &(Option<String>, HashMap<String, Value>)| {
                    let label_ok = label.as_ref().map_or(true, |l| {
                        let vl = v.label.as_ref();
                        vl == l || vl.starts_with(&format!("{}:", l))
                    });
                    let props_ok = props.iter().all(|(k, expected)| {
                        v.properties.get(k).map_or(false, |actual| {
                            // Assuming to_property_value is available in scope
                            to_property_value(expected.clone()).ok().map_or(false, |pv| actual == &pv)
                        })
                    });
                    label_ok && props_ok
                };

                // 1. Identify starting and ending vertices from the in-memory collection
                for v in all_vertices_ref.values() {
                    if matches(v, &left_pat) { left_ids.insert(v.id); }
                    if matches(v, &right_pat) { right_ids.insert(v.id); }
                }

                let mut matched_edge_ids = HashSet::new();
                // matched_vertex_ids correctly contains SerializableUuid
                let mut matched_vertex_ids = left_ids.union(&right_ids).copied().collect::<HashSet<_>>();

                // 2. Perform the Breadth-First Search (BFS) for paths (up to 2 hops, as per original logic)
                // FIX 1: The queue must store the underlying Uuid, not SerializableUuid, 
                // or the queue must be Vec<(SerializableUuid, u32)>. 
                // Since the matched_vertex_ids uses SerializableUuid, we'll keep the queue consistent.
                let mut queue: Vec<(SerializableUuid, u32)> = matched_vertex_ids.iter().map(|&id| (id, 0)).collect();
                let mut visited_edges = HashSet::new();

                while let Some((current_id, hop)) = queue.pop() {
                    if hop >= 2 { continue; }

                    // Iterate over all edges in the graph
                    for edge in all_edges_ref.values() {
                        if visited_edges.contains(&edge.id.0) { continue; }

                        // These are Uuid
                        let (from_id_uuid, to_id_uuid) = (edge.outbound_id.0, edge.inbound_id.0);
                        
                        // This is SerializableUuid
                        let current_id_uuid = current_id.0; 

                        let next_id_uuid = if from_id_uuid == current_id_uuid {
                            Some(to_id_uuid)
                        } else if to_id_uuid == current_id_uuid {
                            Some(from_id_uuid)
                        } else {
                            None
                        };

                        if let Some(next_uuid) = next_id_uuid {
                            // FIX 2: Convert the raw Uuid back to SerializableUuid before insertion
                            let next_id = SerializableUuid(next_uuid);
                            
                            visited_edges.insert(edge.id.0);
                            matched_edge_ids.insert(edge.id.0);
                            matched_vertex_ids.insert(next_id); // Insert the SerializableUuid
                            
                            if hop < 1 { 
                                queue.push((next_id, hop + 1)); // Push the SerializableUuid
                            }
                        }
                    }
                }

                let vertices: Vec<Vertex> = matched_vertex_ids.into_iter()
                    // FIX: Extract the inner Uuid from the SerializableUuid before lookup.
                    // Assuming SerializableUuid is a newtype wrapper like struct SerializableUuid(pub Uuid);
                    .filter_map(|id| all_vertices_ref.get(&id.0).cloned()) 
                    .collect();

                let edges: Vec<Edge> = matched_edge_ids.into_iter()
                    .filter_map(|id| all_edges_ref.get(&id).cloned())
                    .collect();

                // Read guard dropped here

                Ok(json!({ "vertices": vertices, "edges": edges }))
            }
        }
    })
}

/// Helper to compare CypherValues with an operator
fn compare_values(left: &CypherValue, op: &str, right: &CypherValue) -> bool {
    match (left, op, right) {
        // --- Regex Comparison ---
        (l, "=~", r) => {
            let val_str = format_val(l);
            let pattern = format_val(r);
            // Compile regex and check for match
            match Regex::new(&pattern) {
                Ok(re) => re.is_match(&val_str),
                Err(e) => {
                    println!("===> Invalid Regex Pattern '{}': {}", pattern, e);
                    false
                }
            }
        },

        // --- String Search Operators ---
        (l, "CONTAINS", r) => {
            let ls = format_val(l);
            let rs = format_val(r);
            ls.contains(&rs)
        },
        (l, "STARTS WITH", r) => {
            format_val(l).starts_with(&format_val(r))
        },
        (l, "ENDS WITH", r) => {
            format_val(l).ends_with(&format_val(r))
        },

        // --- Integer vs Integer ---
        (CypherValue::Integer(a), "<", CypherValue::Integer(b)) => a < b,
        (CypherValue::Integer(a), "<=", CypherValue::Integer(b)) => a <= b,
        (CypherValue::Integer(a), ">", CypherValue::Integer(b)) => a > b,
        (CypherValue::Integer(a), ">=", CypherValue::Integer(b)) => a >= b,
        (CypherValue::Integer(a), "=", CypherValue::Integer(b)) => a == b,
        (CypherValue::Integer(a), "!=", CypherValue::Integer(b)) => a != b,

        // --- Float vs Float ---
        (CypherValue::Float(a), "<", CypherValue::Float(b)) => a < b,
        (CypherValue::Float(a), "<=", CypherValue::Float(b)) => a <= b,
        (CypherValue::Float(a), ">", CypherValue::Float(b)) => a > b,
        (CypherValue::Float(a), ">=", CypherValue::Float(b)) => a >= b,
        (CypherValue::Float(a), "=", CypherValue::Float(b)) => (a - b).abs() < f64::EPSILON,
        (CypherValue::Float(a), "!=", CypherValue::Float(b)) => (a - b).abs() >= f64::EPSILON,

        // --- Mixed Numeric: Integer vs Float ---
        (CypherValue::Integer(a), "<", CypherValue::Float(b)) => (*a as f64) < *b,
        (CypherValue::Integer(a), "<=", CypherValue::Float(b)) => (*a as f64) <= *b,
        (CypherValue::Integer(a), ">", CypherValue::Float(b)) => (*a as f64) > *b,
        (CypherValue::Integer(a), ">=", CypherValue::Float(b)) => (*a as f64) >= *b,
        (CypherValue::Integer(a), "=", CypherValue::Float(b)) => (*a as f64 - *b).abs() < f64::EPSILON,
        (CypherValue::Integer(a), "!=", CypherValue::Float(b)) => (*a as f64 - *b).abs() >= f64::EPSILON,

        // --- Mixed Numeric: Float vs Integer ---
        (CypherValue::Float(a), "<", CypherValue::Integer(b)) => *a < (*b as f64),
        (CypherValue::Float(a), "<=", CypherValue::Integer(b)) => *a <= (*b as f64),
        (CypherValue::Float(a), ">", CypherValue::Integer(b)) => *a > (*b as f64),
        (CypherValue::Float(a), ">=", CypherValue::Integer(b)) => *a >= (*b as f64),
        (CypherValue::Float(a), "=", CypherValue::Integer(b)) => (*a - *b as f64).abs() < f64::EPSILON,
        (CypherValue::Float(a), "!=", CypherValue::Integer(b)) => (*a - *b as f64).abs() >= f64::EPSILON,

        // --- Equality/Inequality Catch-all ---
        (l, "=", r) => format_val(l) == format_val(r),
        (l, "!=", r) => format_val(l) != format_val(r),

        // --- Failure Log ---
        _ => {
            println!("===> Unsupported comparison: {:?} {} {:?}", left, op, right);
            false
        }
    }
}

/// Helper to extract string representation for coercion
fn format_val(val: &CypherValue) -> String {
    match val {
        CypherValue::String(s) => s.clone(),
        CypherValue::Uuid(u) => u.to_string(),
        CypherValue::Integer(i) => i.to_string(),
        CypherValue::Float(f) => f.to_string(),
        CypherValue::Bool(b) => b.to_string(),
        CypherValue::Null => "".to_string(),
        _ => format!("{:?}", val), 
    }
}

pub fn parse_order_by_items(input: &str) -> IResult<&str, Vec<OrderByItem>> {
    separated_list1(
        delimited(multispace0, char(','), multispace0),
        |input| {
            let (input, _) = multispace0.parse(input)?;
            
            let start = input;
            
            // Parse expression and track consumed input
            let (input, _) = alt((
                parse_function_call_full,
                map(
                    tuple((parse_identifier, char('.'), parse_identifier)),
                    |_| Expression::Literal(CypherValue::Null)
                ),
                map(parse_identifier, |id| Expression::Variable(id.to_string())),
            )).parse(input)?;
            
            let consumed = start.len() - input.len();
            let expr_str = &start[..consumed];
            
            let (input, _) = multispace0.parse(input)?;
            
            // Parse ASC/DESC - make it optional since whitespace might not be present
            let (input, direction) = opt(alt((
                map(tag_no_case("ASC"), |_| true),
                map(tag_no_case("DESC"), |_| false),
            ))).parse(input)?;
            
            Ok((input, OrderByItem {
                expression: expr_str.trim().to_string(),
                ascending: direction.unwrap_or(true),
            }))
        }
    ).parse(input)
}

// Required to parse the ON CREATE SET / ON MATCH SET sub-clauses in MERGE.
// Returns a parser function defined by the given keyword ("ON CREATE" or "ON MATCH").
// --- 1. Fix: parse_on_set_clause (Cypher MERGE Clause) ---
// FIX 1: Change return type from Value to PropertyValue
fn parse_on_set_clause(on_type: &str) -> impl Fn(&str) -> IResult<&str, Vec<(String, String, Value)>> {
    move |input| {
        map(
            preceded(
                pair(tag_no_case(on_type), terminated(tag_no_case("SET"), multispace1)),
                // parse_set_assignment_tuple returns (String, String, Value)
                separated_list1(preceded(multispace0::<&str, NomErrorType<&str>>, char(',')), parse_set_assignment_tuple),
            ),
            |assignments| assignments,
        ).parse(input)
    }
}

/// Helper to get type name for debugging
fn type_name_of_val(v: &Value) -> &str {
    match v {
        Value::String(_) => "String",
        Value::Number(_) => "Number",
        Value::Bool(_) => "Bool",
        Value::Null => "Null",
        Value::Array(_) => "Array",
        Value::Object(_) => "Object",
    }
}
/// Helper to convert Cypher `Value` → `PropertyValue`
pub fn to_property_value(v: Value) -> GraphResult<PropertyValue> {
    // Check for special function calls first
    if let Value::Object(ref m) = v {
        if let Some(func) = m.get("__CYPHER_FUNC__") {
            if func.as_str() == Some("timestamp") {
                return Ok(PropertyValue::Integer(Utc::now().timestamp_millis()));
            }
        }
    }
    
    match v {
        Value::String(s) => Ok(PropertyValue::String(s)),
        Value::Number(n) if n.is_i64() => Ok(PropertyValue::Integer(n.as_i64().unwrap())),
        Value::Number(n) if n.is_f64() => Ok(PropertyValue::Float(SerializableFloat(n.as_f64().unwrap()))),
        Value::Bool(b) => Ok(PropertyValue::Boolean(b)),
        Value::Null => Err(GraphError::InternalError("Null values not supported in properties".into())),
        Value::Array(_) => Err(GraphError::InternalError("Array values not supported in properties".into())),
        Value::Object(_) => Err(GraphError::InternalError("Nested objects not supported in properties".into())),
        _ => Err(GraphError::InternalError("Unsupported property value type".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_is_cypher() {
        assert!(is_cypher("MATCH (n:Person) RETURN n"));
        assert!(is_cypher("CREATE (n:Person {name: 'Alice'})"));
        assert!(!is_cypher("SELECT * FROM table"));
    }

    #[test]
    fn test_parse_create_node() {
        let query = "CREATE (n:Person {name: 'Alice', age: 30})";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::CreateNode {
            label: "Person".to_string(),
            properties: HashMap::from([
                ("name".to_string(), json!("Alice")),
                ("age".to_string(), json!(30)),
            ]),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_create_node_without_variable() {
        let query = "CREATE (:Person {name: 'Alice', age: 30})";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::CreateNode {
            label: "Person".to_string(), // Should use the label
            properties: HashMap::from([
                ("name".to_string(), json!("Alice")),
                ("age".to_string(), json!(30)),
            ]),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_create_node_with_float() {
        let query = "CREATE (:Person {id: \"alice\", name: \"Alice\", age: 30, active: true, score: 95.5})";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::CreateNode {
            label: "Person".to_string(),
            properties: HashMap::from([
                ("id".to_string(), json!("alice")),
                ("name".to_string(), json!("Alice")),
                ("age".to_string(), json!(30)),
                ("active".to_string(), json!(true)),
                ("score".to_string(), json!(95.5)),
            ]),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_create_nodes_with_ampersand_labels() {
        let query = "CREATE (charlie:Person&Actor {name: 'Charlie Sheen'}), (oliver:Person&Director {name: 'Oliver Stone'})";
        let result = parse_cypher(query).unwrap();
        match result {
            CypherQuery::CreateNodes { nodes } => {
                assert_eq!(nodes.len(), 2);
                assert_eq!(nodes[0].0, "Person:Actor");
                assert_eq!(nodes[1].0, "Person:Director");
            }
            _ => panic!("Expected CreateNodes variant"),
        }
    }

    #[test]
    fn test_parse_create_nodes_with_colon_labels() {
        let query = "CREATE (charlie:Person:Actor {name: 'Charlie Sheen'}), (oliver:Person:Director {name: 'Oliver Stone'})";
        let result = parse_cypher(query).unwrap();
        match result {
            CypherQuery::CreateNodes { nodes } => {
                assert_eq!(nodes.len(), 2);
                assert_eq!(nodes[0].0, "Person:Actor");
                assert_eq!(nodes[1].0, "Person:Director");
            }
            _ => panic!("Expected CreateNodes variant"),
        }
    }

    #[test]
    fn test_parse_match_simple_return() {
        let query = "MATCH (n:Person) RETURN n";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::MatchNode {
            label: Some("Person".to_string()),
            properties: HashMap::new(),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_match_multiple_nodes() {
        let query = "MATCH (charlie:Person {name: 'Charlie Sheen'}), (oliver:Person {name: 'Oliver Stone'})";
        let result = parse_cypher(query).unwrap();
        match result {
            CypherQuery::MatchMultipleNodes { nodes } => {
                assert_eq!(nodes.len(), 2);
                assert_eq!(nodes[0].0, Some("charlie".to_string()));
                assert_eq!(nodes[0].1, Some("Person".to_string()));
                assert_eq!(nodes[1].0, Some("oliver".to_string()));
                assert_eq!(nodes[1].1, Some("Person".to_string()));
            }
            _ => panic!("Expected MatchMultipleNodes variant"),
        }
    }

    #[test]
    fn test_parse_match_complex_return() {
        let query = "MATCH (n) RETURN n.name, labels(n) AS labels";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::MatchNode {
            label: None,
            properties: HashMap::new(),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_match_count_return() {
        let query = "MATCH (n) RETURN count(n) AS total_vertices";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::MatchNode {
            label: None,
            properties: HashMap::new(),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_create_complex_pattern() {
        let query = "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})";
        let result = parse_cypher(query).unwrap();
        match result {
            CypherQuery::CreateComplexPattern { nodes, relationships } => {
                assert_eq!(nodes.len(), 2);
                assert_eq!(relationships.len(), 1);
                assert_eq!(relationships[0].0, "KNOWS");
                assert_eq!(relationships[0].2, false); // outgoing
            }
            _ => panic!("Expected CreateComplexPattern variant"),
        }
    }

    #[test] 
    fn test_parse_create_complex_bidirectional() {
        let query = "CREATE (a)-[:REL1]->(b)<-[:REL2]-(c)";
        let result = parse_cypher(query).unwrap();
        match result {
            CypherQuery::CreateComplexPattern { nodes, relationships } => {
                assert_eq!(nodes.len(), 3);
                assert_eq!(relationships.len(), 2); 
                assert_eq!(relationships[0].0, "REL1");
                assert_eq!(relationships[0].2, false); // outgoing
                assert_eq!(relationships[1].0, "REL2");
                assert_eq!(relationships[1].2, true); // incoming
            }
            _ => panic!("Expected CreateComplexPattern variant"),
        }
    }

    #[test]
    fn test_parse_set_kv() {
        let query = "SET mykey = 'myvalue'";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::SetKeyValue {
            key: "mykey".to_string(),
            value: "myvalue".to_string(),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_match_with_where() {
        let input = r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n"#;
        let result = full_statement_parser(input);
        assert!(result.is_ok());
        let (remainder, _) = result.unwrap();
        assert_eq!(remainder.trim(), "");
    }
    
    #[test]
    fn test_match_set_with_where() {
        let input = r#"MATCH (p:Patient {type: "VIP"}) WHERE ID(p) = "123" SET p.status = "MERGED" RETURN p"#;
        let result = full_statement_parser(input);
        assert!(result.is_ok());
        let (remainder, _) = result.unwrap();
        assert_eq!(remainder.trim(), "");
    }
}

// ==============================================================================
// TESTING: Add comprehensive tests for multiple record handling
// ==============================================================================

#[cfg(test)]
mod multiple_records_tests {
    use super::*;

    #[test]
    fn test_variable_length_parsing() {
        assert_eq!(parse_variable_length("*").unwrap().1, (Some(1), None));
        assert_eq!(parse_variable_length("*2").unwrap().1, (Some(1), Some(2)));
        assert_eq!(parse_variable_length("*0..2").unwrap().1, (Some(0), Some(2)));
        assert_eq!(parse_variable_length("*1..5").unwrap().1, (Some(1), Some(5)));
        assert_eq!(parse_variable_length("*1..").unwrap().1, (Some(1), None));
    }

   #[test]
    fn test_match_all_nodes() {
        let query = "MATCH (n:Person) RETURN n";
        // Using parse_cypher (which calls parse_cypher_statement/full_statement_parser)
        let result = parse_cypher(query).unwrap();
        
        // FIX: Match against all 4 fields: patterns, where_clause, with_clause, return_clause
        match result {
            CypherQuery::MatchPattern { patterns, where_clause, with_clause, return_clause } => {
                assert_eq!(patterns.len(), 1);
                assert!(where_clause.is_none());
                assert!(with_clause.is_none());
                
                // Verify RETURN clause is captured
                let ret = return_clause.expect("Should have a return clause");
                assert_eq!(ret.items.len(), 1);
                assert_eq!(ret.items[0].expression, "n");
            }
            _ => panic!("Should parse as MatchPattern, got {:?}", result),
        }
    }

    #[test]
    fn test_match_multiple_comma_separated() {
        let query = "MATCH (a:Person), (b:Movie) RETURN a, b";
        let result = parse_cypher(query).unwrap();
        
        match result {
            CypherQuery::MatchPattern { patterns, where_clause, with_clause, return_clause } => {
                // Should handle both patterns
                assert_eq!(patterns.len(), 2);
                assert!(where_clause.is_none());
                assert!(with_clause.is_none());

                // Verify RETURN clause captures both 'a' and 'b'
                let ret = return_clause.expect("Should have a return clause");
                assert_eq!(ret.items.len(), 2);
                assert_eq!(ret.items[0].expression, "a");
                assert_eq!(ret.items[1].expression, "b");
            }
            _ => panic!("Should handle multiple patterns, got {:?}", result),
        }
    }

    #[test]
    fn test_parse_uuid_property() {
        let input = "a928227a-165c-4fc4-abd1-796583a26d8d";
        let result = parse_property_value(input).unwrap();
        assert_eq!(result.1, Value::String("a928227a-165c-4fc4-abd1-796583a26d8d".to_string()));
    }
}