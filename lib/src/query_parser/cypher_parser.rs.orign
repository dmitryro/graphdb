// lib/src/query_parser/cypher_parser.rs
// Updated: 2025-11-17 - Added support for multiple nodes in MATCH and complex CREATE patterns

use log::{debug, error, info, warn, trace};
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt},
    multi::{separated_list0, separated_list1, many0},
    number::complete::double,
    sequence::{delimited, preceded, tuple},
    IResult,
    Parser,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;
use models::identifiers::{Identifier, SerializableUuid};
use models::{Vertex, Edge};
use models::errors::{GraphError, GraphResult};
use models::properties::SerializableFloat;
use models::properties::PropertyValue;
use crate::database::Database;
use crate::storage_engine::{GraphStorageEngine, StorageEngine};
use crate::query_exec_engine::query_exec_engine::QueryExecEngine;
use crate::config::StorageConfig;

// Enum to represent parsed Cypher queries
#[derive(Debug, PartialEq)]
pub enum CypherQuery {
    CreateNode {
        label: String,
        properties: HashMap<String, Value>,
    },
    CreateNodes {
        nodes: Vec<(String, HashMap<String, Value>)>, // (label, properties)
    },
    MatchNode {
        label: Option<String>,
        properties: HashMap<String, Value>,
    },
    MatchMultipleNodes {
        nodes: Vec<(Option<String>, Option<String>, HashMap<String, Value>)>, // (var, label, properties)
    },
    CreateEdge {
        from_id: SerializableUuid,
        edge_type: String,
        to_id: SerializableUuid,
    },
    CreateComplexPattern {
        // Pattern like: (a)-[:REL]->(b) or (a)-[:REL1]->(b)<-[:REL2]-(c)
        nodes: Vec<(Option<String>, Option<String>, HashMap<String, Value>)>, // (var, label, props)
        relationships: Vec<(String, HashMap<String, Value>, bool)>, // (type, props, is_incoming)
    },
    SetNode {
        id: SerializableUuid,
        properties: HashMap<String, Value>,
    },
    DeleteNode {
        id: SerializableUuid,
    },
    SetKeyValue {
        key: String,
        value: String,
    },
    GetKeyValue {
        key: String,
    },
    DeleteKeyValue {
        key: String,
    },
    CreateIndex {
        label: String,
        properties: Vec<String>,
    },
}

// Checks if a query is likely a Cypher query
pub fn is_cypher(query: &str) -> bool {
    let cypher_keywords = ["MATCH", "CREATE", "SET", "RETURN", "DELETE"];
    cypher_keywords.iter().any(|kw| query.trim().to_uppercase().starts_with(kw))
}

// Parse a Cypher identifier (e.g., variable name or label) - now includes '&' for multi-label syntax
fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '&').parse(input)
}

// Parse a string literal (e.g., 'Alice' or "Alice")
fn parse_string_literal(input: &str) -> IResult<&str, &str> {
    alt((
        delimited(
            char('\''),
            take_while1(|c: char| c != '\'' && c != '\\'),
            char('\'')
        ),
        delimited(
            char('"'),
            take_while1(|c: char| c != '"' && c != '\\'),
            char('"')
        ),
        // Handle empty strings
        map(tag("''"), |_| ""),
        map(tag("\"\""), |_| ""),
    ))
    .parse(input)
}

// Parse a number literal (integer or float)
fn parse_number_literal(input: &str) -> IResult<&str, Value> {
    map(
        nom::number::complete::double,
        |n| {
            // Check if it's actually an integer to preserve type
            if n.fract() == 0.0 && n >= (i64::MIN as f64) && n <= (i64::MAX as f64) {
                json!(n as i64)
            } else {
                json!(n)
            }
        }
    ).parse(input)
}

// Parse a property value (string or number)
fn parse_property_value(input: &str) -> IResult<&str, Value> {
    alt((
        map(parse_string_literal, |s| json!(s)),
        parse_number_literal,
        map(tag("true"), |_| json!(true)),
        map(tag("false"), |_| json!(false)),
        map(tag("null"), |_| Value::Null),
    ))
    .parse(input)
}

// Parse a single key-value property pair
fn parse_property(input: &str) -> IResult<&str, (String, Value)> {
    let (input, (key, _, value)) = tuple((
        parse_identifier,
        preceded(multispace0, char(':')),
        preceded(multispace0, parse_property_value),
    ))
    .parse(input)?;
    Ok((input, (key.to_string(), value)))
}

// Parse a list of properties enclosed in curly braces
fn parse_properties(input: &str) -> IResult<&str, HashMap<String, Value>> {
    map(
        delimited(
            preceded(multispace0, char('{')),
            opt(separated_list1(
                preceded(multispace0, char(',')),
                preceded(multispace0, parse_property)
            )),
            preceded(multispace0, char('}')),
        ),
        |props| props.unwrap_or_default().into_iter().collect(),
    )
    .parse(input)
}


// Parse a node pattern like `(n:Person {name: 'Alice'})` or `(:Person {name: 'Alice'})` or `(n:Person:Actor {name: 'Bob'})`
// or `(n:Person&Actor {name: 'Charlie'})` - supports both : and & as label separators
fn parse_node(input: &str) -> IResult<&str, (Option<String>, Option<String>, HashMap<String, Value>)> {
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, var) = opt(parse_identifier).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Parse labels (one or more, separated by ':' or '&')
    let (input, labels) = if input.starts_with(':') {
        let (input, _) = char(':').parse(input)?;
        let (input, first_label) = parse_identifier.parse(input)?;
        
        // Parse additional labels separated by ':' or '&'
        let (input, additional_labels) = many0(
            preceded(
                alt((char(':'), char('&'))),
                parse_identifier
            )
        ).parse(input)?;
        
        // Combine labels with colon separator (standard Cypher format)
        let combined_label = if additional_labels.is_empty() {
            first_label.to_string()
        } else {
            let mut all_labels = vec![first_label];
            all_labels.extend(additional_labels);
            all_labels.join(":")
        };
        
        (input, Some(combined_label))
    } else {
        (input, None)
    };
    
    let (input, _) = multispace0.parse(input)?;
    let (input, props) = opt(parse_properties).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;
    
    Ok((input, (
        var.map(|s| s.to_string()),
        labels,
        props.unwrap_or_default(),
    )))
}


// Parse multiple nodes separated by commas: (n:Person), (m:Movie)
fn parse_multiple_nodes(input: &str) -> IResult<&str, Vec<(Option<String>, Option<String>, HashMap<String, Value>)>> {
    separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_node
    ).parse(input)
}

// Parse a relationship pattern like `-[:KNOWS]->` or `<-[:KNOWS]-`
fn parse_relationship(input: &str) -> IResult<&str, (String, HashMap<String, Value>, bool)> {
    alt((
        // Outgoing: -[:TYPE {props}]->
        map(
            tuple((
                char('-'),
                char('['),
                preceded(char(':'), parse_identifier),
                opt(parse_properties),
                char(']'),
                tag("->"),
            )),
            |(_, _, rel_type, props, _, _)| (rel_type.to_string(), props.unwrap_or_default(), false)
        ),
        // Incoming: <-[:TYPE {props}]-
        map(
            tuple((
                tag("<-"),
                char('['),
                preceded(char(':'), parse_identifier),
                opt(parse_properties),
                char(']'),
                char('-'),
            )),
            |(_, _, rel_type, props, _, _)| (rel_type.to_string(), props.unwrap_or_default(), true)
        ),
    )).parse(input)
}


// Parse a `CREATE` nodes query - support multiple nodes separated by commas
fn parse_create_nodes(input: &str) -> IResult<&str, CypherQuery> {
    map(
        tuple((
            tag("CREATE"),
            multispace1,
            parse_multiple_nodes,  // Use the multiple nodes parser
        )),
        |(_, _, nodes)| {
            let node_data: Vec<(String, HashMap<String, Value>)> = nodes
                .into_iter()
                .map(|(var, label, props)| {
                    let actual_label = label.unwrap_or_else(|| var.clone().unwrap_or_else(|| "Node".to_string()));
                    (actual_label, props)
                })
                .collect();

            CypherQuery::CreateNodes { nodes: node_data }
        },
    )
    .parse(input)
}

// Parse a `CREATE INDEX` query - handle Cypher index syntax: CREATE INDEX FOR (n:Label) ON (n.property)
fn parse_create_index(input: &str) -> IResult<&str, CypherQuery> {
    let (input, (_, _, _, _, _, _, node_pattern, _, _, _, prop_pattern)) = tuple((
        tag("CREATE"),
        multispace1,
        tag("INDEX"),
        multispace1,
        tag("FOR"),
        multispace1,
        parse_node, // Parse the node pattern like (n:Person)
        multispace1,
        tag("ON"),
        multispace1,
        parse_property_pattern, // Parse the property pattern like (n.name)
    )).parse(input)?;
    
    let (_, label, _) = node_pattern;
    // Extract just the property names (without the variable prefix like 'n.')
    let property_names: Vec<String> = prop_pattern.iter()
        .map(|prop| {
            prop.split('.').nth(1).unwrap_or(prop).to_string()
        })
        .collect();
    
    Ok((input, CypherQuery::CreateIndex {
        label: label.unwrap_or_default(),
        properties: property_names,
    }))
}

// Parse a property pattern like (n.name) or (n.age, n.city) - used in CREATE INDEX ON (n.property)
fn parse_property_pattern(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Parse property access like n.name
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

// Parse property access like n.name or m.age
fn parse_property_access(input: &str) -> IResult<&str, String> {
    let (input, (var, _, prop)) = tuple((
        parse_identifier,
        char('.'),
        parse_identifier,
    )).parse(input)?;
    
    Ok((input, format!("{}.{}", var, prop)))
}

// Parse complex CREATE patterns with relationships
// Handles: CREATE (a)-[:REL]->(b), CREATE (a)-[:REL1]->(b)<-[:REL2]-(c)
fn parse_create_complex_pattern(input: &str) -> IResult<&str, CypherQuery> {
    let (input, _) = tag("CREATE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // Parse first node
    let (input, first_node) = parse_node(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Check if this is a simple node creation or a pattern with relationships
    // If next char is not '-' or '<', it's just a node
    if !input.starts_with('-') && !input.starts_with('<') {
        // Just a single node, not a pattern
        let (var, label, props) = first_node;
        return Ok((input, CypherQuery::CreateNode {
            label: label.unwrap_or_else(|| var.clone().unwrap_or_else(|| "Node".to_string())),
            properties: props,
        }));
    }
    
    // Parse relationships and nodes in chain
    let mut nodes = vec![first_node];
    let mut relationships = Vec::new();
    
    let mut remaining = input;
    loop {
        // Try to parse a relationship
        match parse_relationship(remaining) {
            Ok((rest, (rel_type, rel_props, is_incoming))) => {
                relationships.push((rel_type, rel_props, is_incoming));
                let (rest, _) = multispace0.parse(rest)?;
                
                // Parse the next node
                match parse_node(rest) {
                    Ok((rest, node)) => {
                        nodes.push(node);
                        let (rest, _) = multispace0.parse(rest)?;
                        remaining = rest;
                        
                        // Check if there's another relationship
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
    
    Ok((remaining, CypherQuery::CreateComplexPattern {
        nodes,
        relationships,
    }))
}

// Parse a `CREATE` node query (single node)
fn parse_create_node(input: &str) -> IResult<&str, CypherQuery> {
    map(
        tuple((tag("CREATE"), multispace1, parse_node)),
        |(_, _, (var, label, props))| CypherQuery::CreateNode {
            label: label.unwrap_or_else(|| var.clone().unwrap_or_else(|| "Node".to_string())),
            properties: props,
        },
    )
    .parse(input)
}

// Parse a `MATCH` with multiple nodes
fn parse_match_multiple_nodes(input: &str) -> IResult<&str, CypherQuery> {
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
            // For now, use the first node's label and properties for matching
            // In a full implementation, this would match all nodes in the pattern
            let (_, label, props) = &nodes[0];
            CypherQuery::MatchNode {
                label: label.clone(),
                properties: props.clone(),
            }
        },
    )
    .parse(input)
}

// Parse a `MATCH` node query - handle both with and without RETURN clause
fn parse_match_node(input: &str) -> IResult<&str, CypherQuery> {
    let (input, (_, _, node)) = tuple((
        tag("MATCH"),
        multispace1,
        parse_node,
    )).parse(input)?;
    
    let (_, label, props) = node;
    
    // Check if there's a RETURN clause after the MATCH
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
        label: label,
        properties: props,
    }))
}

// Parse complex RETURN expressions (handles variables, properties, functions, aliases)
fn parse_return_expressions(input: &str) -> IResult<&str, ()> {
    let (input, _) = multispace0.parse(input)?;
    
    // Parse the first expression
    let (input, _) = parse_return_expression(input)?;
    
    // Parse additional expressions separated by commas
    let (input, _) = many0(preceded(
        tuple((multispace0, char(','), multispace0)),
        parse_return_expression
    )).parse(input)?;
    
    Ok((input, ()))
}

// Parse a single RETURN expression like n, n.name, labels(n), count(n) AS alias
fn parse_return_expression(input: &str) -> IResult<&str, ()> {
    let (input, _) = multispace0.parse(input)?;
    
    // Handle function calls like labels(n), count(n), etc.
    let (input, _) = alt((
        // Function call with parentheses: labels(n), count(n), etc.
        tuple((
            parse_identifier,  // function name
            char('('),
            parse_identifier, // variable name inside parentheses
            char(')'),
            opt(tuple((multispace0, tag("AS"), multispace0, parse_identifier))), // optional AS alias
        )).map(|_| ()),
        // Simple variable or property access: n, n.name, etc.
        tuple((
            parse_identifier,
            opt(preceded(char('.'), parse_identifier)), // optional property access like .name
            opt(tuple((multispace0, tag("AS"), multispace0, parse_identifier))), // optional AS alias
        )).map(|_| ()),
    )).parse(input)?;
    
    Ok((input, ()))
}

// Parse a `CREATE` edge query
fn parse_create_edge(input: &str) -> IResult<&str, CypherQuery> {
    map(
        tuple((
            tag("CREATE"),
            multispace1,
            parse_node,
            multispace0,
            parse_relationship,
            multispace0,
            parse_node,
        )),
        |(_, _, (_var1, _label1, _props1), _, (rel_type, _rel_props, _is_incoming), _, (_var2, _label2, _props2))| CypherQuery::CreateEdge {
            from_id: SerializableUuid(Uuid::new_v4()), // Placeholder; real ID from storage
            edge_type: rel_type,
            to_id: SerializableUuid(Uuid::new_v4()),    // Placeholder; real ID from storage
        },
    )
    .parse(input)
}

// Parse a `SET` node query
fn parse_set_node(input: &str) -> IResult<&str, CypherQuery> {
    map(
        tuple((
            tag("SET"),
            multispace1,
            parse_identifier,
            multispace0,
            parse_properties,
        )),
        |(_, _, _var, _, props)| CypherQuery::SetNode {
            id: SerializableUuid(Uuid::new_v4()), // Placeholder; real ID from storage
            properties: props,
        },
    )
    .parse(input)
}

// Parse a `DELETE` node query
fn parse_delete_node(input: &str) -> IResult<&str, CypherQuery> {
    map(
        tuple((tag("DELETE"), multispace1, parse_identifier)),
        |(_, _, _var)| CypherQuery::DeleteNode {
            id: SerializableUuid(Uuid::new_v4()), // Placeholder; real ID from storage
        },
    )
    .parse(input)
}

// Parse a `SET` key-value query
fn parse_set_kv(input: &str) -> IResult<&str, CypherQuery> {
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
    )
    .parse(input)
}

// Parse a `GET` key-value query (using MATCH ... RETURN to fetch by variable)
fn parse_get_kv(input: &str) -> IResult<&str, CypherQuery> {
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
    )
    .parse(input)
}

// Parse a `DELETE` key-value query (same shape as delete node, treated as key delete)
fn parse_delete_kv(input: &str) -> IResult<&str, CypherQuery> {
    map(
        tuple((tag("DELETE"), multispace1, parse_identifier)),
        |(_, _, key)| CypherQuery::DeleteKeyValue {
            key: key.to_string(),
        },
    )
    .parse(input)
}


// Main parser for Cypher queries - support multi-statement queries by taking the first valid statement
pub fn parse_cypher(query: &str) -> Result<CypherQuery, String> {
    if !is_cypher(query) {
        return Err("Not a valid Cypher query.".to_string());
    }

    let query = query.trim();
    
    // Handle multi-statement queries by splitting on newlines and semicolons and taking the first valid statement
    let statements: Vec<&str> = query
        .split(&['\n', ';'])  // Split on both newlines and semicolons
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .collect();
    
    if statements.len() > 1 {
        // Multi-statement query - parse the first statement that matches our supported patterns
        for stmt in &statements {
            // Strip trailing semicolons from each statement fragment
            let trimmed_stmt = stmt.trim().trim_end_matches(';').trim();
            if trimmed_stmt.is_empty() {
                continue;
            }
            
            // Try to identify the statement type based on the first keyword
            let upper_stmt = trimmed_stmt.to_uppercase();
            if upper_stmt.starts_with("MATCH") {
                // Try MATCH with multiple nodes first, then single node
                let mut match_parser = alt((
                    parse_match_multiple_nodes,
                    parse_match_node,
                ));
                
                if let Ok((remaining, parsed_query)) = match_parser.parse(trimmed_stmt) {
                    if remaining.trim().is_empty() {
                        info!("Parsed MATCH statement from multi-statement query: {}", trimmed_stmt);
                        println!("===> PARSED MATCH STATEMENT FROM MULTI-STATEMENT QUERY: {}", trimmed_stmt);
                        return Ok(parsed_query);
                    }
                    // If there's remaining text but it's not a valid continuation, continue to next statement
                    warn!("MATCH statement parsed but remaining text: '{}'", remaining);
                    continue;
                }
            } else if upper_stmt.starts_with("CREATE") {
                // Check if it's CREATE INDEX first
                if upper_stmt.starts_with("CREATE INDEX") {
                    if let Ok((remaining, parsed_query)) = parse_create_index.parse(trimmed_stmt) {
                        if remaining.trim().is_empty() {
                            info!("Parsed CREATE INDEX statement from multi-statement query: {}", trimmed_stmt);
                            println!("===> PARSED CREATE INDEX STATEMENT FROM MULTI-STATEMENT QUERY: {}", trimmed_stmt);
                            return Ok(parsed_query);
                        }
                        warn!("CREATE INDEX statement parsed but remaining text: '{}'", remaining);
                        continue;
                    }
                }
                
                // Try CREATE patterns - complex patterns first, then simple
                let mut create_parser = alt((
                    parse_create_nodes,  // Try multiple nodes first
                    parse_create_node,
                    parse_create_edge,
                ));
                
                if let Ok((remaining, parsed_query)) = create_parser.parse(trimmed_stmt) {
                    if remaining.trim().is_empty() {
                        info!("Parsed CREATE statement from multi-statement query: {}", trimmed_stmt);
                        println!("===> PARSED CREATE STATEMENT FROM MULTI-STATEMENT QUERY: {}", trimmed_stmt);
                        return Ok(parsed_query);
                    }
                    // If there's remaining text, continue to next statement
                    warn!("CREATE statement parsed but remaining text: '{}'", remaining);
                    continue;
                }
            } else if upper_stmt.starts_with("SET") {
                if let Ok((remaining, parsed_query)) = parse_set_kv.parse(trimmed_stmt) {
                    if remaining.trim().is_empty() {
                        info!("Parsed SET statement from multi-statement query: {}", trimmed_stmt);
                        println!("===> PARSED SET STATEMENT FROM MULTI-STATEMENT QUERY: {}", trimmed_stmt);
                        return Ok(parsed_query);
                    }
                    // If there's remaining text, continue to next statement
                    continue;
                }
            } else if upper_stmt.starts_with("DELETE") {
                if let Ok((remaining, parsed_query)) = parse_delete_kv.parse(trimmed_stmt) {
                    if remaining.trim().is_empty() {
                        info!("Parsed DELETE statement from multi-statement query: {}", trimmed_stmt);
                        println!("===> PARSED DELETE STATEMENT FROM MULTI-STATEMENT QUERY: {}", trimmed_stmt);
                        return Ok(parsed_query);
                    }
                    // If there's remaining text, continue to next statement
                    continue;
                }
            }
        }
        // If no statement in the multi-line query was valid, try to parse the whole query as a single statement
    }

    // Single statement or fallback to whole query - strip trailing semicolon before parsing
    let query_to_parse = query.trim_end_matches(';').trim();
    
    let mut parser = alt((
        parse_create_index,        // Try CREATE INDEX first
        parse_create_nodes,        // Try multiple nodes first
        parse_create_node,
        parse_match_multiple_nodes, // Try multiple nodes first
        parse_match_node,
        parse_create_edge,
        parse_set_node,
        parse_delete_node,
        parse_set_kv,
        parse_get_kv,
        parse_delete_kv,
    ));

    match parser.parse(query_to_parse) {
        Ok((remaining, parsed_query)) => {
            if !remaining.trim().is_empty() {
                Err(format!("Failed to fully consume input, remaining: {:?}", remaining.trim()))
            } else {
                Ok(parsed_query)
            }
        }
        Err(e) => Err(format!("Failed to parse Cypher query: {:?}", e)),
    }
}

/// Execute a raw Cypher string using a QueryExecEngine instance
pub async fn execute_cypher_from_string(
    query: &str,
    storage: Arc<dyn GraphStorageEngine + Send + Sync>,
) -> GraphResult<Value> {
    let parsed = parse_cypher(query).map_err(|e| GraphError::StorageError(e))?;
    // Create a minimal Database wrapper — config is unused in execute_cypher
    let db = Arc::new(Database {
        storage: storage.clone(),
        config: StorageConfig::default(),
    });
    execute_cypher(parsed, &db, storage).await
}

/// Execute a parsed Cypher query against the database and storage engine
pub async fn execute_cypher(
    query: CypherQuery,
    _db: &Database,
    storage: Arc<dyn GraphStorageEngine + Send + Sync>,
) -> GraphResult<Value> {
    match query {
        CypherQuery::CreateNode { label, properties } => {
            let props: GraphResult<HashMap<String, PropertyValue>> = properties
                .into_iter()
                .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                .collect();
            let vertex = Vertex {
                id: SerializableUuid(Uuid::new_v4()),
                label: Identifier::new(label)?,
                properties: props?,
            };
            storage.create_vertex(vertex.clone()).await?;
            Ok(json!({ "vertex": vertex }))
        }

        CypherQuery::CreateNodes { nodes } => {
            let mut created_vertices = Vec::new();
            for (label, properties) in nodes {
                let props: GraphResult<HashMap<String, PropertyValue>> = properties
                    .into_iter()
                    .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                    .collect();
                let vertex = Vertex {
                    id: SerializableUuid(Uuid::new_v4()),
                    label: Identifier::new(label)?,
                    properties: props?,
                };
                storage.create_vertex(vertex.clone()).await?;
                created_vertices.push(vertex);
            }
            Ok(json!({ "vertices": created_vertices }))
        }

        CypherQuery::MatchNode { label, properties } => {
            let vertices = storage.get_all_vertices().await?;
            let props: GraphResult<HashMap<String, PropertyValue>> = properties
                .into_iter()
                .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                .collect();
            let props = props?;
            let filtered = vertices.into_iter().filter(|v| {
                // Check if the vertex label matches the query label
                // For hierarchical labels like "Person:Director", check if the query label is a prefix
                let matches_label = if let Some(query_label) = &label {
                    let vertex_label_str = v.label.as_ref();
                    // Check for exact match or if vertex label starts with query label followed by ':'
                    vertex_label_str == query_label || 
                    vertex_label_str.starts_with(&format!("{}:", query_label))
                } else {
                    true // If no label specified in query, match all
                };
                
                let matches_props = props.iter().all(|(k, expected_val)| {
                    v.properties.get(k).map_or(false, |actual_val| actual_val == expected_val)
                });
                matches_label && matches_props
            }).collect::<Vec<_>>();
            Ok(json!({ "vertices": filtered }))
        }

        CypherQuery::MatchMultipleNodes { nodes } => {
            // For now, just match the first node and return it
            // In a full implementation, this would match all nodes and return them as a collection
            if let Some((_, label, properties)) = nodes.first() {
                let vertices = storage.get_all_vertices().await?;
                let props: GraphResult<HashMap<String, PropertyValue>> = properties
                    .iter()
                    .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                    .collect();
                let props = props?;
                let filtered = vertices.into_iter().filter(|v| {
                    let matches_label = label.as_ref().map_or(true, |l| v.label.as_ref() == l);
                    let matches_props = props.iter().all(|(k, expected_val)| {
                        v.properties.get(k).map_or(false, |actual_val| actual_val == expected_val)
                    });
                    matches_label && matches_props
                }).collect::<Vec<_>>();
                Ok(json!({ "vertices": filtered, "note": "Currently only matching first node in pattern" }))
            } else {
                Ok(json!({ "vertices": [] }))
            }
        }

        CypherQuery::CreateComplexPattern { nodes, relationships } => {
            // For now, just create the nodes and log that relationships are not yet implemented
            let mut created_vertices = Vec::new();
            for (var, label, properties) in nodes {
                let props: GraphResult<HashMap<String, PropertyValue>> = properties
                    .into_iter()
                    .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                    .collect();
                let vertex = Vertex {
                    id: SerializableUuid(Uuid::new_v4()),
                    label: Identifier::new(label.unwrap_or_else(|| var.clone().unwrap_or_else(|| "Node".to_string())))?,
                    properties: props?,
                };
                storage.create_vertex(vertex.clone()).await?;
                created_vertices.push(vertex);
            }
            warn!("Complex pattern with {} relationships parsed but not yet fully implemented", relationships.len());
            Ok(json!({ 
                "vertices": created_vertices,
                "note": format!("Created {} nodes. Relationships ({}) not yet implemented.", created_vertices.len(), relationships.len())
            }))
        }

        CypherQuery::CreateEdge {
            from_id,
            edge_type,
            to_id,
        } => {
            let edge = Edge {
                id: SerializableUuid(Uuid::new_v4()),
                outbound_id: from_id,
                edge_type: Identifier::new(edge_type)?,
                inbound_id: to_id,
                label: "relationship".to_string(),
                properties: BTreeMap::new(),
            };
            storage.create_edge(edge.clone()).await?;
            Ok(json!({ "edge": edge }))
        }

        CypherQuery::SetNode { id, properties } => {
            let mut vertex = storage.get_vertex(&id.0).await?.ok_or_else(|| {
                GraphError::StorageError(format!("Vertex not found: {}", id.0))
            })?;
            let props: GraphResult<HashMap<String, PropertyValue>> = properties
                .into_iter()
                .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                .collect();
            vertex.properties.extend(props?);
            storage.update_vertex(vertex.clone()).await?;
            Ok(json!({ "vertex": vertex }))
        }

        CypherQuery::DeleteNode { id } => {
            storage.delete_vertex(&id.0).await?;
            Ok(json!({ "deleted": id }))
        }

        CypherQuery::SetKeyValue { key, value } => {
            let kv_key = key.clone().into_bytes();
            storage.insert(kv_key, value.as_bytes().to_vec()).await?;
            storage.flush().await?;
            Ok(json!({ "key": key, "value": value }))
        }

        CypherQuery::GetKeyValue { key } => {
            let kv_key = key.clone().into_bytes();
            let value = storage.retrieve(&kv_key).await?;
            Ok(json!({
                "key": key,
                "value": value.map(|v| String::from_utf8_lossy(&v).to_string())
            }))
        }

        CypherQuery::DeleteKeyValue { key } => {
            let kv_key = key.clone().into_bytes();
            let existed = storage.retrieve(&kv_key).await?.is_some();
            if existed {
                storage.delete(&kv_key).await?;
                storage.flush().await?;
            }
            Ok(json!({ "key": key, "deleted": existed }))
        }

        CypherQuery::CreateIndex { label, properties } => {
            // For now, just return a success response indicating index creation is not implemented
            warn!("CREATE INDEX command is not implemented yet. Index for label '{}' on properties {:?} ignored.", label, properties);
            println!("===> WARNING: CREATE INDEX COMMAND NOT IMPLEMENTED YET. INDEX FOR LABEL '{}' ON PROPERTIES {:?} IGNORED.", label, properties);
            Ok(json!({ "status": "success", "message": "Index creation not implemented", "label": label, "properties": properties }))
        }
    }
}

/// Helper to convert Cypher `Value` → `PropertyValue`
fn to_property_value(v: Value) -> GraphResult<PropertyValue> {
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
}