// lib/src/query_parser/cypher_parser.rs
// Updated: 2025-09-01 - Fixed E0382 by cloning `key` before into_bytes() in execute_cypher
// to avoid borrow-after-move errors in SetKeyValue, GetKeyValue, and DeleteKeyValue.

use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt},
    multi::{separated_list0, separated_list1, many0}, // Added many0
    sequence::{delimited, preceded, tuple},
    IResult,
    Parser,
};
use serde_json::{json, Value};
use std::collections::{ BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;
use models::identifiers::{Identifier, SerializableUuid};
use models::{Vertex, Edge, properties::PropertyValue};
use models::errors::{GraphError, GraphResult};
use models::properties::SerializableFloat;
use crate::database::Database;
use crate::storage_engine::{GraphStorageEngine, StorageEngine};

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
    CreateEdge {
        from_id: SerializableUuid,
        edge_type: String,
        to_id: SerializableUuid,
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
}

// Checks if a query is likely a Cypher query
pub fn is_cypher(query: &str) -> bool {
    let cypher_keywords = ["MATCH", "CREATE", "SET", "RETURN", "DELETE"];
    cypher_keywords.iter().any(|kw| query.trim().to_uppercase().starts_with(kw))
}

// Parse a Cypher identifier (e.g., variable name or label)
fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_').parse(input)
}

// Parse a string literal (e.g., 'Alice' or "Alice")
fn parse_string_literal(input: &str) -> IResult<&str, &str> {
    alt((
        delimited(char('\''), take_while1(|c: char| c != '\''), char('\'')),
        delimited(char('"'), take_while1(|c: char| c != '"'), char('"')),
    ))
    .parse(input)
}

// Parse a number literal
fn parse_number_literal(input: &str) -> IResult<&str, Value> {
    map(nom::character::complete::i64, |n| json!(n)).parse(input)
}

// Parse a property value (string or number)
fn parse_property_value(input: &str) -> IResult<&str, Value> {
    alt((
        map(parse_string_literal, |s| json!(s)),
        parse_number_literal,
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
            separated_list0(preceded(multispace0, char(',')), parse_property),
            preceded(multispace0, char('}')),
        ),
        |props| props.into_iter().collect(),
    )
    .parse(input)
}

// Parse a node pattern like `(n:Person {name: 'Alice'})` or `(n:Person:Actor {name: 'Bob'})`
// or `(n:Person&Actor {name: 'Charlie'})` - supports both : and & as label separators
fn parse_node(input: &str) -> IResult<&str, (Option<String>, Option<String>, HashMap<String, Value>)> {
    let (input, _) = char('(').parse(input)?;
    let (input, var) = opt(parse_identifier).parse(input)?;
    
    // Parse labels (one or more, separated by ':' or '&')
    let (input, labels) = if input.starts_with(':') {
        let (input, _) = char(':').parse(input)?;
        let (input, first_label) = parse_identifier.parse(input)?;
        
        // Support both ':' and '&' as label separators
        let (input, _) = many0(alt((
            preceded(char(':'), parse_identifier),
            preceded(char('&'), parse_identifier),
        ))).parse(input)?;
        
        (input, Some(first_label.to_string()))
    } else {
        (input, None)
    };
    
    let (input, _) = multispace0.parse(input)?;
    let (input, props) = opt(parse_properties).parse(input)?;
    let (input, _) = char(')').parse(input)?;
    
    Ok((input, (
        var.map(|s| s.to_string()),
        labels,
        props.unwrap_or_default(),
    )))
}

// Parse a relationship pattern like `-[:KNOWS]->`
fn parse_relationship(input: &str) -> IResult<&str, String> {
    let (input, (_, _, rel_type, _, _)) = tuple((
        char('-'),
        char('['),
        preceded(char(':'), parse_identifier),
        char(']'),
        tag("->"),
    ))
    .parse(input)?;
    Ok((input, rel_type.to_string()))
}

// Parse a `CREATE` nodes query - support multiple nodes separated by commas
fn parse_create_nodes(input: &str) -> IResult<&str, CypherQuery> {
    map(
        tuple((
            tag("CREATE"),
            multispace1,
            separated_list1(
                delimited(multispace0, char(','), multispace0),
                parse_node,
            ),
        )),
        |(_, _, nodes)| {
            let node_data: Vec<(String, HashMap<String, Value>)> = nodes
                .into_iter()
                .map(|(var, label, props)| {
                    let actual_label = label.unwrap_or_else(|| var.clone().unwrap_or_default());
                    (actual_label, props)
                })
                .collect();
            
            CypherQuery::CreateNodes {
                nodes: node_data,
            }
        },
    )
    .parse(input)
}

// Parse a `CREATE` node query
fn parse_create_node(input: &str) -> IResult<&str, CypherQuery> {
    map(
        tuple((tag("CREATE"), multispace1, parse_node)),
        |(_, _, (_, label, props))| CypherQuery::CreateNode {
            label: label.unwrap_or_default(),
            properties: props,
        },
    )
    .parse(input)
}

// Parse a `MATCH` node query - handle complex RETURN clauses
fn parse_match_node(input: &str) -> IResult<&str, CypherQuery> {
    let (input, (_, _, node, _, return_clause)) = tuple((
        tag("MATCH"),
        multispace1,
        parse_node,
        multispace1,
        tag("RETURN"),
    )).parse(input)?;
    
    let (_, label, props) = node;
    
    // Parse complex RETURN expressions like:
    // - n.name
    // - labels(n) 
    // - count(n)
    // - n, r, m
    // - n.name, labels(n) AS labels
    // - count(n) AS total_vertices
    
    let (input, _) = parse_return_expressions(input)?;
    
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
        |(_, _, ( _var1, _label1, _props1), _, rel_type, _, (_var2, _label2, _props2))| CypherQuery::CreateEdge {
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

// Main parser for Cypher queries
pub fn parse_cypher(query: &str) -> Result<CypherQuery, String> {
    if !is_cypher(query) {
        return Err("Not a valid Cypher query.".to_string());
    }

    let query = query.trim();
    let mut parser = alt((
        parse_create_nodes,  // Add this before parse_create_node to handle multi-node first
        parse_create_node,
        parse_match_node,
        parse_create_edge,
        parse_set_node,
        parse_delete_node,
        parse_set_kv,
        parse_get_kv,
        parse_delete_kv,
    ));

    match parser.parse(query) {
        Ok((remaining, parsed_query)) => {
            if !remaining.trim().is_empty() {
                Err(format!("Failed to fully consume input, remaining: {:?}", remaining))
            } else {
                Ok(parsed_query)
            }
        }
        Err(e) => Err(format!("Failed to parse Cypher query: {:?}", e)),
    }
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
                let matches_label = label.as_ref().map_or(true, |l| v.label.as_ref() == l);
                let matches_props = props.iter().all(|(k, expected_val)| {
                    v.properties.get(k).map_or(false, |actual_val| actual_val == expected_val)
                });
                matches_label && matches_props
            }).collect::<Vec<_>>();
            Ok(json!({ "vertices": filtered }))
        }

        CypherQuery::CreateEdge {
            from_id,
            edge_type,
            to_id,
        } => {
            let edge = Edge {
                id: SerializableUuid(Uuid::new_v4()),
                outbound_id: from_id,
                t: Identifier::new(edge_type)?,
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
    fn test_parse_create_nodes_with_ampersand_labels() {
        let query = "CREATE (charlie:Person&Actor {name: 'Charlie Sheen'}), (oliver:Person&Director {name: 'Oliver Stone'})";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::CreateNodes {
            nodes: vec![
                ("Person".to_string(), HashMap::from([  // Using first label
                    ("name".to_string(), json!("Charlie Sheen")),
                ])),
                ("Person".to_string(), HashMap::from([  // Using first label
                    ("name".to_string(), json!("Oliver Stone")),
                ])),
            ],
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_create_nodes_with_colon_labels() {
        let query = "CREATE (charlie:Person:Actor {name: 'Charlie Sheen'}), (oliver:Person:Director {name: 'Oliver Stone'})";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::CreateNodes {
            nodes: vec![
                ("Person".to_string(), HashMap::from([  // Using first label
                    ("name".to_string(), json!("Charlie Sheen")),
                ])),
                ("Person".to_string(), HashMap::from([  // Using first label
                    ("name".to_string(), json!("Oliver Stone")),
                ])),
            ],
        };
        assert_eq!(result, expected);
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
