// src/query_parser/query_types.rs
// Complete — includes full WHERE evaluation, no external file needed

use serde_json::{json, Value};
use std::collections::{HashSet, HashMap};
use tokio::sync::RwLock;
use tokio::sync::OnceCell;
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use crate::config::QueryResult;
use models::{Vertex, Edge};
use models::errors::{GraphError, GraphResult};
use models::properties::{ PropertyValue, SerializableFloat, HashablePropertyMap} ;
use models::identifiers::SerializableUuid;
use std::result::Result; 
use std::sync::{Arc};
use std::fmt;
use strum_macros::{Display, AsRefStr};

#[derive(Debug, Default)]
pub struct GlobalPersistenceContext {
    // Maps variable names to DB UUIDs (e.g., "e" -> UUID)
    pub bindings: RwLock<HashMap<String, Uuid>>,
}

impl GlobalPersistenceContext {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct SymbolTable {
    pub variables: HashMap<String, SerializableUuid>,
    pub relationships: HashMap<String, SerializableUuid>,
}

impl SymbolTable {
    pub fn new() -> Self {
        Self {
            variables: HashMap::new(),
            relationships: HashMap::new(),
        }
    }
    
    pub fn bind_variable(&mut self, var_name: String, vertex_id: SerializableUuid) {
        if !var_name.is_empty() {
            self.variables.insert(var_name, vertex_id);
        }
    }
    
    pub fn bind_relationship(&mut self, var_name: String, edge_id: SerializableUuid) {
        if !var_name.is_empty() {
            self.relationships.insert(var_name, edge_id);
        }
    }
    
    pub fn get_variable(&self, var_name: &str) -> Option<&SerializableUuid> {
        self.variables.get(var_name)
    }
    
    pub fn get_relationship(&self, var_name: &str) -> Option<&SerializableUuid> {
        self.relationships.get(var_name)
    }
}

// This definition allows you to specify both T and E when using StdResult.
pub type StdResult<T, E> = Result<T, E>;
/// (variable_name_opt, label_opt, properties_map)
// Changed from Option<String> to Vec<String>
pub type NodePattern = (Option<String>, Vec<String>, HashMap<String, Value>);

/// (variable_name_opt, label_opt, length_range_opt, properties_map, direction_opt)
/// direction_opt: true for ->, false for <-, None for --
pub type RelPattern = (
    Option<String>,                    // Variable name
    Option<String>,                    // Relationship Type (e.g., 'WORKS_AT')
    Option<(Option<u32>, Option<u32>)>, // Variable length: [*1..5]
    HashMap<String, Value>,            // Properties
    Option<bool>,                      // Direction (Left/Right/None)
);

// Type alias for the parser's raw output (before conversion)
pub type ParsedPatternsReturnType = Vec<(
    Option<String>,
    Vec<(Option<String>, Option<String>, HashMap<String, Value>)>,
    Vec<RelPattern>
)>;

// Type alias for the execution format (after conversion)
pub type ExecutionPatternsReturnType = Vec<(
    Option<String>,
    Vec<(Option<String>, Vec<String>, HashMap<String, Value>)>,
    Vec<RelPattern>
)>;


/// 1. Define the Literal enum if it's not imported from elsewhere
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Number(f64),
    Float(SerializableFloat),
    Boolean(bool),
    Integer(i64),
    Null,
}

#[derive(Debug, Clone)]
pub enum LogicalExpr {
    Condition(String, BinaryOp, PropertyValue),
    And(Box<LogicalExpr>, Box<LogicalExpr>),
    Or(Box<LogicalExpr>, Box<LogicalExpr>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CypherExpression {
    Literal(Value),
    PropertyLookup {
        var: String,
        prop: String,
    },
    BinaryOp {
        left: Box<CypherExpression>,
        op: String,
        right: Box<CypherExpression>,
    },
    // --- Added UnaryOp Variant ---
    UnaryOp {
        op: UnaryOp,
        expr: Box<CypherExpression>,
    },
    Variable(String),
    FunctionCall {
        name: String,
        args: Vec<CypherExpression>,
    },
/// Represents regex matching: var.prop =~ 'pattern'
    Regex {
        variable: String,
        property: String,
        pattern: String,
    },
    List(Vec<CypherExpression>),
    
    /// Represents predicates like: any(var IN list WHERE condition)
    /// This is distinct from FunctionCall because it introduces a new 
    /// scope variable used within the condition expression.
    Predicate {
        /// The predicate name (ANY, ALL, NONE, SINGLE)
        name: String,
        /// The iteration variable name (e.g., 'x' in 'any(x IN ...)')
        variable: String,
        /// The list expression being iterated over
        list: Box<CypherExpression>,
        /// The boolean condition to evaluate for each element
        condition: Box<CypherExpression>,
    },
    DynamicPropertyAccess {
        variable: String,
        property_expr: Box<CypherExpression>,
    },
    ArrayIndex {
        expr: Box<CypherExpression>,
        index: Box<CypherExpression>,
    },
}

/// Represents the possible outcomes of executing a Cypher query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CypherResponse {
    /// Indicates successful completion of a mutation (CREATE, SET, DELETE, etc.)
    /// The inner Value typically holds a status object (e.g., { "message": "Success" }).
    Success(Value),

    /// Represents the structured data returned by a read query (MATCH ... RETURN).
    /// The inner Value is typically an array of result rows.
    ResultSet(Value),

    /// Returns a list of the created entities (Nodes/Edges).
    /// Used for CREATE statements where no explicit RETURN is specified.
    CreatedEntities(Value),

    /// General error wrapper (often preferred to let the external function handle the error,
    /// but useful if you want to explicitly wrap errors that are not GraphError).
    Error(String),
}

/// An internal wrapper to parse and manipulate the opaque String data 
/// inside the QueryResult enum, treating it as a JSON array of result rows.
pub struct InternalQueryResult {
    pub data: Value, 
}

impl InternalQueryResult {
    /// Initializes an empty result set for the start of a CHAIN operation.
    fn empty() -> Self {
        InternalQueryResult { data: Value::Array(vec![]) }
    }
    
    /// Parses the opaque QueryResult::Success(String) into a manipulable JSON Value.
    fn from_query_result(qr: QueryResult) -> GraphResult<Self> {
        match qr {
            QueryResult::Success(s) => serde_json::from_str(&s)
                .map(|data| InternalQueryResult { data })
                .map_err(|e| GraphError::DeserializationError(
                    format!("Failed to parse query result string as JSON: {}", e)
                )),
            QueryResult::Null => Ok(Self::empty()),
        }
    }
    
    /// Serializes the JSON Value back into QueryResult::Success(String) or Null.
    fn to_query_result(self) -> GraphResult<QueryResult> {
        if self.data.is_array() && self.data.as_array().map_or(true, |a| a.is_empty()) {
            Ok(QueryResult::Null)
        } else {
            serde_json::to_string(&self.data)
                .map(QueryResult::Success)
                .map_err(|e| GraphError::SerializationError(
                    format!("Failed to serialize result back to string: {}", e)
                ))
        }
    }
    
    /// Checks for minimal compatibility (i.e., both results are JSON arrays).
    fn check_compatibility(&self, other: &Self) -> GraphResult<()> {
        if !self.data.is_array() || !other.data.is_array() {
            return Err(GraphError::QueryExecutionError(
                "UNION operands must produce structured, compatible data (JSON array expected).".to_string(),
            ));
        }
        // In a real database, detailed schema comparison (column names/types) would happen here.
        Ok(())
    }
    
    /// Logic for UNION ALL: combines results and retains duplicates.
    fn union_all(self, other: Self) -> Self {
        let mut combined_array = Vec::new();
        if let Some(arr) = self.data.as_array() { combined_array.extend_from_slice(arr); }
        if let Some(arr) = other.data.as_array() { combined_array.extend_from_slice(arr); }
        InternalQueryResult { data: Value::Array(combined_array) }
    }
    
    /// Logic for UNION (DISTINCT): combines results and removes duplicates.
    fn union_distinct(self, other: Self) -> Self {
        let mut distinct_set: HashSet<Value> = HashSet::new();
        let mut combined_array = Vec::new();

        let process_array = |data: Value, set: &mut HashSet<Value>, vec: &mut Vec<Value>| {
            if let Some(arr) = data.as_array() {
                for item in arr {
                    // Assumes the result rows (JSON objects) are hashable
                    if set.insert(item.clone()) {
                        vec.push(item.clone());
                    }
                }
            }
        };

        process_array(self.data, &mut distinct_set, &mut combined_array);
        process_array(other.data, &mut distinct_set, &mut combined_array);

        InternalQueryResult { data: Value::Array(combined_array) }
    }
}

/// (path_variable_name_opt, nodes_vec, relationships_vec)
/// Represents a complex path structure, likely used for MATCH.
pub type Pattern = (Option<String>, Vec<NodePattern>, Vec<RelPattern>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchPattern {
    pub nodes: Vec<NodePattern>,
    pub relationships: Vec<RelPattern>,
}

// The complex type that parse_match_clause_patterns returns.
// This type is derived from the previous log and is used as the O (Output) type in IResult.
pub type PatternsReturnType = std::vec::Vec<(
    std::option::Option<std::string::String>,
    std::vec::Vec<(
        std::option::Option<std::string::String>,
        std::option::Option<std::string::String>,
        std::collections::HashMap<std::string::String, serde_json::Value>,
    )>,
    std::vec::Vec<(
        std::option::Option<std::string::String>,
        std::option::Option<std::string::String>,
        std::option::Option<(std::option::Option<u32>, std::option::Option<u32>)>,
        std::collections::HashMap<std::string::String, serde_json::Value>,
        std::option::Option<bool>,
    )>,
)>;

// NOTE: Removed the redundant 'WhereClause(pub Value)' alias here
// and kept the definitive WhereClause struct definition below.

// =================================================================
// TYPES EXPLICITLY REQUESTED BY THE COMPILER ERRORS/PLACEHOLDERS
// =================================================================

// 1. crate::query::QueryReturnItem
// Represents an expression returned or projected (e.g., n.name, COUNT(n), x AS alias)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Serialize, Deserialize)]
pub struct QueryReturnItem {
    pub expression: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Serialize, Deserialize)]
pub struct WithItem {
    pub expression: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)] // ← Add Serialize
pub struct ReturnClause {
    pub items: Vec<ReturnItem>,
    pub distinct: bool,
    pub order_by: Option<Vec<(String, bool)>>,
    pub skip: Option<usize>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReturnItem {
    pub expression: String,
    pub alias: Option<String>,
}

// 2. crate::query::OrderByItem
// Represents a field and its sort order in ORDER BY clause
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Serialize, Deserialize )]
pub struct OrderByItem {
    pub expression: String,
    pub ascending: bool, // true for ASC, false for DESC
}

// 3. crate::query::RemoveItem
// Represents an item being removed (a label or a property)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Serialize, Deserialize)]
pub enum RemoveItem {
    Label {
        variable: String,
        label: String,
    },
    Property {
        variable: String,
        property_name: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)] // Add PartialEq if not already there
pub enum CypherValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    // Add this variant:
    Uuid(SerializableUuid), 
    Vertex(Vertex),
    Edge(Edge),
    Map(HashMap<String, CypherValue>),
    List(Vec<CypherValue>),
}

impl fmt::Display for CypherValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CypherValue::Null => write!(f, ""),
            CypherValue::Bool(b) => write!(f, "{}", b),
            CypherValue::Integer(i) => write!(f, "{}", i),
            CypherValue::Float(fl) => write!(f, "{}", fl),
            CypherValue::String(s) => write!(f, "{}", s),
            CypherValue::Uuid(u) => write!(f, "{}", u),
            // For Vertex/Edge IDs, we need the raw string representation
            CypherValue::Vertex(v) => write!(f, "{}", v.id),
            CypherValue::Edge(e) => write!(f, "{}", e.id),
            CypherValue::Map(m) => write!(f, "{:?}", m),
            CypherValue::List(l) => write!(f, "{:?}", l),
        }
    }
}

// =================================================================
// CYPHER QUERY ENUM (WITH CRITICAL UPDATES FOR CHAINING)
// =================================================================


#[derive(Debug, Clone, PartialEq,)]
pub enum CypherQuery {
    CreateNode {
        label: String,
        properties: HashMap<String, Value>,
    },
    CreateNodes {
        nodes: Vec<(String, HashMap<String, Value>)>,
    },
    MatchNode {
        label: Option<String>,
        properties: HashMap<String, Value>,
    },
    MatchMultipleNodes {
        nodes: Vec<NodePattern>,
    },
    MatchRemove {
        match_patterns: Vec<Pattern>,
        where_clause: Option<WhereClause>, // Added
        with_clause: Option<ParsedWithClause>,
        remove_clauses: Vec<(String, String)>,
    },
    CreateComplexPattern {
        nodes: Vec<NodePattern>,
        relationships: Vec<RelPattern>,
    },
    CreateStatement {
        patterns: Vec<Pattern>,
        return_items: Vec<String>,
    },
    MatchPattern {
        patterns: Vec<Pattern>,
        where_clause: Option<WhereClause>, 
        with_clause: Option<ParsedWithClause>,
        return_clause: Option<ReturnClause>, 
    },
    MatchSet {
        match_patterns: Vec<Pattern>, // Adjust type name to match your codebase
        with_clause: Option<ParsedWithClause>,
        where_clause: Option<WhereClause>,
        set_clauses: Vec<(String, String, Expression)>, // Change Value to Expression
    },
    MatchCreateSet {
        match_patterns: Vec<Pattern>,
        where_clause: Option<WhereClause>, // Added
        with_clause: Option<ParsedWithClause>,
        create_patterns: Vec<Pattern>,
        set_clauses: Vec<(String, String, Expression)>,
    },
    MatchCreate {
        match_patterns: Vec<Pattern>,
        where_clause: Option<WhereClause>, // Added
        with_clause: Option<ParsedWithClause>,
        create_patterns: Vec<Pattern>,
    },
    CreateEdgeBetweenExisting {
        source_var: String,
        rel_type: String,
        properties: HashMap<String, Value>,
        target_var: String,
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
    CreateIndex {
        label: String,
        properties: Vec<String>,
    },
    MatchPath {
        path_var: String,
        left_node: String,
        right_node: String,
        return_clause: String,
    },
    DeleteEdges {
        edge_variable: String,
        pattern: MatchPattern,
        where_clause: Option<WhereClause>,
        with_clause: Option<ParsedWithClause>,
    },
    DetachDeleteNodes {
        node_variable: String,
        label: Option<String>,
    },
    Merge {
        patterns: Vec<Pattern>,
        where_clause: Option<WhereClause>, // Added (Supported by Cypher in MERGE)
        with_clause: Option<ParsedWithClause>,
        on_create_set: Vec<(String, String, Expression)>, 
        on_match_set: Vec<(String, String, Expression)>,
        return_clause: Option<ReturnClause>,
    },
    // NEW: Standalone SET clause for chaining
    SetStatement { 
        assignments: Vec<(String, String, Expression)>, 
    },
    
    // NEW: Standalone DELETE/DETACH DELETE clause
    DeleteStatement { 
        variables: Vec<String>, // List of variable names to delete
        detach: bool,
    },
    // NEW: Standalone REMOVE clause
    RemoveStatement { 
        removals: Vec<(String, String)>, // e.g., ("n", "label") or ("n", "property")
    },
    ReturnStatement {
        projection_string: String,
        order_by: Vec<OrderByItem>, // Placeholder for ORDER BY expressions
        skip: Option<i64>,        // Parsed value for SKIP
        limit: Option<i64>,       // Parsed value for LIMIT
        is_distinct: bool, 
    },
    // NEW: UNWIND clause support
    Unwind {
        expression: Expression, // The list expression to unwind
        variable: String,       // The variable name to bind each element
    },
    Chain(Vec<CypherQuery>),
    Batch(Vec<CypherQuery>),
    Union(Box<CypherQuery>, bool, Box<CypherQuery>),
}

// Ensure the inner WithClause also has them if it's a separate struct
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub condition: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedWithClause {
    pub items: Vec<QueryReturnItem>,
    pub distinct: bool,
    /// Uses WhereClause to maintain the Expression tree for MPI logical filtering
    pub where_clause: Option<WhereClause>, 
    pub order_by: Vec<OrderByItem>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
}

// =================================================================
// EXPRESSION AND EVALUATION LOGIC
// =================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum PropertyAccess {
    Vertex(String, String),
    Edge(String, String),
    Parameter(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Literal(CypherValue),
    Property(PropertyAccess),
    Variable(String),
    List(Vec<Expression>),
    Binary {
        op: BinaryOp,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expression>,
    },
    LabelPredicate {
        variable: String,
        label: String,
    },
    Predicate {
        name: String,            // "any", "all", etc.
        variable: String,        // "label"
        list: Box<Expression>,   // "labels(e)"
        condition: Box<Expression>, // "label IN ['...']"
    },
    Regex {
        variable: String,
        property: String,
        pattern: String,
    },
    FunctionCall {
        name: String,
        args: Vec<Expression>,
    },
    PropertyComparison {
        variable: String,
        property: String,
        operator: String,
        value: Value,
    },
    FunctionComparison {
        function: String,
        argument: String,
        operator: String,
        value: Value,
    },
    StartsWith {
        variable: String,
        property: String,
        prefix: String,
    },
    // ADD THESE TWO:
    EndsWith {
        variable: String,
        property: String,
        suffix: String,
    },
    Contains {
        variable: String,
        property: String,
        substring: String,
    },
    DynamicPropertyAccess {
        variable: String,
        property_expr: Box<Expression>, // Expression that evaluates to property name
    },
    ArrayIndex {
        expr: Box<Expression>,
        index: Box<Expression>,
    },
    And { left: Box<Expression>, right: Box<Expression> },
    Or { left: Box<Expression>, right: Box<Expression> },
    /// Logical Prefix Negation: NOT (condition)
    Not(Box<Expression>),
    /// Null check: expr IS NULL
    IsNull(Box<Expression>),
    /// Not null check: expr IS NOT NULL
    IsNotNull(Box<Expression>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionItem {
    Variable(String),
    Property(PropertyAccess),
    Alias {
        expression: Expression,
        alias: String,
    },
    Expression(Expression),
}

#[derive(Debug, Clone, PartialEq, Display, AsRefStr)]
pub enum BinaryOp {
    #[strum(serialize = "=")]
    Eq,
    #[strum(serialize = "<>")]
    Neq,
    #[strum(serialize = "<")]
    Lt,
    #[strum(serialize = "<=")]
    Lte,
    #[strum(serialize = ">")]
    Gt,
    #[strum(serialize = ">=")]
    Gte,
    #[strum(serialize = "AND")]
    And,
    #[strum(serialize = "OR")]
    Or,
    #[strum(serialize = "XOR")]
    Xor,
    #[strum(serialize = "+")]
    Plus,
    #[strum(serialize = "-")]
    Minus,
    #[strum(serialize = "*")]
    Mul,
    #[strum(serialize = "/")]
    Div,
    #[strum(serialize = "%")]
    Mod,
    #[strum(serialize = "CONTAINS")]
    Contains,
    #[strum(serialize = "STARTS WITH")]
    StartsWith,
    #[strum(serialize = "ENDS WITH")]
    EndsWith,
    #[strum(serialize = "=~")]
    Regex,
    #[strum(serialize = "IN")]
    In,
    #[strum(serialize = "NOT IN")]
    NotIn, 
}

#[derive(Debug, Clone, PartialEq, Display, AsRefStr, Serialize, Deserialize)]
pub enum UnaryOp {
    #[strum(serialize = "NOT")]
    Not, 
    #[strum(serialize = "-")]
    Neg,
    #[strum(serialize = "IS NOT NULL")]
    IsNotNull, 
    #[strum(serialize = "IS NULL")]
    IsNull,   
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub condition: Expression,
}

#[derive(Debug, Clone, Default)]
pub struct EvaluationContext {
    pub variables: HashMap<String, CypherValue>,
    pub parameters: HashMap<String, CypherValue>,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionContext {
    // Variable name -> Set of Vertex UUIDs (multiple possible bindings)
    pub vertex_bindings: HashMap<String, HashSet<uuid::Uuid>>,
    // Relationship variable -> Set of Edge UUIDs  
    pub edge_bindings: HashMap<String, HashSet<uuid::Uuid>>,
    // Current working set of vertices (for filtering/matching)
    pub current_vertices: Vec<Vertex>,
    // Current working set of edges
    pub current_edges: Vec<Edge>,
    // NEW: Variable name -> Value (for scalars, strings, numbers from UNWIND)
    pub scalar_rows: Vec<HashMap<String, CypherValue>>,
}

/// Global query context manager for tracking MPI state across sequential statements
pub static GLOBAL_QUERY_CONTEXT: OnceCell<Arc<RwLock<ExecutionContext>>> = OnceCell::const_new();

/// Helper to initialize or retrieve the global context
pub async fn get_global_context() -> Arc<RwLock<ExecutionContext>> {
    GLOBAL_QUERY_CONTEXT
        .get_or_init(|| async {
            Arc::new(RwLock::new(ExecutionContext::default()))
        })
        .await
        .clone()
}

// =================================================================
// IMPL BLOCKS
// =================================================================

impl From<&Value> for CypherValue {
    fn from(v: &Value) -> Self {
        match v {
            Value::Null => CypherValue::Null,
            Value::Bool(b) => CypherValue::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    CypherValue::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    CypherValue::Float(f)
                } else {
                    CypherValue::Null
                }
            }
            Value::String(s) => CypherValue::String(s.clone()),
            Value::Array(arr) => CypherValue::List(arr.iter().map(CypherValue::from).collect()),
            Value::Object(obj) => CypherValue::Map(
                obj.iter()
                    .map(|(k, v)| (k.clone(), CypherValue::from(v)))
                    .collect(),
            ),
        }
    }
}

impl CypherValue {
    pub fn from_json(val: Value) -> Self {
        match val {
            Value::Null => CypherValue::Null,
            Value::Bool(b) => CypherValue::Bool(b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    CypherValue::Integer(i)
                } else {
                    CypherValue::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            Value::String(s) => CypherValue::String(s),
            Value::Array(arr) => {
                CypherValue::List(arr.into_iter().map(Self::from_json).collect())
            }
            Value::Object(obj) => {
                let mut map = HashMap::new();
                for (k, v) in obj {
                    map.insert(k, Self::from_json(v));
                }
                CypherValue::Map(map)
            }
        }
    }

    /// Converts CypherValue to a "flat" JSON value, stripping Enum tags.
    /// This prevents the "Nested objects not supported" error in the storage layer.
    pub fn to_json(&self) -> Value {
        match self {
            CypherValue::Null => Value::Null,
            CypherValue::Bool(b) => Value::Bool(*b),
            CypherValue::Integer(i) => Value::Number((*i).into()),
            CypherValue::Float(f) => serde_json::Number::from_f64(*f)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            CypherValue::String(s) => Value::String(s.clone()),
            // Handle the Uuid variant by converting it to a string
            CypherValue::Uuid(u) => Value::String(u.0.to_string()),
            CypherValue::List(list) => {
                Value::Array(list.iter().map(|v| v.to_json()).collect())
            }
            CypherValue::Map(map) => {
                let mut obj = serde_json::Map::new();
                for (k, v) in map {
                    obj.insert(k.clone(), v.to_json());
                }
                Value::Object(obj)
            }
            CypherValue::Vertex(v) => {
                serde_json::to_value(v).unwrap_or(Value::Null)
            }
            CypherValue::Edge(e) => {
                serde_json::to_value(e).unwrap_or(Value::Null)
            }
        }
    }

    pub fn as_bool(&self) -> bool {
        match self {
            CypherValue::Bool(b) => *b,
            _ => false,
        }
    }
}

impl From<PropertyValue> for CypherValue {
    fn from(pv: PropertyValue) -> Self {
        match pv {
            PropertyValue::Null => CypherValue::Null,
            PropertyValue::Boolean(b) => CypherValue::Bool(b),
            PropertyValue::Integer(i) => CypherValue::Integer(i),
            PropertyValue::I32(i) => CypherValue::Integer(i as i64),
            PropertyValue::Float(f) => CypherValue::Float(f.0),
            PropertyValue::String(s) => CypherValue::String(s),
            PropertyValue::Uuid(u) => CypherValue::String(u.0.to_string()),
            PropertyValue::Byte(b) => CypherValue::Integer(b as i64),

            PropertyValue::DateTime(dt) => {
                // Convert DateTime<Utc> to ISO 8601 / RFC 3339 string
                // This is the standard way Cypher represents datetime values as strings
                CypherValue::String(dt.0.to_rfc3339())
            }

            PropertyValue::List(list) => {
                CypherValue::List(list.into_iter().map(CypherValue::from).collect())
            }

            PropertyValue::Map(map) => {
                let mut cypher_map = std::collections::HashMap::new();
                for (k, v) in map.0 {
                    cypher_map.insert(k.to_string(), CypherValue::from(v));
                }
                CypherValue::Map(cypher_map)
            }

            PropertyValue::Vertex(v) => {
                let mut vertex_map = std::collections::HashMap::new();
                for (k, val) in v.0.properties {
                    vertex_map.insert(k.to_string(), CypherValue::from(val));
                }
                CypherValue::Map(vertex_map)
            }
        }
    }
}

impl EvaluationContext {
    pub fn from_json(value: &serde_json::Value) -> Self {
        let mut variables = std::collections::HashMap::new();
        if let Some(obj) = value.as_object() {
            for (k, v) in obj {
                variables.insert(k.clone(), CypherValue::from_json(v.clone()));
            }
        }
        Self {
            variables,
            parameters: std::collections::HashMap::new(),
        }
    }

    pub fn from_match(graph_match: &GraphMatch) -> Self {
        let mut variables = HashMap::new();
        for (var_name, vertex) in &graph_match.vertices {
            variables.insert(var_name.clone(), CypherValue::Vertex(vertex.clone()));
        }
        for (var_name, edge) in &graph_match.edges {
            variables.insert(var_name.clone(), CypherValue::Edge(edge.clone()));
        }
        Self {
            variables,
            parameters: HashMap::new(),
        }
    }

    pub fn with_parameters(mut self, params: HashMap<String, Value>) -> Self {
        self.parameters = params
            .into_iter()
            .map(|(k, v)| (k, CypherValue::from(&v)))
            .collect();
        self
    }

    pub fn get(&self, name: &str) -> Option<&CypherValue> {
        if name.starts_with('$') {
            self.parameters.get(&name[1..])
        } else {
            self.variables.get(name)
        }
    }

    /// Creates a context from a single Vertex for WHERE clause filtering.
    /// FIX: Added `var_name` parameter to support variable-based lookups (e.g., `n.name`)
    pub fn from_vertex(var_name: &str, vertex: &Vertex) -> Self {
        let mut variables = HashMap::new();
        
        // 1. Register the vertex under the variable name provided in the MATCH clause (e.g., "n")
        // This allows the expression evaluator to resolve `n.name`.
        variables.insert(var_name.to_string(), CypherValue::Vertex(vertex.clone()));

        // 2. Maintain a properties map for generic or internal property access
        let mut properties_map = HashMap::new();
        for (key, val) in &vertex.properties {
            properties_map.insert(key.clone(), CypherValue::from(val.clone()));
        }

        // Standard Cypher: allow access via the generic "properties" name 
        variables.insert("properties".to_string(), CypherValue::Map(properties_map));
        
        Self { 
            variables,
            parameters: HashMap::new(), 
        }
    }

    /// Creates a context from matched query bindings.
    pub fn from_bindings(bindings: &HashMap<String, serde_json::Value>) -> Self {
        let mut variables = HashMap::new();

        for (key, val) in bindings {
            variables.insert(key.clone(), CypherValue::from_json(val.clone()));
        }

        Self { 
            variables,
            parameters: HashMap::new(),
        }
    }

    pub fn from_uuid_bindings(bindings: &HashMap<String, SerializableUuid>) -> Self {
        let mut variables = HashMap::new();

        for (key, uuid) in bindings {
            // Convert the UUID to a string representation for Cypher evaluation
            variables.insert(key.clone(), CypherValue::String(uuid.0.to_string()));
        }

        Self {
            variables,
            parameters: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GraphMatch {
    pub vertices: HashMap<String, Vertex>,
    pub edges: HashMap<String, Edge>,
}

impl GraphMatch {
    pub fn new() -> Self {
        Self {
            vertices: HashMap::new(),
            edges: HashMap::new(),
        }
    }
}

/// Represents the final result of a write operation (CREATE, MERGE, SET, DELETE).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub created_nodes: HashSet<Uuid>,
    pub updated_nodes: HashSet<Uuid>,
    pub deleted_nodes: HashSet<Uuid>,
    pub created_edges: HashSet<Uuid>,
    pub updated_edges: HashSet<Uuid>,
    pub deleted_edges: HashSet<Uuid>,
    pub updated_kv_keys: HashSet<String>,
    // ADDED: Track number of properties modified (e.g., u.count + 1)
    pub properties_set_count: usize,
    pub metadata: HashMap<String, String>,
}

impl ExecutionResult {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_created_node(&mut self, id: Uuid) {
        self.created_nodes.insert(id);
    }

    pub fn add_updated_node(&mut self, id: Uuid) {
        self.updated_nodes.insert(id);
    }

    pub fn add_deleted_node(&mut self, id: Uuid) {
        self.deleted_nodes.insert(id);
    }

    pub fn add_created_edge(&mut self, id: Uuid) {
        self.created_edges.insert(id);
    }

    pub fn add_updated_edge(&mut self, id: Uuid) {
        self.updated_edges.insert(id);
    }

    pub fn add_deleted_edge(&mut self, id: Uuid) {
        self.deleted_edges.insert(id);
    }

    pub fn add_updated_kv_key(&mut self, key: String) {
        self.updated_kv_keys.insert(key);
    }

    // ADDED: Helper to increment property count
    pub fn inc_properties_set(&mut self) {
        self.properties_set_count += 1;
    }

    pub fn has_mutations(&self) -> bool {
        !(self.created_nodes.is_empty()
            && self.updated_nodes.is_empty()
            && self.deleted_nodes.is_empty()
            && self.created_edges.is_empty()
            && self.updated_edges.is_empty()
            && self.deleted_edges.is_empty()
            && self.updated_kv_keys.is_empty()
            && self.properties_set_count == 0) // Updated to include prop count
    }

    pub fn created_count(&self) -> usize {
        self.created_nodes.len() + self.created_edges.len()
    }

    pub fn updated_count(&self) -> usize {
        self.updated_nodes.len() + self.updated_edges.len()
    }

    pub fn add_metadata(&mut self, key: &str, value: String) {
        self.metadata.insert(key.to_string(), value);
    }

    pub fn extend(&mut self, other: ExecutionResult) {
        self.created_nodes.extend(other.created_nodes);
        self.updated_nodes.extend(other.updated_nodes);
        self.deleted_nodes.extend(other.deleted_nodes);
        self.created_edges.extend(other.created_edges);
        self.updated_edges.extend(other.updated_edges);
        self.deleted_edges.extend(other.deleted_edges);
        self.updated_kv_keys.extend(other.updated_kv_keys);
        self.properties_set_count += other.properties_set_count;
        // ADDED: Merge metadata
        self.metadata.extend(other.metadata);
    }
}

/// Represents a single change operation that needs to be persisted to the database.
#[derive(Debug, Clone, PartialEq)]
pub enum Mutation {
    CreateNode(Uuid),
    UpdateNode(Uuid),
    DeleteNode(Uuid),
    CreateEdge(Uuid),
    UpdateEdge(Uuid),
    DeleteEdge(Uuid),
    SetKeyValue(String),
    DeleteKeyValue(String),
    CreateIndex(String),
    DropIndex(String),
}

impl WhereClause {
    pub fn evaluate(&self, ctx: &EvaluationContext) -> GraphResult<bool> {
        // 1. Evaluate the inner expression tree
        let result = self.condition.evaluate(ctx)?;

        // 2. Convert CypherValue to bool (Cypher truthiness)
        match result {
            CypherValue::Bool(b) => Ok(b),
            CypherValue::Null => Ok(false), // WHERE NULL is false
            _ => {
                // Technically, Cypher treats non-booleans in WHERE as false/null,
                // but logging this can help with debugging query logic.
                Ok(false)
            }
        }
    }
}

impl Expression {
    pub fn evaluate(&self, ctx: &EvaluationContext) -> GraphResult<CypherValue> {
        match self {
            Expression::Literal(val) => Ok(val.clone()),
            Expression::Variable(name) => ctx
                .variables
                .get(name)
                .cloned()
                .ok_or_else(|| GraphError::EvaluationError(format!("Variable '{name}' not found"))),
            Expression::List(elements) => {
                let mut evaluated_elements = Vec::with_capacity(elements.len());
                for expr in elements {
                    evaluated_elements.push(expr.evaluate(ctx)?);
                }
                Ok(CypherValue::List(evaluated_elements))
            },
            Expression::Property(access) => match access {
                PropertyAccess::Vertex(var, prop) => {
                    if let Some(CypherValue::Vertex(v)) = ctx.variables.get(var) {
                        // Special handling for metadata fields to support MPI golden record tracing
                        if prop == "id" {
                            Ok(CypherValue::String(v.id.to_string()))
                        } else if prop == "label" {
                            Ok(CypherValue::String(v.label.to_string()))
                        } else {
                            Ok(v.properties
                                .get(prop)
                                .map(property_value_to_cypher)
                                .unwrap_or(CypherValue::Null))
                        }
                    } else {
                        Err(GraphError::EvaluationError(format!("Vertex variable '{var}' not found")))
                    }
                }
                PropertyAccess::Edge(var, prop) => {
                    if let Some(CypherValue::Edge(e)) = ctx.variables.get(var) {
                        if prop == "id" {
                            Ok(CypherValue::String(e.id.to_string()))
                        } else {
                            Ok(e.properties
                                .get(prop)
                                .map(property_value_to_cypher)
                                .unwrap_or(CypherValue::Null))
                        }
                    } else {
                        Err(GraphError::EvaluationError(format!("Edge variable '{var}' not found")))
                    }
                }
                PropertyAccess::Parameter(name) => ctx
                    .parameters
                    .get(name)
                    .cloned()
                    .ok_or_else(|| GraphError::EvaluationError(format!("Parameter '${name}' not provided"))),
            },
            Expression::DynamicPropertyAccess { variable, property_expr } => {
                let target_val = ctx.variables.get(variable)
                    .ok_or_else(|| GraphError::EvaluationError(format!("Variable '{}' not found", variable)))?;
                
                let prop_name_val = property_expr.evaluate(ctx)?;
                let prop_name = match prop_name_val {
                    CypherValue::String(s) => s,
                    CypherValue::Null => return Ok(CypherValue::Null),
                    _ => return Err(GraphError::EvaluationError("Property name must be a string".into())),
                };
                
                match target_val {
                    CypherValue::Vertex(v) => {
                        match v.properties.get(&prop_name) {
                            Some(prop_val) => Ok(property_value_to_cypher(prop_val)),
                            None => Ok(CypherValue::Null),
                        }
                    },
                    CypherValue::Edge(e) => {
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
                    _ => Err(GraphError::EvaluationError(format!("Cannot access properties on variable '{}'", variable))),
                }
            },
            Expression::ArrayIndex { expr, index } => {
                let array_val = expr.evaluate(ctx)?;
                let index_val = index.evaluate(ctx)?;
                
                let idx = match index_val {
                    CypherValue::Integer(i) => i as usize,
                    CypherValue::Float(f) => f as usize,
                    _ => return Err(GraphError::EvaluationError("Array index must be a number".into())),
                };
                
                match array_val {
                    CypherValue::List(items) => {
                        if idx < items.len() {
                            Ok(items[idx].clone())
                        } else {
                            Ok(CypherValue::Null)
                        }
                    }
                    _ => Err(GraphError::EvaluationError("Cannot index non-list value".into())),
                }
            },
            Expression::LabelPredicate { variable, label } => {
                if let Some(val) = ctx.variables.get(variable) {
                    match val {
                        CypherValue::Vertex(v) => {
                            Ok(CypherValue::Bool(v.label.to_string() == *label))
                        }
                        _ => Ok(CypherValue::Bool(false)),
                    }
                } else {
                    Err(GraphError::EvaluationError(format!(
                        "Variable '{variable}' not found for label check"
                    )))
                }
            },
            Expression::Binary { op, left, right } => {
                let l = left.evaluate(ctx)?;
                let r = right.evaluate(ctx)?;
                op.apply(&l, &r)
            },
            Expression::Unary { op, expr } => {
                let val = expr.evaluate(ctx)?;
                match op {
                    UnaryOp::Not => {
                        if let CypherValue::Bool(b) = val { 
                            Ok(CypherValue::Bool(!b)) 
                        } else { 
                            Err(GraphError::EvaluationError("NOT requires boolean".into())) 
                        }
                    },
                    UnaryOp::Neg => {
                        match val {
                            CypherValue::Integer(i) => Ok(CypherValue::Integer(-i)),
                            CypherValue::Float(f) => Ok(CypherValue::Float(-f)),
                            _ => Err(GraphError::EvaluationError("Negative requires numeric".into())),
                        }
                    },
                    UnaryOp::IsNotNull => Ok(CypherValue::Bool(!matches!(val, CypherValue::Null))),
                    UnaryOp::IsNull => Ok(CypherValue::Bool(matches!(val, CypherValue::Null))),
                }
            },
            Expression::Not(expr) => {
                // Correctly using 'ctx' from the function signature
                let val = expr.evaluate(ctx)?;
                if let CypherValue::Bool(b) = val {
                    Ok(CypherValue::Bool(!b))
                } else {
                    Err(GraphError::EvaluationError("NOT requires boolean".into()))
                }
            }, 
            Expression::Predicate { name, variable, list, condition } => {
                let list_val = list.evaluate(ctx)?;
                if let CypherValue::List(elements) = list_val {
                    match name.to_uppercase().as_str() {
                        "ANY" => {
                            for item in elements {
                                let mut nested_vars = ctx.variables.clone();
                                nested_vars.insert(variable.clone(), item);
                                let nested_ctx = EvaluationContext { 
                                    variables: nested_vars, 
                                    parameters: ctx.parameters.clone() 
                                };
                                if let CypherValue::Bool(true) = condition.evaluate(&nested_ctx)? {
                                    return Ok(CypherValue::Bool(true));
                                }
                            }
                            Ok(CypherValue::Bool(false))
                        },
                        "ALL" => {
                            for item in elements {
                                let mut nested_vars = ctx.variables.clone();
                                nested_vars.insert(variable.clone(), item);
                                let nested_ctx = EvaluationContext { 
                                    variables: nested_vars, 
                                    parameters: ctx.parameters.clone() 
                                };
                                if !matches!(condition.evaluate(&nested_ctx)?, CypherValue::Bool(true)) {
                                    return Ok(CypherValue::Bool(false));
                                }
                            }
                            Ok(CypherValue::Bool(true))
                        },
                        _ => Err(GraphError::EvaluationError(format!("Unsupported predicate: {name}")))
                    }
                } else {
                    Ok(CypherValue::Null)
                }
            },
            Expression::FunctionCall { name, args } => {
                match name.to_uppercase().as_str() {
                   "ID" => {
                        // Evaluates the argument first to handle both ID(n) and ID(existing_id_var)
                        let val = args.get(0)
                            .ok_or_else(|| GraphError::EvaluationError("ID() requires 1 argument".into()))?
                            .evaluate(ctx)?;
                        match val {
                            CypherValue::Vertex(v) => Ok(CypherValue::String(v.id.to_string())),
                            CypherValue::Edge(e) => Ok(CypherValue::String(e.id.to_string())),
                            CypherValue::Uuid(u) => Ok(CypherValue::String(u.to_string())),
                            CypherValue::String(s) => Ok(CypherValue::String(s)),
                            _ => Ok(CypherValue::Null),
                        }
                    },
                    "KEYS" => {
                        let val = args.get(0)
                            .ok_or_else(|| GraphError::EvaluationError("keys() requires 1 argument".into()))?
                            .evaluate(ctx)?;
                        match val {
                            CypherValue::Vertex(v) => {
                                // 1. Get all keys from the dynamic properties map
                                let mut key_list: Vec<CypherValue> = v.properties.keys()
                                    .map(|k| CypherValue::String(k.clone()))
                                    .collect();
                                
                                // 2. Add the virtual/hardcoded metadata keys
                                // Ensuring "id" and "label" (or "event_type") are visible to UNWIND
                                key_list.push(CypherValue::String("id".to_string()));
                                key_list.push(CypherValue::String("label".to_string()));
                                
                                Ok(CypherValue::List(key_list))
                            },
                            CypherValue::Edge(e) => {
                                let mut key_list: Vec<CypherValue> = e.properties.keys()
                                    .map(|k| CypherValue::String(k.clone()))
                                    .collect();
                                key_list.push(CypherValue::String("id".to_string()));
                                Ok(CypherValue::List(key_list))
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
                    "LABELS" => {
                        let val = args.get(0)
                            .ok_or_else(|| GraphError::EvaluationError("LABELS() requires 1 argument".into()))?
                            .evaluate(ctx)?;
                        match val {
                            CypherValue::Vertex(v) => Ok(CypherValue::List(vec![
                                // FIX: Use to_string() on the label Identifier
                                CypherValue::String(v.label.to_string())
                            ])),
                            _ => Ok(CypherValue::Null),
                        }
                    },
                    "COUNT" => {
                        // Aggregation COUNT(*) is usually handled in the ReturnStatement grouping logic,
                        // but this handles COUNT(list_var)
                        let val = args.get(0)
                            .ok_or_else(|| GraphError::EvaluationError("count() requires 1 argument".into()))?
                            .evaluate(ctx)?;
                        match val {
                            CypherValue::List(l) => Ok(CypherValue::Integer(l.len() as i64)),
                            CypherValue::Null => Ok(CypherValue::Integer(0)),
                            _ => Ok(CypherValue::Integer(1)),
                        }
                    },
                    "TYPE" => {
                        let val = args.get(0)
                            .ok_or_else(|| GraphError::EvaluationError("TYPE() requires 1 argument".into()))?
                            .evaluate(ctx)?;
                        match val {
                            CypherValue::Edge(e) => Ok(CypherValue::String(e.label.to_string())),
                            _ => Ok(CypherValue::Null),
                        }
                    },
                    "PROPERTIES" => {
                        let val = args.get(0)
                            .ok_or_else(|| GraphError::EvaluationError("properties() requires 1 argument".into()))?
                            .evaluate(ctx)?;
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
                    "LEVENSHTEIN" => {
                        if args.len() != 2 {
                            return Err(GraphError::EvaluationError(
                                "levenshtein() requires exactly 2 string arguments".into()
                            ));
                        }
                        let left = args[0].evaluate(ctx)?;
                        let right = args[1].evaluate(ctx)?;
                        let s1 = match left {
                            CypherValue::String(s) => s,
                            CypherValue::Null => return Ok(CypherValue::Null),
                            _ => return Err(GraphError::EvaluationError("levenshtein(): first argument must be string".into())),
                        };
                        let s2 = match right {
                            CypherValue::String(s) => s,
                            CypherValue::Null => return Ok(CypherValue::Null),
                            _ => return Err(GraphError::EvaluationError("levenshtein(): second argument must be string".into())),
                        };
                        let distance = strsim::levenshtein(&s1, &s2) as i64;
                        Ok(CypherValue::Integer(distance))
                    },
                    "STRING" => {
                        if args.len() != 1 {
                            return Err(GraphError::EvaluationError("string() requires exactly 1 argument".into()));
                        }
                        let arg_val = args[0].evaluate(ctx)?;
                        match arg_val {
                            CypherValue::String(s) => Ok(CypherValue::String(s)),
                            CypherValue::Integer(i) => Ok(CypherValue::String(i.to_string())),
                            CypherValue::Float(f) => Ok(CypherValue::String(f.to_string())),
                            CypherValue::Bool(b) => Ok(CypherValue::String(b.to_string())),
                            CypherValue::Uuid(u) => Ok(CypherValue::String(u.to_string())),
                            CypherValue::Vertex(v) => Ok(CypherValue::String(v.id.to_string())),
                            CypherValue::Edge(e) => Ok(CypherValue::String(e.id.to_string())),
                            CypherValue::Null => Ok(CypherValue::Null),
                            _ => Err(GraphError::EvaluationError("string() cannot convert this type".into())),
                        }
                    },
                    _ => Err(GraphError::EvaluationError(format!("Unknown function: {}", name)))
                }
            }
            Expression::Regex { variable, property, pattern } => {
                let target = ctx.variables.get(variable)
                    .ok_or_else(|| GraphError::EvaluationError(format!("Variable '{variable}' not found")))?;
                
                let prop_val = match target {
                    CypherValue::Vertex(v) => {
                        if property == "id" { CypherValue::String(v.id.to_string()) }
                        else { v.properties.get(property).map(property_value_to_cypher).unwrap_or(CypherValue::Null) }
                    },
                    CypherValue::Edge(e) => {
                        if property == "id" { CypherValue::String(e.id.to_string()) }
                        else { e.properties.get(property).map(property_value_to_cypher).unwrap_or(CypherValue::Null) }
                    },
                    _ => return Err(GraphError::EvaluationError(format!("Variable '{variable}' is not a Vertex or Edge"))),
                };

                match prop_val {
                    CypherValue::String(s) => {
                        let re = regex::Regex::new(pattern)
                            .map_err(|e| GraphError::EvaluationError(format!("Invalid regex pattern: {}", e)))?;
                        Ok(CypherValue::Bool(re.is_match(&s)))
                    },
                    _ => Ok(CypherValue::Bool(false)),
                }
            }
            Expression::PropertyComparison { variable, property, operator, value } => {
                let var_value = ctx.variables.get(variable)
                    .ok_or_else(|| GraphError::QueryError(format!("Variable '{}' not found", variable)))?;

                let left_hand_side = if property.is_empty() {
                    var_value.clone()
                } else {
                    match var_value {
                        CypherValue::Vertex(v) => {
                            if property == "id" { CypherValue::String(v.id.to_string()) }
                            else { v.properties.get(property).map(|pv| CypherValue::from(pv.clone())).unwrap_or(CypherValue::Null) }
                        },
                        CypherValue::Edge(e) => {
                            if property == "id" { CypherValue::String(e.id.to_string()) }
                            else { e.properties.get(property).map(|pv| CypherValue::from(pv.clone())).unwrap_or(CypherValue::Null) }
                        },
                        _ => return Err(GraphError::QueryError(format!("Variable '{}' is not a Vertex or Edge", variable))),
                    }
                };

                let right_hand_side = CypherValue::from_json(value.clone());
                evaluate_comparison(&left_hand_side, operator, &right_hand_side)
            }
            Expression::StartsWith { variable, property, prefix } => {
                let val = self.get_target_property(ctx, variable, property)?;
                match val {
                    CypherValue::String(s) => Ok(CypherValue::Bool(s.starts_with(prefix))),
                    _ => Ok(CypherValue::Bool(false)),
                }
            }
            Expression::EndsWith { variable, property, suffix } => {
                let val = self.get_target_property(ctx, variable, property)?;
                match val {
                    CypherValue::String(s) => Ok(CypherValue::Bool(s.ends_with(suffix))),
                    _ => Ok(CypherValue::Bool(false)),
                }
            }
            Expression::Contains { variable, property, substring } => {
                let val = self.get_target_property(ctx, variable, property)?;
                match val {
                    CypherValue::String(s) => Ok(CypherValue::Bool(s.contains(substring))),
                    _ => Ok(CypherValue::Bool(false)),
                }
            }
            Expression::FunctionComparison { function, argument, operator, value } => {
                let left_val = match function.to_uppercase().as_str() {
                    "ID" => {
                        let target = ctx.variables.get(argument)
                            .ok_or_else(|| GraphError::EvaluationError(format!("Variable '{argument}' not found")))?;
                        match target {
                            CypherValue::Vertex(v) => CypherValue::String(v.id.to_string()),
                            CypherValue::Edge(e) => CypherValue::String(e.id.to_string()),
                            CypherValue::Uuid(u) => CypherValue::String(u.to_string()),
                            _ => return Err(GraphError::EvaluationError("ID() needs Vertex/Edge/Uuid".to_string())),
                        }
                    }
                    _ => return Err(GraphError::EvaluationError(format!("Unknown function: {function}"))),
                };

                let op_upper = operator.to_uppercase();
                if op_upper == "IS NOT NULL" {
                    return Ok(CypherValue::Bool(!matches!(left_val, CypherValue::Null)));
                } else if op_upper == "IS NULL" {
                    return Ok(CypherValue::Bool(matches!(left_val, CypherValue::Null)));
                }

                let right_val = if value.is_string() {
                    CypherValue::String(value.as_str().unwrap_or("").to_string())
                } else {
                    CypherValue::String(value.to_string().trim_matches('"').to_string())
                };

                evaluate_comparison(&left_val, operator, &right_val)
            }
            Expression::And { left, right } => {
                let l_val = left.evaluate(ctx)?;
                let r_val = right.evaluate(ctx)?;
                match (l_val, r_val) {
                    (CypherValue::Bool(l), CypherValue::Bool(r)) => Ok(CypherValue::Bool(l && r)),
                    (CypherValue::Bool(false), _) | (_, CypherValue::Bool(false)) => Ok(CypherValue::Bool(false)),
                    (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Null),
                    _ => Err(GraphError::EvaluationError("Logical AND requires boolean operands".into())),
                }
            }
            Expression::Or { left, right } => {
                let l_val = left.evaluate(ctx)?;
                let r_val = right.evaluate(ctx)?;
                match (l_val, r_val) {
                    (CypherValue::Bool(l), CypherValue::Bool(r)) => Ok(CypherValue::Bool(l || r)),
                    (CypherValue::Bool(true), _) | (_, CypherValue::Bool(true)) => Ok(CypherValue::Bool(true)),
                    (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Null),
                    _ => Err(GraphError::EvaluationError("Logical OR requires boolean operands".into())),
                }
            }
            Expression::IsNull(expr) => {
                let val = expr.evaluate(ctx)?;
                Ok(CypherValue::Bool(matches!(val, CypherValue::Null)))
            },

            Expression::IsNotNull(expr) => {
                let val = expr.evaluate(ctx)?;
                Ok(CypherValue::Bool(!matches!(val, CypherValue::Null)))
            },
        }
    }

    /// Internal helper to fetch vertex/edge properties for string comparisons
    fn get_target_property(&self, ctx: &EvaluationContext, var: &str, prop: &str) -> GraphResult<CypherValue> {
        let target = ctx.variables.get(var)
            .ok_or_else(|| GraphError::EvaluationError(format!("Variable '{var}' not found")))?;
        
        match target {
            CypherValue::Vertex(v) => {
                if prop == "id" { Ok(CypherValue::String(v.id.to_string())) }
                else { Ok(v.properties.get(prop).map(property_value_to_cypher).unwrap_or(CypherValue::Null)) }
            },
            CypherValue::Edge(e) => {
                if prop == "id" { Ok(CypherValue::String(e.id.to_string())) }
                else { Ok(e.properties.get(prop).map(property_value_to_cypher).unwrap_or(CypherValue::Null)) }
            },
            _ => Err(GraphError::EvaluationError(format!("Variable '{var}' is not a graph element"))),
        }
    }
}

impl BinaryOp {
    pub fn apply(&self, left: &CypherValue, right: &CypherValue) -> GraphResult<CypherValue> {
        match self {
            BinaryOp::Eq => Ok(CypherValue::Bool(left == right)),
            BinaryOp::Neq => Ok(CypherValue::Bool(left != right)),
            BinaryOp::Lt => compare(left, right, |a, b| a < b),
            BinaryOp::Lte => compare(left, right, |a, b| a <= b),
            BinaryOp::Gt => compare(left, right, |a, b| a > b),
            BinaryOp::Gte => compare(left, right, |a, b| a >= b),
            BinaryOp::And => {
                match (left, right) {
                    (CypherValue::Bool(false), _) | (_, CypherValue::Bool(false)) => Ok(CypherValue::Bool(false)),
                    (CypherValue::Bool(true), CypherValue::Bool(true)) => Ok(CypherValue::Bool(true)),
                    _ => Ok(CypherValue::Null),
                }
            },
            BinaryOp::Or => {
                match (left, right) {
                    (CypherValue::Bool(true), _) | (_, CypherValue::Bool(true)) => Ok(CypherValue::Bool(true)),
                    (CypherValue::Bool(false), CypherValue::Bool(false)) => Ok(CypherValue::Bool(false)),
                    _ => Ok(CypherValue::Null),
                }
            },
            BinaryOp::Xor => {
                match (left, right) {
                    (CypherValue::Bool(l), CypherValue::Bool(r)) => Ok(CypherValue::Bool(l ^ r)),
                    _ => Ok(CypherValue::Null),
                }
            },
            BinaryOp::Plus => add(left, right),
            BinaryOp::Minus => subtract(left, right),
            BinaryOp::Mul => multiply(left, right),
            BinaryOp::Div => divide(left, right),
            BinaryOp::Mod => {
                if let (CypherValue::Integer(l), CypherValue::Integer(r)) = (left, right) {
                    if *r == 0 { return Err(GraphError::EvaluationError("Division by zero".into())); }
                    Ok(CypherValue::Integer(l % r))
                } else {
                    Err(GraphError::EvaluationError("Modulo requires integers".into()))
                }
            },
            BinaryOp::In => {
                match right {
                    CypherValue::List(list) => Ok(CypherValue::Bool(list.contains(left))),
                    CypherValue::Null => Ok(CypherValue::Null),
                    _ => Err(GraphError::EvaluationError("Right side of IN must be a list".into()))
                }
            },
            BinaryOp::NotIn => {
                match right {
                    CypherValue::List(list) => Ok(CypherValue::Bool(!list.contains(left))),
                    CypherValue::Null => Ok(CypherValue::Null),
                    _ => Err(GraphError::EvaluationError("Right side of NOT IN must be a list".into()))
                }
            },
            BinaryOp::Contains => {
                match (left, right) {
                    (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Bool(false)),
                    (l, r) => {
                        let left_str = l.to_string();
                        let right_str = r.to_string();
                        println!("====> CHECKING  IF {:?} CONTAINS {:?}", left_str, right_str);
                        // This is the line that was missing or failing:
                        Ok(CypherValue::Bool(left_str.contains(&right_str)))
                    }
                }
            },
            BinaryOp::StartsWith => {
                match (left, right) {
                    (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Bool(false)),
                    (l, r) => Ok(CypherValue::Bool(l.to_string().starts_with(&r.to_string()))),
                }
            },
            BinaryOp::EndsWith => {
                match (left, right) {
                    (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Bool(false)),
                    (l, r) => Ok(CypherValue::Bool(l.to_string().ends_with(&r.to_string()))),
                }
            },
            BinaryOp::Regex => {
                match (left, right) {
                    (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Null),
                    (l, r) => {
                        let s = l.to_string();
                        let p = r.to_string();
                        let re = regex::Regex::new(&p).map_err(|e| GraphError::EvaluationError(e.to_string()))?;
                        Ok(CypherValue::Bool(re.is_match(&s)))
                    },
                }
            }
        }
    }
}

impl UnaryOp {
    pub fn apply(&self, val: &CypherValue) -> GraphResult<CypherValue> {
        match self {
            UnaryOp::Not => Ok(CypherValue::Bool(!to_bool(val)?)),
            UnaryOp::Neg => match val {
                CypherValue::Integer(i) => Ok(CypherValue::Integer(-i)),
                CypherValue::Float(f) => Ok(CypherValue::Float(-f)),
                _ => Err(GraphError::EvaluationError("Cannot negate non-numeric value".into())),
            },
            // --- FIXED: Handling the new variants ---
            UnaryOp::IsNotNull => {
                Ok(CypherValue::Bool(!matches!(val, CypherValue::Null)))
            },
            UnaryOp::IsNull => {
                Ok(CypherValue::Bool(matches!(val, CypherValue::Null)))
            },
        }
    }
}

impl From<CypherValue> for PropertyValue {
    fn from(cv: CypherValue) -> Self {
        match cv {
            CypherValue::Null => PropertyValue::Null,
            CypherValue::Bool(b) => PropertyValue::Boolean(b),
            CypherValue::Integer(i) => PropertyValue::Integer(i),
            CypherValue::Float(f) => PropertyValue::Float(SerializableFloat(f)),
            CypherValue::String(s) => PropertyValue::String(s),
            CypherValue::Uuid(u) => PropertyValue::Uuid(u),
            CypherValue::List(l) => {
                PropertyValue::List(l.into_iter().map(PropertyValue::from).collect())
            }
            CypherValue::Map(m) => {
                // Use .as_str().into() to satisfy Identifier::from(&str)
                let p_map: HashMap<models::Identifier, PropertyValue> = m
                    .into_iter()
                    .map(|(k, v)| (k.as_str().into(), PropertyValue::from(v)))
                    .collect();
                
                PropertyValue::Map(HashablePropertyMap(p_map))
            }
            _ => PropertyValue::Null,
        }
    }
}

impl PartialEq for CypherValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (CypherValue::Null, CypherValue::Null) => true,
            (CypherValue::Bool(a), CypherValue::Bool(b)) => a == b,
            (CypherValue::Integer(a), CypherValue::Integer(b)) => a == b,
            (CypherValue::String(a), CypherValue::String(b)) => a == b,
            (CypherValue::Uuid(a), CypherValue::Uuid(b)) => a == b,

            // Cross-type comparison: Uuid and String
            (CypherValue::Uuid(a), CypherValue::String(b)) => a.to_string() == *b,
            (CypherValue::String(a), CypherValue::Uuid(b)) => *a == b.to_string(),

            // Fallback for different variants
            _ => false,
        }
    }
}

pub fn expression_to_cypher(expr: &Expression) -> String {
    match expr {
        Expression::Literal(value) => literal_to_cypher(value),
        
        Expression::Property(prop) => match prop {
            PropertyAccess::Vertex(var, p) | PropertyAccess::Edge(var, p) => format!("{}.{}", var, p),
            PropertyAccess::Parameter(name) => format!("${}", name),
        },
            
        Expression::Variable(name) => name.clone(),
        
        Expression::List(items) => {
            let item_strs: Vec<String> = items.iter()
                .map(|i| expression_to_cypher(i))
                .collect();
            format!("[{}]", item_strs.join(", "))
        },
        
        Expression::DynamicPropertyAccess { variable, property_expr } => {
            let prop_str = expression_to_cypher(property_expr);
            format!("{}[{}]", variable, prop_str)
        },
        Expression::ArrayIndex { expr, index } => {
            let expr_str = expression_to_cypher(expr);
            let index_str = expression_to_cypher(index);
            format!("{}[{}]", expr_str, index_str)
        },
        Expression::Binary { op, left, right } => {
            let left_str = expression_to_cypher(left);
            let right_str = expression_to_cypher(right);
            format!("({} {} {})", left_str, op, right_str)
        },
        
        Expression::Unary { op, expr } => {
            let expr_str = expression_to_cypher(expr);
            match op {
                UnaryOp::Not => format!("NOT {}", expr_str),
                UnaryOp::Neg => format!("-{}", expr_str),
                UnaryOp::IsNull => format!("{} IS NULL", expr_str),
                UnaryOp::IsNotNull => format!("{} IS NOT NULL", expr_str),
            }
        },
        
        Expression::LabelPredicate { variable, label } => {
            format!("{}:{}", variable, label)
        },
        
        Expression::Predicate { name, variable, list, condition } => {
            let list_str = expression_to_cypher(list);
            let cond_str = expression_to_cypher(condition);
            format!("{}({} IN {} WHERE {})", name.to_uppercase(), variable, list_str, cond_str)
        },
        
        Expression::FunctionCall { name, args } => {
            let arg_strs: Vec<String> = args.iter()
                .map(|arg| expression_to_cypher(arg))
                .collect();
            format!("{}({})", name, arg_strs.join(", "))
        },
        
        Expression::PropertyComparison { variable, property, operator, value } => {
            let val_str = serde_json::to_string(value).unwrap_or_default();
            format!("{}.{} {} {}", variable, property, operator, val_str)
        },
        
        Expression::FunctionComparison { function, argument, operator, value } => {
            let val_str = serde_json::to_string(value).unwrap_or_default();
            format!("{}({}) {} {}", function, argument, operator, val_str)
        },

        Expression::Regex { variable, property, pattern } => {
            format!("{}.{} =~ '{}'", variable, property, pattern)
        },
        
        Expression::StartsWith { variable, property, prefix } => {
            format!("{}.{} STARTS WITH '{}'", variable, property, prefix)
        },

        // --- FIXED: Added missing arms ---
        Expression::EndsWith { variable, property, suffix } => {
            format!("{}.{} ENDS WITH '{}'", variable, property, suffix)
        },
        
        Expression::Contains { variable, property, substring } => {
            format!("{}.{} CONTAINS '{}'", variable, property, substring)
        },
        Expression::Not(expr) => {
            let expr_str = expression_to_cypher(expr);
            format!("NOT ({})", expr_str)
        },

        Expression::IsNull(expr) => {
            let expr_str = expression_to_cypher(expr);
            format!("{} IS NULL", expr_str)
        },

        Expression::IsNotNull(expr) => {
            let expr_str = expression_to_cypher(expr);
            format!("{} IS NOT NULL", expr_str)
        },
        Expression::And { left, right } => {
            let left_str = expression_to_cypher(left);
            let right_str = expression_to_cypher(right);
            format!("({} AND {})", left_str, right_str)
        },
        
        Expression::Or { left, right } => {
            let left_str = expression_to_cypher(left);
            let right_str = expression_to_cypher(right);
            format!("({} OR {})", left_str, right_str)
        },
    }
}

pub fn literal_to_cypher(value: &CypherValue) -> String {
    match value {
        CypherValue::Null => "null".to_string(),
        CypherValue::Bool(b) => b.to_string(),
        CypherValue::Integer(i) => i.to_string(),
        CypherValue::Float(f) => f.to_string(),
        CypherValue::String(s) => format!("'{}'", s.replace("'", "''")),
        CypherValue::Uuid(uuid) => format!("'{}'", uuid.0.to_string()),
        CypherValue::List(items) => {
            let item_strs: Vec<String> = items.iter()
                .map(|item| literal_to_cypher(item))
                .collect();
            format!("[{}]", item_strs.join(", "))
        },
        // For Vertex/Edge, return a placeholder (shouldn't occur in WHERE clauses)
        _ => "/* UNSUPPORTED LITERAL */".to_string(),
    }
}

pub fn property_value_to_cypher(pv: &PropertyValue) -> CypherValue {
    match pv {
        PropertyValue::Null => CypherValue::Null,
        PropertyValue::Boolean(b) => CypherValue::Bool(*b),
        PropertyValue::Integer(i) => CypherValue::Integer(*i),
        PropertyValue::I32(i) => CypherValue::Integer(*i as i64),
        PropertyValue::Float(f) => CypherValue::Float(f.0),
        PropertyValue::String(s) => CypherValue::String(s.clone()),
        PropertyValue::Uuid(u) => CypherValue::Uuid(u.clone()),
        PropertyValue::Byte(b) => CypherValue::Integer(*b as i64),

        PropertyValue::DateTime(dt) => {
            // Standardizing on RFC 3339 for MPI traceability
            CypherValue::String(dt.0.to_rfc3339())
        }

        PropertyValue::List(list) => {
            CypherValue::List(list.iter().map(property_value_to_cypher).collect())
        }

        PropertyValue::Map(map) => {
            let new_map: HashMap<String, CypherValue> = map
                .0
                .iter()
                .map(|(k, v)| (k.to_string(), property_value_to_cypher(v)))
                .collect();
            CypherValue::Map(new_map)
        }

        // FIX: Access the inner Vertex through the UnhashableVertex wrapper
        PropertyValue::Vertex(v) => {
            // v is &UnhashableVertex. 
            // v.0 is the inner Vertex.
            // v.0.id is the ID (likely a UUID or similar string-convertible type).
            CypherValue::String(v.0.id.to_string())
        },
    }
}

pub fn cypher_serialize(query: CypherQuery) -> String {
    match query {
        CypherQuery::MatchPattern { patterns, where_clause, with_clause, return_clause } => {
            let mut parts = Vec::new();
            
            // Build MATCH patterns
            let pattern_strs: Vec<String> = patterns.iter()
                .map(|(var, nodes, rels)| {
                    // Serialize nodes
                    let node_strs: Vec<String> = nodes.iter()
                        .map(|(v, labels, props)| {
                            let var_part = v.as_ref().map(|s| s.clone()).unwrap_or_else(|| "_".to_string());
                            let label_part = if !labels.is_empty() {
                                format!(":{}", labels.join(":"))
                            } else { "".to_string() };
                            
                            let prop_part = if !props.is_empty() {
                                let props_str: Vec<String> = props.iter()
                                    .map(|(k, v)| {
                                        format!("{}: {}", k, serde_json::to_string(v).unwrap_or_default())
                                    })
                                    .collect();
                                format!(" {{{}}}", props_str.join(", "))
                            } else { "".to_string() };
                            
                            format!("({}{})", var_part, label_part) + &prop_part
                        })
                        .collect();
                    
                    // Serialize relationships (simplified)
                    let mut full_pattern = node_strs.join("-");
                    if !rels.is_empty() {
                        // Add relationship patterns if needed
                    }
                    
                    full_pattern
                })
                .collect();
            
            parts.push(format!("MATCH {}", pattern_strs.join(", ")));
            
            // Add WHERE clause
            if let Some(wc) = where_clause {
                parts.push(format!("WHERE {}", expression_to_cypher(&wc.condition)));
            }
            
            // Add WITH clause
            if let Some(with) = with_clause {
                let items: Vec<String> = with.items.iter()
                    .map(|item| {
                        if let Some(alias) = &item.alias {
                            format!("{} AS {}", item.expression, alias)
                        } else {
                            item.expression.clone()
                        }
                    })
                    .collect();
                parts.push(format!("WITH {}", items.join(", ")));
                if let Some(wc) = &with.where_clause {
                    parts.push(format!("WHERE {}", expression_to_cypher(&wc.condition)));
                }
            }
            
            // Add RETURN clause
            if let Some(ret) = return_clause {
                let items: Vec<String> = ret.items.iter()
                    .map(|item| {
                        if let Some(alias) = &item.alias {
                            format!("{} AS {}", item.expression, alias)
                        } else {
                            item.expression.clone()
                        }
                    })
                    .collect();
                parts.push(format!("RETURN {}", items.join(", ")));
                
                if let Some(skip) = ret.skip {
                    parts.push(format!("SKIP {}", skip));
                }
                if let Some(limit) = ret.limit {
                    parts.push(format!("LIMIT {}", limit));
                }
            }
            
            parts.join(" ")
        }
        
        // Handle UNWIND
        CypherQuery::Unwind { expression, variable } => {
            format!("UNWIND {} AS {}", expression_to_cypher(&expression), variable)
        }
        
        // Add other query types as needed
        _ => format!("/* UNSUPPORTED: {:?} */", query),
    }
}

/// Helper to unify property fetching for string matching
pub fn get_property_for_expression(
    ctx: &EvaluationContext, 
    var: &str, 
    prop: &str
) -> StdResult<CypherValue, GraphError> {
    let target = ctx.variables.get(var)
        .ok_or_else(|| GraphError::QueryExecutionError(format!("Variable '{}' not found", var)))?;
        
    match target {
        CypherValue::Vertex(v) => {
            if prop == "id" { Ok(CypherValue::String(v.id.to_string())) }
            else if prop == "label" { Ok(CypherValue::String(v.label.to_string())) }
            else { Ok(v.properties.get(prop).map(property_value_to_cypher).unwrap_or(CypherValue::Null)) }
        },
        CypherValue::Edge(e) => {
            if prop == "id" { Ok(CypherValue::String(e.id.to_string())) }
            else { Ok(e.properties.get(prop).map(property_value_to_cypher).unwrap_or(CypherValue::Null)) }
        },
        _ => Err(GraphError::QueryExecutionError("Target is not a graph element".into())),
    }
}

// Helper function to convert PropertyValue to serde_json::Value
fn property_value_to_json(prop_val: PropertyValue) -> Value {
    println!("===> property_value_to_json START");
    match prop_val {
        PropertyValue::String(s) => Value::String(s),
        PropertyValue::Integer(i) => Value::Number(i.into()),
        PropertyValue::I32(i) => Value::Number(i.into()),
        PropertyValue::Float(f) => Value::Number(serde_json::Number::from_f64(f.0).unwrap_or(serde_json::Number::from(0))),
        PropertyValue::Boolean(b) => Value::Bool(b),
        PropertyValue::Uuid(uuid) => Value::String(uuid.to_string()),
        PropertyValue::Byte(b) => Value::Number(b.into()),
        PropertyValue::Vertex(v) => {
            Value::String(format!("Vertex({:?})", v.0))
        },
        PropertyValue::DateTime(dt) => {
            // Convert to RFC 3339 / ISO 8601 string (standard JSON timestamp format)
            Value::String(dt.0.to_rfc3339())
        },
        PropertyValue::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (key, val) in map.0.into_iter() {
                obj.insert(key.0.to_string(), property_value_to_json(val));
            }
            Value::Object(obj)
        },
        // Fix: Added missing arms to satisfy exhaustiveness check
        PropertyValue::Null => Value::Null,
        PropertyValue::List(list) => {
            Value::Array(list.into_iter().map(property_value_to_json).collect())
        }
    }
}

/// Helper to handle the string-based operators from the parser
/// Helper to handle the string-based operators from the parser
fn evaluate_comparison(left: &CypherValue, op: &str, right: &CypherValue) -> GraphResult<CypherValue> {
    match op {
        // Equality
        "=" | "==" => Ok(CypherValue::Bool(left == right)),
        "!=" | "<>" => Ok(CypherValue::Bool(left != right)),

        // --- ADDED REGEX OPERATOR ---
        "=~" => {
            match (left, right) {
                (CypherValue::String(text), CypherValue::String(pattern)) => {
                    let re = regex::Regex::new(pattern)
                        .map_err(|e| GraphError::EvaluationError(format!("Invalid regex: {}", e)))?;
                    Ok(CypherValue::Bool(re.is_match(text)))
                }
                // Cypher convention: regex against Null is Null (or False in some implementations)
                (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Bool(false)),
                _ => Ok(CypherValue::Bool(false)),
            }
        }

        // Numeric & String Comparisons
        ">" => Ok(CypherValue::Bool(match (left, right) {
            (CypherValue::Float(l), CypherValue::Float(r)) => l > r,
            (CypherValue::Integer(l), CypherValue::Integer(r)) => l > r,
            (CypherValue::String(l), CypherValue::String(r)) => l > r,
            _ => false, 
        })),

        "<" => Ok(CypherValue::Bool(match (left, right) {
            (CypherValue::Float(l), CypherValue::Float(r)) => l < r,
            (CypherValue::Integer(l), CypherValue::Integer(r)) => l < r,
            (CypherValue::String(l), CypherValue::String(r)) => l < r,
            _ => false,
        })),

        ">=" => Ok(CypherValue::Bool(match (left, right) {
            (CypherValue::Float(l), CypherValue::Float(r)) => l >= r,
            (CypherValue::Integer(l), CypherValue::Integer(r)) => l >= r,
            (CypherValue::String(l), CypherValue::String(r)) => l >= r,
            _ => false,
        })),

        "<=" => Ok(CypherValue::Bool(match (left, right) {
            (CypherValue::Float(l), CypherValue::Float(r)) => l <= r,
            (CypherValue::Integer(l), CypherValue::Integer(r)) => l <= r,
            (CypherValue::String(l), CypherValue::String(r)) => l <= r,
            _ => false,
        })),

        _ => Err(GraphError::EvaluationError(format!("Unsupported operator: {op}"))),
    }
}

fn to_bool(v: &CypherValue) -> GraphResult<bool> {
    match v {
        CypherValue::Bool(b) => Ok(*b),
        CypherValue::Null => Ok(false),
        _ => Err(GraphError::EvaluationError("Expected boolean".into())),
    }
}

fn compare<F>(a: &CypherValue, b: &CypherValue, op: F) -> GraphResult<CypherValue>
where
    F: Fn(f64, f64) -> bool,
{
    let a_val = to_f64(a)?;
    let b_val = to_f64(b)?;
    Ok(CypherValue::Bool(op(a_val, b_val)))
}

fn to_f64(v: &CypherValue) -> GraphResult<f64> {
    match v {
        CypherValue::Integer(i) => Ok(*i as f64),
        CypherValue::Float(f) => Ok(*f),
        CypherValue::String(s) => s.parse::<f64>().map_err(|_| {
            GraphError::EvaluationError("Cannot convert string to number".into())
        }),
        _ => Err(GraphError::EvaluationError("Cannot convert to number".into())),
    }
}

fn add(a: &CypherValue, b: &CypherValue) -> GraphResult<CypherValue> {
    match (a, b) {
        (CypherValue::Integer(x), CypherValue::Integer(y)) => Ok(CypherValue::Integer(x + y)),
        (CypherValue::Float(x), CypherValue::Float(y)) => Ok(CypherValue::Float(x + y)),
        (CypherValue::Integer(x), CypherValue::Float(y)) => Ok(CypherValue::Float(*x as f64 + y)),
        (CypherValue::Float(x), CypherValue::Integer(y)) => Ok(CypherValue::Float(x + *y as f64)),
        (CypherValue::String(x), CypherValue::String(y)) => Ok(CypherValue::String(format!("{x}{y}"))),
        _ => Err(GraphError::EvaluationError("Unsupported operands for +".into())),
    }
}

fn subtract(a: &CypherValue, b: &CypherValue) -> GraphResult<CypherValue> {
    Ok(CypherValue::Float(to_f64(a)? - to_f64(b)?))
}

fn multiply(a: &CypherValue, b: &CypherValue) -> GraphResult<CypherValue> {
    Ok(CypherValue::Float(to_f64(a)? * to_f64(b)?))
}

fn divide(a: &CypherValue, b: &CypherValue) -> GraphResult<CypherValue> {
    let divisor = to_f64(b)?;
    if divisor == 0.0 {
        Err(GraphError::EvaluationError("Division by zero".into()))
    } else {
        Ok(CypherValue::Float(to_f64(a)? / divisor))
    }
}

impl From<CypherExpression> for Expression {
    fn from(ce: CypherExpression) -> Self {
        match ce {
            // --- ADDED UNARY OP VARIANT ---
            CypherExpression::UnaryOp { op, expr } => {
                match op {
                    UnaryOp::Not => Expression::Not(Box::new(Expression::from(*expr))),
                    UnaryOp::IsNull => Expression::IsNull(Box::new(Expression::from(*expr))),
                    UnaryOp::IsNotNull => Expression::IsNotNull(Box::new(Expression::from(*expr))),
                    UnaryOp::Neg => {
                        // If your Expression enum doesn't have Neg yet, 
                        // you might need to add it or handle it as a specific function/op.
                        panic!("Unary Negation not yet implemented in Expression conversion");
                    }
                }
            },
            CypherExpression::Literal(val) => 
                Expression::Literal(CypherValue::from_json(val)),
            
            CypherExpression::Variable(s) => 
                Expression::Variable(s),
            
            CypherExpression::PropertyLookup { var, prop } => {
                Expression::Property(PropertyAccess::Vertex(var, prop))
            },

            // --- ADDED REGEX VARIANT ---
            CypherExpression::Regex { variable, property, pattern } => {
                Expression::Regex { variable, property, pattern }
            },

            CypherExpression::DynamicPropertyAccess { variable, property_expr } => {
                Expression::DynamicPropertyAccess {
                    variable,
                    property_expr: Box::new(Expression::from(*property_expr)),
                }
            },
            CypherExpression::ArrayIndex { expr, index } => {
                Expression::ArrayIndex {
                    expr: Box::new(Expression::from(*expr)),
                    index: Box::new(Expression::from(*index)),
                }
            },
            CypherExpression::Predicate { name, variable, list, condition } => {
                Expression::Predicate {
                    name,
                    variable,
                    list: Box::new(Expression::from(*list)),
                    condition: Box::new(Expression::from(*condition)),
                }
            },

            CypherExpression::FunctionCall { name, args } => {
                Expression::FunctionCall {
                    name,
                    args: args.into_iter().map(Expression::from).collect(),
                }
            },
            
            CypherExpression::List(elements) => {
                Expression::List(
                    elements.into_iter().map(Expression::from).collect()
                )
            },

            CypherExpression::BinaryOp { left, op, right } => {
                let op_upper = op.to_uppercase();

                // 1. Peek at 'left' and 'right' for optimization.
                let optimized = match (&*left, &*right) {
                    (CypherExpression::PropertyLookup { var, prop }, CypherExpression::Literal(val)) => {
                        // Special handling for Regex operator =~
                        if op_upper == "=~" {
                            if let serde_json::Value::String(pattern) = val {
                                Some(Expression::Regex {
                                    variable: var.clone(),
                                    property: prop.clone(),
                                    pattern: pattern.clone(),
                                })
                            } else { None }
                        } else {
                            Some(Expression::PropertyComparison {
                                variable: var.clone(),
                                property: prop.clone(),
                                operator: op.clone(),
                                value: val.clone(),
                            })
                        }
                    }
                    (CypherExpression::FunctionCall { name, args }, _) if args.len() == 1 => {
                        if let Some(CypherExpression::Variable(arg_var)) = args.get(0) {
                            if is_literal_or_literal_list(&*right) {
                                let json_val = expression_to_json_value_owned(Expression::from(*right.clone()));
                                if let Ok(v) = json_val {
                                    Some(Expression::FunctionComparison {
                                        function: name.clone(),
                                        argument: arg_var.clone(),
                                        operator: op.clone(),
                                        value: v,
                                    })
                                } else { None }
                            } else { None }
                        } else { None }
                    }
                    _ => None,
                };

                if let Some(expr) = optimized {
                    return expr;
                }

                // 3. Fallback
                match op_upper.as_str() {
                    "AND" => Expression::And {
                        left: Box::new(Expression::from(*left)),
                        right: Box::new(Expression::from(*right)),
                    },
                    "OR" => Expression::Or {
                        left: Box::new(Expression::from(*left)),
                        right: Box::new(Expression::from(*right)),
                    },
                    ":" => {
                        if let (CypherExpression::Variable(var), CypherExpression::Variable(label)) = (&*left, &*right) {
                            return Expression::LabelPredicate {
                                variable: var.clone(),
                                label: label.clone(),
                            };
                        }
                        panic!("Label predicate ':' requires variable on left and label on right");
                    },
                    "STARTS WITH" => {
                        if let CypherExpression::PropertyLookup { var, prop } = &*left {
                            if let CypherExpression::Literal(serde_json::Value::String(prefix)) = &*right {
                                return Expression::StartsWith {
                                    variable: var.clone(),
                                    property: prop.clone(),
                                    prefix: prefix.clone(),
                                };
                            }
                        }
                        panic!("STARTS WITH requires left=property, right=string literal");
                    },
                    // Handle the regex operator if it wasn't caught by the optimization peek
                    "=~" => {
                        if let (CypherExpression::PropertyLookup { var, prop }, Expression::Literal(CypherValue::String(pat))) = 
                            (&*left, Expression::from(*right)) {
                                Expression::Regex {
                                    variable: var.clone(),
                                    property: prop.clone(),
                                    pattern: pat,
                                }
                        } else {
                            panic!("Regex operator =~ requires property lookup on left and string on right");
                        }
                    }
                    _ => {
                        let binary_op = match op_upper.as_str() {
                            "=" | "==" => BinaryOp::Eq,
                            "!=" | "<>" => BinaryOp::Neq,
                            ">" => BinaryOp::Gt,
                            "<" => BinaryOp::Lt,
                            ">=" => BinaryOp::Gte,
                            "<=" => BinaryOp::Lte,
                            "+" => BinaryOp::Plus,
                            "-" => BinaryOp::Minus,
                            "*" => BinaryOp::Mul,
                            "/" => BinaryOp::Div,
                            "IN" => BinaryOp::In,
                            _ => panic!("Unsupported operator: {}", op),
                        };

                        Expression::Binary {
                            op: binary_op,
                            left: Box::new(Expression::from(*left)),
                            right: Box::new(Expression::from(*right)),
                        }
                    }
                }
            }
        }
    }
}

fn is_literal_or_literal_list(ce: &CypherExpression) -> bool {
    match ce {
        CypherExpression::Literal(_) => true,
        CypherExpression::List(elements) => elements.iter().all(is_literal_or_literal_list),
        _ => false,
    }
}

// Helper to bridge the gap between Expression and JSON for optimized structs
fn expression_to_json_value_owned(expr: Expression) -> Result<serde_json::Value, String> {
    match expr {
        Expression::Literal(cv) => Ok(cv.to_json()),
        Expression::List(elements) => {
            let mut arr = Vec::new();
            for e in elements {
                if let Expression::Literal(cv) = e {
                    arr.push(cv.to_json());
                } else {
                    return Err("Lists in comparisons must be literals".into());
                }
            }
            Ok(serde_json::Value::Array(arr))
        }
        _ => Err("Not a literal or literal list".into()),
    }
}