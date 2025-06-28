use graphql_parser::parse_query; // Ensure this import exists

pub enum QueryType {
    Cypher,
    SQL,
    GraphQL,
}

pub fn parse_query_from_string(query: &str) -> Result<QueryType, String> {
    // Try to parse as a GraphQL query
    if let Ok(_) = parse_query::<&str>(query) {
        return Ok(QueryType::GraphQL);
    }

    // Try to parse as Cypher or SQL queries
    if query.starts_with("MATCH") || query.contains("RETURN") {
        return Ok(QueryType::Cypher);
    } else if query.contains("SELECT") || query.contains("FROM") {
        return Ok(QueryType::SQL);
    }

    Err("Unrecognized query format".to_string())
}

