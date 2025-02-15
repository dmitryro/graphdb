use graphql_parser::parse_query; // Ensure this import exists

pub enum QueryType {
    Cypher,
    SQL,
    GraphQL,
}

pub fn parse_query_from_string(query: &str) -> Result<QueryType, String> {
    match parse_query::<&str>(query) {
        Ok(_) => Ok(QueryType::GraphQL),
        Err(_) => {
            if query.starts_with("MATCH") || query.contains("RETURN") {
                Ok(QueryType::Cypher)
            } else if query.contains("SELECT") || query.contains("FROM") {
                Ok(QueryType::SQL)
            } else {
                Err("Unrecognized query format".to_string())
            }
        }
    }
}

