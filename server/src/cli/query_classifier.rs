// src/cli/query_classifier.rs
/// Infers the query language (cypher or sql) from the input string.
/// Returns `None` if ambiguous or unknown.
pub fn infer_language(query: &str) -> Option<&'static str> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Step 1: Extract "meaningful" tokens — ignore strings, comments
    let tokens = extract_keywords(trimmed);
    if tokens.is_empty() {
        return None;
    }
    // Step 2: Cypher — strong signals
    let first = tokens.first()?;
    if ["MATCH", "CREATE", "MERGE", "DETACH", "DELETE", "REMOVE", "SET"]
        .iter()
        .any(|&kw| kw == first.as_str())
    {
        return Some("cypher");
    }
    // Step 3: SQL — SELECT + FROM/INTO/JOIN
    if first == "SELECT" {
        if tokens
            .iter()
            .any(|t| ["FROM", "JOIN", "INNER", "LEFT", "RIGHT"].contains(&t.as_str()))
        {
            return Some("sql");
        }
    }
    if ["INSERT", "UPDATE", "DELETE"]
        .iter()
        .any(|&kw| kw == first.as_str())
    {
        if tokens
            .iter()
            .any(|t| ["INTO", "FROM", "SET"].contains(&t.as_str()))
        {
            return Some("sql");
        }
    }
    // Step 4: Fallback patterns
    let has_cypher_pattern = tokens
        .iter()
        .any(|t| ["RETURN", "WHERE"].contains(&t.as_str()))
        && tokens
            .iter()
            .any(|t| t.contains('(') && t.contains(')'));
    let has_sql_pattern = tokens
        .iter()
        .any(|t| ["SELECT", "FROM", "JOIN", "WHERE"].contains(&t.as_str()))
        && !has_cypher_pattern;
    if has_cypher_pattern {
        Some("cypher")
    } else if has_sql_pattern {
        Some("sql")
    } else {
        None
    }
}

/// Extracts uppercase keywords, skipping content inside quotes or comments
fn extract_keywords(input: &str) -> Vec<String> {
    let mut keywords = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut i = 0;
    let chars: Vec<char> = input.chars().collect();
    while i < chars.len() {
        let c = chars[i];
        // Toggle quote modes
        if c == '\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
            i += 1;
            continue;
        }
        if c == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
            i += 1;
            continue;
        }
        // Skip comments
        if c == '-' && i + 1 < chars.len() && chars[i + 1] == '-' {
            // Skip until newline
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '/' && i + 1 < chars.len() && chars[i + 1] == '*' {
            i += 2;
            while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            i += 2;
            continue;
        }
        // Outside quotes: collect word chars
        if !in_single_quote && !in_double_quote {
            if c.is_ascii_alphabetic() || c == '_' {
                current.push(c.to_ascii_uppercase());
            } else if !current.is_empty() {
                keywords.push(current.clone());
                current.clear();
            }
        }
        i += 1;
    }
    if !current.is_empty() {
        keywords.push(current);
    }
    keywords
}