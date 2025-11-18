// server/src/cli/handlers_index.rs
use anyhow::Result;
use lib::query_exec_engine::query_exec_engine::QueryExecEngine;
use std::sync::Arc;

pub async fn handle_index_command(
    engine: Arc<QueryExecEngine>,
    action: lib::commands::IndexAction,
) -> Result<()> {
    match action {
        lib::commands::IndexAction::Create { label, property } => {
            let cypher = format!(r#"CREATE INDEX FOR (n:{label}) ON (n.{property})"#);
            let res = engine.execute_cypher(&cypher).await?;
            println!("Index created: {label}.{property}\n{res}");
        }
        lib::commands::IndexAction::Search { term, top } => {
            let limit = top.map(|t| t.to_string()).unwrap_or("10".to_string());
            let cypher = format!(
                r#"CALL db.index.fulltext.queryNodes("notesIndex", "{term}") 
                   YIELD node, score 
                   RETURN node, score 
                   ORDER BY score DESC LIMIT {limit}"#
            );
            let res = engine.execute_cypher(&cypher).await?;
            println!("Search results for '{term}':\n{res}");
        }
        lib::commands::IndexAction::Rebuild => {
            let res = engine.execute_cypher("CALL db.index.fulltext.awaitEventuallyConsistentIndexRefresh()").await?;
            println!("Full-text indexes rebuilt:\n{res}");
        }
        lib::commands::IndexAction::List => {
            let res = engine.execute_cypher("SHOW INDEXES").await?;
            println!("All indexes:\n{res}");
        }
        lib::commands::IndexAction::Stats => {
            let res = engine.execute_cypher("CALL db.indexes()").await?;
            println!("Index statistics:\n{res}");
        }
        lib::commands::IndexAction::Drop { label, property } => {
            let cypher = format!(r#"DROP INDEX ON :{label}({property})"#);
            let res = engine.execute_cypher(&cypher).await?;
            println!("Dropped index {label}.{property}\n{res}");
        }
    }
    Ok(())
}