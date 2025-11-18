// server/src/cli/handlers_graph.rs
use anyhow::Result;
use lib::query_exec_engine::query_exec_engine::QueryExecEngine;
use uuid::Uuid;
use std::sync::Arc;

/// This function **must** be named `handle_graph_command` — no exceptions.
pub async fn handle_graph_command(
    engine: Arc<QueryExecEngine>,
    action: lib::commands::GraphAction,
) -> Result<()> {
    match action {
        lib::commands::GraphAction::InsertPerson { name, age, city } => {
            let name = name.as_deref().unwrap_or("Unknown");
            let age = age.map(|a| a.to_string()).unwrap_or("0".to_string());
            let city = city.as_deref().unwrap_or("Unknown");

            let cypher = format!(
                r#"CREATE (p:Person {{id: $id, name: $name, age: $age, city: $city}}) RETURN p"#,
                // We'll use parameters when supported — for now inline
            );

            let full_query = format!(
                r#"CREATE (p:Person {{id: "{}", name: "{}", age: {}, city: "{}"}}) RETURN p"#,
                Uuid::new_v4(), name, age, city
            );

            let result = engine.execute_cypher(&full_query).await?;
            println!("Created Person node: {}", result);
        }

        lib::commands::GraphAction::MedicalRecord {
            patient_name,
            patient_age,
            diagnosis_code,
        } => {
            let patient_name = patient_name.as_deref().unwrap_or("John Doe");
            let age = patient_age.map(|a| a.to_string()).unwrap_or("0".to_string());
            let code = diagnosis_code.as_deref().unwrap_or("Z00.0");

            let cypher = format!(
                r#"
                CREATE (p:Patient {{name: "{}", age: {}}})
                CREATE (d:Diagnosis {{code: "{}", description: "Routine checkup"}})
                CREATE (p)-[:HAS_DIAGNOSIS]->(d)
                RETURN p, d
                "#,
                patient_name, age, code
            );

            let result = engine.execute_cypher(&cypher).await?;
            println!("Medical record created for {} (code: {})", patient_name, code);
            println!("{}", result);
        }

        lib::commands::GraphAction::DeleteNode { id } => {
            let cypher = format!(r#"MATCH (n) WHERE n.id = "{}" DETACH DELETE n"#, id);
            let result = engine.execute_cypher(&cypher).await?;
            println!("Node {} deleted: {}", id, result);
        }

        lib::commands::GraphAction::LoadData { path } => {
            println!("Bulk load from {} — not yet implemented (use Cypher CSV LOAD)", path.display());
            // Future: CALL apoc.load.json() or custom loader
        }
    }

    Ok(())
}
