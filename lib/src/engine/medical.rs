// lib/src/engine/medical.rs

use crate::engine::{Graph, vertex::Vertex as EngineVertex, Edge};
use models::medical::{Patient, Diagnosis};
use models::Identifier;
use models::ToVertex;
use models::vertices::Vertex as ModelVertex; // Alias to avoid conflict with EngineVertex
use uuid::Uuid;
use models::identifiers::SerializableUuid;
// This import is correct for models::properties::PropertyValue
use models::properties::PropertyValue;

/// Convert model Vertex to engine Vertex
fn convert_model_vertex_to_engine_vertex(v: &ModelVertex) -> crate::engine::vertex::Vertex {
    use std::collections::BTreeMap;

    let mut eng_vertex = crate::engine::vertex::Vertex::new(v.label.to_string());
    eng_vertex.id = Uuid::from(v.id); // Convert SerializableUuid to Uuid
    eng_vertex.properties = v.properties.iter()
        .map(|(k, prop_val)| {
            let string_value = match prop_val {
                PropertyValue::String(s) => s.clone(),
                // Add conversion logic for other PropertyValue variants if needed
                _ => {
                    eprintln!("Warning: Encountered unsupported PropertyValue type for conversion to EngineVertex properties. Key: {}", k);
                    "UNSUPPORTED_PROPERTY_TYPE".to_string()
                },
            };
            (k.clone(), PropertyValue::String(string_value))
        })
        .collect::<BTreeMap<String, PropertyValue>>();
    eng_vertex
}

/// Convert Patient model to engine Vertex
pub fn patient_to_vertex(patient: &Patient) -> EngineVertex {
    let model_vertex = patient.to_vertex();
    convert_model_vertex_to_engine_vertex(&model_vertex)
}

/// Convert Diagnosis model to engine Vertex
pub fn diagnosis_to_vertex(diagnosis: &Diagnosis) -> EngineVertex {
    let model_vertex = diagnosis.to_vertex();
    convert_model_vertex_to_engine_vertex(&model_vertex)
}

/// Utility to create an Edge from a patient to a diagnosis
pub fn create_has_diagnosis_edge(patient_id: Uuid, diagnosis_id: Uuid) -> Edge {
    Edge::new(
        patient_id, // Argument 1: Uuid (outbound_id)
        diagnosis_id, // Argument 2: Uuid (inbound_id)
        "HAS_DIAGNOSIS", // Argument 3: &str (edge type as string literal)
        Identifier::new("HAS_DIAGNOSIS".to_string()).expect("Invalid identifier for HAS_DIAGNOSIS"), // Argument 4: Identifier (edge type as Identifier)
    )
}

/// Insert patient, diagnosis and their relation into the graph
pub fn insert_patient_with_diagnosis(graph: &mut Graph, patient: Patient, diagnosis: Diagnosis) {
    let patient_vertex = patient_to_vertex(&patient);
    let diagnosis_vertex = diagnosis_to_vertex(&diagnosis);
    let edge = create_has_diagnosis_edge(patient_vertex.id, diagnosis_vertex.id);

    graph.add_vertex(patient_vertex);
    graph.add_vertex(diagnosis_vertex);
    graph.add_edge(edge);
}

/// Placeholder for further medical domain expansions
pub fn extend_medical_graph() {
    // TODO: Add encounter, medication, provider logic here.
}
