// lib/src/graph_engine/medical.rs
use crate::graph::Graph;
use models::edges::Edge;
use models::medical::{Patient, Diagnosis};
use models::identifiers::{Identifier, SerializableUuid};
use models::vertices::Vertex as ModelVertex;
use models::ToVertex;
use models::properties::PropertyValue;
use uuid::Uuid;
use std::collections::{BTreeMap, HashMap};

/// Convert a **model** vertex (from `models`) into the **engine’s in-memory** representation.
///
/// The engine no longer has its own `Vertex` type – it re-uses `ModelVertex` directly.
/// This function only converts `SerializableUuid → Uuid` and ensures properties are `String`-based
/// (engine currently only supports `PropertyValue::String` in its internal maps).
/// Convert a **model** vertex (from `models`) into the **engine’s in-memory** representation.
fn convert_model_vertex_to_engine_vertex(v: &ModelVertex) -> ModelVertex {
    let mut engine_vertex = ModelVertex {
        id: v.id,
        label: v.label.clone(),
        properties: HashMap::new(), // same type as model
    };

    // Convert all property values to String for engine compatibility
    for (k, prop_val) in &v.properties {
        let string_value = match prop_val {
            PropertyValue::String(s) => s.clone(),
            PropertyValue::Integer(i) => i.to_string(),
            PropertyValue::Float(f) => f.0.to_string(),
            PropertyValue::Boolean(b) => b.to_string(),
            PropertyValue::Uuid(u) => u.0.to_string(),
        };
        engine_vertex
            .properties
            .insert(k.clone(), PropertyValue::String(string_value));
    }

    engine_vertex
}

/// Convert `Patient` → engine-ready vertex
pub fn patient_to_vertex(patient: &Patient) -> ModelVertex {
    let model_vertex = patient.to_vertex();
    convert_model_vertex_to_engine_vertex(&model_vertex)
}

/// Convert `Diagnosis` → engine-ready vertex
pub fn diagnosis_to_vertex(diagnosis: &Diagnosis) -> ModelVertex {
    let model_vertex = diagnosis.to_vertex();
    convert_model_vertex_to_engine_vertex(&model_vertex)
}

/// Create a `HAS_DIAGNOSIS` edge from patient to diagnosis
pub fn create_has_diagnosis_edge(patient_id: Uuid, diagnosis_id: Uuid) -> Edge {
    Edge::new(
        SerializableUuid(patient_id),           // outbound_id
        Identifier::new("HAS_DIAGNOSIS".to_string()).expect("Invalid identifier"),
        SerializableUuid(diagnosis_id),         // inbound_id
    )
    .with_property("relationship", PropertyValue::String("HAS_DIAGNOSIS".to_string()))
}

/// Insert patient + diagnosis + relationship into the graph
pub fn insert_patient_with_diagnosis(graph: &mut Graph, patient: Patient, diagnosis: Diagnosis) {
    let patient_vertex = patient_to_vertex(&patient);
    let diagnosis_vertex = diagnosis_to_vertex(&diagnosis);
    let edge = create_has_diagnosis_edge(patient_vertex.id.0, diagnosis_vertex.id.0);

    graph.add_vertex(patient_vertex);
    graph.add_vertex(diagnosis_vertex);
    graph.add_edge(edge);
}

/// Placeholder for future medical domain logic
pub fn extend_medical_graph() {
    // TODO: Add encounter, medication, provider, etc.
}
