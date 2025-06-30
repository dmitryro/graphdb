// lib/src/engine/medical.rs

use crate::engine::{Graph, vertex::Vertex as EngineVertex, Edge};
use crate::models::medical::{Patient, Diagnosis};
use crate::models::identifiers::Identifier;
use crate::models::ToVertex;
use uuid::Uuid;

/// Convert model Vertex to engine Vertex
fn convert_model_vertex_to_engine_vertex(v: &crate::models::vertices::Vertex) -> crate::engine::vertex::Vertex {
    use std::collections::BTreeMap;
    use crate::engine::properties::PropertyValue;

    let mut eng_vertex = crate::engine::vertex::Vertex::new(v.t.to_string());
    eng_vertex.id = v.id;
    eng_vertex.properties = v.properties.iter()
        .map(|(k, v)| (k.clone(), PropertyValue::String(v.clone())))
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
        patient_id,
        diagnosis_id,
        "HAS_DIAGNOSIS",
        Identifier::new("HAS_DIAGNOSIS").expect("Invalid identifier"),
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

