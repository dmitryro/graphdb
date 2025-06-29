// lib/src/models/medical/triage.rs
use chrono::{DateTime, Utc};
use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Triage {
    pub id: i32,
    pub encounter_id: i32, // Links to an existing Encounter
    pub patient_id: i32, // Redundant if always linked to Encounter, but useful for direct patient queries
    pub triage_nurse_id: i32, // Links to a Nurse
    pub triage_level: String, // e.g., "ESI 3", "Urgent", "Emergent"
    pub chief_complaint: String,
    pub presenting_symptoms: Option<String>, // Consider a more structured type (e.g., Vec<String>) for future
    pub pain_score: Option<i32>, // e.g., 0-10
    pub triage_notes: Option<String>,
    pub assessed_at: DateTime<Utc>,
}

impl ToVertex for Triage {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Triage").unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("encounter_id", &self.encounter_id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        v.add_property("triage_nurse_id", &self.triage_nurse_id.to_string());
        v.add_property("triage_level", &self.triage_level);
        v.add_property("chief_complaint", &self.chief_complaint);
        if let Some(ref val) = self.presenting_symptoms {
            v.add_property("presenting_symptoms", val);
        }
        if let Some(ref val) = self.pain_score {
            v.add_property("pain_score", &val.to_string());
        }
        if let Some(ref val) = self.triage_notes {
            v.add_property("triage_notes", val);
        }
        v.add_property("assessed_at", &self.assessed_at.to_rfc3339());
        v
    }
}
