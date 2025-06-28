// lib/src/models/medical/vitals.rs
use crate::models::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Vitals {
    pub id: i32,
    pub patient_id: i32,
    pub weight: Option<f32>,
    pub height: Option<f32>,
    pub blood_pressure_systolic: Option<i32>,
    pub blood_pressure_diastolic: Option<i32>,
    pub temperature: Option<f32>,
    pub heart_rate: Option<i32>,
    pub created_at: DateTime<Utc>,
}

impl ToVertex for Vitals {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("Vitals").expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());

        if let Some(weight) = self.weight {
            vertex.add_property("weight", &weight.to_string());
        }
        if let Some(height) = self.height {
            vertex.add_property("height", &height.to_string());
        }
        if let Some(systolic) = self.blood_pressure_systolic {
            vertex.add_property("blood_pressure_systolic", &systolic.to_string());
        }
        if let Some(diastolic) = self.blood_pressure_diastolic {
            vertex.add_property("blood_pressure_diastolic", &diastolic.to_string());
        }
        if let Some(temp) = self.temperature {
            vertex.add_property("temperature", &temp.to_string());
        }
        if let Some(hr) = self.heart_rate {
            vertex.add_property("heart_rate", &hr.to_string());
        }

        vertex.add_property("created_at", &self.created_at.to_rfc3339());

        vertex
    }
}

