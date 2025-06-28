use crate::models::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Nurse {
    pub id: i32,
    pub name: String,
    pub credentials: String,
    pub specialization: Option<String>,
    pub assigned_doctor_id: Option<i32>,
    pub shift_schedule: Option<String>,
    pub contact_info: String,
    pub employment_status: String,
}

impl ToVertex for Nurse {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("Nurse").expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("name", &self.name);
        vertex.add_property("credentials", &self.credentials);
        if let Some(ref spec) = self.specialization {
            vertex.add_property("specialization", spec);
        }
        if let Some(ref doc_id) = self.assigned_doctor_id {
            vertex.add_property("assigned_doctor_id", &doc_id.to_string());
        }
        if let Some(ref shift) = self.shift_schedule {
            vertex.add_property("shift_schedule", shift);
        }
        vertex.add_property("contact_info", &self.contact_info);
        vertex.add_property("employment_status", &self.employment_status);

        vertex
    }
}

