use chrono::{DateTime, Utc};
use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Immunization {
    pub id: i32,
    pub patient_id: i32,
    pub vaccine_name: String,
    pub administration_date: DateTime<Utc>,
    pub administered_by: Option<i32>,
    pub notes: Option<String>,
}

impl ToVertex for Immunization {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Immunization").unwrap());

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());
        vertex.add_property("vaccine_name", &self.vaccine_name);
        vertex.add_property("administration_date", &self.administration_date.to_rfc3339());
        if let Some(ref adm) = self.administered_by {
            vertex.add_property("administered_by", &adm.to_string());
        }
        if let Some(ref n) = self.notes {
            vertex.add_property("notes", n);
        }

        vertex
    }
}

