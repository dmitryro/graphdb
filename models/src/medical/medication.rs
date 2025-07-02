// medication.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Medication {
    pub id: i32,
    pub name: String,
    pub brand_name: Option<String>,
    pub generic_name: Option<String>,
    pub medication_class: String,
}

impl ToVertex for Medication {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Medication".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("name", &self.name);
        if let Some(ref val) = self.brand_name {
            v.add_property("brand_name", val);
        }
        if let Some(ref val) = self.generic_name {
            v.add_property("generic_name", val);
        }
        v.add_property("medication_class", &self.medication_class);
        v
    }
}
