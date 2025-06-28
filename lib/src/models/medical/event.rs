use chrono::{DateTime, Utc};
use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Event {
    pub id: i32,
    pub patient_id: i32,
    pub event_type: String,
    pub event_date: DateTime<Utc>,
    pub description: String,
}

impl ToVertex for Event {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Event").unwrap());

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());
        vertex.add_property("event_type", &self.event_type);
        vertex.add_property("event_date", &self.event_date.to_rfc3339());
        vertex.add_property("description", &self.description);

        vertex
    }
}

