// lib/src/models/medical/refill.rs
use crate::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Refill {
    pub id: i32,
    pub prescription_id: i32,
    pub date_requested: DateTime<Utc>,
    pub date_fulfilled: Option<DateTime<Utc>>,
    pub status: String,
}

impl ToVertex for Refill {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Refill".to_string()).unwrap());

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("prescription_id", &self.prescription_id.to_string());
        vertex.add_property("date_requested", &self.date_requested.to_rfc3339());
        if let Some(ref v) = self.date_fulfilled {
            vertex.add_property("date_fulfilled", &v.to_rfc3339());
        }
        vertex.add_property("status", &self.status);

        vertex
    }
}
