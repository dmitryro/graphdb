use chrono::{DateTime, Utc};
use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct HL7Message {
    pub id: i32,
    pub message_type: String,
    pub message_content: String,
    pub received_date: DateTime<Utc>,
    pub sent_date: Option<DateTime<Utc>>,
    pub status: String,
}

impl ToVertex for HL7Message {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("HL7Message").unwrap());

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("message_type", &self.message_type);
        vertex.add_property("message_content", &self.message_content);
        vertex.add_property("received_date", &self.received_date.to_rfc3339());
        if let Some(ref sent) = self.sent_date {
            vertex.add_property("sent_date", &sent.to_rfc3339());
        }
        vertex.add_property("status", &self.status);

        vertex
    }
}

