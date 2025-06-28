// lib/src/models/medical/x12edi_message.rs
use crate::models::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct X12EDIMessage {
    pub id: i32,
    pub transaction_set_id: String,
    pub transaction_set_control_number: String,
    pub interchange_control_number: String,
    pub sender_id: String,
    pub receiver_id: String,
    pub message_content: String,
    pub received_date: DateTime<Utc>,
    pub sent_date: Option<DateTime<Utc>>,
    pub status: String,
}

impl ToVertex for X12EDIMessage {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("X12EDIMessage").expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("transaction_set_id", &self.transaction_set_id);
        vertex.add_property("transaction_set_control_number", &self.transaction_set_control_number);
        vertex.add_property("interchange_control_number", &self.interchange_control_number);
        vertex.add_property("sender_id", &self.sender_id);
        vertex.add_property("receiver_id", &self.receiver_id);
        vertex.add_property("message_content", &self.message_content);
        vertex.add_property("received_date", &self.received_date.to_rfc3339());
        vertex.add_property("status", &self.status);

        if let Some(sent_date) = self.sent_date {
            vertex.add_property("sent_date", &sent_date.to_rfc3339());
        }

        vertex
    }
}

