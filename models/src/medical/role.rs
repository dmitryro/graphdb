// lib/src/models/medical/role.rs
use crate::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Role {
    pub id: u32,
    pub name: String,
    pub permissions: Vec<String>,
    pub created_at: DateTime<Utc>,
}

impl ToVertex for Role {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Role".to_string()).unwrap());
        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("name", &self.name);
        vertex.add_property("permissions", &self.permissions.join(","));
        vertex.add_property("created_at", &self.created_at.to_rfc3339());

        vertex
    }
}

