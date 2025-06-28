use chrono::{DateTime, Utc};
use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Patient {
    pub id: i32,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub date_of_birth: Option<DateTime<Utc>>,
    pub gender: Option<String>,
    pub address: Option<String>,
    pub contact_number: Option<String>,
    pub email: Option<String>,
    pub social_security_number: Option<String>,
    pub mpi_id: Option<i32>,
}

impl ToVertex for Patient {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("Patient").expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        // Add id as a property explicitly
        vertex.add_property("id", &self.id.to_string());

        if let Some(ref v) = self.first_name {
            vertex.add_property("first_name", v);
        }
        if let Some(ref v) = self.last_name {
            vertex.add_property("last_name", v);
        }
        if let Some(ref v) = self.date_of_birth {
            vertex.add_property("date_of_birth", &v.to_rfc3339());
        }
        if let Some(ref v) = self.gender {
            vertex.add_property("gender", v);
        }
        if let Some(ref v) = self.address {
            vertex.add_property("address", v);
        }
        if let Some(ref v) = self.contact_number {
            vertex.add_property("contact_number", v);
        }
        if let Some(ref v) = self.email {
            vertex.add_property("email", v);
        }
        if let Some(ref v) = self.social_security_number {
            vertex.add_property("ssn", v);
        }
        if let Some(ref v) = self.mpi_id {
            vertex.add_property("mpi_id", &v.to_string());
        }

        vertex
    }
}

