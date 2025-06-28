use crate::models::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct BillingAddress {
    pub id: i32,
    pub patient_id: Option<i32>,
    pub address: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub postal_code: Option<String>,
    pub country: Option<String>,
}

impl ToVertex for BillingAddress {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("BillingAddress").expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());

        if let Some(ref v) = self.patient_id {
            vertex.add_property("patient_id", &v.to_string());
        }
        if let Some(ref v) = self.address {
            vertex.add_property("address", v);
        }
        if let Some(ref v) = self.city {
            vertex.add_property("city", v);
        }
        if let Some(ref v) = self.state {
            vertex.add_property("state", v);
        }
        if let Some(ref v) = self.postal_code {
            vertex.add_property("postal_code", v);
        }
        if let Some(ref v) = self.country {
            vertex.add_property("country", v);
        }

        vertex
    }
}

