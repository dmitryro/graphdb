use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Pharmacy {
    pub id: i32,
    pub name: String,
    pub address: Option<String>,
    pub contact_number: Option<String>,
    pub email: Option<String>,
    pub pharmacy_type: Option<String>,
}

impl ToVertex for Pharmacy {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Pharmacy".to_string()).unwrap());
        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("name", &self.name);
        if let Some(ref v) = self.address { vertex.add_property("address", v); }
        if let Some(ref v) = self.contact_number { vertex.add_property("contact_number", v); }
        if let Some(ref v) = self.email { vertex.add_property("email", v); }
        if let Some(ref v) = self.pharmacy_type { vertex.add_property("pharmacy_type", v); }

        vertex
    }
}

