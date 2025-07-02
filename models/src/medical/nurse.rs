// models/src/medical/nurse.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Nurse {
    pub id: i32,
    pub user_id: i32, // Links to an existing User
    pub license_number: String,
    pub specialty: Option<String>, // e.g., "ER", "ICU", "Pediatric"
    pub years_of_experience: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for Nurse {
    fn to_vertex(&self) -> Vertex {
        // FIX: Convert "&str" to String using .to_string()
        let id_type = Identifier::new("Nurse".to_string()).expect("Invalid Identifier");
        let mut v = Vertex::new(id_type);
        v.add_property("id", &self.id.to_string());
        v.add_property("user_id", &self.user_id.to_string());
        v.add_property("license_number", &self.license_number);
        if let Some(ref val) = self.specialty {
            v.add_property("specialty", val);
        }
        v.add_property("years_of_experience", &self.years_of_experience.to_string());
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v
    }
}
