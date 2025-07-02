// models/src/medical/hospital.rs
use chrono::{DateTime, Utc}; // Keep these as they are used in the struct fields
// Corrected imports for types within the 'models' crate
use crate::{
    identifiers::Identifier,
    // Note: PropertyMap is likely not needed here anymore if Vertex::new no longer takes it directly.
    // However, if the `Vertex::new` function expects a `PropertyMap`, it needs to be imported.
    // Based on the previous errors and common patterns, `Vertex::new` usually takes an Identifier.
    // If it's truly not used, remove this line. For now, I'll remove it assuming `Vertex::new` signature.
    // properties::PropertyMap, // Removed this import, assuming it's no longer directly passed to Vertex::new
    vertices::Vertex, // Get Vertex from its module
    ToVertex,         // Get ToVertex from the crate root re-export
};

#[derive(Debug, Clone)]
pub struct Hospital {
    pub id: i32,
    pub name: String,
    pub address_id: i32, // Assuming this is an ID to another vertex (Address)
    pub phone: Option<String>,
    pub website: Option<String>,
    pub admin_contact_user_id: Option<i32>, // Assuming this is an ID to a User vertex
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for Hospital {
    fn to_vertex(&self) -> Vertex {
        // Correctly pass String to Identifier::new
        let mut v = Vertex::new(Identifier::new("Hospital".to_string()).unwrap());

        // FIX: Ensure all keys are string literals (`"key"`) and values are `&str`.
        // Numbers (`i32`) need `.to_string().as_str()` or `.to_string().as_ref()`.
        // Strings (`String`) can be passed as `&self.field_name` or `&val`.
        // DateTime needs `.to_rfc3339().as_str()`.

        v.add_property("id", &self.id.to_string()); // Key is &str, value is &str (from temp String)
        v.add_property("name", &self.name);         // Key is &str, value is &str (from &String)
        v.add_property("address_id", &self.address_id.to_string()); // Key is &str, value is &str (from temp String)

        if let Some(ref val) = self.phone {
            v.add_property("phone", val); // Key is &str, value is &str (from &String)
        }
        if let Some(ref val) = self.website {
            v.add_property("website", val); // Key is &str, value is &str (from &String)
        }
        if let Some(ref val) = self.admin_contact_user_id {
            v.add_property("admin_contact_user_id", &val.to_string()); // Key is &str, value is &str (from temp String)
        }

        // Convert DateTime to RFC3339 string and then take a reference.
        // We need to create a temporary String, then take a reference to it.
        // This is safe because the temporary String lives until the end of the `add_property` call.
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v
    }
}
