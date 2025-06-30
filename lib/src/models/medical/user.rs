// lib/src/models/medical/user.rs
// Updated: 2025-06-30 - Restoring all original fields, Chrono usage, ToVertex impl,
// adding Serialize/Deserialize for User, and fixing Option<String> Display in ToVertex.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize}; // <-- Ensure these are imported
use crate::models::{Vertex, ToVertex, identifiers::Identifier}; // <-- Restore original imports

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)] // <-- CRUCIAL: Added Serialize, Deserialize for User
pub struct User {
    pub id: Option<String>,
    pub first: String,
    pub last: String,
    pub username: String,
    pub email: String,
    pub password: String, // Keep as per your original, though usually plain password isn't stored
    pub password_hash: String, // This is for the hashed password
    pub phone: Option<String>, // Restored
    pub created_at: DateTime<Utc>, // Restored
    pub updated_at: DateTime<Utc>, // Restored
    pub role_id: u32, // Restored
    pub last_login: Option<i64>, // Re-adding, as it was in my previous correct User struct.
                                 // If this wasn't in your original, you can remove it.
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)] // Already had Serialize, Deserialize
pub struct Login {
    pub username: String,
    pub password: String,
}

impl ToVertex for User {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("User").unwrap());
        // FIX for Option<String> Display error
        v.add_property("id", &self.id.as_ref().map_or_else(|| "".to_string(), |s| s.clone()));
        v.add_property("first", &self.first);
        v.add_property("last", &self.last);
        v.add_property("username", &self.username);
        v.add_property("email", &self.email);
        // BE CAREFUL: Storing plain password is not recommended. If `password` is only for input,
        // it shouldn't be part of the User struct that gets stored or converted to a Vertex for general display.
        // For demonstration, keeping as per your original `password` field.
        v.add_property("password", &self.password);
        // Do not add password_hash here for general vertex display unless explicitly needed
        // v.add_property("password_hash", &self.password_hash);

        if let Some(ref phone) = self.phone {
            v.add_property("phone", phone);
        }
        v.add_property("created_at", &self.created_at.to_rfc3339()); // Restored
        v.add_property("updated_at", &self.updated_at.to_rfc3339()); // Restored
        v.add_property("role_id", &self.role_id.to_string()); // Restored
        // Add last_login to vertex if it's a field
        if let Some(ref last_login_ts) = self.last_login {
            v.add_property("last_login", &last_login_ts.to_string());
        }
        v
    }
}
