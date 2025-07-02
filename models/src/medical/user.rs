// models/src/medical/user.rs
// Updated: 2025-06-30 - Implementing password hashing with bcrypt,
// removing plaintext password from stored User struct, and handling ID generation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use bcrypt::{DEFAULT_COST, hash, verify, BcryptError}; // Import bcrypt utilities
use uuid::Uuid; // For generating robust unique IDs

use crate::{Vertex, ToVertex, identifiers::Identifier};

// --- DTO for New User Registration ---
// This struct will be used when receiving new user registration data via API.
// It temporarily holds the plaintext password for hashing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewUser {
    pub first: String,
    pub last: String,
    pub username: String,
    pub email: String,
    pub password: String, // Plaintext password for input
    pub phone: Option<String>,
    pub role_id: u32,
}

// --- Stored User Struct ---
// This struct represents how a User is stored in the database.
// It contains the password hash, NOT the plaintext password.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct User {
    pub id: String, // Changed to String as it will be generated (e.g., UUID)
    pub first: String,
    pub last: String,
    pub username: String,
    pub email: String,
    pub password_hash: String, // This is for the hashed password. Removed the plaintext 'password' field.
    pub updated_at: DateTime<Utc>,
    pub role_id: u32,
    pub phone: Option<String>, // Corrected based on error
    pub created_at: DateTime<Utc>, // Corrected based on error
    pub last_login: Option<DateTime<Utc>>, // Corrected based on error suggestion
}

impl User {
    /// Hashes a plaintext password.
    pub fn hash_password(password: &str) -> Result<String, BcryptError> {
        hash(password, DEFAULT_COST)
    }

    /// Verifies a plaintext password against a stored hash.
    pub fn verify_password(password: &str, hash: &str) -> Result<bool, BcryptError> {
        verify(password, hash)
    }

    /// Creates a new `User` instance from a `NewUser` DTO, hashing the password.
    pub fn from_new_user(new_user: NewUser) -> Result<Self, BcryptError> {
        let now = Utc::now();
        let password_hash = Self::hash_password(&new_user.password)?;

        Ok(User {
            id: Uuid::new_v4().to_string(), // Generate a new UUID for the ID
            first: new_user.first,
            last: new_user.last,
            username: new_user.username,
            email: new_user.email,
            password_hash, // Store the hash, not the plaintext
            phone: new_user.phone,
            created_at: now,
            updated_at: now,
            role_id: new_user.role_id,
            last_login: None, // Set initially to None
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Login {
    pub username: String,
    pub password: String, // Plaintext password for login attempt
}

impl ToVertex for User {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("User".to_string()).unwrap());

        v.add_property("id", &self.id); // ID is now guaranteed to be String
        v.add_property("first", &self.first);
        v.add_property("last", &self.last);
        v.add_property("username", &self.username);
        v.add_property("email", &self.email);

        // IMPORTANT: Do NOT add `password_hash` to the public Vertex representation
        // unless you have a very specific, secure, internal reason.
        // It's a hash, but still sensitive data.
        // v.add_property("password_hash", &self.password_hash);

        if let Some(ref phone) = self.phone {
            v.add_property("phone", phone);
        }
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v.add_property("role_id", &self.role_id.to_string());
        if let Some(ref last_login_ts) = self.last_login {
            v.add_property("last_login", &last_login_ts.to_string());
        }
        v
    }
}
