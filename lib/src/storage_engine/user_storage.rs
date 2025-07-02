// lib/src/storage_engine/user_storage.rs
use async_trait::async_trait;
use sled::{Db, Tree, Batch};
use uuid::Uuid;
use bincode::{
    config::{self, Configuration, BigEndian, Fixint},
    serde::{decode_from_slice, encode_to_vec},
};

use models::errors::GraphResult as ModelsResult;
use crate::errors::{GraphError, Result};
use models::identifiers::Identifier;
use models::medical::{Login, User};
use models::util;


#[async_trait]
pub trait UserStorageEngine: Send + Sync + 'static {
    /// Adds a new user to the storage.
    async fn add_user(&self, user: &User) -> Result<()>;
    /// Updates an existing user in the storage.
    async fn update_user(&self, user: &User) -> Result<()>;
    /// Deletes a user by their username.
    async fn delete_user(&self, username: &str) -> Result<()>;
    /// Retrieves a user by their username.
    async fn get_user_by_username(&self, username: &str) -> Result<Option<User>>;
    /// Retrieves a user by their unique ID.
    /// Note: This can be inefficient for Sled if not using a secondary index.
    async fn get_user_by_id(&self, id: &Uuid) -> Result<Option<User>>;
    /// Authenticates a user based on their login credentials.
    async fn authenticate_user(&self, login: &Login) -> Result<Option<User>>;
}

/// Sled-backed implementation of the `UserStorageEngine` trait.
pub struct SledUserStorage {
    tree: Tree,
    config: Configuration<BigEndian, Fixint>,
}

impl SledUserStorage {
    /// Creates a new `SledUserStorage` instance.
    /// Opens a specific Sled tree named "users".
    pub fn new(db: &Db) -> Result<Self> {
        let tree = db.open_tree("users")?;
        Ok(Self {
            tree,
            config: bincode_config(),
        })
    }
}

/// Provides a standard bincode configuration.
fn bincode_config() -> Configuration<BigEndian, Fixint> {
    config::standard()
        .with_big_endian()
        .with_fixed_int_encoding()
}

#[async_trait]
impl UserStorageEngine for SledUserStorage {
    async fn add_user(&self, user: &User) -> Result<()> {
        let key = util::build(&[
            util::Component::Identifier(
                Identifier::new(user.username.clone())
                    // FIX: Map Validation error to InvalidData
                    .map_err(|e| GraphError::InvalidData(format!("Invalid username: {}", e)))?
            ),
            util::Component::Uuid(
                Uuid::parse_str(&user.id)
                    // FIX: Map Conversion error to InvalidData
                    .map_err(|e| GraphError::InvalidData(format!("Invalid UUID format: {}", e)))?
            ),
        ]);
        let user_bytes = encode_to_vec(user, self.config.clone())?;

        let mut batch = sled::Batch::default();
        batch.insert(key, user_bytes.as_slice());
        self.tree.apply_batch(batch)?;
        Ok(())
    }

    async fn update_user(&self, user: &User) -> Result<()> {
        let key = util::build(&[
            util::Component::Identifier(
                Identifier::new(user.username.clone())
                    // FIX: Map Validation error to InvalidData
                    .map_err(|e| GraphError::InvalidData(format!("Invalid username: {}", e)))?
            ),
            util::Component::Uuid(
                Uuid::parse_str(&user.id)
                    // FIX: Map Conversion error to InvalidData
                    .map_err(|e| GraphError::InvalidData(format!("Invalid UUID format: {}", e)))?
            ),
        ]);
        let user_bytes = encode_to_vec(user, self.config.clone())?;

        let mut batch = sled::Batch::default();
        batch.insert(key, user_bytes.as_slice());
        self.tree.apply_batch(batch)?;
        Ok(())
    }

    async fn delete_user(&self, username: &str) -> Result<()> {
        let low_key_prefix = util::build(&[
            util::Component::Identifier(
                Identifier::new(username.to_string())
                    // FIX: Map Validation error to InvalidData
                    .map_err(|e| GraphError::InvalidData(format!("Invalid username: {}", e)))?
            )
        ]);
        let iter = self.tree.scan_prefix(&low_key_prefix);
        let mut batch = sled::Batch::default();

        for item in iter {
            let (key_ivec, _) = item?;
            batch.remove(&key_ivec);
        }
        self.tree.apply_batch(batch)?;
        Ok(())
    }

    async fn get_user_by_username(&self, username: &str) -> Result<Option<User>> {
        let low_key_prefix = util::build(&[
            util::Component::Identifier(
                Identifier::new(username.to_string())
                    // FIX: Map Validation error to InvalidData
                    .map_err(|e| GraphError::InvalidData(format!("Invalid username: {}", e)))?
            )
        ]);
        if let Some(result) = self.tree.scan_prefix(&low_key_prefix).next() {
            let (_key_ivec, value_ivec) = result?;
            let (user, _): (User, usize) = decode_from_slice(&value_ivec, self.config.clone())?;
            Ok(Some(user))
        } else {
            Ok(None)
        }
    }

    async fn get_user_by_id(&self, id: &Uuid) -> Result<Option<User>> {
        for item in self.tree.iter() {
            let (_key_ivec, value_ivec) = item?;
            let (user, _): (User, usize) = decode_from_slice(&value_ivec, self.config.clone())?;
            if Uuid::parse_str(&user.id)
                // FIX: Map Conversion error to InvalidData
                .map_err(|e| GraphError::InvalidData(format!("Invalid UUID format: {}", e)))? == *id {
                return Ok(Some(user));
            }
        }
        Ok(None)
    }

    async fn authenticate_user(&self, login: &Login) -> Result<Option<User>> {
        if let Some(user) = self.get_user_by_username(&login.username).await? {
            match User::verify_password(&login.password, &user.password_hash) {
                Ok(is_valid) => {
                    if is_valid {
                        Ok(Some(user))
                    } else {
                        // Passwords do not match, but user exists. This is an authentication failure.
                        Err(GraphError::AuthenticationError("Incorrect password.".to_string()))
                    }
                },
                // FIX: Map bcrypt::BcryptError to AuthenticationError or PasswordHashingError
                // PasswordHashingError is more specific to the cause if the hash itself is bad.
                // AuthenticationError is more general for failed login. Let's use AuthenticationError
                // as it's the result of the login attempt.
                Err(e) => Err(GraphError::AuthenticationError(format!("Password verification failed: {}", e))),
            }
        } else {
            Ok(None) // User not found, so no authentication possible
        }
    }
}
