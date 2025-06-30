// lib/src/storage_engine/user_storage.rs
// Updated: 2025-06-30 - Fixing argon2::Version to use default.
// Updated: 2025-06-30 - Added Send + Sync to UserStorageEngine trait.
// Updated: 2025-06-30 - Fixed argon2::Version, StdError for password_hash::Error, and unused imports/mut.

use anyhow::{Context, Result}; // Removed 'anyhow' from import as it's unused
use sled::{Db, Tree};
use async_trait::async_trait;

use argon2::{
    Algorithm, Argon2, Params, Version,
};
use password_hash::{
    PasswordHash, PasswordHasher, PasswordVerifier,
    rand_core::OsRng,
    SaltString,
};

// Use the full path for User and Login models
use crate::models::medical::{User, Login};

#[async_trait]
pub trait UserStorageEngine: Send + Sync {
    async fn create_user(&self, user: &User) -> Result<User>;
    async fn get_user_by_id(&self, user_id: &str) -> Result<Option<User>>;
    async fn get_user_by_username(&self, username: &str) -> Result<Option<User>>;
    async fn authenticate_user(&self, login: &Login) -> Result<Option<User>>;
    async fn update_user(&self, user: &User) -> Result<User>;
    async fn delete_user(&self, user_id: &str) -> Result<()>;
}

// SledUserStorage implementation
#[derive(Debug, Clone)]
pub struct SledUserStorage {
    db: Db,
    users_tree: Tree,
}

impl SledUserStorage {
    pub fn new(db: Db) -> Self {
        let users_tree = db.open_tree("users").expect("Failed to open users tree");
        SledUserStorage { db, users_tree }
    }

    fn hash_password(&self, password: &str) -> Result<String> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::new(
            Algorithm::Argon2id,
            Version::default(), // <-- CHANGED: Use Version::default() or Version::V19
            Params::new(15000, 2, 1, None).unwrap(), // Using default params for simplicity
        );
        let password_hash = argon2.hash_password(password.as_bytes(), &salt)
            .map_err(|e| anyhow::anyhow!("Failed to hash password: {}", e))? // <-- FIXED: Convert error to anyhow::Error
            .to_string();
        Ok(password_hash)
    }

    fn verify_password(&self, password: &str, password_hash: &str) -> Result<bool> {
        let parsed_hash = PasswordHash::new(password_hash)
            .map_err(|e| anyhow::anyhow!("Failed to parse password hash: {}", e))?; // <-- FIXED: Convert error to anyhow::Error
        Ok(Argon2::default().verify_password(password.as_bytes(), &parsed_hash).is_ok())
    }
}

#[async_trait]
impl UserStorageEngine for SledUserStorage {
    async fn create_user(&self, user: &User) -> Result<User> { // <-- FIXED: Removed `mut` from `user`
        let hashed_password = self.hash_password(&user.password)?;
        let mut new_user = user.clone(); // Clone to make it mutable
        new_user.password_hash = hashed_password;
        new_user.password = String::new(); // Clear plaintext password

        let user_id = uuid::Uuid::new_v4().to_string();
        new_user.id = Some(user_id.clone());

        let user_data = serde_json::to_vec(&new_user)
            .context("Failed to serialize user data")?;

        self.users_tree.insert(user_id.as_bytes(), user_data)
            .context("Failed to insert user into Sled")?;

        Ok(new_user)
    }

    async fn get_user_by_id(&self, user_id: &str) -> Result<Option<User>> {
        let user_data_ivec = self.users_tree.get(user_id.as_bytes())
            .context("Failed to retrieve user by ID from Sled")?;
        
        user_data_ivec.map(|ivec| {
            serde_json::from_slice(&ivec)
                .context("Failed to deserialize user data")
        }).transpose()
    }

    async fn get_user_by_username(&self, username: &str) -> Result<Option<User>> {
        // Iterate through the tree to find the user by username.
        // In a real-world scenario with Sled, you might use secondary indices for this.
        for item_result in self.users_tree.iter() {
            let item = item_result.context("Failed to iterate user tree")?;
            let user: User = serde_json::from_slice(&item.1)
                .context("Failed to deserialize user data during username lookup")?;
            if user.username == username {
                return Ok(Some(user));
            }
        }
        Ok(None)
    }

    async fn authenticate_user(&self, login: &Login) -> Result<Option<User>> {
        if let Some(user) = self.get_user_by_username(&login.username).await? {
            if self.verify_password(&login.password, &user.password_hash)? {
                // Update last_login timestamp upon successful authentication
                let mut authenticated_user = user.clone();
                authenticated_user.last_login = Some(chrono::Utc::now().timestamp());
                // No need to persist this immediately as `create_user` or `update_user`
                // would handle persistence. For a `last_login` update, you might want a
                // dedicated lightweight update method or integrate into `update_user`.
                // For now, we return the updated user object.
                return Ok(Some(authenticated_user));
            }
        }
        Ok(None)
    }

    async fn update_user(&self, user: &User) -> Result<User> {
        let user_id = user.id.as_ref().context("User ID is required for update")?;
        let user_data = serde_json::to_vec(user)
            .context("Failed to serialize user data for update")?;

        self.users_tree.insert(user_id.as_bytes(), user_data)
            .context("Failed to update user in Sled")?;
        
        Ok(user.clone())
    }

    async fn delete_user(&self, user_id: &str) -> Result<()> {
        self.users_tree.remove(user_id.as_bytes())
            .context("Failed to delete user from Sled")?;
        Ok(())
    }
}
