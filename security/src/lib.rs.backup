// security/src/lib.rs
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Argon2, PasswordHash, PasswordVerifier,
};
use jsonwebtoken::{encode, decode, Header, EncodingKey, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use sled::{Db, IVec};
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;
use anyhow::Result; // Only keep Result
use std::time::{SystemTime, UNIX_EPOCH};

/// User registration data transfer object (DTO).
#[derive(Debug, Deserialize, Serialize)]
pub struct UserRegistration {
    pub first: String,
    pub last: String,
    pub username: String,
    pub email: String,
    pub password: String,
    pub role_id: u32,
    pub phone: String,
}

/// User login data transfer object (DTO).
#[derive(Debug, Deserialize, Serialize)]
pub struct UserLogin {
    pub username: String,
    pub password: String,
}

/// User structure for storage.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct User {
    pub id: String, // Unique ID, perhaps generated UUID or sequential
    pub first: String,
    pub last: String,
    pub username: String,
    pub email: String,
    pub password_hash: String, // Hashed password
    pub role_id: u32,
    pub phone: String,
    pub created_at: u64,
    pub last_login_at: Option<u64>,
}

/// Claims for JWT.
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String, // Subject (username)
    pub exp: u64,    // Expiration time
    pub iat: u64,    // Issued at
    pub role_id: u32,
}

/// Custom authentication errors.
#[derive(Debug)]
pub enum AuthError {
    UserExists,
    InvalidCredentials,
    InternalError(String),
    JwtError(String),
    StorageInitializationError(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AuthError::UserExists => write!(f, "Username already exists"),
            AuthError::InvalidCredentials => write!(f, "Invalid username or password"),
            AuthError::InternalError(msg) => write!(f, "Internal server error: {}", msg),
            AuthError::JwtError(msg) => write!(f, "JWT error: {}", msg),
            AuthError::StorageInitializationError(msg) => write!(f, "Storage initialization error: {}", msg),
        }
    }
}

impl std::error::Error for AuthError {}

// UserConflictError is no longer tied to sled's transaction error system
// as we are handling conflict resolution (user exists) at a higher level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserConflictError(pub String);

impl fmt::Display for UserConflictError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "User conflict: {}", self.0)
    }
}

impl std::error::Error for UserConflictError {}


// --- SledUserStorage (User Data Persistence Layer) ---

#[derive(Debug, Clone)]
pub struct SledUserStorage {
    db: Arc<Db>,
    users_tree: sled::Tree,
    // Add a tree for username-to-ID mapping to quickly check for existing users
    username_to_id_tree: sled::Tree,
    next_id: Arc<Mutex<u64>>, // For simple sequential IDs
}

impl SledUserStorage {
    /// Initializes Sled database and opens/creates the 'users' tree.
    pub async fn new(db_path: &str) -> Result<Self, AuthError> {
        let db = sled::open(db_path).map_err(|e| {
            AuthError::StorageInitializationError(format!("Failed to open Sled DB: {}", e))
        })?;

        let users_tree = db.open_tree("users").map_err(|e| {
            AuthError::StorageInitializationError(format!("Failed to open 'users' tree: {}", e))
        })?;

        let username_to_id_tree = db.open_tree("username_to_id").map_err(|e| {
            AuthError::StorageInitializationError(format!("Failed to open 'username_to_id' tree: {}", e))
        })?;

        // Initialize next_id based on existing data
        let mut max_id: u64 = 0;
        for item_res in users_tree.iter() {
            let (_, value) = item_res.map_err(|e| AuthError::InternalError(format!("Failed to iterate users tree: {}", e)))?;
            let user: User = serde_json::from_slice(&value).map_err(|e| AuthError::InternalError(format!("Failed to deserialize user during init: {}", e)))?;
            if let Ok(id_val) = user.id.parse::<u64>() {
                if id_val > max_id {
                    max_id = id_val;
                }
            }
        }
        let next_id = Arc::new(Mutex::new(max_id + 1));


        Ok(Self {
            db: Arc::new(db),
            users_tree,
            username_to_id_tree,
            next_id,
        })
    }

    /// Stores a new user in the database.
    /// This method assumes the 'user exists' check has already been performed.
    pub async fn store_user(&self, user: User) -> Result<(), AuthError> {
        let user_id_clone = user.id.clone();
        let username_clone = user.username.clone();
        let user_bytes = serde_json::to_vec(&user)
            .map_err(|e| AuthError::InternalError(format!("Failed to serialize user: {}", e)))?;

        // Using `insert` directly. If this operation could lead to a race condition where two
        // users with the same username are inserted simultaneously, you would need
        // a more robust locking strategy or a Sled transaction *if* the `sled` transaction
        // API were cooperating. For now, relying on the pre-check.
        self.users_tree.insert(user_id_clone.as_bytes(), user_bytes.clone())
            .map_err(|e| AuthError::InternalError(format!("Failed to insert user into users_tree: {}", e)))?;
        self.username_to_id_tree.insert(username_clone.as_bytes(), user_id_clone.as_bytes())
            .map_err(|e| AuthError::InternalError(format!("Failed to insert username into username_to_id_tree: {}", e)))?;

        Ok(())
    }

    /// Retrieves a user by username.
    pub async fn get_user_by_username(&self, username: &str) -> Result<Option<User>, AuthError> {
        let user_id_ivec = self.username_to_id_tree.get(username.as_bytes())
            .map_err(|e| AuthError::InternalError(format!("Failed to get user ID by username: {}", e)))?;

        if let Some(user_id_ivec) = user_id_ivec {
            let user_bytes_ivec = self.users_tree.get(user_id_ivec)
                .map_err(|e| AuthError::InternalError(format!("Failed to get user by ID: {}", e)))?;
            if let Some(user_bytes) = user_bytes_ivec {
                let user: User = serde_json::from_slice(&user_bytes)
                    .map_err(|e| AuthError::InternalError(format!("Failed to deserialize user: {}", e)))?;
                Ok(Some(user))
            } else {
                Ok(None) // Username mapped to ID, but user not found by ID (inconsistent state)
            }
        } else {
            Ok(None) // Username not found
        }
    }

    // You can add more methods here, like `update_user`, `delete_user`, etc.
}

// --- Global/Singleton Storage Access ---
// This pattern allows `register_user` and `login_user` to access the storage
// without explicitly passing it around everywhere.
lazy_static::lazy_static! {
    static ref USER_STORAGE: Arc<Mutex<Option<SledUserStorage>>> = Arc::new(Mutex::new(None));
}

/// Initializes the user storage. This should be called once at application startup.
pub async fn init_user_storage(db_path: &str) -> Result<(), AuthError> {
    let mut storage_guard = USER_STORAGE.lock().await;
    if storage_guard.is_none() {
        let storage = SledUserStorage::new(db_path).await?;
        *storage_guard = Some(storage);
    }
    Ok(())
}

/// Hashes a password using Argon2.
fn hash_password(password: &str) -> Result<String, AuthError> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    argon2.hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|e| AuthError::InternalError(format!("Failed to hash password: {}", e)))
}

/// Verifies a password against a hash.
fn verify_password(password: &str, hashed_password: &str) -> Result<bool, AuthError> {
    let password_hash = PasswordHash::new(hashed_password)
        .map_err(|e| AuthError::InternalError(format!("Failed to parse password hash: {}", e)))?;
    Argon2::default().verify_password(password.as_bytes(), &password_hash)
        .map(|_| true)
        .map_err(|e| {
            if e.to_string().contains("Password verify failed") {
                AuthError::InvalidCredentials
            } else {
                AuthError::InternalError(format!("Failed to verify password: {}", e))
            }
        })
}

/// Generates a JWT token.
fn generate_jwt_token(username: &str, role_id: u32) -> Result<String, AuthError> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)
        .map_err(|e| AuthError::JwtError(format!("System time error: {}", e)))?
        .as_secs();

    let claims = Claims {
        sub: username.to_string(),
        exp: now + (60 * 60 * 24), // Token expires in 24 hours
        iat: now,
        role_id,
    };

    // Use a strong, secret key in a real application, loaded from environment variables or config.
    // For this example, a hardcoded key is used for simplicity.
    let secret = "your-super-secret-jwt-key-that-should-be-at-least-32-bytes-long";
    let encoding_key = EncodingKey::from_secret(secret.as_bytes());

    encode(&Header::default(), &claims, &encoding_key)
        .map_err(|e| AuthError::JwtError(format!("Failed to encode JWT: {}", e)))
}

/// Registers a new user.
pub async fn register_user(registration: UserRegistration) -> Result<(), AuthError> {
    let storage_guard = USER_STORAGE.lock().await;
    let storage = storage_guard.as_ref().ok_or_else(|| AuthError::StorageInitializationError("User storage not initialized".to_string()))?;

    // --- NEW LOGIC FOR "USER EXISTS" CHECK ---
    // Perform an early check if the username already exists.
    if storage.get_user_by_username(&registration.username).await?.is_some() {
        return Err(AuthError::UserExists);
    }

    let hashed_password = hash_password(&registration.password)?;
    let mut next_id_guard = storage.next_id.lock().await;
    let current_id = *next_id_guard;
    *next_id_guard += 1;

    let new_user = User {
        id: current_id.to_string(),
        first: registration.first,
        last: registration.last,
        username: registration.username,
        email: registration.email,
        password_hash: hashed_password,
        role_id: registration.role_id,
        phone: registration.phone,
        created_at: SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| AuthError::InternalError(format!("System time error: {}", e)))?
            .as_secs(),
        last_login_at: None,
    };

    // Now, call store_user. It will not attempt a sled transaction
    // for user existence because that's handled above.
    storage.store_user(new_user).await?;
    Ok(())
}

/// Logs in a user. Returns a JWT token on success.
pub async fn login_user(login: UserLogin) -> Result<String, AuthError> {
    let storage_guard = USER_STORAGE.lock().await;
    let storage = storage_guard.as_ref().ok_or_else(|| AuthError::StorageInitializationError("User storage not initialized".to_string()))?;

    let user = storage.get_user_by_username(&login.username).await?
        .ok_or(AuthError::InvalidCredentials)?; // User not found

    if verify_password(&login.password, &user.password_hash)? {
        let token = generate_jwt_token(&user.username, user.role_id)?;

        // Update last_login_at (optional, but good practice)
        let mut updated_user = user;
        updated_user.last_login_at = Some(SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| AuthError::InternalError(format!("System time error: {}", e)))?
            .as_secs());
        storage.store_user(updated_user).await?; // Re-store to update last_login_at

        Ok(token)
    } else {
        Err(AuthError::InvalidCredentials) // Password mismatch
    }
}


/// Decodes and validates a JWT token.
pub fn validate_jwt_token(token: &str) -> Result<Claims, AuthError> {
    let secret = "your-super-secret-jwt-key-that-should-be-at-least-32-bytes-long";
    let decoding_key = DecodingKey::from_secret(secret.as_bytes());
    let validation = Validation::default();

    decode::<Claims>(token, &decoding_key, &validation)
        .map(|data| data.claims)
        .map_err(|e| AuthError::JwtError(format!("Failed to decode or validate JWT: {}", e)))
}
