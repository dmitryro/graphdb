// security/src/lib.rs
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Argon2, PasswordHash, PasswordVerifier,
};
use jsonwebtoken::{encode, decode, Header, EncodingKey, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use anyhow::Result;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use chrono::Utc;

// --- NEW/UPDATED IMPORTS ---
use storage_daemon_server::storage_client::StorageClient;
use models::medical::{User, NewUser}; // <--- Import User AND NewUser from your models crate
// --- END NEW/UPDATED IMPORTS ---

/// User registration data transfer object (DTO).
// This struct is aligned with models::medical::NewUser for easy conversion
#[derive(Debug, Deserialize, Serialize)]
pub struct UserRegistration {
    pub first: String,
    pub last: String,
    pub username: String,
    pub email: String,
    pub password: String,
    pub role_id: u32,
    pub phone: Option<String>,
}

/// User login data transfer object (DTO).
#[derive(Debug, Deserialize, Serialize)]
pub struct UserLogin {
    pub username: String,
    pub password: String,
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
    PasswordHashError(String), // New error variant for bcrypt errors
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AuthError::UserExists => write!(f, "Username already exists"),
            AuthError::InvalidCredentials => write!(f, "Invalid username or password"),
            AuthError::InternalError(msg) => write!(f, "Internal server error: {}", msg),
            // FIX: Corrected the JwtError arm. It should be:
            AuthError::JwtError(msg) => write!(f, "JWT error: {}", msg),
            AuthError::StorageInitializationError(msg) => write!(f, "Storage initialization error: {}", msg),
            AuthError::PasswordHashError(msg) => write!(f, "Password hashing error: {}", msg),
        }
    }
}

impl std::error::Error for AuthError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserConflictError(pub String);

impl fmt::Display for UserConflictError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "User conflict: {}", self.0)
    }
}

impl std::error::Error for UserConflictError {}

// --- DEPRECATED/REPLACED: Manual Argon2 hashing in security crate ---
// If models crate uses bcrypt for User::from_new_user, it's better to
// align security crate's hashing/verification.
// For now, only for login_user (existing users) we might still use Argon2.
// This is a point of inconsistency to address if you fully switch to bcrypt.
/// Hashes a password using Argon2.
fn hash_password_argon2(password: &str) -> Result<String, AuthError> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    argon2.hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|e| AuthError::InternalError(format!("Failed to hash password with Argon2: {}", e)))
}

/// Verifies a password against an Argon2 hash.
fn verify_password_argon2(password: &str, hashed_password: &str) -> Result<bool, AuthError> {
    let password_hash = PasswordHash::new(hashed_password)
        .map_err(|e| AuthError::InternalError(format!("Failed to parse Argon2 password hash: {}", e)))?;
    Argon2::default().verify_password(password.as_bytes(), &password_hash)
        .map(|_| true)
        .map_err(|e| {
            if e.to_string().contains("Password verify failed") {
                AuthError::InvalidCredentials
            } else {
                AuthError::InternalError(format!("Failed to verify Argon2 password: {}", e))
            }
        })
}
// --- END DEPRECATED/REPLACED ---


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

    let secret = "your-super-secret-jwt-key-that-should-be-at-least-32-bytes-long";
    let encoding_key = EncodingKey::from_secret(secret.as_bytes());

    encode(&Header::default(), &claims, &encoding_key)
        .map_err(|e| AuthError::JwtError(format!("Failed to encode JWT: {}", e)))
}

/// Registers a new user.
pub async fn register_user(
    registration: UserRegistration,
    storage_client: Arc<StorageClient>,
) -> Result<(), AuthError> {
    // Perform an early check if the username already exists using the StorageClient.
    // NOTE: This assumes StorageClient has a `get_user_by_username` method.
    // If not, you'll need to define how the StorageClient interacts with user data.
    if storage_client.get_user_by_username(&registration.username).await
        .map_err(|e| AuthError::InternalError(format!("Storage error during user existence check: {}", e)))?
        .is_some()
    {
        return Err(AuthError::UserExists);
    }

    // Convert UserRegistration DTO to models::medical::NewUser
    let new_user_dto = NewUser {
        first: registration.first,
        last: registration.last,
        username: registration.username,
        email: registration.email,
        password: registration.password, // Plaintext password
        phone: registration.phone,
        role_id: registration.role_id,
    };

    // Use User::from_new_user to create the final User object with hashed password and timestamps
    // NOTE: This assumes `User::from_new_user` handles password hashing (e.g., bcrypt)
    let new_user = User::from_new_user(new_user_dto)
        .map_err(|e| AuthError::PasswordHashError(format!("Failed to hash password for new user: {}", e)))?;


    // NOTE: This assumes StorageClient has a `create_user` method that accepts a `User` struct.
    storage_client.create_user(new_user).await
        .map_err(|e| AuthError::InternalError(format!("Failed to create user in storage: {}", e)))?;
    Ok(())
}

/// Logs in a user. Returns a JWT token on success.
pub async fn login_user(
    login: UserLogin,
    storage_client: Arc<StorageClient>,
) -> Result<String, AuthError> {
    // NOTE: This assumes StorageClient has a `get_user_by_username` method that returns `Option<User>`.
    let user = storage_client.get_user_by_username(&login.username).await
        .map_err(|e| AuthError::InternalError(format!("Storage error during user retrieval: {}", e)))?
        .ok_or(AuthError::InvalidCredentials)?; // User not found

    // For now, still using Argon2 verification here.
    // If all users are now bcrypt, switch this to bcrypt::verify.
    // NOTE: This assumes `user.password_hash` contains the Argon2 hash.
    if verify_password_argon2(&login.password, &user.password_hash)? {
        let token = generate_jwt_token(&user.username, user.role_id)?;
        Ok(token)
    } else {
        Err(AuthError::InvalidCredentials)
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
