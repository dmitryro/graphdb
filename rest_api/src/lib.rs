// rest_api/src/lib.rs
// Restored: 2025-07-02 - Reverted to Hyper-based server and restored all specified endpoints.
// Includes mock implementations for external dependencies (security, daemon_api).
// Updated: 2025-07-02 - Re-introduced regex-based query parsing in /api/v1/query endpoint.
// Fixed: 2025-07-02 - Changed login endpoint path from /api/v1/login to /api/v1/auth.

use hyper::{Body, Request, Response, Server, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use std::net::SocketAddr;
use std::convert::Infallible;
use serde_json::json;
use tokio::task;
use std::str::FromStr;
use tokio::sync::{oneshot, Mutex}; // Added Mutex for Arc<Mutex<Option<oneshot::Sender<()>>>>
use std::sync::Arc; // Needed for Arc<StorageClient>
use serde::{Deserialize, Serialize}; // Needed for UserRegistration, UserLogin, StoreQueryRequest
use std::borrow::Cow; // For fix_damaged_json

// Import query parsing logic from the `lib` crate
use lib::query_parser::{parse_query_from_string, QueryType};


// --- MOCK DEFINITIONS FOR EXTERNAL DEPENDENCIES ---
// These are placeholders for actual logic residing in other crates (e.g., `security`, `storage_daemon_server`).
// In a real setup, you would import these from their respective crates.

// Mock for `storage_daemon_server::storage_client::StorageClient`
#[derive(Debug)]
pub struct StorageClient;

impl StorageClient {
    pub fn new(_db_path: String) -> Self { // Changed to non-async for simpler mock
        println!("[MOCK] StorageClient::new called. (Path: {})", _db_path);
        StorageClient {}
    }
    // Add mock methods that the handlers might call, e.g., `get`, `put`.
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        println!("[MOCK] StorageClient::get called for key: {}", key);
        Ok(Some(format!("mock_value_for_{}", key)))
    }
    pub async fn put(&self, key: &str, value: &str) -> anyhow::Result<()> {
        println!("[MOCK] StorageClient::put called for key: {}, value: {}", key, value);
        Ok(())
    }
}

// Mock for `security::AuthError`
#[derive(Debug)]
pub enum AuthError {
    UserExists,
    InvalidCredentials,
    InternalError(String),
    JwtError(String),
    StorageInitializationError(String),
    PasswordHashError(String),
}

// Implement Display for AuthError for easy formatting
impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::UserExists => write!(f, "User already exists"),
            AuthError::InvalidCredentials => write!(f, "Invalid credentials"),
            AuthError::InternalError(msg) => write!(f, "Internal error: {}", msg),
            AuthError::JwtError(msg) => write!(f, "JWT error: {}", msg),
            AuthError::StorageInitializationError(msg) => write!(f, "Storage initialization error: {}", msg),
            AuthError::PasswordHashError(msg) => write!(f, "Password hashing error: {}", msg),
        }
    }
}

// Mock for `security::UserRegistration`
#[derive(Debug, Deserialize)]
pub struct UserRegistration {
    pub username: String,
    pub password: String,
}

// Mock for `security::UserLogin`
#[derive(Debug, Deserialize)]
pub struct UserLogin {
    pub username: String,
    pub password: String,
}

// Mock for `security` module functions
mod security {
    use super::{AuthError, UserRegistration, UserLogin, StorageClient};
    use std::sync::Arc;
    use anyhow::Result;

    pub async fn register_user(
        _data: UserRegistration,
        _storage_client: Arc<StorageClient>,
    ) -> Result<(), AuthError> {
        println!("[MOCK] security::register_user called.");
        // Simulate success or failure
        if _data.username == "existing_user" {
            Err(AuthError::UserExists)
        } else {
            Ok(())
        }
    }

    pub async fn login_user(
        _data: UserLogin,
        _storage_client: Arc<StorageClient>,
    ) -> Result<String, AuthError> {
        println!("[MOCK] security::login_user called.");
        // Simulate success or failure
        if _data.username == "testuser" && _data.password == "password" {
            Ok("mock_jwt_token_12345".to_string())
        } else {
            Err(AuthError::InvalidCredentials)
        }
    }
}

// Mock for `daemon_api::DaemonError`
#[derive(Debug)]
pub enum DaemonError {
    ApiError(String),
    IoError(String),
    ParseError(String),
}

// Mock for `daemon_api` module functions
mod daemon_api {
    use super::DaemonError;
    use anyhow::Result; // Use anyhow::Result for mock functions

    // SHUTDOWN_FLAG is not directly used for REST API server shutdown anymore
    // but might be for other daemon processes. Keeping the mock for completeness.
    // In a real scenario, this might be an AtomicBool or similar.
    pub static SHUTDOWN_FLAG: bool = false; // Mock, not actually used for signaling here

    pub fn stop_daemon() -> Result<(), DaemonError> {
        println!("[MOCK] daemon_api::stop_daemon called.");
        // Simulate success
        Ok(())
    }

    pub fn stop_port_daemon(_port: u16) -> Result<(), DaemonError> {
        println!("[MOCK] daemon_api::stop_port_daemon called for port: {}", _port);
        // Simulate success
        Ok(())
    }
}

// --- END MOCK DEFINITIONS ---

// Placeholder for logging. Replace with your actual logging solution (e.g., `log` crate).
fn log_rest_event(message: &str) {
    println!("[REST_API] {}", message);
}

// Struct for handling generic query requests, as discussed previously
#[derive(Debug, Deserialize, Serialize)]
pub struct StoreQueryRequest {
    pub query: String,
    pub persist: bool, // Flag to indicate whether to persist the query
}

// Placeholder function for storing queries, as discussed previously
async fn store_query_in_engine(query: String, persist: bool) -> anyhow::Result<()> {
    log_rest_event(&format!(
        "SIMULATING QUERY STORAGE: Query: \"{}\", Persist: {}",
        query, persist
    ));
    // In a real application, this would interact with your database/storage layer
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    Ok(())
}

// Helper function to try and fix malformed JSON, as provided previously
fn fix_damaged_json(input: &str) -> Cow<str> {
    let mut changed = false;
    let mut cleaned_json_owned = input.trim().to_string();

    if cleaned_json_owned.starts_with('\'') {
        cleaned_json_owned.remove(0);
        changed = true;
    }
    if cleaned_json_owned.ends_with('\'') {
        cleaned_json_owned.pop();
        changed = true;
    }

    let original_len = cleaned_json_owned.len();
    cleaned_json_owned = cleaned_json_owned.trim_end_matches("\\n").to_string();
    if cleaned_json_owned.len() != original_len { changed = true; }

    let original_len = cleaned_json_owned.len();
    cleaned_json_owned = cleaned_json_owned.trim_end_matches('\n').to_string();
    if cleaned_json_owned.len() != original_len { changed = true; }

    let mut current_slice = cleaned_json_owned.as_str();
    if let Some(start_idx) = current_slice.find('{').or_else(|| current_slice.find('[')) {
        if start_idx > 0 { changed = true; }
        if let Some(end_idx_relative) = current_slice[start_idx..].rfind('}').or_else(|| current_slice[start_idx..].rfind(']')) {
            let effective_end_idx = start_idx + end_idx_relative + 1;
            if effective_end_idx < current_slice.len() { changed = true; }
            current_slice = &current_slice[start_idx..effective_end_idx];
        }
    }
    cleaned_json_owned = current_slice.to_string();

    let mut temp_string = String::with_capacity(cleaned_json_owned.len());
    let mut chars = cleaned_json_owned.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            temp_string.push(c);
            if let Some(next_c) = chars.peek() {
                if *next_c == 'n' || *next_c == 't' || *next_c == '\\' || *next_c == '"' {
                    temp_string.push(chars.next().unwrap());
                }
            }
        } else if c == '\n' {
            temp_string.push_str("\\n");
            changed = true;
        } else if c == '\t' {
            temp_string.push_str("\\t");
            changed = true;
        } else {
            temp_string.push(c);
        }
    }

    if changed {
        Cow::Owned(temp_string)
    } else {
        Cow::Borrowed(input)
    }
}


/// The main request handler for the Hyper server.
/// This function dispatches requests to appropriate sub-handlers based on path and method.
async fn handle_request(
    req: Request<Body>,
    storage_client: Arc<StorageClient>, // Passed from the server's service_fn
    shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>, // For server shutdown
) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path();
    let method = req.method();

    log_rest_event(&format!("Received request: {} {}", method, path));

    match (method, path) {
        (&hyper::Method::GET, "/") => {
            let response = json!({
                "status": "success",
                "message": "Welcome to the GraphDB REST API"
            });
            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
        }
        (&hyper::Method::GET, "/api/v1/version") => {
            let response = json!({
                "version": env!("CARGO_PKG_VERSION"),
                "api_status": "active"
            });
            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
        }
        (&hyper::Method::GET, "/api/v1/health") => {
            let response = json!({
                "status": "ok",
                "message": "REST API is healthy"
            });
            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
        }
        (&hyper::Method::POST, "/api/v1/start_daemon") => {
            log_rest_event("REST API: Received request to start daemon.");
            let result = task::spawn_blocking(move || {
                // This is a dummy call; replace with your actual daemon start logic
                Ok::<(), DaemonError>(()) // Simulate success
            }).await.unwrap();

            match result {
                Ok(_) => {
                    let response = json!({
                        "status": "success",
                        "message": "Daemon start request received (implementation pending)"
                    });
                    Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
                }
                Err(e) => {
                    let response = json!({
                        "status": "error",
                        "message": format!("Failed to start daemon: {:?}", e)
                    });
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                        .unwrap())
                }
            }
        }
        (&hyper::Method::POST, "/api/v1/stop_all_daemons") => {
            log_rest_event("REST API: Received request to stop all daemons.");
            let result = task::spawn_blocking(move || {
                daemon_api::stop_daemon()
            }).await.unwrap();

            match result {
                Ok(_) => {
                    let response = json!({
                        "status": "success",
                        "message": "All daemons stop request sent"
                    });
                    log_rest_event("REST API: All daemons stop request sent (success)");
                    Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
                }
                Err(e) => {
                    let response = json!({
                        "status": "error",
                        "message": format!("Failed to send stop signal to all daemons: {:?}", e)
                    });
                    log_rest_event(&format!("REST API: Failed to send stop signal to all daemons: {:?}", e));
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                        .unwrap())
                }
            }
        }
        (&hyper::Method::POST, path) if path.starts_with("/api/v1/stop/") => {
            let port_str = path.strip_prefix("/api/v1/stop/").unwrap_or("");
            match port_str.parse::<u16>() {
                Ok(port) => {
                    log_rest_event(&format!("REST API: Received request to stop daemon on port {}", port));
                    let result = tokio::task::spawn_blocking(move || {
                        daemon_api::stop_port_daemon(port)
                    }).await.unwrap();
                    match result {
                        Ok(()) => {
                            let response = json!({
                                "status": "success",
                                "message": format!("Daemon on port {} stopped successfully", port)
                            });
                            log_rest_event(&format!("REST API: Daemon on port {} stopped (success)", port));
                            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
                        }
                        Err(e) => {
                            let response = json!({
                                "status": "error",
                                "message": format!("Failed to stop daemon on port {}: {:?}", port, e)
                            });
                            log_rest_event(&format!("REST API: Failed to stop daemon on port {}: {:?}", port, e));
                            Ok(Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body(Body::from(serde_json::to_string(&response).unwrap()))
                                .unwrap())
                        }
                    }
                }
                Err(_) => {
                    let response = json!({
                        "status": "error",
                        "message": "Invalid port number"
                    });
                    log_rest_event("REST API: Invalid port number for stop request");
                    Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                        .unwrap())
                }
            }
        }
        (&hyper::Method::POST, "/api/v1/stop") => {
            log_rest_event("REST API: Received graceful shutdown request for this server.");
            let mut tx_guard = shutdown_tx.lock().await;
            if let Some(tx) = tx_guard.take() {
                let _ = tx.send(()); // Send shutdown signal
                let response = json!({
                    "status": "success",
                    "message": "REST API server initiating graceful shutdown."
                });
                Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
            } else {
                let response = json!({
                    "status": "error",
                    "message": "Shutdown signal already sent or not available."
                });
                Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                    .unwrap())
            }
        }
        // RE-ADDED and CORRECTED: /api/v1/register endpoint
        (&hyper::Method::POST, "/api/v1/register") => {
            log_rest_event("REST API: Received registration request.");
            let whole_body = hyper::body::to_bytes(req.into_body()).await;

            match whole_body {
                Ok(body_bytes) => {
                    match serde_json::from_slice::<UserRegistration>(&body_bytes) {
                        Ok(register_data) => {
                            let storage_client_clone = Arc::clone(&storage_client); // Clone for the async task
                            let registration_result = task::spawn(async move {
                                security::register_user(register_data, storage_client_clone).await
                            }).await.unwrap(); // Handle JoinError

                            match registration_result {
                                Ok(_) => {
                                    let response = json!({
                                        "status": "success",
                                        "message": "User registered successfully"
                                    });
                                    log_rest_event("REST API: User registered successfully.");
                                    Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
                                }
                                Err(e) => {
                                    let (status_code, message) = match e {
                                        AuthError::UserExists => (StatusCode::CONFLICT, "Username already exists".to_string()),
                                        AuthError::InvalidCredentials => (StatusCode::BAD_REQUEST, "Invalid registration data provided".to_string()),
                                        AuthError::InternalError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Internal server error: {}", msg)),
                                        AuthError::JwtError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, format!("JWT related error during registration: {}", msg)),
                                        AuthError::StorageInitializationError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Storage initialization error: {}", msg)),
                                        AuthError::PasswordHashError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Password hashing error: {}", msg)),
                                    };
                                    let response = json!({
                                        "status": "error",
                                        "message": message
                                    });
                                    log_rest_event(&format!("REST API: User registration failed: {:?}", e));
                                    Ok(Response::builder()
                                        .status(status_code)
                                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                                        .unwrap())
                                }
                            }
                        }
                        Err(e) => {
                            let response = json!({
                                "status": "error",
                                "message": format!("Invalid registration data: {}", e)
                            });
                            log_rest_event(&format!("REST API: Invalid registration data: {}", e));
                            Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(serde_json::to_string(&response).unwrap()))
                                .unwrap())
                        }
                    }
                }
                Err(e) => {
                    let response = json!({
                        "status": "error",
                        "message": format!("Failed to read request body: {}", e)
                    });
                    log_rest_event(&format!("REST API: Failed to read request body for registration: {}", e));
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                        .unwrap())
                }
            }
        }
        // RE-ADDED and CORRECTED: /api/v1/auth endpoint (was /api/v1/login)
        (&hyper::Method::POST, "/api/v1/auth") => { // Changed from /api/v1/login to /api/v1/auth
            log_rest_event("REST API: Received login request.");
            let whole_body = hyper::body::to_bytes(req.into_body()).await;

            match whole_body {
                Ok(body_bytes) => {
                    match serde_json::from_slice::<UserLogin>(&body_bytes) {
                        Ok(login_data) => {
                            let storage_client_clone = Arc::clone(&storage_client); // Clone for the async task
                            let login_result = task::spawn(async move {
                                security::login_user(login_data, storage_client_clone).await
                            }).await.unwrap(); // Handle JoinError

                            match login_result {
                                Ok(token) => {
                                    let response = json!({
                                        "status": "success",
                                        "message": "User logged in successfully",
                                        "token": token
                                    });
                                    log_rest_event("REST API: User logged in successfully.");
                                    Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
                                }
                                Err(e) => {
                                    let (status_code, message) = match e {
                                        AuthError::InvalidCredentials => (StatusCode::UNAUTHORIZED, "Invalid username or password".to_string()),
                                        AuthError::InternalError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Internal server error: {}", msg)),
                                        AuthError::UserExists => (StatusCode::BAD_REQUEST, "Unexpected error: User exists during login check".to_string()), // Should not happen during login
                                        AuthError::JwtError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, format!("JWT generation error: {}", msg)),
                                        AuthError::StorageInitializationError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Storage initialization error: {}", msg)),
                                        AuthError::PasswordHashError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Password hashing error: {}", msg)),
                                    };
                                    let response = json!({
                                        "status": "error",
                                        "message": message
                                    });
                                    log_rest_event(&format!("REST API: User login failed: {:?}", e));
                                    Ok(Response::builder()
                                        .status(status_code)
                                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                                        .unwrap())
                                }
                            }
                        }
                        Err(e) => {
                            let response = json!({
                                "status": "error",
                                "message": format!("Invalid login data: {}", e)
                            });
                            log_rest_event(&format!("REST API: Invalid login data: {}", e));
                            Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(serde_json::to_string(&response).unwrap()))
                                .unwrap())
                        }
                    }
                }
                Err(e) => {
                    let response = json!({
                        "status": "error",
                        "message": format!("Failed to read request body: {}", e)
                    });
                    log_rest_event(&format!("REST API: Failed to read request body for login: {}", e));
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                        .unwrap())
                }
            }
        }
        // Endpoint for submitting a query
        (&hyper::Method::POST, "/api/v1/query") => {
            log_rest_event("REST API: Received query request.");
            let whole_body = hyper::body::to_bytes(req.into_body()).await;

            match whole_body {
                Ok(body_bytes) => {
                    let body_str = String::from_utf8_lossy(&body_bytes);
                    let cleaned_body = fix_damaged_json(&body_str);

                    match serde_json::from_str::<StoreQueryRequest>(&cleaned_body) {
                        Ok(query_request) => {
                            let query_clone = query_request.query.clone();
                            let persist_clone = query_request.persist;

                            // Perform regex-based query validation here
                            match parse_query_from_string(&query_clone) {
                                Ok(query_type) => {
                                    println!("[REST_API] Query identified as: {:?}", query_type);
                                    let query_result = task::spawn(async move {
                                        store_query_in_engine(query_clone, persist_clone).await
                                    })
                                    .await
                                    .unwrap(); // Handle JoinError

                                    match query_result {
                                        Ok(_) => {
                                            let response = json!({
                                                "status": "success",
                                                "message": "Query processed successfully",
                                                "query_type": format!("{:?}", query_type) // Include identified type
                                            });
                                            log_rest_event("REST API: Query processed successfully.");
                                            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
                                        }
                                        Err(e) => {
                                            let response = json!({
                                                "status": "error",
                                                "message": format!("Failed to process query: {}", e)
                                            });
                                            log_rest_event(&format!("REST API: Failed to process query: {}", e));
                                            Ok(Response::builder()
                                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                                .body(Body::from(serde_json::to_string(&response).unwrap()))
                                                .unwrap())
                                        }
                                    }
                                }
                                Err(e) => {
                                    // Query parsing failed (regex didn't match)
                                    let response = json!({
                                        "status": "error",
                                        "message": format!("Invalid query format: {}", e)
                                    });
                                    log_rest_event(&format!("REST API: Invalid query format: {}", e));
                                    Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                                        .unwrap())
                                }
                            }
                        }
                        Err(e) => {
                            let response = json!({
                                "status": "error",
                                "message": format!("Invalid query data or JSON format: {}", e)
                            });
                            log_rest_event(&format!("REST API: Invalid query data or JSON format: {}", e));
                            Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(serde_json::to_string(&response).unwrap()))
                                .unwrap())
                        }
                    }
                }
                Err(e) => {
                    let response = json!({
                        "status": "error",
                        "message": format!("Failed to read request body: {}", e)
                    });
                    log_rest_event(&format!("REST API: Failed to read request body for query: {}", e));
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                        .unwrap())
                }
            }
        }
        _ => {
            let response = json!({
                "status": "error",
                "message": "Not Found"
            });
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(serde_json::to_string(&response).unwrap()))
                .unwrap())
        }
    }
}

/// Starts the REST API server using Hyper.
///
/// This function sets up and runs the Hyper HTTP server, binding to the
/// specified port. It also includes a `oneshot::Receiver` for graceful shutdown.
///
/// # Arguments
/// * `port`: The port number on which the server should listen.
/// * `shutdown_rx`: A `oneshot::Receiver` that, when triggered, will initiate
///   a graceful shutdown of the HTTP server.
/// * `db_path`: The path to the database, passed to the StorageClient.
///
/// # Returns
/// A `Result` indicating success or an error if the server fails to start or run.
pub async fn start_server(
    port: u16,
    shutdown_rx: oneshot::Receiver<()>,
    db_path: String, // This is `storage_daemon_address` from the old code
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("[REST_API] Attempting to start server on port {}", port);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    log_rest_event(&format!("REST API listening on http://{}", addr));

    // Initialize the storage client (mock for now)
    let storage_client = Arc::new(StorageClient::new(db_path)); // Use the new mock StorageClient
    let storage_client_clone = storage_client.clone(); // Clone for the service closure

    // Create a channel for internal server shutdown signal
    let (server_shutdown_tx, server_shutdown_rx) = oneshot::channel();
    let server_shutdown_tx_arc = Arc::new(Mutex::new(Some(server_shutdown_tx))); // Arc for handler access

    let service = make_service_fn(move |_conn| {
        let storage_client_for_service = storage_client_clone.clone();
        let server_shutdown_tx_for_service = server_shutdown_tx_arc.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                handle_request(
                    req,
                    storage_client_for_service.clone(),
                    server_shutdown_tx_for_service.clone(),
                )
            }))
        }
    });

    let server = Server::bind(&addr).serve(service);

    let graceful = server.with_graceful_shutdown(async {
        // Wait for either the CLI shutdown signal OR the internal /api/v1/stop signal
        tokio::select! {
            _ = shutdown_rx => {
                println!("[REST_API] Shutdown signal received from CLI. Initiating graceful server shutdown.");
            }
            _ = server_shutdown_rx => {
                println!("[REST_API] Internal /api/v1/stop signal received. Initiating graceful server shutdown.");
            }
        }
    });

    println!("[REST_API] Server started on http://0.0.0.0:{}/", port);

    graceful.await?;

    println!("[REST_API] Server gracefully shut down.");
    Ok(())
}

