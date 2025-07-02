// rest_api/src/lib.rs

use hyper::{Body, Request, Response, Server, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use std::net::SocketAddr;
use std::convert::Infallible;
use serde_json::json;
use tokio::task;
use std::str::FromStr;
use tokio::sync::oneshot;
use std::sync::Arc; // Needed for Arc<StorageClient>
use serde::{Deserialize, Serialize}; // Needed for UserRegistration, UserLogin, StoreQueryRequest

// Assuming these are defined in your project.
// Adjust paths if your crate structure is different.
use daemon_api::{self, DaemonError, SHUTDOWN_FLAG}; // Assuming SHUTDOWN_FLAG is still relevant for some daemon control
use security::{self, UserRegistration, UserLogin, AuthError}; // Corrected: use security crate types
use storage_daemon_server::storage_client::StorageClient; // Added: Import StorageClient

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
fn fix_damaged_json(input: &str) -> std::borrow::Cow<str> {
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
        std::borrow::Cow::Owned(temp_string)
    } else {
        std::borrow::Cow::Borrowed(input)
    }
}


// This function now correctly accepts the Arc<StorageClient>
async fn handle_request(
    req: Request<Body>,
    storage_client: Arc<StorageClient>, // Passed from the server's service_fn
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
        // RE-ADDED and CORRECTED: /api/v1/login endpoint
        (&hyper::Method::POST, "/api/v1/login") => {
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

                            let query_result = task::spawn(async move {
                                store_query_in_engine(query_clone, persist_clone).await
                            })
                            .await
                            .unwrap(); // Handle JoinError

                            match query_result {
                                Ok(_) => {
                                    let response = json!({
                                        "status": "success",
                                        "message": "Query processed successfully"
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

// start_server function now takes the storage_daemon_address
pub async fn start_server(
    port: u16,
    mut shutdown_rx: oneshot::Receiver<()>,
    storage_daemon_address: String, // Added storage_daemon_address parameter
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    log_rest_event(&format!("REST API listening on http://{}", addr));

    // Initialize StorageClient once and clone its Arc for each request
    let storage_client = Arc::new(StorageClient::new(storage_daemon_address));

    let make_svc = make_service_fn(move |_conn| {
        // Clone Arc for each new service handler (each connection)
        let storage_client_clone = Arc::clone(&storage_client);

        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                // Clone Arc again for each request within a connection
                // This allows handle_request to own a copy of the Arc
                handle_request(req, Arc::clone(&storage_client_clone))
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_svc);

    let graceful_shutdown = server.with_graceful_shutdown(async {
        shutdown_rx.await.ok(); // Wait for the shutdown signal
        log_rest_event("REST API received explicit shutdown signal via oneshot channel.");
    });

    if let Err(e) = graceful_shutdown.await {
        eprintln!("REST API server error: {}", e);
    }
    Ok(())
}
