// rest_api/src/lib.rs
// Corrected: 2025-06-30 - Fixed regex lookaround panic by replacing problematic
// regexes in `fix_damaged_json` with manual string iteration and replacement
// for escaping newlines/tabs.

use hyper::{Body, Request, Response, Server, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use serde_json::json;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::fs::OpenOptions;
use std::io::Write;
use anyhow::Result;
// No longer need `regex::Regex` if we avoid lookarounds
// use regex::Regex; // <--- REMOVED THIS
use std::borrow::Cow;
// No longer need `lazy_static` if we avoid Regex compilation
// use lazy_static::lazy_static; // <--- REMOVED THIS

// Imports from daemon_api (only daemon management - start/stop)
use daemon_api::{
    start_daemon,
    stop_daemon,
    DaemonError,
};

// Imports from security (user auth functions and DTOs)
use security::{
    register_user,
    login_user,
    AuthError,
    UserRegistration,
    UserLogin,
};

use graphdb_lib::query_parser::{parse_query_from_string, QueryType};
use serde::{Deserialize, Serialize};

// StoreQueryRequest remains here or in a common types crate
#[derive(Debug, Deserialize, Serialize)]
pub struct StoreQueryRequest {
    pub query: String,
    pub persist: bool, // Flag to indicate whether to persist the query
}

fn log_rest_event(message: &str) {
    if let Ok(mut log) = OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/graphdb-rest.log") {
        let _ = writeln!(log, "{message}");
    }
}

// Placeholder for query storage logic
async fn store_query_in_engine(query: String, persist: bool) -> Result<()> {
    log_rest_event(&format!(
        "SIMULATING QUERY STORAGE: Query: \"{}\", Persist: {}",
        query, persist
    ));
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    Ok(())
}

/// **WARNING: HIGHLY EXPERIMENTAL AND FRAGILE.**
/// Attempts to "fix" malformed JSON strings by removing leading/trailing garbage
/// and escaping unescaped newlines/tabs within the content.
/// This function is not a robust JSON parser/validator. It makes assumptions
/// about the nature of the "damage".
///
/// Returns a `Cow<'static, str>` to avoid unnecessary allocations if no changes are made.
fn fix_damaged_json(input: &str) -> Cow<str> {
    log_rest_event(&format!("Attempting to fix damaged JSON input: {:?}", input));

    let mut changed = false;
    let mut cleaned_json_owned = input.trim().to_string();

    // 1. Remove leading/trailing backslashes and single quotes
    // This is often seen in `curl -d 'JSON'` where shell escapes are included.
    if cleaned_json_owned.starts_with('\'') {
        cleaned_json_owned.remove(0);
        changed = true;
    }
    if cleaned_json_owned.ends_with('\'') {
        cleaned_json_owned.pop();
        changed = true;
    }

    // Remove any trailing `\n` characters that might come from bad copy-pasting
    let original_len = cleaned_json_owned.len();
    cleaned_json_owned = cleaned_json_owned.trim_end_matches("\\n").to_string();
    if cleaned_json_owned.len() != original_len { changed = true; }

    let original_len = cleaned_json_owned.len();
    cleaned_json_owned = cleaned_json_owned.trim_end_matches('\n').to_string();
    if cleaned_json_owned.len() != original_len { changed = true; }


    // 2. Try to find the actual JSON content if there's garbage before/after
    // This assumes JSON starts with { or [ and ends with } or ]
    let mut current_slice = cleaned_json_owned.as_str();
    if let Some(start_idx) = current_slice.find('{').or_else(|| current_slice.find('[')) {
        if start_idx > 0 { changed = true; } // If we're stripping leading chars
        // Find the last closing brace/bracket after the start_idx
        if let Some(end_idx_relative) = current_slice[start_idx..].rfind('}').or_else(|| current_slice[start_idx..].rfind(']')) {
            let effective_end_idx = start_idx + end_idx_relative + 1;
            if effective_end_idx < current_slice.len() { changed = true; } // If we're stripping trailing chars
            current_slice = &current_slice[start_idx..effective_end_idx];
            log_rest_event(&format!("JSON boundaries identified. Extracted: {}", current_slice));
        } else {
             log_rest_event("Could not find matching closing brace/bracket after start. Proceeding with trimmed string.");
        }
    } else {
        log_rest_event("Could not find starting brace/bracket. Proceeding with trimmed string.");
    }
    cleaned_json_owned = current_slice.to_string(); // Update cleaned_json_owned to the isolated part


    // 3. Escape common unescaped characters within the JSON string.
    // Replace regex with manual iteration and replacement to avoid lookaround issues.
    let mut temp_string = String::with_capacity(cleaned_json_owned.len());
    let mut chars = cleaned_json_owned.chars().peekable();
    let mut current_pos = 0;

    while let Some(c) = chars.next() {
        if c == '\\' {
            temp_string.push(c); // Always push the backslash
            if let Some(next_c) = chars.peek() {
                // If it's an escaped character, push the next character
                if *next_c == 'n' || *next_c == 't' || *next_c == '\\' || *next_c == '"' {
                    temp_string.push(chars.next().unwrap()); // consume and push
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
        log_rest_event(&format!("Finished JSON fixing attempt. Result: {}", temp_string));
        Cow::Owned(temp_string)
    } else {
        log_rest_event(&format!("Finished JSON fixing attempt. Result: {}", cleaned_json_owned));
        Cow::Borrowed(input) // Return original reference if no changes were made
    }
}


async fn handle_request(
    req: Request<Body>,
    shutdown_flag: Arc<Mutex<bool>>,
    rest_api_port: u16,
) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path();
    let method = req.method();

    match (method, path) {
        (&hyper::Method::GET, "/api/v1/health") => {
            let response = json!({
                "status": "ok",
                "message": "GraphDB REST API is healthy"
            });
            log_rest_event("REST API: Health check requested, responding ok.");
            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
        }

        (&hyper::Method::GET, "/api/v1/version") => {
            let response = json!({
                "version": env!("CARGO_PKG_VERSION")
            });
            log_rest_event(&format!("REST API: Version requested, responding {}", env!("CARGO_PKG_VERSION")));
            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
        }

        (&hyper::Method::POST, "/api/v1/query") => {
            let whole_body = hyper::body::to_bytes(req.into_body()).await.unwrap_or_default();
            let body_str = match std::str::from_utf8(&whole_body) {
                Ok(s) => s,
                Err(e) => {
                    log_rest_event(&format!("REST API: Failed to parse request body as UTF-8 for query endpoint: {}", e));
                    let response = json!({"status": "error", "message": "Invalid UTF-8 in request body"});
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                        .unwrap());
                }
            };
            let fixed_body_str = fix_damaged_json(body_str);

            let input: serde_json::Value = serde_json::from_str(&fixed_body_str).unwrap_or_default();
            let query = input.get("query").and_then(|q| q.as_str()).unwrap_or("").to_string();
            let mut response = json!({
                "status": "error",
                "message": "No query provided"
            });

            if !query.is_empty() {
                match parse_query_from_string(&query) {
                    Ok(query_type) => {
                        let message = match query_type {
                            QueryType::Cypher => format!("Cypher query detected: {}", query),
                            QueryType::SQL => format!("SQL query detected: {}", query),
                            QueryType::GraphQL => format!("GraphQL query detected: {}", query),
                        };
                        response = json!({
                            "status": "success",
                            "message": message
                        });
                        log_rest_event(&format!("REST API: Query parsed successfully: {}", message));
                    }
                    Err(e) => {
                        response = json!({
                            "status": "error",
                            "message": format!("Failed to parse query: {}", e)
                        });
                        log_rest_event(&format!("REST API: Failed to parse query: {}", e));
                    }
                }
            } else {
                log_rest_event("REST API: Query endpoint received no query.");
            }

            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
        }


        // POST to store a query with a persistence flag
        (&hyper::Method::POST, "/api/v1/store_query") => {
            let whole_body = hyper::body::to_bytes(req.into_body()).await.unwrap_or_default();
            let body_str = match std::str::from_utf8(&whole_body) {
                Ok(s) => s,
                Err(e) => {
                    log_rest_event(&format!("REST API: Failed to parse request body as UTF-8 for store_query endpoint: {}", e));
                    let response = json!({"status": "error", "message": "Invalid UTF-8 in request body"});
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                        .unwrap());
                }
            };
            let fixed_body_str = fix_damaged_json(body_str);

            let store_query_data: Result<StoreQueryRequest, _> = serde_json::from_str(&fixed_body_str);

            match store_query_data {
                Ok(data) => {
                    log_rest_event(&format!("REST API: Received request to store query: \"{}\" with persist: {}", data.query, data.persist));

                    let query_to_store = data.query.clone();
                    let persist_flag = data.persist;
                    // Now calling the local `store_query_in_engine` function
                    let result_future = tokio::task::spawn(async move {
                         store_query_in_engine(query_to_store, persist_flag).await
                    }).await.unwrap();

                    match result_future {
                        Ok(()) => {
                            let response = json!({
                                "status": "success",
                                "message": format!("Query stored successfully. Persist flag: {}", persist_flag)
                            });
                            log_rest_event(&format!("REST API: Query stored successfully. Persist flag: {}", persist_flag));
                            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
                        }
                        Err(e) => {
                            let response = json!({
                                "status": "error",
                                "message": format!("Failed to store query: {:?}", e)
                            });
                            log_rest_event(&format!("REST API: Failed to store query: {:?}", e));
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
                        "message": format!("Invalid input for store_query: {}", e)
                    });
                    log_rest_event(&format!("REST API: Invalid input for store_query: {}", e));
                    Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                        .unwrap())
                }
            }
        }

        // POST to register a new user - NOW CALLING security::register_user
        (&hyper::Method::POST, "/api/v1/register") => {
            let whole_body = hyper::body::to_bytes(req.into_body()).await.unwrap_or_default();
            let body_str = match std::str::from_utf8(&whole_body) {
                Ok(s) => s,
                Err(e) => {
                    log_rest_event(&format!("REST API: Failed to parse request body as UTF-8 for register endpoint: {}", e));
                    let response = json!({"status": "error", "message": "Invalid UTF-8 in request body"});
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                        .unwrap());
                }
            };
            // Apply the fixer function before deserialization
            let fixed_body_str = fix_damaged_json(body_str);
            let registration_data: Result<UserRegistration, _> = serde_json::from_str(&fixed_body_str);

            match registration_data {
                Ok(user_reg) => {
                    log_rest_event(&format!("REST API: Received registration request for user: {}", user_reg.username));

                    // Call the async register_user function from the security crate
                    let result_future = tokio::task::spawn(async move {
                        security::register_user(user_reg).await
                    }).await.unwrap();

                    match result_future {
                        Ok(()) => {
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

        // POST to login a user - NOW CALLING security::login_user
        (&hyper::Method::POST, "/api/v1/login") => {
            let whole_body = hyper::body::to_bytes(req.into_body()).await.unwrap_or_default();
            let body_str = match std::str::from_utf8(&whole_body) {
                Ok(s) => s,
                Err(e) => {
                    log_rest_event(&format!("REST API: Failed to parse request body as UTF-8 for login endpoint: {}", e));
                    let response = json!({"status": "error", "message": "Invalid UTF-8 in request body"});
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                        .unwrap());
                }
            };
            // Apply the fixer function before deserialization
            let fixed_body_str = fix_damaged_json(body_str);
            let login_data: Result<UserLogin, _> = serde_json::from_str(&fixed_body_str);

            match login_data {
                Ok(user_login) => {
                    log_rest_event(&format!("REST API: Received login request for user: {}", user_login.username));

                    // Call the async login_user function from the security crate
                    let result_future = tokio::task::spawn(async move {
                        security::login_user(user_login).await
                    }).await.unwrap();

                    match result_future {
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
                                AuthError::UserExists => (StatusCode::BAD_REQUEST, "Unexpected error: User exists during login check".to_string()),
                                AuthError::JwtError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, format!("JWT generation error: {}", msg)),
                                AuthError::StorageInitializationError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Storage initialization error: {}", msg)),
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

        (&hyper::Method::POST, path) if path.starts_with("/api/v1/start/port/") => {
            let port_str = path.strip_prefix("/api/v1/start/port/").unwrap();
            match port_str.parse::<u16>() {
                Ok(port) => {
                    if port == rest_api_port {
                        let response = json!({
                            "status": "error",
                            "message": format!("Port {} is reserved for the REST API", port)
                        });
                        log_rest_event(&format!("REST API: Attempt to start daemon on REST API port {} rejected", port));
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(serde_json::to_string(&response).unwrap()))
                            .unwrap());
                    }
                    log_rest_event(&format!("REST API: Received request to start daemon on port {}", port));
                    let skip_ports = vec![rest_api_port];
                    let result = tokio::task::spawn_blocking(move || {
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap()
                            .block_on(daemon_api::start_daemon(Some(port), None, skip_ports))
                    }).await.unwrap();
                    match result {
                        Ok(()) => {
                            let response = json!({
                                "status": "success",
                                "message": format!("Daemon started on port {}", port)
                            });
                            log_rest_event(&format!("REST API: Daemon started on port {} (success)", port));
                            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
                        }
                        Err(e) => {
                            let response = json!({
                                "status": "error",
                                "message": format!("Failed to start daemon on port {}: {:?}", port, e)
                            });
                            log_rest_event(&format!("REST API: Failed to start daemon on port {}: {:?}", port, e));
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
                    log_rest_event("REST API: Invalid port number requested");
                    Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                        .unwrap())
                }
            }
        }
        (&hyper::Method::POST, path) if path.starts_with("/api/v1/start/cluster/") => {
            let range_str = path.strip_prefix("/api/v1/start/cluster/").unwrap();
            let parts: Vec<&str> = range_str.split('-').collect();
            if parts.len() == 2 {
                match (parts[0].parse::<u16>(), parts[1].parse::<u16>()) {
                    (Ok(start_port), Ok(end_port)) if start_port <= end_port && end_port - start_port <= 10 => {
                        if (start_port..=end_port).contains(&rest_api_port) {
                            let response = json!({
                                "status": "error",
                                "message": format!("Cluster range {}-{} overlaps REST API port {}", start_port, end_port, rest_api_port)
                            });
                            log_rest_event(&format!("REST API: Attempt to start cluster overlapping REST API port {} rejected", rest_api_port));
                            return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(serde_json::to_string(&response).unwrap()))
                                .unwrap());
                        }
                        log_rest_event(&format!("REST API: Received request to start cluster {}-{}", start_port, end_port));
                        let cluster = Some(format!("{}-{}", start_port, end_port));
                        let skip_ports = vec![rest_api_port];
                        let result = tokio::task::spawn_blocking(move || {
                            tokio::runtime::Builder::new_current_thread()
                                .enable_all()
                                .build()
                                .unwrap()
                                .block_on(daemon_api::start_daemon(None, cluster, skip_ports))
                        }).await.unwrap();
                        match result {
                            Ok(()) => {
                                let response = json!({
                                    "status": "success",
                                    "message": format!("Daemon cluster started on ports {}-{}", start_port, end_port)
                                });
                                log_rest_event(&format!("REST API: Cluster started on ports {}-{} (success)", start_port, end_port));
                                Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
                            }
                            Err(e) => {
                                let response = json!({
                                    "status": "error",
                                    "message": format!("Failed to start daemon cluster on ports {}-{}: {:?}", start_port, end_port, e)
                                });
                                log_rest_event(&format!("REST API: Failed to start cluster on ports {}-{}: {:?}", start_port, end_port, e));
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
                            "message": "Invalid port range or range exceeds 10 ports"
                        });
                        log_rest_event("REST API: Invalid cluster range requested or range exceeds 10 ports");
                        Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(serde_json::to_string(&response).unwrap()))
                            .unwrap())
                    }
                }
            } else {
                let response = json!({
                    "status": "error",
                    "message": "Invalid cluster range format. Use start-end (e.g., 9001-9005)"
                });
                log_rest_event("REST API: Invalid cluster range format");
                Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                    .unwrap())
            }
        }
        (&hyper::Method::POST, "/api/v1/stop") => {
            log_rest_event("REST API: Received request to stop all daemons and REST API server.");
            let _ = tokio::task::spawn_blocking(move || {
                daemon_api::stop_daemon()
            }).await.unwrap();

            let response = json!({
                "status": "success",
                "message": "All daemons and REST API server stopped"
            });
            *shutdown_flag.lock().unwrap() = true;
            log_rest_event("REST API: All daemons and REST API server stopped (success)");
            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
        }
        _ => {
            let response = json!({
                "status": "error",
                "message": "Endpoint not found or method not allowed"
            });
            log_rest_event(&format!("REST API: Unknown endpoint or method: {} {}", method, path));
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(serde_json::to_string(&response).unwrap()))
                .unwrap())
        }
    }
}

pub async fn start_server(
    listen_port: u16,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>
) -> Result<(), hyper::Error> {
    let addr = SocketAddr::from(([127, 0, 0, 1], listen_port));
    let shutdown_flag = Arc::new(Mutex::new(false));
    let shutdown_flag_for_service = Arc::clone(&shutdown_flag);
    let make_service = make_service_fn(move |_conn| {
        let shutdown_flag = Arc::clone(&shutdown_flag_for_service);
        let rest_api_port = listen_port;
        async move {
            Ok::<_, Infallible>(service_fn(move |req| handle_request(req, Arc::clone(&shutdown_flag), rest_api_port)))
        }
    });
    let server = Server::bind(&addr)
        .serve(make_service)
        .with_graceful_shutdown(async {
            tokio::select! {
                _ = shutdown_rx => {
                    log_rest_event("REST API: Oneshot shutdown signal received. Shutting down server.");
                },
                _ = async {
                    while !*shutdown_flag.lock().unwrap() {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                    log_rest_event("REST API: Internal shutdown flag set. Shutting down server.");
                } => {},
            }
        });
    println!("REST API server running on http://{}", addr);
    log_rest_event(&format!("REST API server running on http://{}", addr));
    server.await
}
