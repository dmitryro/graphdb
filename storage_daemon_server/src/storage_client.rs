// storage_daemon_server/src/storage_client.rs

// Import StorageRequest and StorageResponse from the parent crate's root
use crate::{StorageRequest, StorageResponse};
use anyhow::{Result, Context}; // Import Context for .context()
use tokio::time::Duration; // For simulating async operations
use serde_json; // ADD THIS LINE
use models::medical::{User, NewUser}; // Make sure this is correctly imported for the mock methods

#[derive(Debug, Clone)]
pub struct StorageClient {
    address: String,
    // Potentially a gRPC client stub or other connection here
}

impl StorageClient {
    pub fn new(address: String) -> Self {
        println!("StorageClient: Creating new client for address: {}", address);
        StorageClient { address }
    }

    pub async fn connect(address: String) -> Result<Self> {
        println!("StorageClient: Attempting to connect to {}", address);
        // In a real implementation, this would involve establishing a gRPC connection
        // or other network connection.
        tokio::time::sleep(Duration::from_millis(50)).await; // Simulate async connection
        println!("StorageClient: Connected to {}", address);
        Ok(StorageClient { address })
    }

    // Example client method using StorageRequest and StorageResponse
    pub async fn send_request(&self, request: StorageRequest) -> Result<StorageResponse> {
        println!("StorageClient: Sending request: {:?} to {}", request, self.address);
        // Simulate network call and response
        tokio::time::sleep(Duration::from_millis(30)).await;

        match request.operation.as_str() {
            "GET" => {
                // Mock behavior for get_user_by_username
                if request.key.starts_with("user_role:") {
                    if request.key == "user_role:admin" {
                        Ok(StorageResponse {
                            success: true,
                            message: "Data retrieved successfully".to_string(),
                            data: Some("admin_role_data".to_string()),
                        })
                    } else if request.key == "user_role:testuser" {
                        Ok(StorageResponse {
                            success: true,
                            message: "Data retrieved successfully".to_string(),
                            data: Some("user_role_data".to_string()),
                        })
                    } else {
                        Ok(StorageResponse {
                            success: true,
                            message: "Data retrieved successfully".to_string(),
                            data: None, // User not found
                        })
                    }
                } else if request.key.starts_with("user_data:") {
                    // Mock for get_user_by_username
                    if request.key == "user_data:existing_user" {
                        // Mock a serialized User object
                        let mock_user_json = serde_json::to_string(&serde_json::json!({
                            "id": "some-uuid",
                            "first": "Test",
                            "last": "User",
                            "username": "existing_user",
                            "email": "test@example.com",
                            "password_hash": "$argon2id$v=19$m=16,t=2,p=1$c29tZXNhbHQ$c29tZWhhc2g", // Mock hash
                            "phone": null,
                            "role_id": 1,
                            "created_at": chrono::Utc::now().timestamp(), // Use chrono for mock timestamp
                            "updated_at": chrono::Utc::now().timestamp()
                        })).unwrap();
                        Ok(StorageResponse {
                            success: true,
                            message: "User data retrieved".to_string(),
                            data: Some(mock_user_json),
                        })
                    } else {
                        Ok(StorageResponse {
                            success: true,
                            message: "User not found".to_string(),
                            data: None,
                        })
                    }
                }
                else {
                     Ok(StorageResponse {
                        success: true,
                        message: "Data retrieved successfully".to_string(),
                        data: Some(format!("mock_data_for_{}", request.key)),
                    })
                }
            },
            "PUT" => {
                // Mock for create_user
                if request.key.starts_with("user_data:") {
                    println!("StorageClient: Mocking user creation for key: {}", request.key);
                    Ok(StorageResponse {
                        success: true,
                        message: "User created successfully (mock)".to_string(),
                        data: None,
                    })
                } else {
                    Ok(StorageResponse {
                        success: true,
                        message: format!("Data '{}' put for key '{}'", request.value.unwrap_or_default(), request.key),
                        data: None,
                    })
                }
            },
            _ => {
                Ok(StorageResponse {
                    success: false,
                    message: format!("Unsupported operation: {}", request.operation),
                    data: None,
                })
            }
        }
    }

    // --- Mock methods to satisfy `security/src/lib.rs`'s expectations ---
    // These methods wrap `send_request` to provide the specific API expected by the security crate.

    // Mock for `get_user_by_username`
    pub async fn get_user_by_username(&self, username: &str) -> Result<Option<User>> { // Changed from models::medical::User to User (from models::medical import)
        let request = StorageRequest {
            key: format!("user_data:{}", username), // Assuming users are stored by "user_data:<username>"
            value: None,
            operation: "GET".to_string(),
        };
        let response = self.send_request(request).await?;
        if response.success {
            if let Some(data_str) = response.data {
                // Attempt to deserialize the User object
                let user: User = serde_json::from_str(&data_str)
                    .context(format!("Failed to deserialize User from storage data: {}", data_str))?;
                Ok(Some(user))
            } else {
                Ok(None) // User not found
            }
        } else {
            anyhow::bail!("Failed to get user by username: {}", response.message)
        }
    }

    // Mock for `create_user`
    pub async fn create_user(&self, user: User) -> Result<()> { // Changed from models::medical::User to User
        let user_json = serde_json::to_string(&user)
            .context("Failed to serialize User for storage")?;
        let request = StorageRequest {
            key: format!("user_data:{}", user.username), // Store by username
            value: Some(user_json),
            operation: "PUT".to_string(),
        };
        let response = self.send_request(request).await?;
        if response.success {
            Ok(())
        } else {
            anyhow::bail!("Failed to create user in storage: {}", response.message)
        }
    }
    // --- End Mock methods ---
}
