use actix_web::{web, App, HttpServer, HttpResponse, Responder, middleware::Logger};
use serde::{Deserialize, Serialize};
use chrono::{Utc, DateTime};
use std::net::SocketAddr;
use std::sync::Arc;

// --- Mocked security module ---
mod security {
    pub mod roles {
        #[derive(Clone)]
        pub struct RolesConfig;
        impl RolesConfig {
            pub fn from_yaml_file(_path: &str) -> Result<Self, ()> {
                Ok(RolesConfig)
            }
        }
    }
    pub mod middleware {
        use serde::{Serialize, Deserialize};
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct Claims {
            pub sub: String,
            pub role_id: u32,
            pub exp: usize,
        }
        pub fn create_jwt(_claims: &Claims, _secret: &[u8]) -> String {
            "mock.jwt.token".to_string()
        }
    }
}

// --- Dummy User model (replace with your actual one!) ---
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: u32,
    pub first: String,
    pub last: String,
    pub username: String,
    pub email: String,
    pub password: String,
    pub phone: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub role_id: u32,
}

// --------- UserStore trait definition ---------
#[async_trait::async_trait]
pub trait UserStore: Send + Sync + 'static {
    async fn register_user(&self, req: &RegisterRequest) -> Result<User, String>;
    async fn authenticate(&self, username: &str, password: &str) -> Result<Option<User>, String>;
}

// --------- Request/Response Structs ---------
#[derive(Deserialize)]
struct RegisterRequest {
    first: String,
    last: String,
    username: String,
    email: String,
    password: String,
    phone: Option<String>,
    role_id: u32,
}

#[derive(Deserialize)]
struct AuthRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct AuthResponse {
    token: String,
    user_id: u32,
    role_id: u32,
}

async fn health() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "message": "GraphDB REST API is healthy",
        "status": "ok"
    }))
}

// --------- Endpoint Handlers ---------
async fn register(
    req: web::Json<RegisterRequest>,
    store: web::Data<Arc<dyn UserStore>>,
) -> impl Responder {
    match store.register_user(&req).await {
        Ok(user) => HttpResponse::Ok().json(user),
        Err(err) => HttpResponse::BadRequest().body(format!("Registration failed: {err}")),
    }
}

async fn auth(
    req: web::Json<AuthRequest>,
    store: web::Data<Arc<dyn UserStore>>,
    _roles: web::Data<security::roles::RolesConfig>,
    jwt_secret: web::Data<Vec<u8>>,
) -> impl Responder {
    match store.authenticate(&req.username, &req.password).await {
        Ok(Some(user)) => {
            let claims = security::middleware::Claims {
                sub: user.username.clone(),
                role_id: user.role_id,
                exp: (Utc::now() + chrono::Duration::hours(8)).timestamp() as usize,
            };
            let token = security::middleware::create_jwt(&claims, &jwt_secret);
            HttpResponse::Ok().json(AuthResponse {
                token,
                user_id: user.id,
                role_id: user.role_id,
            })
        }
        Ok(None) => HttpResponse::Unauthorized().body("Invalid credentials"),
        Err(err) => HttpResponse::InternalServerError().body(format!("Auth error: {err}")),
    }
}

// --------- Dummy setup_store_from_args ---------
fn setup_store_from_args() -> Arc<dyn UserStore> {
    Arc::new(InMemoryUserStore::default())
}

// --------- Dummy InMemoryUserStore Implementation ---------
#[derive(Default)]
struct InMemoryUserStore;

#[async_trait::async_trait]
impl UserStore for InMemoryUserStore {
    async fn register_user(&self, req: &RegisterRequest) -> Result<User, String> {
        Ok(User {
            id: 1,
            first: req.first.clone(),
            last: req.last.clone(),
            username: req.username.clone(),
            email: req.email.clone(),
            password: req.password.clone(),
            phone: req.phone.clone(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            role_id: req.role_id,
        })
    }
    async fn authenticate(&self, username: &str, password: &str) -> Result<Option<User>, String> {
        if username == "alicesmith" && password == "supersecret" {
            Ok(Some(User {
                id: 1,
                first: "Alice".into(),
                last: "Smith".into(),
                username: username.into(),
                email: "alice@example.com".into(),
                password: password.into(),
                phone: Some("1234567890".into()),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                role_id: 2,
            }))
        } else {
            Ok(None)
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    let port: u16 = 8082;
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    println!("GraphDB REST API server starting on http://{}", addr);

    let roles_config = security::roles::RolesConfig::from_yaml_file("security/roles_permissions.yaml").unwrap();
    let jwt_secret = b"super_secret_key".to_vec();
    let store: Arc<dyn UserStore> = setup_store_from_args();

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(web::Data::new(store.clone()))
            .app_data(web::Data::new(roles_config.clone()))
            .app_data(web::Data::new(jwt_secret.clone()))
            .service(
                web::scope("/api/v1")
                    .route("/health", web::get().to(health))
                    .route("/register", web::post().to(register))
                    .route("/auth", web::post().to(auth))
            )
    })
    .bind(addr)?
    .run()
    .await
}
