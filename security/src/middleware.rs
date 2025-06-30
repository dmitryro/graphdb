use actix_web::{dev::ServiceRequest, dev::ServiceResponse, Error, HttpMessage, web, HttpResponse};
use actix_service::{Service, Transform};
use futures_util::future::{ok, Ready, LocalBoxFuture};
use crate::models::medical::user::User;
use crate::security::roles::RolesConfig;
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::{Deserialize};
use std::rc::Rc;
use std::future::{ready, Ready as StdReady};

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
    role_id: u32,
    exp: usize,
}

pub struct AuthMiddleware {
    pub secret: Vec<u8>,
    pub roles_config: Rc<RolesConfig>,
    pub required_permission: String,
}

impl<S, B> Transform<S, ServiceRequest> for AuthMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = AuthMiddlewareService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(AuthMiddlewareService {
            service: Rc::new(service),
            secret: self.secret.clone(),
            roles_config: Rc::clone(&self.roles_config),
            required_permission: self.required_permission.clone(),
        })
    }
}

pub struct AuthMiddlewareService<S> {
    service: Rc<S>,
    secret: Vec<u8>,
    roles_config: Rc<RolesConfig>,
    required_permission: String,
}

impl<S, B> Service<ServiceRequest> for AuthMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let secret = self.secret.clone();
        let roles_config = Rc::clone(&self.roles_config);
        let required_permission = self.required_permission.clone();
        let svc = Rc::clone(&self.service);

        Box::pin(async move {
            let jwt = req.headers().get("Authorization")
                .and_then(|hv| hv.to_str().ok())
                .and_then(|auth| auth.strip_prefix("Bearer "))
                .ok_or_else(|| actix_web::error::ErrorUnauthorized("Missing or invalid token"))?;

            let token_data = decode::<Claims>(&jwt, &DecodingKey::from_secret(&secret), &Validation::default())
                .map_err(|_| actix_web::error::ErrorUnauthorized("Invalid token"))?;

            let perms = roles_config.get_permissions(token_data.claims.role_id)
                .ok_or_else(|| actix_web::error::ErrorForbidden("Role not found"))?;

            if !perms.contains(&required_permission) && !perms.contains(&"superuser".to_string()) {
                return Err(actix_web::error::ErrorForbidden("Permission denied"));
            }

            // Optionally, attach user info to request extensions here for downstream use
            req.extensions_mut().insert(token_data.claims);

            svc.call(req).await
        })
    }
}
