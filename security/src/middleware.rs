// security/src/middleware.rs
// Updated: 2025-06-30 - Corrected `map_into_left_body` usage.

use actix_web::{
    Error, HttpResponse,
    dev::{Service, ServiceRequest, ServiceResponse, Transform},
    body::{EitherBody, BoxBody},
};
use futures::future::LocalBoxFuture;
use std::rc::Rc;
use std::task::{Context, Poll};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::future::{ready, Ready};


#[derive(Debug, Deserialize, Serialize)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
}

pub struct AuthMiddleware;

impl<S> Transform<S, ServiceRequest> for AuthMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<EitherBody<BoxBody>>, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse<EitherBody<BoxBody>>;
    type Error = Error;
    type InitError = ();
    type Transform = AuthMiddlewareService<S>;
    type Future = Ready<Result<<Self as Transform<S, ServiceRequest>>::Transform, <Self as Transform<S, ServiceRequest>>::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(AuthMiddlewareService { service: Rc::new(service) }))
    }
}

pub struct AuthMiddlewareService<S> {
    service: Rc<S>,
}

impl<S> Service<ServiceRequest> for AuthMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<EitherBody<BoxBody>>, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse<EitherBody<BoxBody>>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<<Self as Service<ServiceRequest>>::Response, <Self as Service<ServiceRequest>>::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), <Self as Service<ServiceRequest>>::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> <Self as Service<ServiceRequest>>::Future {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        validation.validate_nbf = true;

        let secret = std::env::var("JWT_SECRET")
            .expect("JWT_SECRET must be set in the environment or a .env file");

        let token = req.headers()
            .get("Authorization")
            .and_then(|header| header.to_str().ok())
            .and_then(|header| header.strip_prefix("Bearer "))
            .map(|s| s.to_string());

        let s_cloned = self.service.clone();

        Box::pin(async move {
            match token {
                Some(t) => {
                    match jsonwebtoken::decode::<Claims>(
                        &t,
                        &DecodingKey::from_secret(secret.as_ref()),
                        &validation,
                    ) {
                        Ok(_token_data) => {
                            s_cloned.call(req).await
                        },
                        Err(e) => {
                            eprintln!("JWT validation error: {:?}", e);
                            Ok(req.into_response(
                                HttpResponse::Unauthorized()
                                    .body("Invalid Token")
                                    .map_into_left_body() // <-- CORRECTED: Call map_into_left_body on HttpResponse directly
                            ))
                        }
                    }
                },
                None => {
                    Ok(req.into_response(
                        HttpResponse::Unauthorized()
                            .body("Missing Token")
                            .map_into_left_body() // <-- CORRECTED: Call map_into_left_body on HttpResponse directly
                    ))
                }
            }
        })
    }
}
