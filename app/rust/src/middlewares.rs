use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use axum_extra::extract::CookieJar;

use crate::models::{Owner, User};
use crate::repo::chair::EffortlessChair;
use crate::{AppState, Error};

#[cfg(not(feature = "speed"))]
pub async fn log_slow_requests(req: Request, next: Next) -> Result<Response, Error> {
    Ok(next.run(req).await)
}

#[cfg(feature = "speed")]
pub async fn log_slow_requests(
    State(AppState { speed, .. }): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, Error> {
    use std::time::Instant;
    let uri = req.uri().clone();
    let mut path = uri.path();
    let method = req.method().clone();

    let begin = Instant::now();
    let res = next.run(req).await;
    let e = begin.elapsed();

    if path.starts_with("/api/chair/rides") && path.ends_with("/status") {
        path = "/api/chair/rides/:id/status";
    }
    if path.starts_with("/api/app/rides") && path.ends_with("/evaluation") {
        path = "/api/app/rides/:id/evaluation";
    }
    let key = format!("{method} {path}");
    speed.on_request(&key, e).await;

    Ok(res)
}

pub async fn app_auth_middleware(
    State(AppState { repo, .. }): State<AppState>,
    jar: CookieJar,
    mut req: Request,
    next: Next,
) -> Result<Response, Error> {
    let Some(c) = jar.get("app_session") else {
        return Err(Error::Unauthorized("app_session cookie is required"));
    };
    let access_token = c.value();
    let Some(user): Option<User> = repo.user_get_by_access_token(access_token).await? else {
        return Err(Error::Unauthorized("invalid access token"));
    };

    req.extensions_mut().insert(user);

    Ok(next.run(req).await)
}

pub async fn owner_auth_middleware(
    State(AppState { repo, .. }): State<AppState>,
    jar: CookieJar,
    mut req: Request,
    next: Next,
) -> Result<Response, Error> {
    let Some(c) = jar.get("owner_session") else {
        return Err(Error::Unauthorized("owner_session cookie is required"));
    };
    let access_token = c.value();
    let Some(owner): Option<Owner> = repo.owner_get_by_access_token(access_token).await? else {
        return Err(Error::Unauthorized("invalid access token"));
    };

    req.extensions_mut().insert(owner);

    Ok(next.run(req).await)
}

pub async fn chair_auth_middleware(
    State(AppState { repo, .. }): State<AppState>,
    jar: CookieJar,
    mut req: Request,
    next: Next,
) -> Result<Response, Error> {
    let Some(c) = jar.get("chair_session") else {
        return Err(Error::Unauthorized("chair_session cookie is required"));
    };
    let access_token = c.value();
    let Some(chair): Option<EffortlessChair> = repo.chair_get_by_access_token(access_token).await?
    else {
        return Err(Error::Unauthorized("invalid access token"));
    };

    req.extensions_mut().insert(chair);

    Ok(next.run(req).await)
}
