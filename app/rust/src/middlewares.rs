use std::time::Duration;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use axum_extra::extract::CookieJar;

use crate::models::{Chair, Owner, User};
use crate::{AppState, Error};

pub async fn log_slow_requests(req: Request, next: Next) -> Result<Response, Error> {
    let uri = req.uri().clone();
    let path = uri.path();
    let method = req.method().clone();

    tokio::pin! {
        let response_fut = next.run(req);
    }

    let mut secs = 0;

    let res = loop {
        tokio::pin! {
            let timeout = tokio::time::sleep(Duration::from_secs(1));
        }
        tokio::select! {
            _ = &mut timeout => {
                secs += 1;
                tracing::debug!("{method} {path} is taking {secs} seconds and continuing...");
            }
            res = &mut response_fut => {
                break res;
            }
        }
    };

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
    let Some(chair): Option<Chair> = repo.chair_get_by_access_token(access_token).await? else {
        return Err(Error::Unauthorized("invalid access token"));
    };

    req.extensions_mut().insert(chair);

    Ok(next.run(req).await)
}
