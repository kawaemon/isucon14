use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;

use crate::{AppState, Error};

pub fn spawn_matching_thread(state: AppState) {
    tokio::spawn(async move {
        loop {
            // if let Err(e) = do_matching(&state).await {
            //     tracing::warn!("matching failed: {e:?}; continuing anyway");
            // }
            state.repo.do_matching().await;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    });
}

pub fn internal_routes() -> axum::Router<AppState> {
    axum::Router::new().route(
        "/api/internal/matching",
        axum::routing::get(internal_get_matching),
    )
}

async fn internal_get_matching(_: State<AppState>) -> Result<StatusCode, Error> {
    Ok(StatusCode::NO_CONTENT)
}
