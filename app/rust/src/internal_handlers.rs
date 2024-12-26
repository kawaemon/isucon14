use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;

use crate::{AppState, Error};

crate::conf_env!(static MATCHING_INTERVAL_MS: u64 = {
    from: "MATCHING_INTERVAL_MS",
    default: "250",
});

pub fn spawn_matching_thread(state: AppState) {
    tokio::spawn(async move {
        loop {
            state.repo.do_matching().await;
            tokio::time::sleep(Duration::from_millis(*MATCHING_INTERVAL_MS)).await;
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
