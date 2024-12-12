use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;

use crate::models::{Chair, Ride};
use crate::{AppState, Error};

pub fn spawn_matching_thread(state: AppState) {
    tokio::spawn(async move {
        loop {
            if let Err(e) = do_matching(&state).await {
                tracing::warn!("matching failed: {e:?}; continuing anyway");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
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

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
async fn do_matching(AppState { pool, .. }: &AppState) -> Result<StatusCode, Error> {
    let waiting_rides: Vec<Ride> =
        sqlx::query_as("SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at")
            .fetch_all(pool)
            .await?;

    let mut matches = 0;

    for ride in waiting_rides {
        for _ in 0..10 {
            let Some(matched): Option<Chair> =
            sqlx::query_as("SELECT * FROM chairs INNER JOIN (SELECT id FROM chairs WHERE is_active = TRUE ORDER BY RAND() LIMIT 1) AS tmp ON chairs.id = tmp.id LIMIT 1")
                .fetch_optional(pool)
                .await?
        else {
            return Ok(StatusCode::NO_CONTENT);
        };

            let empty: bool = sqlx::query_scalar(
                "SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = ?) GROUP BY ride_id) is_completed WHERE completed = FALSE",
            )
            .bind(&matched.id)
            .fetch_one(pool)
            .await?;

            if empty {
                sqlx::query("UPDATE rides SET chair_id = ? WHERE id = ?")
                    .bind(matched.id)
                    .bind(ride.id)
                    .execute(pool)
                    .await?;
                matches += 1;
                break;
            }
        }
    }

    if matches > 0 {
        tracing::info!("matched {matches}");
    }

    Ok(StatusCode::NO_CONTENT)
}
