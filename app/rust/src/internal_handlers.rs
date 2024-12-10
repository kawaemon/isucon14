use axum::extract::State;
use axum::http::StatusCode;

use crate::models::{Chair, Ride};
use crate::{AppState, Error};

pub fn internal_routes() -> axum::Router<AppState> {
    axum::Router::new().route(
        "/api/internal/matching",
        axum::routing::get(internal_get_matching),
    )
}

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
async fn internal_get_matching(
    State(AppState { pool, .. }): State<AppState>,
) -> Result<StatusCode, Error> {
    // MEMO: 一旦最も待たせているリクエストに適当な空いている椅子マッチさせる実装とする。おそらくもっといい方法があるはず…
    let rides: Vec<Ride> =
        sqlx::query_as("SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at")
            .fetch_all(&pool)
            .await?;

    let rides_count = rides.len();
    let mut matches = 0;
    let mut trials = 0;

    for ride in rides {
        for _ in 0..10 {
            trials += 1;
            let Some(matched): Option<Chair> =
            sqlx::query_as("SELECT * FROM chairs INNER JOIN (SELECT id FROM chairs WHERE is_active = TRUE ORDER BY RAND() LIMIT 1) AS tmp ON chairs.id = tmp.id LIMIT 1")
                .fetch_optional(&pool)
                .await?
            else {
                return Ok(StatusCode::NO_CONTENT);
            };

            let empty: bool = sqlx::query_scalar(
                "SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = ?) GROUP BY ride_id) is_completed WHERE completed = FALSE",
            )
            .bind(&matched.id)
            .fetch_one(&pool)
            .await?;

            if empty {
                sqlx::query("UPDATE rides SET chair_id = ? WHERE id = ?")
                    .bind(matched.id)
                    .bind(ride.id)
                    .execute(&pool)
                    .await?;
                matches += 1;
                break;
            }
        }
    }

    if rides_count > 0 {
        tracing::info!(
            "matching: waiting={rides_count}, matched {matches} with {trials} trials => {:.02}",
            (trials as f64) / (matches as f64)
        );
    }

    Ok(StatusCode::NO_CONTENT)
}
