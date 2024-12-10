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
    let rides: Vec<Ride> =
        sqlx::query_as("SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at")
            .fetch_all(&pool)
            .await?;

    let active_chairs: Vec<Chair> = sqlx::query_as("select * from chairs where is_active = true")
        .fetch_all(&pool)
        .await?;
    let active_count = active_chairs.len();

    let mut free_chairs = vec![];
    for act in active_chairs {
        let empty: bool = sqlx::query_scalar(
                "SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = ?) GROUP BY ride_id) is_completed WHERE completed = FALSE",
            )
            .bind(&act.id)
            .fetch_one(&pool)
            .await?;
        if empty {
            free_chairs.push(act);
        }
    }
    let free_count = free_chairs.len();

    let rides_count = rides.len();
    let mut matches = 0;

    for ride in rides {
        let Some(free_chair) = free_chairs.pop() else {
            break;
        };
        sqlx::query("UPDATE rides SET chair_id = ? WHERE id = ?")
            .bind(free_chair.id)
            .bind(ride.id)
            .execute(&pool)
            .await?;
        matches += 1;
    }

    if rides_count > 0 {
        tracing::info!(
            "matching: waiting={rides_count}, active={active_count}, free={free_count}, matched={matches}",
        );
    }

    Ok(StatusCode::NO_CONTENT)
}
