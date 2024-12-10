use axum::extract::State;
use axum::http::StatusCode;

use crate::models::{Chair, Ride, RideStatus};
use crate::{AppState, Error};

pub fn internal_routes() -> axum::Router<AppState> {
    axum::Router::new().route(
        "/api/internal/matching",
        axum::routing::get(internal_get_matching),
    )
}

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
// COMPLETED 通知送信後であることが必須
async fn internal_get_matching(
    State(AppState { pool, cache, .. }): State<AppState>,
) -> Result<StatusCode, Error> {
    let waiting_rides: Vec<Ride> =
        sqlx::query_as("SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at")
            .fetch_all(&pool)
            .await?;

    let active_chairs = cache
        .chair_auth_cache
        .read()
        .await
        .iter()
        .filter(|(_k, v)| v.is_active)
        .map(|(_k, v)| v.clone())
        .collect::<Vec<_>>();

    let mut free_chairs: Vec<Chair> = vec![];
    for active_chair in active_chairs {
        let ok: bool = sqlx::query_scalar(
            "
                select count(*) = 0
                from (
                    select count(chair_sent_at is not null) = 6 as ok
                    from ride_statuses
                    where ride_id in (select id from rides where chair_id = ?)
                    group by ride_id
                ) as tmp
                where ok = false
            ",
        )
        .bind(&active_chair.id)
        .fetch_one(&pool)
        .await?;
        if ok {
            free_chairs.push(active_chair);
        }
    }
    let free_count = free_chairs.len();

    let rides_count = waiting_rides.len();
    let mut matches = 0;

    for ride in waiting_rides {
        let Some(chair) = free_chairs.pop() else {
            break;
        };
        sqlx::query("UPDATE rides SET chair_id = ? WHERE id = ?")
            .bind(&chair.id)
            .bind(&ride.id)
            .execute(&pool)
            .await?;
        matches += 1;
    }

    if rides_count > 0 {
        tracing::info!("matching: waiting={rides_count}, free={free_count}, matches={matches}",);
    }

    Ok(StatusCode::NO_CONTENT)
}
