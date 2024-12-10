use axum::extract::State;
use axum::http::StatusCode;
use ulid::Ulid;

use crate::models::{Chair, Ride, RideStatus};
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
    for active_chair in active_chairs {
        let latest: Option<RideStatus> = sqlx::query_as(
            "select *
            from (select * from rides where chair_id = ?) as myrides
            inner join ride_statuses on myrides.id = ride_statuses.ride_id
            order by ride_statuses.created_at desc
            limit 1
        ",
        )
        .bind(&active_chair.id)
        .fetch_optional(&pool)
        .await?;

        let ok = latest.is_none()
            || latest
                .as_ref()
                .is_some_and(|x| x.status == "COMPLETED" && x.chair_sent_at.is_some());

        if ok {
            let f = if let Some(l) = latest {
                format!("{},{:?}", l.status, l.chair_sent_at)
            } else {
                "none".to_owned()
            };
            tracing::info!("marking {} as free: latest = {f}", active_chair.id);
            free_chairs.push(active_chair);
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
            .bind(&free_chair.id)
            .bind(&ride.id)
            .execute(&pool)
            .await?;
        sqlx::query("INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)")
            .bind(Ulid::new().to_string())
            .bind(&ride.id)
            .bind("MATCHING")
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
