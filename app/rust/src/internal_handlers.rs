use axum::http::StatusCode;
use std::time::Duration;

use crate::models::{Chair, Ride};
use crate::{AppState, Error};

pub fn spawn_matcher(state: AppState) {
    tokio::spawn(async move {
        loop {
            if let Err(e) = matching(state.clone()).await {
                tracing::warn!("failed to matching: {e:?}; continuing anyway");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    });
}

async fn matching(AppState { pool, .. }: AppState) -> Result<StatusCode, Error> {
    let waiting_rides: Vec<Ride> =
        sqlx::query_as("SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at")
            .fetch_all(&pool)
            .await?;
    let wait_len = waiting_rides.len();

    let active_chairs: Vec<Chair> = sqlx::query_as("select * from chairs where is_active = true")
        .fetch_all(&pool)
        .await?;
    let act_len = active_chairs.len();

    let mut free_chairs: Vec<Chair> = {
        let mut res = vec![];
        for c in active_chairs {
            let free: bool = sqlx::query_scalar(
                "select count(*) = 0 from rides where chair_id = ? and evaluation is null",
            )
            .bind(&c.id)
            .fetch_one(&pool)
            .await?;
            if free {
                res.push(c);
            }
        }
        res
    };
    let free_len = free_chairs.len();

    let mut match_count = 0;
    for ride in waiting_rides {
        // TODO: 座標や速度に基づいて計算する
        let Some(chair) = free_chairs.pop() else {
            break;
        };
        match_count += 1;
        sqlx::query("UPDATE rides SET chair_id = ? WHERE id = ?")
            .bind(chair.id)
            .bind(ride.id)
            .execute(&pool)
            .await?;
    }

    if match_count != 0 {
        tracing::info!("wait={wait_len}, active={act_len}, free={free_len}, matched={match_count}");
    }

    Ok(StatusCode::NO_CONTENT)
}
