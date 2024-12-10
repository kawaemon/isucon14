use axum::extract::State;
use axum::http::StatusCode;

use crate::chair_handlers::{ChairGetNotificationResponseData, SimpleUser};
use crate::models::{Chair, Ride, RideStatus};
use crate::{AppState, Error, QcNotification};

pub fn internal_routes() -> axum::Router<AppState> {
    axum::Router::new().route(
        "/api/internal/matching",
        axum::routing::get(internal_get_matching),
    )
}

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
// COMPLETED 通知送信後であることが必須
async fn internal_get_matching() -> Result<StatusCode, Error> {
    Ok(StatusCode::NO_CONTENT)
}

pub async fn do_matching(
    AppState {
        pool, cache, queue, ..
    }: AppState,
) -> Result<(), Error> {
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
    let active_count = active_chairs.len();

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
        let status: RideStatus = sqlx::query_as(
            "select * from ride_statuses where ride_id=? order by created_at asc limit 1",
        )
        .bind(&ride.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        queue
            .chair_notification_queue
            .lock()
            .await
            .get_mut(&chair.id)
            .unwrap()
            .push(QcNotification {
                status_id: status.id,
                data: ChairGetNotificationResponseData {
                    ride_id: ride.id.clone(),
                    pickup_coordinate: ride.pickup_coordinate(),
                    destination_coordinate: ride.destination_coordinate(),
                    user: SimpleUser {
                        name: {
                            let cache = cache.user_auth_cache.read().await;
                            let cache = cache.iter().find(|x| x.1.id == ride.user_id).unwrap().1;
                            format!("{} {}", cache.firstname, cache.lastname)
                        },
                        id: ride.user_id,
                    },
                    status: "MATCHING".to_owned(),
                },
            });
        matches += 1;
    }

    if rides_count > 0 {
        tracing::info!("matching: waiting={rides_count}, active={active_count}, free={free_count}, matches={matches}",);
    }
    Ok(())
}
