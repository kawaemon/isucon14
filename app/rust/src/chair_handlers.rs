use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::Event;
use axum::response::Sse;
use axum_extra::extract::cookie::Cookie;
use axum_extra::extract::CookieJar;
use chrono::Utc;
use futures::Stream;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::StreamExt;
use ulid::Ulid;

use crate::models::{Chair, ChairLocation, Id, Owner, Ride, RideStatus, RideStatusEnum, User};
use crate::{AppState, Coordinate, Error};

pub fn chair_routes(app_state: AppState) -> axum::Router<AppState> {
    let routes =
        axum::Router::new().route("/api/chair/chairs", axum::routing::post(chair_post_chairs));

    let authed_routes = axum::Router::new()
        .route(
            "/api/chair/activity",
            axum::routing::post(chair_post_activity),
        )
        .route(
            "/api/chair/coordinate",
            axum::routing::post(chair_post_coordinate),
        )
        .route(
            "/api/chair/notification",
            axum::routing::get(chair_get_notification),
        )
        .route(
            "/api/chair/rides/:ride_id/status",
            axum::routing::post(chair_post_ride_status),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            app_state.clone(),
            crate::middlewares::chair_auth_middleware,
        ));

    routes.merge(authed_routes)
}

#[derive(Debug, serde::Deserialize)]
struct ChairPostChairsRequest {
    name: String,
    model: String,
    chair_register_token: String,
}

#[derive(Debug, serde::Serialize)]
struct ChairPostChairsResponse {
    id: String,
    owner_id: Id<Owner>,
}

async fn chair_post_chairs(
    State(AppState { pool, .. }): State<AppState>,
    jar: CookieJar,
    axum::Json(req): axum::Json<ChairPostChairsRequest>,
) -> Result<(CookieJar, (StatusCode, axum::Json<ChairPostChairsResponse>)), Error> {
    let Some(owner): Option<Owner> =
        sqlx::query_as("SELECT * FROM owners WHERE chair_register_token = ?")
            .bind(req.chair_register_token)
            .fetch_optional(&pool)
            .await?
    else {
        return Err(Error::Unauthorized("invalid chair_register_token"));
    };

    let chair_id = Ulid::new().to_string();
    let access_token = crate::secure_random_str(32);

    sqlx::query("INSERT INTO chairs (id, owner_id, name, model, is_active, access_token) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(&chair_id)
        .bind(&owner.id)
        .bind(req.name)
        .bind(req.model)
        .bind(false)
        .bind(&access_token)
        .execute(&pool)
        .await?;

    let jar = jar.add(Cookie::build(("chair_session", access_token)).path("/"));

    Ok((
        jar,
        (
            StatusCode::CREATED,
            axum::Json(ChairPostChairsResponse {
                id: chair_id,
                owner_id: owner.id,
            }),
        ),
    ))
}

#[derive(Debug, serde::Deserialize)]
struct PostChairActivityRequest {
    is_active: bool,
}

async fn chair_post_activity(
    State(AppState { pool, .. }): State<AppState>,
    axum::Extension(chair): axum::Extension<Chair>,
    axum::Json(req): axum::Json<PostChairActivityRequest>,
) -> Result<StatusCode, Error> {
    sqlx::query("UPDATE chairs SET is_active = ? WHERE id = ?")
        .bind(req.is_active)
        .bind(chair.id)
        .execute(&pool)
        .await?;

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, serde::Serialize)]
struct ChairPostCoordinateResponse {
    recorded_at: i64,
}

async fn chair_post_coordinate(
    State(AppState { pool, repo, .. }): State<AppState>,
    axum::Extension(chair): axum::Extension<Chair>,
    axum::Json(req): axum::Json<Coordinate>,
) -> Result<axum::Json<ChairPostCoordinateResponse>, Error> {
    let mut tx = pool.begin().await?;

    let created_at = repo.chair_location_update(&mut tx, &chair.id, req).await?;

    if let Some((ride, status)) = repo.rides_get_assigned(&mut tx, &chair.id).await? {
        if status != RideStatusEnum::Completed && status != RideStatusEnum::Canceled {
            if req.latitude == ride.pickup_latitude
                && req.longitude == ride.pickup_longitude
                && status == RideStatusEnum::Enroute
            {
                repo.ride_status_update(&mut tx, &ride.id, RideStatusEnum::Pickup)
                    .await?;
            }

            if req.latitude == ride.destination_latitude
                && req.longitude == ride.destination_longitude
                && status == RideStatusEnum::Carrying
            {
                repo.ride_status_update(&mut tx, &ride.id, RideStatusEnum::Arrived)
                    .await?;
            }
        }
    }

    tx.commit().await?;

    Ok(axum::Json(ChairPostCoordinateResponse {
        recorded_at: created_at.timestamp_millis(),
    }))
}

#[derive(Debug, serde::Serialize)]
struct SimpleUser {
    id: Id<User>,
    name: String,
}

#[derive(Debug, serde::Serialize)]
struct ChairGetNotificationResponseData {
    ride_id: Id<Ride>,
    user: SimpleUser,
    pickup_coordinate: Coordinate,
    destination_coordinate: Coordinate,
    status: RideStatusEnum,
}

async fn chair_get_notification(
    State(state): State<AppState>,
    axum::Extension(chair): axum::Extension<Chair>,
) -> Sse<impl Stream<Item = Result<Event, Error>>> {
    let stream =
        IntervalStream::new(tokio::time::interval(Duration::from_millis(30))).then(move |_| {
            let state = state.clone();
            let chair = chair.clone();
            async move {
                let s = chair_get_notification_inner(&state, &chair).await?;
                let s = serde_json::to_string(&s).unwrap();
                Ok(Event::default().data(s))
            }
        });

    Sse::new(stream)
}

async fn chair_get_notification_inner(
    AppState { pool, .. }: &AppState,
    chair: &Chair,
) -> Result<Option<ChairGetNotificationResponseData>, Error> {
    let mut tx = pool.begin().await?;

    let Some(ride): Option<Ride> =
        sqlx::query_as("SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1")
            .bind(&chair.id)
            .fetch_optional(&mut *tx)
            .await?
    else {
        return Ok(None);
    };

    let yet_sent_ride_status: Option<RideStatus> =
        sqlx::query_as("SELECT * FROM ride_statuses WHERE ride_id = ? AND chair_sent_at IS NULL ORDER BY created_at ASC LIMIT 1")
        .bind(&ride.id)
        .fetch_optional(&mut *tx)
        .await?;
    let (yet_sent_ride_status_id, status) = if let Some(yet_sent_ride_status) = yet_sent_ride_status
    {
        (Some(yet_sent_ride_status.id), yet_sent_ride_status.status)
    } else {
        (
            None,
            crate::get_latest_ride_status(&mut *tx, &ride.id).await?,
        )
    };

    let user: User = sqlx::query_as("SELECT * FROM users WHERE id = ? FOR SHARE")
        .bind(ride.user_id)
        .fetch_one(&mut *tx)
        .await?;

    if let Some(yet_sent_ride_status_id) = yet_sent_ride_status_id {
        sqlx::query("UPDATE ride_statuses SET chair_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?")
            .bind(yet_sent_ride_status_id)
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;

    Ok(Some(ChairGetNotificationResponseData {
        ride_id: ride.id,
        user: SimpleUser {
            id: user.id,
            name: format!("{} {}", user.firstname, user.lastname),
        },
        pickup_coordinate: Coordinate {
            latitude: ride.pickup_latitude,
            longitude: ride.pickup_longitude,
        },
        destination_coordinate: Coordinate {
            latitude: ride.destination_latitude,
            longitude: ride.destination_longitude,
        },
        status,
    }))
}

#[derive(Debug, serde::Deserialize)]
struct PostChairRidesRideIDStatusRequest {
    status: RideStatusEnum,
}

async fn chair_post_ride_status(
    State(AppState { pool, repo, .. }): State<AppState>,
    axum::Extension(chair): axum::Extension<Chair>,
    Path((ride_id,)): Path<(Id<Ride>,)>,
    axum::Json(req): axum::Json<PostChairRidesRideIDStatusRequest>,
) -> Result<StatusCode, Error> {
    let mut tx = pool.begin().await?;

    let Some(ride): Option<Ride> = sqlx::query_as("SELECT * FROM rides WHERE id = ? FOR UPDATE")
        .bind(&ride_id)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Err(Error::NotFound("rides not found"));
    };

    if ride.chair_id.is_none_or(|chair_id| chair_id != chair.id) {
        return Err(Error::BadRequest("not assigned to this ride"));
    }

    let next = match req.status {
        // Acknowledge the ride
        RideStatusEnum::Enroute => RideStatusEnum::Enroute,
        // After Picking up user
        RideStatusEnum::Carrying => {
            let status = crate::get_latest_ride_status(&mut *tx, &ride.id).await?;
            if status != RideStatusEnum::Pickup {
                return Err(Error::BadRequest("chair has not arrived yet"));
            }
            RideStatusEnum::Carrying
        }
        _ => {
            return Err(Error::BadRequest("invalid status"));
        }
    };

    repo.ride_status_update(&mut tx, &ride_id, next).await?;

    tx.commit().await?;

    Ok(StatusCode::NO_CONTENT)
}
