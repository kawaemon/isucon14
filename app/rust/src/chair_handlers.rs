use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum_extra::extract::cookie::Cookie;
use axum_extra::extract::CookieJar;
use ulid::Ulid;

use crate::models::{Chair, ChairLocation, Owner, Ride, RideStatus, User};
use crate::{AppState, Coordinate, Error, NOTIFICATION_RETRY_MS_CHAIR};

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
    owner_id: String,
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
    State(AppState { pool, .. }): State<AppState>,
    axum::Extension(chair): axum::Extension<Chair>,
    axum::Json(req): axum::Json<Coordinate>,
) -> Result<axum::Json<ChairPostCoordinateResponse>, Error> {
    let mut tx = pool.begin().await?;

    let chair_location_id = Ulid::new().to_string();
    sqlx::query(
        "INSERT INTO chair_locations (id, chair_id, latitude, longitude) VALUES (?, ?, ?, ?)",
    )
    .bind(&chair_location_id)
    .bind(&chair.id)
    .bind(req.latitude)
    .bind(req.longitude)
    .execute(&mut *tx)
    .await?;

    let location: ChairLocation = sqlx::query_as("SELECT * FROM chair_locations WHERE id = ?")
        .bind(chair_location_id)
        .fetch_one(&mut *tx)
        .await?;

    let ride: Option<Ride> =
        sqlx::query_as("SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1")
            .bind(chair.id)
            .fetch_optional(&mut *tx)
            .await?;
    if let Some(ride) = ride {
        let status = crate::get_latest_ride_status(&mut *tx, &ride.id).await?;
        if status != "COMPLETED" && status != "CANCELED" {
            if req.latitude == ride.pickup_latitude
                && req.longitude == ride.pickup_longitude
                && status == "ENROUTE"
            {
                sqlx::query("INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)")
                    .bind(Ulid::new().to_string())
                    .bind(&ride.id)
                    .bind("PICKUP")
                    .execute(&mut *tx)
                    .await?;
            }

            if req.latitude == ride.destination_latitude
                && req.longitude == ride.destination_longitude
                && status == "CARRYING"
            {
                sqlx::query("INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)")
                    .bind(Ulid::new().to_string())
                    .bind(&ride.id)
                    .bind("ARRIVED")
                    .execute(&mut *tx)
                    .await?;
            }
        }
    }

    tx.commit().await?;

    Ok(axum::Json(ChairPostCoordinateResponse {
        recorded_at: location.created_at.timestamp_millis(),
    }))
}

#[derive(Debug, serde::Serialize)]
struct SimpleUser {
    id: String,
    name: String,
}

#[derive(Debug, serde::Serialize)]
struct ChairGetNotificationResponse {
    data: Option<ChairGetNotificationResponseData>,
    retry_after_ms: Option<i32>,
}

#[derive(Debug, serde::Serialize)]
struct ChairGetNotificationResponseData {
    ride_id: String,
    user: SimpleUser,
    pickup_coordinate: Coordinate,
    destination_coordinate: Coordinate,
    status: String,
}

async fn chair_get_notification(
    State(AppState { pool, .. }): State<AppState>,
    axum::Extension(chair): axum::Extension<Chair>,
) -> Result<axum::Json<ChairGetNotificationResponse>, Error> {
    let mut tx = pool.begin().await?;

    // もし椅子に一つも ride が存在しなかったら None
    // 存在し、かつ未通知のものが存在すれば未通知のものを通知済みとしてマークして返す、
    // そうでなかれば通知済みの最新のものを返す

    /*
     */

    let Some(ride_status): Option<RideStatus> = sqlx::query_as(
        "select *
        from ride_statuses
        inner join (
            select id from rides where rides.chair_id = ?
        ) as rides_of_chair on rides_of_chair.id = ride_statuses.ride_id
        order by chair_sent_at is null desc, created_at asc
        limit 1",
    )
    .bind(&chair.id)
    .fetch_optional(&mut *tx)
    .await?
    else {
        return Ok(axum::Json(ChairGetNotificationResponse {
            data: None,
            retry_after_ms: Some(NOTIFICATION_RETRY_MS_CHAIR),
        }));
    };

    let ride: Ride = sqlx::query_as("select * from rides where id=?")
        .bind(&ride_status.ride_id)
        .fetch_one(&mut *tx)
        .await?;

    let user: User = sqlx::query_as("SELECT * FROM users WHERE id = ?")
        .bind(&ride.user_id)
        .fetch_one(&mut *tx)
        .await?;

    if ride_status.chair_sent_at.is_none() {
        sqlx::query("UPDATE ride_statuses SET chair_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?")
            .bind(ride_status.id)
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;

    Ok(axum::Json(ChairGetNotificationResponse {
        data: Some(ChairGetNotificationResponseData {
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
            status: ride_status.status,
        }),
        retry_after_ms: Some(NOTIFICATION_RETRY_MS_CHAIR),
    }))
}

#[derive(Debug, serde::Deserialize)]
struct PostChairRidesRideIDStatusRequest {
    status: String,
}

async fn chair_post_ride_status(
    State(AppState { pool, .. }): State<AppState>,
    axum::Extension(chair): axum::Extension<Chair>,
    Path((ride_id,)): Path<(String,)>,
    axum::Json(req): axum::Json<PostChairRidesRideIDStatusRequest>,
) -> Result<StatusCode, Error> {
    let mut tx = pool.begin().await?;

    let Some(ride): Option<Ride> = sqlx::query_as("SELECT * FROM rides WHERE id = ? FOR UPDATE")
        .bind(ride_id)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Err(Error::NotFound("rides not found"));
    };

    if ride.chair_id.is_none_or(|chair_id| chair_id != chair.id) {
        return Err(Error::BadRequest("not assigned to this ride"));
    }

    match req.status.as_str() {
        // Acknowledge the ride
        "ENROUTE" => {
            sqlx::query("INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)")
                .bind(Ulid::new().to_string())
                .bind(ride.id)
                .bind("ENROUTE")
                .execute(&mut *tx)
                .await?;
        }
        // After Picking up user
        "CARRYING" => {
            let status = crate::get_latest_ride_status(&mut *tx, &ride.id).await?;
            if status != "PICKUP" {
                return Err(Error::BadRequest("chair has not arrived yet"));
            }
            sqlx::query("INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)")
                .bind(Ulid::new().to_string())
                .bind(ride.id)
                .bind("CARRYING")
                .execute(&mut *tx)
                .await?;
        }
        _ => {
            return Err(Error::BadRequest("invalid status"));
        }
    };

    tx.commit().await?;

    Ok(StatusCode::NO_CONTENT)
}
