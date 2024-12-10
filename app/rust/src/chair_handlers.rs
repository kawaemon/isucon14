use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum_extra::extract::cookie::Cookie;
use axum_extra::extract::CookieJar;
use chrono::Utc;
use ulid::Ulid;

use crate::models::{Chair, ChairLocation, Owner, Ride, RideStatus, User};
use crate::{
    AppState, ChairLocationCache, Coordinate, Error, RideCache, NOTIFICATION_RETRY_MS_CHAIR,
};

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
    State(AppState { pool, cache, .. }): State<AppState>,
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

    let now = Utc::now();

    sqlx::query("INSERT INTO chairs (id, owner_id, name, model, is_active, access_token, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(&chair_id)
        .bind(&owner.id)
        .bind(&req.name)
        .bind(&req.model)
        .bind(false)
        .bind(&access_token)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await?;
    cache.chair_cache.write().await.insert(
        access_token.clone(),
        Chair {
            id: chair_id.clone(),
            owner_id: owner.id.clone(),
            name: req.name,
            access_token: access_token.clone(),
            model: req.model.clone(),
            is_active: false,
            created_at: now,
            updated_at: now,
        },
    );

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
    State(AppState { pool, cache, .. }): State<AppState>,
    axum::Extension(chair): axum::Extension<Chair>,
    axum::Json(req): axum::Json<PostChairActivityRequest>,
) -> Result<StatusCode, Error> {
    let now = Utc::now();
    sqlx::query("UPDATE chairs SET is_active = ?, updated_at = ? WHERE id = ?")
        .bind(req.is_active)
        .bind(now)
        .bind(&chair.id)
        .execute(&pool)
        .await?;
    {
        let mut cache = cache.chair_cache.write().await;
        let chair = cache.get_mut(&chair.access_token).unwrap();
        chair.is_active = req.is_active;
        chair.updated_at = now;
    }

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, serde::Serialize)]
struct ChairPostCoordinateResponse {
    recorded_at: i64,
}

async fn chair_post_coordinate(
    State(AppState {
        pool,
        cache,
        deferred,
    }): State<AppState>,
    axum::Extension(chair): axum::Extension<Chair>,
    axum::Json(req): axum::Json<Coordinate>,
) -> Result<axum::Json<ChairPostCoordinateResponse>, Error> {
    let now = Utc::now();
    let chair_location_id = Ulid::new().to_string();

    let loc = ChairLocation {
        id: chair_location_id.clone(),
        chair_id: chair.id.clone(),
        latitude: req.latitude,
        longitude: req.longitude,
        created_at: now,
    };
    tokio::spawn(async move {
        deferred.location_queue.lock().await.push(loc);
    });

    {
        let coord = Coordinate {
            latitude: req.latitude,
            longitude: req.longitude,
        };
        let mut cache = cache.chair_location.write().await;
        if let Some(cache) = cache.get_mut(&chair.id) {
            cache.update(coord, &now);
        } else {
            cache.insert(chair.id.clone(), ChairLocationCache::new(coord, &now));
        }
    }

    tokio::spawn(async move {
        let mut tx = pool.begin().await.unwrap();

        let mut should_remove = false;
        if let Some(ride) = cache.chair_ride_cache.read().await.get(&chair.id) {
            if req.latitude == ride.going_to.latitude && req.longitude == ride.going_to.longitude {
                let new_status = match ride.status.as_str() {
                    "ENROUTE" => "PICKUP",
                    "CARRYING" => "ARRIVED",
                    _ => unreachable!(),
                };

                sqlx::query("INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)")
                    .bind(Ulid::new().to_string())
                    .bind(&ride.ride_id)
                    .bind(new_status)
                    .execute(&mut *tx)
                    .await
                    .unwrap();

                cache
                    .ride_status_cache
                    .write()
                    .await
                    .insert(ride.ride_id.clone(), new_status.to_owned());
                should_remove = true;
            }
        }

        if should_remove {
            cache.chair_ride_cache.write().await.remove(&chair.id);
        }

        tx.commit().await.unwrap();
    });

    Ok(axum::Json(ChairPostCoordinateResponse {
        recorded_at: now.timestamp_millis(),
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
    State(AppState { pool, cache, .. }): State<AppState>,
    axum::Extension(chair): axum::Extension<Chair>,
) -> Result<axum::Json<ChairGetNotificationResponse>, Error> {
    let mut tx = pool.begin().await?;

    let Some(ride): Option<Ride> =
        sqlx::query_as("SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1")
            .bind(&chair.id)
            .fetch_optional(&mut *tx)
            .await?
    else {
        return Ok(axum::Json(ChairGetNotificationResponse {
            data: None,
            retry_after_ms: Some(NOTIFICATION_RETRY_MS_CHAIR),
        }));
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
            cache
                .ride_status_cache
                .read()
                .await
                .get(&ride.id)
                .unwrap()
                .clone(),
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
            status,
        }),
        retry_after_ms: Some(NOTIFICATION_RETRY_MS_CHAIR),
    }))
}

#[derive(Debug, serde::Deserialize)]
struct PostChairRidesRideIDStatusRequest {
    status: String,
}

async fn chair_post_ride_status(
    State(AppState { pool, cache, .. }): State<AppState>,
    axum::Extension(chair): axum::Extension<Chair>,
    Path((ride_id,)): Path<(String,)>,
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

    if ride
        .chair_id
        .as_ref()
        .is_none_or(|chair_id| chair_id != &chair.id)
    {
        tracing::error!(
            "not assigned to this ride (expected: {:?}, actual: {:?})",
            chair.id,
            ride.chair_id
        );
        return Err(Error::BadRequest("not assigned to this ride"));
    }

    match req.status.as_str() {
        // Acknowledge the ride
        "ENROUTE" => {
            sqlx::query("INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)")
                .bind(Ulid::new().to_string())
                .bind(&ride.id)
                .bind("ENROUTE")
                .execute(&mut *tx)
                .await?;

            cache
                .ride_status_cache
                .write()
                .await
                .insert(ride.id.clone(), "ENROUTE".to_owned());
        }
        // After Picking up user
        "CARRYING" => {
            if cache.ride_status_cache.read().await.get(&ride.id).unwrap() != "PICKUP" {
                return Err(Error::BadRequest("chair has not arrived yet"));
            }

            sqlx::query("INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)")
                .bind(Ulid::new().to_string())
                .bind(&ride.id)
                .bind("CARRYING")
                .execute(&mut *tx)
                .await?;
            cache
                .ride_status_cache
                .write()
                .await
                .insert(ride.id.clone(), "CARRYING".to_owned());
        }
        _ => {
            return Err(Error::BadRequest("invalid status"));
        }
    };

    cache.chair_ride_cache.write().await.insert(
        ride.chair_id.unwrap(),
        RideCache {
            ride_id,
            going_to: match req.status.as_str() {
                "ENROUTE" => Coordinate {
                    latitude: ride.pickup_latitude,
                    longitude: ride.pickup_longitude,
                },
                "CARRYING" => Coordinate {
                    latitude: ride.destination_latitude,
                    longitude: ride.destination_longitude,
                },
                _ => unreachable!(),
            },
            status: req.status,
        },
    );

    tx.commit().await?;

    Ok(StatusCode::NO_CONTENT)
}
