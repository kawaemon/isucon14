use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::Event;
use axum::response::Sse;
use axum_extra::extract::cookie::Cookie;
use axum_extra::extract::CookieJar;
use futures::Stream;
use tokio_stream::StreamExt;

use crate::models::{Chair, Id, Owner, Ride, RideStatusEnum, User};
use crate::repo::ride::NotificationBody;
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
    id: Id<Chair>,
    owner_id: Id<Owner>,
}

async fn chair_post_chairs(
    State(AppState { repo, .. }): State<AppState>,
    jar: CookieJar,
    axum::Json(req): axum::Json<ChairPostChairsRequest>,
) -> Result<(CookieJar, (StatusCode, axum::Json<ChairPostChairsResponse>)), Error> {
    let Some(owner): Option<Owner> = repo
        .owner_get_by_chair_register_token(None, &req.chair_register_token)
        .await?
    else {
        return Err(Error::Unauthorized("invalid chair_register_token"));
    };

    let chair_id = Id::new();
    let access_token = crate::secure_random_str(32);

    repo.chair_add(
        &chair_id,
        &owner.id,
        &req.name,
        &req.model,
        false,
        &access_token,
    )
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
    State(AppState { repo, .. }): State<AppState>,
    axum::Extension(chair): axum::Extension<Chair>,
    axum::Json(req): axum::Json<PostChairActivityRequest>,
) -> Result<StatusCode, Error> {
    repo.chair_update_is_active(&chair.id, req.is_active)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, serde::Serialize)]
struct ChairPostCoordinateResponse {
    recorded_at: i64,
}

async fn chair_post_coordinate(
    State(AppState { repo, .. }): State<AppState>,
    axum::Extension(chair): axum::Extension<Chair>,
    axum::Json(req): axum::Json<Coordinate>,
) -> Result<axum::Json<ChairPostCoordinateResponse>, Error> {
    let created_at = repo.chair_location_update(None, &chair.id, req).await?;

    if let Some((ride, status)) = repo.rides_get_assigned(None, &chair.id).await? {
        if req == ride.pickup_coord() && status == RideStatusEnum::Enroute {
            repo.ride_status_update(None, &ride.id, RideStatusEnum::Pickup)
                .await?;
        }

        if req == ride.destination_coord() && status == RideStatusEnum::Carrying {
            repo.ride_status_update(None, &ride.id, RideStatusEnum::Arrived)
                .await?;
        }
    }

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
    let ts = state
        .repo
        .chair_get_next_notification_sse(&chair.id)
        .await
        .unwrap();

    let stream =
        tokio_stream::wrappers::BroadcastStream::new(ts.notification_rx).then(move |body| {
            let body = body.unwrap();
            let state = state.clone();
            let chair = chair.clone();
            async move {
                let s = chair_get_notification_inner(&state, &chair.id, body).await?;
                let s = serde_json::to_string(&s).unwrap();
                Ok(Event::default().data(s))
            }
        });

    Sse::new(stream)
}

async fn chair_get_notification_inner(
    AppState { repo, .. }: &AppState,
    chair_id: &Id<Chair>,
    body: Option<NotificationBody>,
) -> Result<Option<ChairGetNotificationResponseData>, Error> {
    let Some(body) = body else {
        return Ok(None);
    };
    let ride = repo.ride_get(None, &body.ride_id).await?.unwrap();
    let user = repo.user_get_by_id(&ride.user_id).await?.unwrap();

    if body.status == RideStatusEnum::Completed {
        let chair_id = chair_id.clone();
        let repo = repo.clone();
        tokio::spawn(async move {
            repo.push_free_chair(&chair_id).await;
            // repo.do_matching().await;
        });
    }

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
        status: body.status,
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

    if ride
        .chair_id
        .as_ref()
        .is_none_or(|chair_id| chair_id != &chair.id)
    {
        return Err(Error::BadRequest("not assigned to this ride"));
    }

    let next = match req.status {
        // Acknowledge the ride
        RideStatusEnum::Enroute => RideStatusEnum::Enroute,
        // After Picking up user
        RideStatusEnum::Carrying => {
            let status = repo.ride_status_latest(&mut tx, &ride.id).await?;
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
