use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::Event;
use axum::response::Sse;
use axum_extra::extract::cookie::Cookie;
use axum_extra::extract::CookieJar;
use futures::Stream;
use tokio_stream::StreamExt;

use crate::models::{Chair, Id, Owner, Ride, RideStatusEnum, User};
use crate::repo::chair::EffortlessChair;
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
    State(state): State<AppState>,
    jar: CookieJar,
    axum::Json(req): axum::Json<ChairPostChairsRequest>,
) -> Result<(CookieJar, (StatusCode, axum::Json<ChairPostChairsResponse>)), Error> {
    let Some(owner): Option<Owner> = state
        .repo
        .owner_get_by_chair_register_token(&req.chair_register_token)?
    else {
        return Err(Error::Unauthorized("invalid chair_register_token"));
    };

    let chair_id = Id::new();
    let access_token = crate::secure_random_str(32);

    state.repo.chair_add(
        &chair_id,
        &owner.id,
        &req.name,
        &req.model,
        false,
        &access_token,
    )?;

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

#[axum::debug_handler]
async fn chair_post_activity(
    State(state): State<AppState>,
    axum::Extension(chair): axum::Extension<EffortlessChair>,
    axum::Json(req): axum::Json<PostChairActivityRequest>,
) -> Result<StatusCode, Error> {
    state
        .repo
        .chair_update_is_active(&chair.id, req.is_active)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, serde::Serialize)]
struct ChairPostCoordinateResponse {
    recorded_at: i64,
}

#[axum::debug_handler]
async fn chair_post_coordinate(
    State(state): State<AppState>,
    axum::Extension(chair): axum::Extension<EffortlessChair>,
    axum::Json(req): axum::Json<Coordinate>,
) -> Result<axum::Json<ChairPostCoordinateResponse>, Error> {
    let created_at = state.repo.chair_location_update(&chair.id, req)?;
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
    axum::Extension(chair): axum::Extension<EffortlessChair>,
) -> Sse<impl Stream<Item = Result<Event, Error>>> {
    let ts = state
        .repo
        .chair_get_next_notification_sse(&chair.id)
        .unwrap();

    let probe = state.chair_notification_stat.on_create();

    let stream =
        tokio_stream::wrappers::BroadcastStream::new(ts.notification_rx).map(move |body| {
            let _probe = &probe;
            let s = chair_get_notification_inner(&state, &chair.id, body.unwrap())?;
            let s = serde_json::to_string(&s).unwrap();
            state.chair_notification_stat.on_write(&chair.id);
            Ok(Event::default().data(s))
        });

    Sse::new(stream)
}

fn chair_get_notification_inner(
    state: &AppState,
    chair_id: &Id<Chair>,
    body: Option<NotificationBody>,
) -> Result<Option<ChairGetNotificationResponseData>, Error> {
    let Some(body) = body else {
        return Ok(None);
    };
    let ride = state.repo.ride_get(&body.ride_id)?.unwrap();
    let user = state.repo.user_get_by_id(&ride.user_id)?.unwrap();

    if body.status == RideStatusEnum::Completed {
        {
            let chair_id = chair_id.clone();
            let repo = state.repo.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                repo.ride_cache.on_chair_status_change(&chair_id, false);
                repo.push_free_chair(&chair_id);
            });
        }
    }

    Ok(Some(ChairGetNotificationResponseData {
        user: SimpleUser {
            id: user.id,
            name: format!("{} {}", user.firstname, user.lastname),
        },
        pickup_coordinate: ride.pickup_coord(),
        destination_coordinate: ride.destination_coord(),
        status: body.status,
        ride_id: ride.id,
    }))
}

#[derive(Debug, serde::Deserialize)]
struct PostChairRidesRideIDStatusRequest {
    status: RideStatusEnum,
}

async fn chair_post_ride_status(
    State(state): State<AppState>,
    axum::Extension(chair): axum::Extension<EffortlessChair>,
    Path((ride_id,)): Path<(Id<Ride>,)>,
    axum::Json(req): axum::Json<PostChairRidesRideIDStatusRequest>,
) -> Result<StatusCode, Error> {
    let Some(ride): Option<Ride> = state.repo.ride_get(&ride_id)? else {
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
            let status = state.repo.ride_status_latest(&ride.id)?;
            if status != RideStatusEnum::Pickup {
                return Err(Error::BadRequest("chair has not arrived yet"));
            }
            RideStatusEnum::Carrying
        }
        _ => {
            return Err(Error::BadRequest("invalid status"));
        }
    };

    state.repo.ride_status_update(&ride_id, next)?;

    Ok(StatusCode::NO_CONTENT)
}
