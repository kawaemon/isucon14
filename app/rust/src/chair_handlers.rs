use std::sync::LazyLock;
use std::time::Duration;

use cookie::Cookie;
use hyper::StatusCode;
use tokio_stream::StreamExt;

use crate::fw::{BoxStream, Controller, Event, SerializeJson, SseStats};
use crate::models::{Chair, Id, Owner, Ride, RideStatusEnum, Symbol, User};
use crate::repo::ride::NotificationBody;
use crate::{AppState, Coordinate, Error};

pub async fn chair_post_chairs(
    c: &mut Controller,
) -> Result<(StatusCode, impl SerializeJson), Error> {
    #[derive(serde::Deserialize)]
    struct Req {
        name: Symbol,
        model: Symbol,
        chair_register_token: Symbol,
    }

    let req: Req = c.body().await?;
    let state = c.state();

    let Some(owner): Option<Owner> = state
        .repo
        .owner_get_by_chair_register_token(req.chair_register_token)?
    else {
        return Err(Error::Unauthorized("invalid chair_register_token"));
    };

    let chair_id = Id::new();
    let access_token = Symbol::new_from(crate::secure_random_str(8));

    state
        .repo
        .chair_add(chair_id, owner.id, req.name, req.model, false, access_token)?;

    c.cookie_add(Cookie::build(("chair_session", access_token.resolve())).path("/"));

    #[derive(serde::Serialize, macros::SerializeJson)]
    struct Res {
        id: Id<Chair>,
        owner_id: Id<Owner>,
    }
    Ok((
        StatusCode::CREATED,
        Res {
            id: chair_id,
            owner_id: owner.id,
        },
    ))
}

pub async fn chair_post_activity(c: &mut Controller) -> Result<StatusCode, Error> {
    #[derive(serde::Deserialize)]
    struct Req {
        is_active: bool,
    }
    let req: Req = c.body().await?;
    let chair = c.auth_chair()?;
    c.state()
        .repo
        .chair_update_is_active(chair.id, req.is_active)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn chair_post_coordinate(c: &mut Controller) -> Result<impl SerializeJson, Error> {
    let req: Coordinate = c.body().await?;
    let chair = c.auth_chair()?;
    let created_at = c.state().repo.chair_location_update(chair.id, req)?;

    #[derive(serde::Serialize, macros::SerializeJson)]
    struct Res {
        recorded_at: i64,
    }
    Ok(Res {
        recorded_at: created_at.timestamp_millis(),
    })
}

static CHAIR_SSE_STATS: LazyLock<SseStats> = LazyLock::new(|| {
    let d = SseStats::default();
    d.spawn_printer("chair");
    d
});

pub fn chair_get_notification(c: &mut Controller) -> Result<BoxStream, Error> {
    let chair = c.auth_chair()?;
    let state = c.state().clone();

    let ts = state
        .repo
        .chair_get_next_notification_sse(chair.id)
        .unwrap();

    let probe = CHAIR_SSE_STATS.on_connect();

    let stream =
        tokio_stream::wrappers::BroadcastStream::new(ts.notification_rx).map(move |body| {
            let _probe = &probe;
            let s = chair_get_notification_inner(&state, chair.id, body.unwrap())?;
            Ok(Event::new(s))
        });

    Ok(Box::new(stream))
}

crate::conf_env!(static CHAIR_MATCHING_DELAY_MS: u64 = {
    from: "CHAIR_MATCHING_DELAY_MS",
    default: "10",
});

fn chair_get_notification_inner(
    state: &AppState,
    chair_id: Id<Chair>,
    body: Option<NotificationBody>,
) -> Result<Option<impl SerializeJson>, Error> {
    let Some(body) = body else {
        return Ok(None);
    };
    let ride = state.repo.ride_get(body.ride_id)?.unwrap();
    let user = state.repo.user_get_by_id(ride.user_id)?.unwrap();

    if body.status == RideStatusEnum::Completed {
        {
            let repo = state.repo.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(*CHAIR_MATCHING_DELAY_MS)).await;
                repo.on_chair_became_free(chair_id);
                repo.push_free_chair(chair_id);
            });
        }
    }

    #[derive(Debug, serde::Serialize, macros::SerializeJson)]
    struct SimpleUser {
        id: Id<User>,
        name: String,
    }
    #[derive(Debug, serde::Serialize, macros::SerializeJson)]
    struct Res {
        ride_id: Id<Ride>,
        user: SimpleUser,
        pickup_coordinate: Coordinate,
        destination_coordinate: Coordinate,
        status: RideStatusEnum,
    }

    Ok(Some(Res {
        user: SimpleUser {
            id: user.id,
            name: format!("{} {}", user.firstname.resolve(), user.lastname.resolve()),
        },
        pickup_coordinate: ride.pickup_coord(),
        destination_coordinate: ride.destination_coord(),
        status: body.status,
        ride_id: ride.id,
    }))
}

pub async fn chair_post_ride_status(
    c: &mut Controller,
    ride_id: Id<Ride>,
) -> Result<StatusCode, Error> {
    #[derive(serde::Deserialize)]
    struct Req {
        status: RideStatusEnum,
    }

    let chair = c.auth_chair()?;
    let req: Req = c.body().await?;
    let state = c.state();

    let Some(ride): Option<Ride> = state.repo.ride_get(ride_id)? else {
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
            let status = state.repo.ride_status_latest(ride.id)?;
            if status != RideStatusEnum::Pickup {
                return Err(Error::BadRequest("chair has not arrived yet"));
            }
            RideStatusEnum::Carrying
        }
        _ => {
            return Err(Error::BadRequest("invalid status"));
        }
    };

    state.repo.ride_status_update(ride_id, next)?;

    Ok(StatusCode::NO_CONTENT)
}
