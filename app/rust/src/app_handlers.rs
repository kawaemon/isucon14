use std::sync::{Arc, LazyLock};

use chrono::Utc;
use cookie::Cookie;
use hyper::StatusCode;
use serde::Serialize;
use tokio_stream::StreamExt;

use crate::fw::{BoxStream, Controller, Event};
use crate::models::{Chair, Coupon, Id, Owner, Ride, RideStatusEnum, Symbol, User};
use crate::repo::ride::NotificationBody;
use crate::repo::Repository;
use crate::{AppState, Coordinate, Error};

pub async fn app_post_users(c: &mut Controller) -> Result<(StatusCode, impl Serialize), Error> {
    #[derive(Debug, serde::Deserialize)]
    struct Req {
        username: Symbol,
        firstname: Symbol,
        lastname: Symbol,
        date_of_birth: Symbol,
        invitation_code: Option<Symbol>,
    }
    let req: Req = c.body().await?;
    let state = &c.state();

    let user_id = Id::new();
    let access_token = Symbol::new_from(crate::secure_random_str(8));
    let invitation_code = Symbol::new_from(crate::secure_random_str(8));

    state.repo.user_add(
        user_id,
        req.username,
        req.firstname,
        req.lastname,
        req.date_of_birth,
        access_token,
        invitation_code,
    )?;

    // 初回登録キャンペーンのクーポンを付与
    state.repo.coupon_add(user_id, *CP_NEW2024, 3000)?;

    // 招待コードを使った登録
    if let Some(req_invitation_code) = req.invitation_code {
        let r_str = req_invitation_code.resolve();
        if !r_str.is_empty() {
            let inv_prefixed_code = Symbol::new_from(format!("INV_{r_str}"));
            // 招待する側の招待数をチェック
            let coupons = state.repo.coupon_get_count_by_code(inv_prefixed_code)?;
            if coupons >= 3 {
                return Err(Error::BadRequest("この招待コードは使用できません。"));
            }

            // ユーザーチェック
            let Some(inviter): Option<User> =
                state.repo.user_get_by_inv_code(req_invitation_code)?
            else {
                return Err(Error::BadRequest("この招待コードは使用できません。"));
            };

            // 招待クーポン付与
            state.repo.coupon_add(user_id, inv_prefixed_code, 1500)?;
            // 招待した人にもRewardを付与
            state.repo.coupon_add(
                inviter.id,
                Symbol::new_from(format!("RWD_{}", Id::<Coupon>::new().resolve())),
                1000,
            )?;
        }
    }

    c.cookie_add(Cookie::build(("app_session", access_token.resolve())).path("/"));

    #[derive(Debug, serde::Serialize)]
    struct Res {
        id: Id<User>,
        invitation_code: Symbol,
    }
    Ok((
        StatusCode::CREATED,
        Res {
            id: user_id,
            invitation_code,
        },
    ))
}

pub async fn app_post_payment_methods(c: &mut Controller) -> Result<StatusCode, Error> {
    #[derive(Debug, serde::Deserialize)]
    struct Req {
        token: Symbol,
    }
    let req: Req = c.body().await?;
    let user = c.auth_app()?;
    c.state().repo.payment_token_add(user.id, req.token)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, serde::Serialize)]
struct GetAppRidesResponse {
    rides: Vec<GetAppRidesResponseItem>,
}

#[derive(Debug, serde::Serialize)]
struct GetAppRidesResponseItem {
    id: Id<Ride>,
    pickup_coordinate: Coordinate,
    destination_coordinate: Coordinate,
    chair: GetAppRidesResponseItemChair,
    fare: i32,
    evaluation: i32,
    requested_at: i64,
    completed_at: i64,
}

#[derive(Debug, serde::Serialize)]
struct GetAppRidesResponseItemChair {
    id: Id<Chair>,
    owner: Symbol,
    name: Symbol,
    model: Symbol,
}

pub fn app_get_rides(c: &mut Controller) -> Result<impl Serialize, Error> {
    let user = c.auth_app()?;
    let state = &c.state();

    let rides: Vec<Ride> = state.repo.rides_by_user(user.id)?;
    let mut items = Vec::with_capacity(rides.len());
    for ride in rides {
        let status = state.repo.ride_status_latest(ride.id)?;
        if status != RideStatusEnum::Completed {
            continue;
        }

        let fare = calculate_discounted_fare(
            &state.repo,
            user.id,
            Some(ride.id),
            ride.pickup_coord(),
            ride.destination_coord(),
        )?;

        let chair = state
            .repo
            .chair_get_by_id_effortless(ride.chair_id.unwrap())?
            .unwrap();
        let owner: Owner = state.repo.owner_get_by_id(chair.owner_id)?.unwrap();

        items.push(GetAppRidesResponseItem {
            pickup_coordinate: ride.pickup_coord(),
            destination_coordinate: ride.destination_coord(),
            id: ride.id,
            chair: GetAppRidesResponseItemChair {
                id: chair.id,
                owner: owner.name,
                name: chair.name,
                model: chair.model,
            },
            fare,
            evaluation: ride.evaluation.unwrap(),
            requested_at: ride.created_at.timestamp_millis(),
            completed_at: ride.updated_at.timestamp_millis(),
        });
    }

    Ok(GetAppRidesResponse { rides: items })
}

#[derive(Debug, serde::Serialize)]
struct AppPostRidesResponse {
    ride_id: Id<Ride>,
    fare: i32,
}

static CP_NEW2024: LazyLock<Symbol> = LazyLock::new(|| Symbol::new_from_ref("CP_NEW2024"));

pub async fn app_post_rides(c: &mut Controller) -> Result<(StatusCode, impl Serialize), Error> {
    #[derive(Debug, serde::Deserialize)]
    struct Req {
        pickup_coordinate: Coordinate,
        destination_coordinate: Coordinate,
    }

    let req: Req = c.body().await?;
    let user = c.auth_app()?;
    let state = &c.state();
    let ride_id = Id::new();

    if state.repo.rides_user_ongoing(user.id)? {
        return Err(Error::Conflict("ride already exists"));
    }

    state.repo.rides_new_and_set_matching(
        ride_id,
        user.id,
        req.pickup_coordinate,
        req.destination_coordinate,
    )?;

    let mut discount = 0;
    let unused_coupons = state.repo.coupon_get_unused_order_by_created_at(user.id)?;

    let coupon_candidate = unused_coupons
        .iter()
        .find(|x| x.code == *CP_NEW2024)
        .or(unused_coupons.first());
    if let Some(coupon) = coupon_candidate {
        state.repo.coupon_set_used(user.id, coupon.code, ride_id)?;
        discount = coupon.discount;
    }

    let metered_fare =
        crate::FARE_PER_DISTANCE * req.pickup_coordinate.distance(req.destination_coordinate);
    let discounted_metered_fare = std::cmp::max(metered_fare - discount, 0);

    let fare = crate::INITIAL_FARE + discounted_metered_fare;

    Ok((StatusCode::ACCEPTED, AppPostRidesResponse { ride_id, fare }))
}

pub async fn app_post_rides_estimated_fare(c: &mut Controller) -> Result<impl Serialize, Error> {
    #[derive(serde::Deserialize)]
    struct Req {
        pickup_coordinate: Coordinate,
        destination_coordinate: Coordinate,
    }

    let user = c.auth_app()?;
    let req: Req = c.body().await?;
    let state = &c.state();
    let discounted = calculate_discounted_fare(
        &state.repo,
        user.id,
        None,
        req.pickup_coordinate,
        req.destination_coordinate,
    )?;

    #[derive(Serialize)]
    struct Res {
        fare: i32,
        discount: i32,
    }
    Ok(Res {
        fare: discounted,
        discount: crate::calculate_fare(req.pickup_coordinate, req.destination_coordinate)
            - discounted,
    })
}

pub async fn app_post_ride_evaluation(
    c: &mut Controller,
    ride_id: Id<Ride>,
) -> Result<impl Serialize, Error> {
    #[derive(serde::Deserialize)]
    struct Req {
        evaluation: i32,
    }
    let req: Req = c.body().await?;
    let state = &c.state();

    if req.evaluation < 1 || req.evaluation > 5 {
        return Err(Error::BadRequest("evaluation must be between 1 and 5"));
    }

    let Some(ride): Option<Ride> = state.repo.ride_get(ride_id)? else {
        return Err(Error::NotFound("ride not found"));
    };

    let status = state.repo.ride_status_latest(ride.id)?;
    if status != RideStatusEnum::Arrived {
        return Err(Error::BadRequest("not arrived yet"));
    }

    let Some(payment_token): Option<Symbol> = state.repo.payment_token_get(ride.user_id)? else {
        return Err(Error::BadRequest("payment token not registered"));
    };

    let fare = calculate_discounted_fare(
        &state.repo,
        ride.user_id,
        Some(ride.id),
        ride.pickup_coord(),
        ride.destination_coord(),
    )?;

    let payment_gateway_url = state.repo.pgw_get()?;

    crate::payment_gateway::request_payment_gateway_post_payment(
        &state.client,
        payment_gateway_url,
        payment_token,
        &crate::payment_gateway::PaymentGatewayPostPaymentRequest { amount: fare },
    )
    .await?;

    let chair_id = ride.chair_id.unwrap();
    let updated_at = state
        .repo
        .rides_set_evaluation(ride_id, chair_id, req.evaluation)?;
    state
        .repo
        .ride_status_update(ride_id, RideStatusEnum::Completed)?;

    #[derive(serde::Serialize)]
    struct Res {
        fare: i32,
        completed_at: i64,
    }
    Ok(Res {
        fare,
        completed_at: updated_at.timestamp_millis(),
    })
}

#[derive(Debug, serde::Serialize)]
struct AppGetNotificationResponseData {
    ride_id: Id<Ride>,
    pickup_coordinate: Coordinate,
    destination_coordinate: Coordinate,
    fare: i32,
    status: RideStatusEnum,
    #[serde(skip_serializing_if = "Option::is_none")]
    chair: Option<AppGetNotificationResponseChair>,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, serde::Serialize)]
struct AppGetNotificationResponseChair {
    id: Id<Chair>,
    name: Symbol,
    model: Symbol,
    stats: ChairStats,
}

#[derive(Debug, serde::Serialize)]
pub struct ChairStats {
    pub total_rides_count: i32,
    pub total_evaluation_avg: f64,
}

pub fn app_get_notification(c: &mut Controller) -> Result<BoxStream, Error> {
    let user = c.auth_app()?;
    let state = c.state().clone();
    let ts = state.repo.user_get_next_notification_sse(user.id).unwrap();

    let stream =
        tokio_stream::wrappers::BroadcastStream::new(ts.notification_rx).map(move |body| {
            let s = app_get_notification_inner(&state, user.id, body.unwrap())?;
            Ok(Event::new(s))
        });

    Ok(Box::new(stream))
}

fn app_get_notification_inner(
    state: &AppState,
    user_id: Id<User>,
    body: Option<NotificationBody>,
) -> Result<Option<AppGetNotificationResponseData>, Error> {
    let Some(body) = body else { return Ok(None) };
    let ride = state.repo.ride_get(body.ride_id)?.unwrap();
    let status = body.status;

    let fare = calculate_discounted_fare(
        &state.repo,
        user_id,
        Some(ride.id),
        ride.pickup_coord(),
        ride.destination_coord(),
    )?;

    let mut data = AppGetNotificationResponseData {
        pickup_coordinate: ride.pickup_coord(),
        destination_coordinate: ride.destination_coord(),
        ride_id: ride.id,
        fare,
        status,
        chair: None,
        created_at: ride.created_at.timestamp_millis(),
        updated_at: ride.updated_at.timestamp_millis(),
    };

    if let Some(chair_id) = ride.chair_id {
        let chair = state.repo.chair_get_by_id_effortless(chair_id)?.unwrap();
        let stats = state.repo.chair_get_stats(chair.id)?;

        data.chair = Some(AppGetNotificationResponseChair {
            id: chair.id,
            name: chair.name,
            model: chair.model,
            stats,
        });
    }

    Ok(Some(data))
}

#[derive(Debug, serde::Serialize)]
struct AppGetNearbyChairsResponse {
    chairs: Vec<AppGetNearbyChairsResponseChair>,
    retrieved_at: i64,
}

#[derive(Debug, serde::Serialize)]
pub struct AppGetNearbyChairsResponseChair {
    pub id: Id<Chair>,
    pub name: Symbol,
    pub model: Symbol,
    pub current_coordinate: Coordinate,
}

pub fn app_get_nearby_chairs(
    c: &mut Controller,
    distance: Option<i32>,
    latitude: i32,
    longitude: i32,
) -> Result<impl Serialize, Error> {
    let distance = distance.unwrap_or(50);
    let coordinate = Coordinate {
        latitude,
        longitude,
    };

    Ok(AppGetNearbyChairsResponse {
        chairs: c.state().repo.chair_nearby(coordinate, distance)?,
        retrieved_at: Utc::now().timestamp(),
    })
}

fn calculate_discounted_fare(
    repo: &Arc<Repository>,
    user_id: Id<User>,
    ride_id: Option<Id<Ride>>,
    pickup: Coordinate,
    dest: Coordinate,
) -> Result<i32, Error> {
    let discount = {
        if let Some(ride_id) = ride_id {
            // すでにクーポンが紐づいているならそれの割引額を参照
            let coupon: Option<Coupon> = repo.coupon_get_by_usedby(ride_id)?;
            coupon.map(|c| c.discount).unwrap_or(0)
        } else {
            // 初回利用クーポンを最優先で使う
            let unused_coupons = repo.coupon_get_unused_order_by_created_at(user_id)?;
            if let Some(coupon) = unused_coupons.iter().find(|x| x.code == *CP_NEW2024) {
                coupon.discount
            } else {
                // 無いなら他のクーポンを付与された順番に使う
                unused_coupons.first().map(|x| x.discount).unwrap_or(0)
            }
        }
    };

    let metered_fare = crate::FARE_PER_DISTANCE * pickup.distance(dest);
    let discounted_metered_fare = std::cmp::max(metered_fare - discount, 0);

    Ok(crate::INITIAL_FARE + discounted_metered_fare)
}
