use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::Event;
use axum::response::Sse;
use axum_extra::extract::CookieJar;
use chrono::Utc;
use futures::Stream;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::StreamExt;

use crate::models::{Chair, Coupon, Id, Owner, Ride, RideStatus, RideStatusEnum, User};
use crate::{AppState, Coordinate, Error};

pub fn app_routes(app_state: AppState) -> axum::Router<AppState> {
    let routes = axum::Router::new().route("/api/app/users", axum::routing::post(app_post_users));

    let authed_routes = axum::Router::new()
        .route(
            "/api/app/payment-methods",
            axum::routing::post(app_post_payment_methods),
        )
        .route(
            "/api/app/rides",
            axum::routing::get(app_get_rides).post(app_post_rides),
        )
        .route(
            "/api/app/rides/estimated-fare",
            axum::routing::post(app_post_rides_estimated_fare),
        )
        .route(
            "/api/app/rides/:ride_id/evaluation",
            axum::routing::post(app_post_ride_evaluation),
        )
        .route(
            "/api/app/notification",
            axum::routing::get(app_get_notification),
        )
        .route(
            "/api/app/nearby-chairs",
            axum::routing::get(app_get_nearby_chairs),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            app_state.clone(),
            crate::middlewares::app_auth_middleware,
        ));

    routes.merge(authed_routes)
}

#[derive(Debug, serde::Deserialize)]
struct AppPostUsersRequest {
    username: String,
    firstname: String,
    lastname: String,
    date_of_birth: String,
    invitation_code: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct AppPostUsersResponse {
    id: Id<User>,
    invitation_code: String,
}

async fn app_post_users(
    State(AppState { pool, repo, .. }): State<AppState>,
    jar: CookieJar,
    axum::Json(req): axum::Json<AppPostUsersRequest>,
) -> Result<(CookieJar, (StatusCode, axum::Json<AppPostUsersResponse>)), Error> {
    let user_id = Id::new();
    let access_token = crate::secure_random_str(32);
    let invitation_code = crate::secure_random_str(15);

    let mut tx = pool.begin().await?;

    repo.user_add(
        &mut tx,
        &user_id,
        &req.username,
        &req.firstname,
        &req.lastname,
        &req.date_of_birth,
        &access_token,
        &invitation_code,
    )
    .await?;

    // 初回登録キャンペーンのクーポンを付与
    sqlx::query("INSERT INTO coupons (user_id, code, discount) VALUES (?, ?, ?)")
        .bind(&user_id)
        .bind("CP_NEW2024")
        .bind(3000)
        .execute(&mut *tx)
        .await?;

    // 招待コードを使った登録
    if let Some(req_invitation_code) = req.invitation_code {
        if !req_invitation_code.is_empty() {
            // 招待する側の招待数をチェック
            let coupons: Vec<Coupon> =
                sqlx::query_as("SELECT * FROM coupons WHERE code = ? FOR UPDATE")
                    .bind(format!("INV_{req_invitation_code}"))
                    .fetch_all(&mut *tx)
                    .await?;
            if coupons.len() >= 3 {
                return Err(Error::BadRequest("この招待コードは使用できません。"));
            }

            // ユーザーチェック
            let Some(inviter): Option<User> =
                sqlx::query_as("SELECT * FROM users WHERE invitation_code = ?")
                    .bind(&req_invitation_code)
                    .fetch_optional(&mut *tx)
                    .await?
            else {
                return Err(Error::BadRequest("この招待コードは使用できません。"));
            };

            // 招待クーポン付与
            sqlx::query("INSERT INTO coupons (user_id, code, discount) VALUES (?, ?, ?)")
                .bind(&user_id)
                .bind(format!("INV_{req_invitation_code}"))
                .bind(1500)
                .execute(&mut *tx)
                .await?;
            // 招待した人にもRewardを付与
            sqlx::query("INSERT INTO coupons (user_id, code, discount) VALUES (?, CONCAT(?, '_', FLOOR(UNIX_TIMESTAMP(NOW(3))*1000)), ?)")
                .bind(inviter.id)
                .bind(format!("RWD_{req_invitation_code}"))
                .bind(1000)
                .execute(&mut *tx)
                .await?;
        }
    }

    tx.commit().await?;

    let jar = jar
        .add(axum_extra::extract::cookie::Cookie::build(("app_session", access_token)).path("/"));

    tracing::info!("new user added");

    Ok((
        jar,
        (
            StatusCode::CREATED,
            axum::Json(AppPostUsersResponse {
                id: user_id,
                invitation_code,
            }),
        ),
    ))
}

#[derive(Debug, serde::Deserialize)]
struct AppPostPaymentMethodsRequest {
    token: String,
}

async fn app_post_payment_methods(
    State(AppState { repo, .. }): State<AppState>,
    axum::Extension(user): axum::Extension<User>,
    axum::Json(req): axum::Json<AppPostPaymentMethodsRequest>,
) -> Result<StatusCode, Error> {
    repo.payment_token_add(&user.id, &req.token).await?;
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
    owner: String,
    name: String,
    model: String,
}

async fn app_get_rides(
    State(AppState { pool, repo, .. }): State<AppState>,
    axum::Extension(user): axum::Extension<User>,
) -> Result<axum::Json<GetAppRidesResponse>, Error> {
    let mut tx = pool.begin().await?;

    let rides: Vec<Ride> =
        sqlx::query_as("SELECT * FROM rides WHERE user_id = ? ORDER BY created_at DESC")
            .bind(&user.id)
            .fetch_all(&mut *tx)
            .await?;

    let mut items = Vec::with_capacity(rides.len());
    for ride in rides {
        let status = repo.ride_status_latest(&mut tx, &ride.id).await?;
        if status != RideStatusEnum::Completed {
            continue;
        }

        let fare = calculate_discounted_fare(
            &mut tx,
            &user.id,
            Some(&ride.id),
            ride.pickup_coord(),
            ride.destination_coord(),
        )
        .await?;

        let chair: Chair = sqlx::query_as("SELECT * FROM chairs WHERE id = ?")
            .bind(&ride.chair_id)
            .fetch_one(&mut *tx)
            .await?;

        let owner: Owner = sqlx::query_as("SELECT * FROM owners WHERE id = ?")
            .bind(chair.owner_id)
            .fetch_one(&mut *tx)
            .await?;

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

    tx.commit().await?;

    Ok(axum::Json(GetAppRidesResponse { rides: items }))
}

#[derive(Debug, serde::Deserialize)]
struct AppPostRidesRequest {
    pickup_coordinate: Coordinate,
    destination_coordinate: Coordinate,
}

#[derive(Debug, serde::Serialize)]
struct AppPostRidesResponse {
    ride_id: Id<Ride>,
    fare: i32,
}

async fn app_post_rides(
    State(AppState { pool, repo, .. }): State<AppState>,
    axum::Extension(user): axum::Extension<User>,
    axum::Json(req): axum::Json<AppPostRidesRequest>,
) -> Result<(StatusCode, axum::Json<AppPostRidesResponse>), Error> {
    let ride_id = Id::new();

    let mut tx = pool.begin().await?;

    if repo.rides_user_ongoing(&mut tx, &user.id).await? {
        return Err(Error::Conflict("ride already exists"));
    }

    repo.rides_new(
        &mut tx,
        &ride_id,
        &user.id,
        req.pickup_coordinate,
        req.destination_coordinate,
    )
    .await?;
    repo.ride_status_update(&mut tx, &ride_id, RideStatusEnum::Matching)
        .await?;

    let ride_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rides WHERE user_id = ?")
        .bind(&user.id)
        .fetch_one(&mut *tx)
        .await?;

    if ride_count == 1 {
        // 初回利用で、初回利用クーポンがあれば必ず使う
        let coupon: Option<Coupon> = sqlx::query_as("SELECT * FROM coupons WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL FOR UPDATE")
            .bind(&user.id)
            .fetch_optional(&mut *tx)
            .await?;
        if coupon.is_some() {
            sqlx::query("UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = 'CP_NEW2024'")
                .bind(&ride_id)
                .bind(&user.id)
                .execute(&mut *tx)
                .await?;
        } else {
            // 無ければ他のクーポンを付与された順番に使う
            let coupon: Option<Coupon> = sqlx::query_as("SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE")
                .bind(&user.id)
                .fetch_optional(&mut *tx)
                .await?;
            if let Some(coupon) = coupon {
                sqlx::query("UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = ?")
                    .bind(&ride_id)
                    .bind(&user.id)
                    .bind(coupon.code)
                    .execute(&mut *tx)
                    .await?;
            }
        }
    } else {
        // 他のクーポンを付与された順番に使う
        let coupon: Option<Coupon> = sqlx::query_as("SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE")
                .bind(&user.id)
                .fetch_optional(&mut *tx)
                .await?;
        if let Some(coupon) = coupon {
            sqlx::query("UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = ?")
                .bind(&ride_id)
                .bind(&user.id)
                .bind(coupon.code)
                .execute(&mut *tx)
                .await?;
        }
    }

    let fare = calculate_discounted_fare(
        &mut tx,
        &user.id,
        Some(&ride_id),
        req.pickup_coordinate,
        req.destination_coordinate,
    )
    .await?;

    tx.commit().await?;

    Ok((
        StatusCode::ACCEPTED,
        axum::Json(AppPostRidesResponse { ride_id, fare }),
    ))
}

#[derive(Debug, serde::Deserialize)]
struct AppPostRidesEstimatedFareRequest {
    pickup_coordinate: Coordinate,
    destination_coordinate: Coordinate,
}

#[derive(Debug, serde::Serialize)]
struct AppPostRidesEstimatedFareResponse {
    fare: i32,
    discount: i32,
}

async fn app_post_rides_estimated_fare(
    State(AppState { pool, .. }): State<AppState>,
    axum::Extension(user): axum::Extension<User>,
    axum::Json(req): axum::Json<AppPostRidesEstimatedFareRequest>,
) -> Result<axum::Json<AppPostRidesEstimatedFareResponse>, Error> {
    let mut tx = pool.begin().await?;

    let discounted = calculate_discounted_fare(
        &mut tx,
        &user.id,
        None,
        req.pickup_coordinate,
        req.destination_coordinate,
    )
    .await?;

    tx.commit().await?;

    Ok(axum::Json(AppPostRidesEstimatedFareResponse {
        fare: discounted,
        discount: crate::calculate_fare(req.pickup_coordinate, req.destination_coordinate)
            - discounted,
    }))
}

#[derive(Debug, serde::Deserialize)]
struct AppPostRideEvaluationRequest {
    evaluation: i32,
}

#[derive(Debug, serde::Serialize)]
struct AppPostRideEvaluationResponse {
    fare: i32,
    completed_at: i64,
}

async fn app_post_ride_evaluation(
    State(AppState { pool, repo, .. }): State<AppState>,
    Path((ride_id,)): Path<(Id<Ride>,)>,
    axum::Json(req): axum::Json<AppPostRideEvaluationRequest>,
) -> Result<axum::Json<AppPostRideEvaluationResponse>, Error> {
    if req.evaluation < 1 || req.evaluation > 5 {
        return Err(Error::BadRequest("evaluation must be between 1 and 5"));
    }

    let mut tx = pool.begin().await?;

    let Some(ride): Option<Ride> = sqlx::query_as("SELECT * FROM rides WHERE id = ?")
        .bind(&ride_id)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Err(Error::NotFound("ride not found"));
    };

    let status = repo.ride_status_latest(&mut tx, &ride.id).await?;
    if status != RideStatusEnum::Arrived {
        return Err(Error::BadRequest("not arrived yet"));
    }

    let updated_at = repo
        .rides_set_evaluation(&mut tx, &ride_id, req.evaluation)
        .await?;

    repo.ride_status_update(&mut tx, &ride_id, RideStatusEnum::Completed)
        .await?;

    let Some(payment_token): Option<String> =
        repo.payment_token_get(&mut tx, &ride.user_id).await?
    else {
        return Err(Error::BadRequest("payment token not registered"));
    };

    let fare = calculate_discounted_fare(
        &mut tx,
        &ride.user_id,
        Some(&ride.id),
        ride.pickup_coord(),
        ride.destination_coord(),
    )
    .await?;

    let payment_gateway_url: String = repo.pgw_get(&mut tx).await?;

    async fn retrieve_rides_order_by_created_at_asc(
        tx: &mut sqlx::MySqlConnection,
        user_id: &Id<User>,
    ) -> Result<i32, Error> {
        sqlx::query_scalar("SELECT count(*) FROM rides WHERE user_id = ?")
            .bind(user_id)
            .fetch_one(tx)
            .await
            .map_err(Error::Sqlx)
    }

    crate::payment_gateway::request_payment_gateway_post_payment(
        &payment_gateway_url,
        &payment_token,
        &crate::payment_gateway::PaymentGatewayPostPaymentRequest { amount: fare },
        &mut tx,
        &ride.user_id,
        retrieve_rides_order_by_created_at_asc,
    )
    .await?;

    tx.commit().await?;

    tracing::info!("evaluation done");

    Ok(axum::Json(AppPostRideEvaluationResponse {
        fare,
        completed_at: updated_at.timestamp_millis(),
    }))
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
    name: String,
    model: String,
    stats: AppGetNotificationResponseChairStats,
}

#[derive(Debug, serde::Serialize)]
struct AppGetNotificationResponseChairStats {
    total_rides_count: i32,
    total_evaluation_avg: f64,
}

async fn app_get_notification(
    State(state): State<AppState>,
    axum::Extension(user): axum::Extension<User>,
) -> Sse<impl Stream<Item = Result<Event, Error>>> {
    let stream =
        IntervalStream::new(tokio::time::interval(Duration::from_millis(30))).then(move |_| {
            let state = state.clone();
            let user = user.clone();
            async move {
                let s = app_get_notification_inner(&state, &user).await?;
                let s = serde_json::to_string(&s).unwrap();
                Ok(Event::default().data(s))
            }
        });

    Sse::new(stream)
}

async fn app_get_notification_inner(
    AppState { pool, repo, .. }: &AppState,
    user: &User,
) -> Result<Option<AppGetNotificationResponseData>, Error> {
    let mut tx = pool.begin().await?;

    let Some(ride): Option<Ride> =
        sqlx::query_as("SELECT * FROM rides WHERE user_id = ? ORDER BY created_at DESC LIMIT 1")
            .bind(&user.id)
            .fetch_optional(&mut *tx)
            .await?
    else {
        return Ok(None);
    };

    let yet_sent_ride_status: Option<RideStatus> = sqlx::query_as("SELECT * FROM ride_statuses WHERE ride_id = ? AND app_sent_at IS NULL ORDER BY created_at ASC LIMIT 1")
        .bind(&ride.id)
        .fetch_optional(&mut *tx)
        .await?;
    let (ride_status_id, status) = if let Some(yet_sent_ride_status) = yet_sent_ride_status {
        (Some(yet_sent_ride_status.id), yet_sent_ride_status.status)
    } else {
        (None, repo.ride_status_latest(&mut tx, &ride.id).await?)
    };

    let fare = calculate_discounted_fare(
        &mut tx,
        &user.id,
        Some(&ride.id),
        ride.pickup_coord(),
        ride.destination_coord(),
    )
    .await?;

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
        let chair: Chair = sqlx::query_as("SELECT * FROM chairs WHERE id = ?")
            .bind(chair_id)
            .fetch_one(&mut *tx)
            .await?;

        let stats = get_chair_stats(&mut tx, &chair.id).await?;

        data.chair = Some(AppGetNotificationResponseChair {
            id: chair.id,
            name: chair.name,
            model: chair.model,
            stats,
        });
    }

    if let Some(ride_status_id) = ride_status_id {
        repo.ride_status_app_notified(&mut tx, &ride_status_id)
            .await?;
    }

    tx.commit().await?;

    Ok(Some(data))
}

async fn get_chair_stats(
    tx: &mut sqlx::MySqlConnection,
    chair_id: &Id<Chair>,
) -> Result<AppGetNotificationResponseChairStats, Error> {
    let rides: Vec<Ride> =
        sqlx::query_as("SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC")
            .bind(chair_id)
            .fetch_all(&mut *tx)
            .await?;

    let mut total_ride_count = 0;
    let mut total_evaluation = 0.0;
    for ride in rides {
        let ride_statuses: Vec<RideStatus> =
            sqlx::query_as("SELECT * FROM ride_statuses WHERE ride_id = ? ORDER BY created_at")
                .bind(&ride.id)
                .fetch_all(&mut *tx)
                .await?;

        if !ride_statuses
            .iter()
            .any(|status| status.status == RideStatusEnum::Arrived)
        {
            continue;
        }
        if !ride_statuses
            .iter()
            .any(|status| (status.status == RideStatusEnum::Carrying))
        {
            continue;
        }
        let is_completed = ride_statuses
            .iter()
            .any(|status| status.status == RideStatusEnum::Completed);
        if !is_completed {
            continue;
        }

        total_ride_count += 1;
        total_evaluation += ride.evaluation.unwrap() as f64;
    }

    let total_evaluation_avg = if total_ride_count > 0 {
        total_evaluation / total_ride_count as f64
    } else {
        0.0
    };

    Ok(AppGetNotificationResponseChairStats {
        total_rides_count: total_ride_count,
        total_evaluation_avg,
    })
}

#[derive(Debug, serde::Deserialize)]
struct AppGetNearbyChairsQuery {
    latitude: i32,
    longitude: i32,
    distance: Option<i32>,
}

#[derive(Debug, serde::Serialize)]
struct AppGetNearbyChairsResponse {
    chairs: Vec<AppGetNearbyChairsResponseChair>,
    retrieved_at: i64,
}

#[derive(Debug, serde::Serialize)]
struct AppGetNearbyChairsResponseChair {
    id: Id<Chair>,
    name: String,
    model: String,
    current_coordinate: Coordinate,
}

async fn app_get_nearby_chairs(
    State(AppState { pool, repo, .. }): State<AppState>,
    Query(query): Query<AppGetNearbyChairsQuery>,
) -> Result<axum::Json<AppGetNearbyChairsResponse>, Error> {
    let distance = query.distance.unwrap_or(50);
    let coordinate = Coordinate {
        latitude: query.latitude,
        longitude: query.longitude,
    };

    let mut tx = pool.begin().await?;

    let chairs: Vec<Chair> = repo.chair_get_completeds(&mut tx).await?;

    let mut nearby_chairs = Vec::new();
    for chair in chairs {
        let Some(chair_coord) = repo.chair_location_get_latest(&mut tx, &chair.id).await? else {
            continue;
        };
        if coordinate.distance(chair_coord) <= distance {
            nearby_chairs.push(AppGetNearbyChairsResponseChair {
                id: chair.id,
                name: chair.name,
                model: chair.model,
                current_coordinate: chair_coord,
            });
        }
    }

    Ok(axum::Json(AppGetNearbyChairsResponse {
        chairs: nearby_chairs,
        retrieved_at: Utc::now().timestamp(),
    }))
}

async fn calculate_discounted_fare(
    tx: &mut sqlx::MySqlConnection,
    user_id: &Id<User>,
    ride_id: Option<&Id<Ride>>,
    pickup: Coordinate,
    dest: Coordinate,
) -> sqlx::Result<i32> {
    let discount = if let Some(ride_id) = ride_id {
        // すでにクーポンが紐づいているならそれの割引額を参照
        let coupon: Option<Coupon> = sqlx::query_as("SELECT * FROM coupons WHERE used_by = ?")
            .bind(ride_id)
            .fetch_optional(&mut *tx)
            .await?;
        coupon.map(|c| c.discount).unwrap_or(0)
    } else {
        // 初回利用クーポンを最優先で使う
        let coupon: Option<Coupon> = sqlx::query_as(
            "SELECT * FROM coupons WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL",
        )
        .bind(user_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(coupon) = coupon {
            coupon.discount
        } else {
            // 無いなら他のクーポンを付与された順番に使う
            let coupon: Option<Coupon> = sqlx::query_as("SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1")
                .bind(user_id)
                .fetch_optional(&mut *tx)
                .await?;
            coupon.map(|c| c.discount).unwrap_or(0)
        }
    };

    let metered_fare = crate::FARE_PER_DISTANCE * pickup.distance(dest);
    let discounted_metered_fare = std::cmp::max(metered_fare - discount, 0);

    Ok(crate::INITIAL_FARE + discounted_metered_fare)
}
