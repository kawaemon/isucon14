use std::{collections::HashMap, sync::Arc};

use axum::{http::StatusCode, response::Response};
use chrono::{DateTime, Utc};
use models::{Chair, ChairLocation, RideStatus, User};
use sqlx::{MySql, Pool};
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone)]
pub struct AppState {
    pub pool: sqlx::MySqlPool,
    pub cache: Arc<AppCache>,
    pub deferred: Arc<AppDeferred>,
}

#[derive(Debug)]
pub struct AppDeferred {
    pub location_queue: Mutex<Vec<ChairLocation>>,
}
impl AppDeferred {
    pub fn new() -> Self {
        Self {
            location_queue: Mutex::new(vec![]),
        }
    }
    pub async fn sync(&self, pool: &Pool<MySql>) {
        let mut loc_queue = self.location_queue.lock().await;

        if loc_queue.is_empty() {
            return;
        }

        let mut builder = sqlx::QueryBuilder::new(
            "INSERT INTO chair_locations (id, chair_id, latitude, longitude, created_at) ",
        );

        builder.push_values(loc_queue.iter(), |mut b, q| {
            b.push_bind(&q.id)
                .push_bind(&q.chair_id)
                .push_bind(q.latitude)
                .push_bind(q.longitude)
                .push_bind(q.created_at);
        });

        builder.build().execute(pool).await.unwrap();

        tracing::info!("pushed {} locations", loc_queue.len());

        loc_queue.clear();
    }
}

#[derive(Debug)]
pub struct RideCache {
    ride_id: String,
    status: String,
    going_to: Coordinate,
}

#[derive(Debug)]
pub struct AppCache {
    pub chair_location: RwLock<HashMap<String, ChairLocationCache>>,
    pub ride_status_cache: RwLock<HashMap<String /* ride_id */, String>>,
    pub chair_ride_cache: RwLock<HashMap<String /* chair_id */, RideCache>>,
    pub chair_cache: RwLock<HashMap<String /* access_token */, Chair>>,
    pub user_cache: RwLock<HashMap<String /* access_token */, User>>,
}
impl AppCache {
    pub async fn new(pool: &Pool<MySql>) -> Self {
        Self {
            chair_location: Self::new_chair_location(pool).await,
            ride_status_cache: Self::new_ride_status_cache(pool).await,
            chair_ride_cache: Self::new_chair_ride_cache(pool).await,
            chair_cache: Self::new_chair_cache(pool).await,
            user_cache: Self::new_user_cache(pool).await,
        }
    }

    pub async fn new_chair_location(
        pool: &Pool<MySql>,
    ) -> RwLock<HashMap<String, ChairLocationCache>> {
        let locations: Vec<ChairLocation> =
            sqlx::query_as("select * from chair_locations order by chair_id, created_at asc")
                .fetch_all(pool)
                .await
                .unwrap();

        tracing::info!("processing {} chair_locations record", locations.len());

        let coord = |s: &ChairLocation| Coordinate {
            latitude: s.latitude,
            longitude: s.longitude,
        };

        let mut res = HashMap::new();
        if locations.is_empty() {
            return RwLock::new(res);
        }

        let mut prev_id = &locations[0].chair_id;
        res.insert(
            locations[0].chair_id.clone(),
            ChairLocationCache::new(coord(&locations[0]), &locations[0].created_at),
        );
        for s in locations.iter().skip(1) {
            if prev_id != &s.chair_id {
                res.insert(
                    s.chair_id.clone(),
                    ChairLocationCache::new(coord(s), &s.created_at),
                );
            } else {
                res.get_mut(&s.chair_id)
                    .unwrap()
                    .update(coord(s), &s.created_at);
            }
            prev_id = &s.chair_id;
        }

        RwLock::new(res)
    }

    pub async fn new_chair_ride_cache(pool: &Pool<MySql>) -> RwLock<HashMap<String, RideCache>> {
        let mut res = HashMap::new();

        #[derive(Debug, sqlx::FromRow)]
        pub struct RideWithStatus {
            pub id: String,
            pub chair_id: Option<String>,
            pub pickup_latitude: i32,
            pub pickup_longitude: i32,
            pub destination_latitude: i32,
            pub destination_longitude: i32,
            pub status: String,
        }

        let rides: Vec<RideWithStatus> = sqlx::query_as("select rides.*, status from rides inner join ride_statuses on ride_statuses.ride_id=rides.id order by ride_statuses.created_at")
            .fetch_all(pool)
            .await
            .unwrap();
        let rides_len = rides.len();

        for ride in rides {
            if ["ENROUTE", "CARRYING"].contains(&ride.status.as_str()) {
                res.insert(
                    ride.chair_id.unwrap(),
                    RideCache {
                        ride_id: ride.id,
                        going_to: match ride.status.as_str() {
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
                        status: ride.status,
                    },
                );
                continue;
            }
            if ["PICKUP", "ARRIVED"].contains(&ride.status.as_str()) {
                res.remove(&ride.chair_id.unwrap());
            }
        }

        tracing::info!(
            "processed {rides_len} records, {} chairs are on duty",
            res.len()
        );

        RwLock::new(res)
    }

    pub async fn new_ride_status_cache(pool: &Pool<MySql>) -> RwLock<HashMap<String, String>> {
        let mut res = HashMap::new();

        let statuses: Vec<RideStatus> =
            sqlx::query_as("select * from ride_statuses order by created_at")
                .fetch_all(pool)
                .await
                .unwrap();
        let records_len = statuses.len();

        for status in statuses {
            res.insert(status.ride_id, status.status);
        }

        tracing::info!(
            "processes {records_len} records and {} rides confirmed",
            res.len()
        );

        RwLock::new(res)
    }

    pub async fn new_chair_cache(pool: &Pool<MySql>) -> RwLock<HashMap<String, Chair>> {
        let mut res = HashMap::new();

        let chairs: Vec<Chair> = sqlx::query_as("select * from chairs")
            .fetch_all(pool)
            .await
            .unwrap();
        let chairs_len = chairs.len();

        for chair in chairs {
            res.insert(chair.access_token.clone(), chair);
        }

        tracing::info!("processed {chairs_len} chairs");

        RwLock::new(res)
    }

    pub async fn new_user_cache(pool: &Pool<MySql>) -> RwLock<HashMap<String, User>> {
        let mut res = HashMap::new();

        let users: Vec<User> = sqlx::query_as("select * from users")
            .fetch_all(pool)
            .await
            .unwrap();
        let users_len = users.len();

        for user in users {
            res.insert(user.access_token.clone(), user);
        }

        tracing::info!("processes {users_len} users");

        RwLock::new(res)
    }
}

#[derive(Debug)]
pub struct ChairLocationCache {
    pub last_coord: Coordinate,
    pub total_distance: usize,
    pub updated_at: DateTime<Utc>,
}
impl ChairLocationCache {
    fn new(coord: Coordinate, at: &DateTime<Utc>) -> Self {
        Self {
            last_coord: coord,
            total_distance: 0,
            updated_at: *at,
        }
    }

    fn update(&mut self, coord: Coordinate, at: &DateTime<Utc>) {
        self.total_distance += self.last_coord.distance(&coord);
        self.last_coord = coord;
        self.updated_at = *at;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SQLx error: {0}")]
    Sqlx(sqlx::Error),
    #[error("failed to initialize: stdout={stdout} stderr={stderr}")]
    Initialize { stdout: String, stderr: String },
    #[error("{0}")]
    PaymentGateway(#[from] crate::payment_gateway::PaymentGatewayError),
    #[error("{0}")]
    BadRequest(&'static str),
    #[error("{0}")]
    Unauthorized(&'static str),
    #[error("{0}")]
    NotFound(&'static str),
    #[error("{0}")]
    Conflict(&'static str),
}
impl From<sqlx::Error> for Error {
    fn from(value: sqlx::Error) -> Self {
        tracing::trace!("{}", std::backtrace::Backtrace::force_capture());
        Self::Sqlx(value)
    }
}
impl axum::response::IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::PaymentGateway(_) => StatusCode::BAD_GATEWAY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        #[derive(Debug, serde::Serialize)]
        struct ErrorBody {
            message: String,
        }
        let message = self.to_string();
        tracing::error!("{message}");

        (status, axum::Json(ErrorBody { message })).into_response()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Coordinate {
    pub latitude: i32,
    pub longitude: i32,
}
impl Coordinate {
    fn distance(&self, rhs: &Self) -> usize {
        (self.latitude - rhs.latitude).unsigned_abs() as usize
            + (self.longitude - rhs.longitude).unsigned_abs() as usize
    }
}

pub fn secure_random_str(b: usize) -> String {
    use rand::RngCore as _;
    let mut buf = vec![0; b];
    let mut rng = rand::thread_rng();
    rng.fill_bytes(&mut buf);
    hex::encode(&buf)
}

// マンハッタン距離を求める
pub fn calculate_distance(
    a_latitude: i32,
    a_longitude: i32,
    b_latitude: i32,
    b_longitude: i32,
) -> i32 {
    (a_latitude - b_latitude).abs() + (a_longitude - b_longitude).abs()
}

const NOTIFICATION_RETRY_MS_APP: i32 = 200;
const NOTIFICATION_RETRY_MS_CHAIR: i32 = 200;

const INITIAL_FARE: i32 = 500;
const FARE_PER_DISTANCE: i32 = 100;

pub fn calculate_fare(
    pickup_latitude: i32,
    pickup_longitude: i32,
    dest_latitude: i32,
    dest_longitude: i32,
) -> i32 {
    let metered_fare = FARE_PER_DISTANCE
        * calculate_distance(
            pickup_latitude,
            pickup_longitude,
            dest_latitude,
            dest_longitude,
        );
    INITIAL_FARE + metered_fare
}

pub mod app_handlers;
pub mod chair_handlers;
pub mod internal_handlers;
pub mod middlewares;
pub mod models;
pub mod owner_handlers;
pub mod payment_gateway;
