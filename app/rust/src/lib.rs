use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::Arc,
};

use axum::{http::StatusCode, response::Response};
use chair_handlers::{ChairGetNotificationResponseData, SimpleUser};
use chrono::{DateTime, Utc};
use models::{Chair, ChairLocation, Ride, RideStatus, User};
use sqlx::{MySql, Pool, Transaction};
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
pub struct ChairCache {
    ride: Option<RideCache>,
    total_rides_count: usize,
    total_evaluation: usize,
}
impl ChairCache {
    pub fn new() -> Self {
        Self {
            ride: None,
            total_evaluation: 0,
            total_rides_count: 0,
        }
    }
}

#[derive(Debug)]
pub struct RideCache {
    ride_id: String,
    status: String,
    going_to: Coordinate,
}

#[derive(Debug, Clone)]
pub struct ChairNotificationQData {
    data: ChairGetNotificationResponseData,
    ride_status_id: String,
}

impl ChairNotificationQData {
    pub async fn from_ride(
        ride: &Ride,
        ride_status: &str,
        ride_status_id: &str,
        pool: &Pool<MySql>,
    ) -> Self {
        let user: User = sqlx::query_as("select * from users where id=?")
            .bind(&ride.user_id)
            .fetch_one(pool)
            .await
            .unwrap();

        ChairNotificationQData {
            data: ChairGetNotificationResponseData {
                ride_id: ride.id.clone(),
                user: SimpleUser {
                    id: user.id,
                    name: format!("{} {}", user.firstname, user.lastname),
                },
                pickup_coordinate: ride.pickup_coordinate(),
                destination_coordinate: ride.destination_coordinate(),
                status: ride_status.to_owned(),
            },
            ride_status_id: ride_status_id.to_owned(),
        }
    }
}

#[derive(Debug)]
pub struct ChairNotificationQEntry {
    data: ChairNotificationQData,
    sent: bool,
}

#[derive(Debug)]
pub struct ChairNotificationQ {
    queue: VecDeque<ChairNotificationQEntry>,
}
impl ChairNotificationQ {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    pub fn push(&mut self, data: ChairNotificationQData) {
        self.queue
            .push_back(ChairNotificationQEntry { data, sent: false });
    }

    /// returns should update db
    pub fn take_next(&mut self) -> Option<(ChairNotificationQData, bool)> {
        if self.queue.is_empty() {
            return None;
        }
        if self.queue.len() == 1 {
            let e = &mut self.queue[0];
            if e.sent {
                return Some((e.data.clone(), false));
            }
            e.sent = true;
            return Some((e.data.clone(), true));
        }
        let mut top = self.queue.pop_front().unwrap();
        if top.sent {
            top = self.queue.pop_front().unwrap();
        }
        top.sent = true;
        Some((top.data, true))
    }
}

#[derive(Debug)]
pub struct AppCache {
    pub chair_location: RwLock<HashMap<String, ChairLocationCache>>,
    pub ride_status_cache: RwLock<HashMap<String /* ride_id */, String>>,
    pub chair_ride_cache: RwLock<HashMap<String /* chair_id */, ChairCache>>,
    pub chair_auth_cache: RwLock<HashMap<String /* access_token */, Chair>>,
    pub user_auth_cache: RwLock<HashMap<String /* access_token */, User>>,

    pub chair_notification_queue: Mutex<HashMap<String /* chair_id */, ChairNotificationQ>>,
}
impl AppCache {
    pub async fn new(pool: &Pool<MySql>) -> Self {
        Self {
            chair_location: Self::new_chair_location(pool).await,
            ride_status_cache: Self::new_ride_status_cache(pool).await,
            chair_ride_cache: Self::new_chair_ride_cache(pool).await,
            chair_auth_cache: Self::new_chair_cache(pool).await,
            user_auth_cache: Self::new_user_cache(pool).await,

            chair_notification_queue: Self::new_chair_notification_queue(pool).await,
        }
    }

    pub async fn new_chair_notification_queue(
        pool: &Pool<MySql>,
    ) -> Mutex<HashMap<String, ChairNotificationQ>> {
        let all_chairs: Vec<String> = sqlx::query_scalar("select id from chairs")
            .fetch_all(pool)
            .await
            .unwrap();

        let mut res = HashMap::new();

        for chair_id in all_chairs {
            let mut target: Vec<RideStatus> = sqlx::query_as(
                "select ride_statuses.*
                from (select * from rides where chair_id = ?) as rides_
                inner join ride_statuses on rides_.id = ride_statuses.ride_id
                where ride_statuses.chair_sent_at is null
                order by ride_statuses.created_at asc",
            )
            .bind(&chair_id)
            .fetch_all(pool)
            .await
            .unwrap();

            if target.is_empty() {
                let d: Option<RideStatus> = sqlx::query_as(
                    "select *
                    from (select * from rides where chair_id = ?) as rides_
                    inner join ride_statuses on rides_.id = ride_statuses.ride_id
                    order by ride_statuses.created_at desc
                    limit 1",
                )
                .bind(&chair_id)
                .fetch_optional(pool)
                .await
                .unwrap();
                if let Some(d) = d {
                    target.push(d);
                }
            }

            let mut q = ChairNotificationQ::new();
            for t in target {
                let ride: Ride = sqlx::query_as("select * from rides where id = ?")
                    .bind(&t.ride_id)
                    .fetch_one(pool)
                    .await
                    .unwrap();
                q.queue.push_back(ChairNotificationQEntry {
                    sent: t.chair_sent_at.is_some(),
                    data: ChairNotificationQData::from_ride(&ride, &t.status, &t.id, pool).await,
                })
            }
            res.insert(chair_id, q);
        }

        Mutex::new(res)
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

    pub async fn new_chair_ride_cache(pool: &Pool<MySql>) -> RwLock<HashMap<String, ChairCache>> {
        let mut res = HashMap::new();

        let chairs: Vec<Chair> = sqlx::query_as("select * from chairs")
            .fetch_all(pool)
            .await
            .unwrap();
        for chair in chairs {
            res.insert(chair.id, ChairCache::new());
        }

        let rides: Vec<Ride> = sqlx::query_as("select * from rides")
            .fetch_all(pool)
            .await
            .unwrap();
        for ride in rides {
            if let Some(eval) = ride.evaluation {
                let chair = res.get_mut(&ride.chair_id.unwrap()).unwrap();
                chair.total_evaluation += eval as usize;
                chair.total_rides_count += 1;
            }
        }

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
                let chair_id = ride.chair_id.unwrap();
                res.get_mut(&chair_id).unwrap().ride = Some(RideCache {
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
                });
                continue;
            }
            if ["PICKUP", "ARRIVED"].contains(&ride.status.as_str()) {
                let chair_id = ride.chair_id.unwrap();
                res.get_mut(&chair_id).unwrap().ride = None;
                continue;
            }
        }

        let duty_count = res.iter().filter(|x| x.1.ride.is_some()).count();
        tracing::info!("processed {rides_len} records, {duty_count} chairs are on duty",);

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
