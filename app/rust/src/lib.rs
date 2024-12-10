use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::Arc,
};

use axum::{http::StatusCode, response::Response};
use chair_handlers::{ChairGetNotificationResponseData, SimpleUser};
use chrono::{DateTime, Utc};
use models::{Chair, ChairLocation, Ride, RideStatus, User};
use sqlx::{MySql, Pool};
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone)]
pub struct AppState {
    pub pool: sqlx::MySqlPool,
    pub cache: Arc<AppCache>,
    pub deferred: Arc<AppDeferred>,
    pub queue: Arc<AppQueue>,
}

#[derive(Debug, Clone)]
pub struct QcNotification {
    data: ChairGetNotificationResponseData,
    status_id: String,
}

#[derive(Debug, Clone)]
pub struct CnqI {
    data: QcNotification,
    sent: bool,
}

#[derive(Debug)]
pub struct Cnq {
    queue: VecDeque<CnqI>,
}
impl Cnq {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }
    pub fn push(&mut self, data: QcNotification) {
        if self.queue.front().is_some_and(|x| x.sent) {
            self.queue.pop_front();
        }
        tracing::info!("cnq push: {data:#?}");
        self.queue.push_back(CnqI { data, sent: false });
    }
    pub fn get_next(&mut self) -> Option<(QcNotification, bool)> {
        if self.queue.is_empty() {
            return None;
        }
        if self.queue.len() == 1 {
            let f = self.queue.front_mut().unwrap();
            let already_sent = f.sent;
            f.sent = true;
            return Some((f.data.clone(), !already_sent));
        }
        let t = self.queue.pop_front().unwrap();
        Some((t.data, true))
    }
}

#[derive(Debug)]
pub struct AppQueue {
    pub chair_notification_queue: Mutex<HashMap<String /* chair_id */, Cnq>>,
}
impl AppQueue {
    pub async fn new(pool: &Pool<MySql>) -> Self {
        let mut res = HashMap::new();

        let rides_list: Vec<Ride> = sqlx::query_as("select * from rides")
            .fetch_all(pool)
            .await
            .unwrap();
        let mut rides: HashMap<String, Ride> = HashMap::new();
        for ride in rides_list {
            rides.insert(ride.id.clone(), ride);
        }

        let users_list: Vec<User> = sqlx::query_as("select * from users")
            .fetch_all(pool)
            .await
            .unwrap();
        let mut users: HashMap<String, User> = HashMap::new();
        for user in users_list {
            users.insert(user.id.clone(), user);
        }

        let chairs_list: Vec<Chair> = sqlx::query_as("select * from chairs")
            .fetch_all(pool)
            .await
            .unwrap();

        let ride_statuses: Vec<RideStatus> =
            sqlx::query_as("select * from ride_statuses order by created_at")
                .fetch_all(pool)
                .await
                .unwrap();

        for chair in chairs_list {
            res.insert(chair.id, Cnq::new());
        }

        for status in ride_statuses {
            let ride = rides.get(&status.ride_id).unwrap();
            let Some(chair_id) = ride.chair_id.as_ref() else {
                continue;
            };

            let queue = res.get_mut(chair_id).unwrap();

            if queue.queue.front().is_some_and(|x| x.sent) {
                queue.queue.pop_front();
            }

            let user = users.get(&ride.user_id).unwrap().clone();
            queue.queue.push_back(CnqI {
                data: QcNotification {
                    status_id: status.id,
                    data: ChairGetNotificationResponseData {
                        ride_id: ride.id.clone(),
                        user: SimpleUser {
                            id: user.id,
                            name: format!("{} {}", user.firstname, user.lastname),
                        },
                        pickup_coordinate: ride.pickup_coordinate(),
                        destination_coordinate: ride.destination_coordinate(),
                        status: status.status,
                    },
                },
                sent: status.chair_sent_at.is_some(),
            });
        }

        for (k, v) in res.iter_mut() {
            if v.queue.len() <= 1 {
                continue;
            }

            loop {
                let mut swap = None;
                let mut prev = v.queue[0].data.data.status.clone();
                for (i, stat) in v.queue.iter().enumerate().skip(1) {
                    if prev == "MATCHING" && stat.data.data.status == "COMPLETED" {
                        swap = Some(i);
                        break;
                    }
                    prev = stat.data.data.status.clone();
                }
                if let Some(s) = swap {
                    v.queue.swap(s, s - 1);
                    tracing::warn!("MATCHING <=> COMPLETED swap occured: i={s}, chair={k}");
                    continue;
                }
                break;
            }
        }

        for (k, v) in res.iter() {
            tracing::info!("### {k} ###");
            for v in &v.queue {
                tracing::info!("{v:?}");
            }
        }

        Self {
            chair_notification_queue: Mutex::new(res),
        }
    }
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

#[derive(Debug)]
pub struct AppCache {
    pub chair_location: RwLock<HashMap<String, ChairLocationCache>>,
    pub ride_status_cache: RwLock<HashMap<String /* ride_id */, String>>,
    pub chair_ride_cache: RwLock<HashMap<String /* chair_id */, ChairCache>>,
    pub chair_auth_cache: RwLock<HashMap<String /* access_token */, Chair>>,
    pub user_auth_cache: RwLock<HashMap<String /* access_token */, User>>,
}
impl AppCache {
    pub async fn new(pool: &Pool<MySql>) -> Self {
        Self {
            chair_location: Self::new_chair_location(pool).await,
            ride_status_cache: Self::new_ride_status_cache(pool).await,
            chair_ride_cache: Self::new_chair_ride_cache(pool).await,
            chair_auth_cache: Self::new_chair_cache(pool).await,
            user_auth_cache: Self::new_user_cache(pool).await,
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
