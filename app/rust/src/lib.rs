use std::{collections::HashMap, sync::Arc};

use axum::{http::StatusCode, response::Response};
use chrono::{DateTime, Utc};
use models::ChairLocation;
use sqlx::{MySql, Pool};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct AppState {
    pub pool: sqlx::MySqlPool,
    pub cache: Arc<AppCache>,
}

#[derive(Debug)]
pub struct AppCache {
    pub chair_location: RwLock<HashMap<String, ChairLocationCache>>,
}
impl AppCache {
    pub async fn new(pool: &Pool<MySql>) -> Self {
        let locations: Vec<ChairLocation> =
            sqlx::query_as("select * from chair_locations order by chair_id, created_at asc")
                .fetch_all(pool)
                .await
                .unwrap();

        let coord = |s: &ChairLocation| Coordinate {
            latitude: s.latitude,
            longitude: s.longitude,
        };

        let mut res = HashMap::new();
        if locations.is_empty() {
            return Self {
                chair_location: RwLock::new(res),
            };
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

        tracing::info!("loaded {} coords", res.len());
        Self {
            chair_location: RwLock::new(res),
        }
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
            updated_at: at.clone(),
        }
    }

    fn update(&mut self, coord: Coordinate, at: &DateTime<Utc>) {
        self.total_distance += self.last_coord.distance(&coord);
        self.last_coord = coord;
        self.updated_at = at.clone();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SQLx error: {0}")]
    Sqlx(#[from] sqlx::Error),
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

pub async fn get_latest_ride_status<'e, E>(executor: E, ride_id: &str) -> sqlx::Result<String>
where
    E: 'e + sqlx::Executor<'e, Database = sqlx::MySql>,
{
    sqlx::query_scalar(
        "SELECT status FROM ride_statuses WHERE ride_id = ? ORDER BY created_at DESC LIMIT 1",
    )
    .bind(ride_id)
    .fetch_one(executor)
    .await
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

const NOTIFICATION_RETRY_MS_APP: i32 = 300;
const NOTIFICATION_RETRY_MS_CHAIR: i32 = 300;

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
