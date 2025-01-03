#![allow(clippy::new_without_default)]
#![allow(clippy::type_complexity)]
#![warn(clippy::future_not_send)]
#![warn(clippy::unused_async)]

pub mod app_handlers;
pub mod chair_handlers;
pub mod dl;
pub mod fw;
pub mod internal_handlers;
// pub mod middlewares;
pub mod models;
pub mod owner_handlers;
pub mod payment_gateway;
pub mod repo;
pub mod speed;

use std::sync::{atomic::AtomicI64, Arc, LazyLock};

use chrono::{DateTime, Utc};
use lasso::{Spur, ThreadedRodeo};
use repo::Repository;

pub type HashMap<K, V> = hashbrown::HashMap<K, V, ahash::RandomState>;
pub type HashSet<K> = hashbrown::HashSet<K, ahash::RandomState>;
pub type ConcurrentHashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;
pub type ConcurrentHashSet<K> = dashmap::DashSet<K, ahash::RandomState>;

pub static INTERNER: LazyLock<ThreadedRodeo<Spur, ahash::RandomState>> =
    LazyLock::new(|| ThreadedRodeo::with_hasher(ahash::RandomState::default()));

#[derive(Debug)]
pub struct AtomicDateTime(AtomicI64);
impl AtomicDateTime {
    pub fn new(d: DateTime<Utc>) -> Self {
        let s = Self(AtomicI64::new(0));
        s.store(d);
        s
    }
    pub fn load(&self) -> DateTime<Utc> {
        let raw = self.0.load(std::sync::atomic::Ordering::Relaxed);
        DateTime::from_timestamp_micros(raw).unwrap()
    }
    pub fn store(&self, d: DateTime<Utc>) {
        let d = d.timestamp_micros();
        self.0.store(d, std::sync::atomic::Ordering::Relaxed);
    }
}

pub type AppState = Arc<AppStateInner>;

#[derive(Debug)]
pub struct AppStateInner {
    pub pool: sqlx::MySqlPool,
    pub repo: Arc<Repository>,

    #[cfg(feature = "speed")]
    pub speed: SpeedStatistics,

    pub client: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("I/O error(hyper): {0}")]
    Hyper(#[from] hyper::Error),
    #[error("JSON decode: {0}")]
    SerdeJson(#[from] serde_json::Error),
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
// impl axum::response::IntoResponse for Error {
//     fn into_response(self) -> Response {
//         let status = match self {
//             Self::BadRequest(_) => StatusCode::BAD_REQUEST,
//             Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
//             Self::NotFound(_) => StatusCode::NOT_FOUND,
//             Self::Conflict(_) => StatusCode::CONFLICT,
//             Self::PaymentGateway(_) => StatusCode::BAD_GATEWAY,
//             _ => StatusCode::INTERNAL_SERVER_ERROR,
//         };
//
//         #[derive(Debug, serde::Serialize)]
//         struct ErrorBody {
//             message: String,
//         }
//         let message = self.to_string();
//         tracing::error!("{message}");
//
//         (status, axum::Json(ErrorBody { message })).into_response()
//     }
// }

#[derive(Debug, Clone, PartialEq, Eq, Copy, serde::Serialize, serde::Deserialize)]
pub struct Coordinate {
    pub latitude: i32,
    pub longitude: i32,
}
impl Coordinate {
    pub fn distance(&self, other: Coordinate) -> i32 {
        (self.latitude.abs_diff(other.latitude) + self.longitude.abs_diff(other.longitude)) as i32
    }
}

pub fn secure_random_str(b: usize) -> String {
    use rand::RngCore as _;
    let mut buf = vec![0; b];
    let mut rng = rand::thread_rng();
    rng.fill_bytes(&mut buf);
    hex::encode(&buf)
}

const INITIAL_FARE: i32 = 500;
const FARE_PER_DISTANCE: i32 = 100;

pub fn calculate_fare(pickup: Coordinate, dest: Coordinate) -> i32 {
    let metered_fare = FARE_PER_DISTANCE * pickup.distance(dest);
    INITIAL_FARE + metered_fare
}

macro_rules! conf_env {
    (static $name:ident: $ty:ty = {from: $env:expr, default: $def:expr,}) => {
        static $name: std::sync::LazyLock<$ty> = std::sync::LazyLock::new(|| {
            let v = std::env::var($env)
                .unwrap_or_else(|_| $def.to_owned())
                .parse()
                .unwrap_or_else(|_| panic!(concat!("invalid ", $env)));
            tracing::info!("{} = {v}", $env);
            v
        });
    };
}
use conf_env;
