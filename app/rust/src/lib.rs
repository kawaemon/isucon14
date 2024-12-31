#![allow(clippy::new_without_default)]
#![warn(clippy::future_not_send)]
#![warn(clippy::unused_async)]

pub mod app_handlers;
pub mod chair_handlers;
pub mod dl;
pub mod internal_handlers;
pub mod middlewares;
pub mod models;
pub mod owner_handlers;
pub mod payment_gateway;
pub mod repo;
pub mod speed;

use std::sync::{atomic::AtomicUsize, Arc};

use axum::{http::StatusCode, response::Response};
use derivative::Derivative;
use models::{Chair, Id, User};
use payment_gateway::PaymentGatewayRestricter;
use repo::Repository;
use speed::SpeedStatistics;
use std::time::Duration;
use tokio::sync::Mutex;

pub type HashMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;
pub type HashSet<K> = std::collections::HashSet<K, ahash::RandomState>;
pub type ConcurrentHashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;
pub type ConcurrentHashSet<K> = dashmap::DashSet<K, ahash::RandomState>;

#[derive(Debug, Clone)]
pub struct AppState {
    pub pool: sqlx::MySqlPool,
    pub repo: Arc<Repository>,
    pub pgw: PaymentGatewayRestricter,
    pub speed: SpeedStatistics,
    pub chair_notification_stat: NotificationStatistics<Chair>,
    pub user_notification_stat: NotificationStatistics<User>,
    pub client: reqwest::Client,
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

#[derive(Debug)]
struct WithDelta {
    total: AtomicUsize,
    new_con: AtomicUsize,
    dis_con: AtomicUsize,
}
impl WithDelta {
    pub fn new() -> Self {
        let t = || AtomicUsize::new(0);
        Self {
            total: t(),
            new_con: t(),
            dis_con: t(),
        }
    }
    pub fn increment(&self) {
        self.total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.new_con
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn decrement(&self) {
        self.total
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        self.dis_con
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn take(&self) -> (usize, usize, usize) {
        let total = self.total.load(std::sync::atomic::Ordering::Relaxed);
        let new_con = self.new_con.swap(0, std::sync::atomic::Ordering::Relaxed);
        let dis_con = self.dis_con.swap(0, std::sync::atomic::Ordering::Relaxed);
        (total, new_con, dis_con)
    }
}

#[derive(Derivative)]
#[derivative(Debug(bound = ""), Clone(bound = ""))]
pub struct NotificationStatistics<T>(NotificationStatisticsInner<T>);

#[derive(Derivative)]
#[derivative(Debug(bound = ""), Clone(bound = ""))]
pub struct NotificationStatisticsInner<T> {
    connections: Arc<WithDelta>,
    writes: Arc<Mutex<HashSet<Id<T>>>>,
}
impl<T: 'static> NotificationStatistics<T> {
    pub fn new() -> Self {
        let inner = NotificationStatisticsInner {
            connections: Arc::new(WithDelta::new()),
            writes: Arc::new(Mutex::new(HashSet::default())),
        };

        tokio::spawn({
            let inner = inner.clone();
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(5000)).await;
                    let (total, new, dis) = inner.connections.take();
                    let writes = inner.writes.lock().await.drain().count();
                    let tname = std::any::type_name::<T>();
                    tracing::info!(
                        "{tname} notifications: total={total} (writes: {writes}), +={new}, -={dis}"
                    );
                }
            }
        });

        Self(inner)
    }

    #[must_use = "hold this until stream drops"]
    pub fn on_create(&self) -> ConnectionProbe<T> {
        ConnectionProbe::new(self.0.clone())
    }

    pub async fn on_write(&self, id: &Id<T>) {
        self.0.writes.lock().await.insert(id.clone());
    }
}

#[derive(Derivative)]
#[derivative(Debug(bound = ""))]
pub struct ConnectionProbe<T>(NotificationStatisticsInner<T>);
impl<T> ConnectionProbe<T> {
    fn new(inner: NotificationStatisticsInner<T>) -> Self {
        inner.connections.increment();
        Self(inner)
    }
}
impl<T> Drop for ConnectionProbe<T> {
    fn drop(&mut self) {
        self.0.connections.decrement();
    }
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
