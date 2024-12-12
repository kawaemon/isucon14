use std::{sync::Arc, time::Duration};

use axum::{http::StatusCode, response::Response};
use models::{Id, Ride, RideStatusEnum};
use repo::Repository;

#[derive(Debug, Clone)]
pub struct AppState {
    pub pool: sqlx::MySqlPool,
    pub repo: Arc<Repository>,
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
const RETRY_MS_APP: Duration = Duration::from_millis(200);
const RETRY_MS_CHAIR: Duration = Duration::from_millis(200);

pub fn calculate_fare(pickup: Coordinate, dest: Coordinate) -> i32 {
    let metered_fare = FARE_PER_DISTANCE * pickup.distance(dest);
    INITIAL_FARE + metered_fare
}

pub mod app_handlers;
pub mod chair_handlers;
pub mod internal_handlers;
pub mod middlewares;
pub mod models;
pub mod owner_handlers;
pub mod payment_gateway;
pub mod repo;
