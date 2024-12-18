use std::{marker::PhantomData, str::FromStr};

use axum::{http::StatusCode, response::Response};
use chrono::{DateTime, Utc};
use sqlx::{mysql::MySqlValueRef, Database, MySql};
use thiserror::Error;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SQLx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("failed to initialize: stdout={stdout} stderr={stderr}")]
    Initialize { stdout: String, stderr: String },
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RideStatusEnum {
    Matching,
    Enroute,
    Pickup,
    Carrying,
    Arrived,
    Completed,
    Canceled,
}
#[derive(Debug, Error)]
pub enum RideStatusParseError {
    #[error("failed to parse ride status")]
    Error,
}
impl<'r> sqlx::Encode<'r, MySql> for RideStatusEnum {
    fn encode_by_ref(
        &self,
        buf: &mut <MySql as Database>::ArgumentBuffer<'r>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        self.to_string().encode_by_ref(buf)
    }
}
impl<'r> sqlx::Decode<'r, MySql> for RideStatusEnum {
    fn decode(value: MySqlValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<MySql>>::decode(value)?;
        let r = s.parse()?;
        Ok(r)
    }
}
impl sqlx::Type<sqlx::MySql> for RideStatusEnum {
    fn type_info() -> <sqlx::MySql as Database>::TypeInfo {
        String::type_info()
    }
    fn compatible(_ty: &<sqlx::MySql as Database>::TypeInfo) -> bool {
        true // w
    }
}
impl std::fmt::Display for RideStatusEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            RideStatusEnum::Matching => "MATCHING",
            RideStatusEnum::Enroute => "ENROUTE",
            RideStatusEnum::Pickup => "PICKUP",
            RideStatusEnum::Carrying => "CARRYING",
            RideStatusEnum::Arrived => "ARRIVED",
            RideStatusEnum::Completed => "COMPLETED",
            RideStatusEnum::Canceled => "CANCELED",
        };
        write!(f, "{s}")
    }
}
impl FromStr for RideStatusEnum {
    type Err = RideStatusParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use RideStatusEnum::*;
        let r = match s {
            "MATCHING" => Matching,
            "ENROUTE" => Enroute,
            "PICKUP" => Pickup,
            "CARRYING" => Carrying,
            "ARRIVED" => Arrived,
            "COMPLETED" => Completed,
            "CANCELED" => Canceled,
            _ => return Err(RideStatusParseError::Error),
        };
        Ok(r)
    }
}

pub struct Id<T>(pub String, PhantomData<fn() -> T>);
impl<T> std::fmt::Debug for Id<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl<T> std::clone::Clone for Id<T> {
    fn clone(&self) -> Self {
        Id(self.0.clone(), PhantomData)
    }
}
impl<T> serde::Serialize for Id<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}
impl<'de, T> serde::Deserialize<'de> for Id<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self(String::deserialize(deserializer)?, PhantomData))
    }
}
impl<T> std::hash::Hash for Id<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}
impl<T> PartialEq<Id<T>> for Id<T> {
    fn eq(&self, other: &Id<T>) -> bool {
        self.0 == other.0
    }
}
impl<T> Eq for Id<T> {}
impl<'r, T> sqlx::Encode<'r, MySql> for Id<T> {
    fn encode_by_ref(
        &self,
        buf: &mut <MySql as Database>::ArgumentBuffer<'r>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        self.0.encode_by_ref(buf)
    }
}
impl<'r, T> sqlx::Decode<'r, MySql> for Id<T> {
    fn decode(value: MySqlValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<MySql>>::decode(value)?;
        Ok(Id::from_str(s))
    }
}
impl<T> sqlx::Type<sqlx::MySql> for Id<T> {
    fn type_info() -> <sqlx::MySql as Database>::TypeInfo {
        String::type_info()
    }
    fn compatible(ty: &<sqlx::MySql as Database>::TypeInfo) -> bool {
        String::compatible(ty)
    }
}
impl<T> Id<T> {
    pub fn new() -> Self {
        Id(ulid::Ulid::new().to_string(), PhantomData)
    }
    pub fn from_str(id: impl Into<String>) -> Self {
        Self(id.into(), PhantomData)
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Chair {
    pub id: Id<Chair>,
    pub owner_id: Id<Owner>,
    pub name: String,
    pub access_token: String,
    pub model: String,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ChairLocation {
    pub id: Id<ChairLocation>,
    pub chair_id: Id<Chair>,
    pub latitude: i32,
    pub longitude: i32,
    pub created_at: DateTime<Utc>,
}
impl ChairLocation {
    pub fn coord(&self) -> Coordinate {
        Coordinate {
            latitude: self.latitude,
            longitude: self.longitude,
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct User {
    pub id: Id<User>,
    pub username: String,
    pub firstname: String,
    pub lastname: String,
    pub date_of_birth: String,
    pub access_token: String,
    pub invitation_code: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct PaymentToken {
    pub user_id: Id<User>,
    pub token: String,
    pub created_at: DateTime<Utc>,
}

pub const INITIAL_FARE: i32 = 500;
pub const FARE_PER_DISTANCE: i32 = 100;
pub fn calc_sale(pickup: Coordinate, dest: Coordinate) -> i32 {
    let metered_fare = FARE_PER_DISTANCE * pickup.distance(dest);
    INITIAL_FARE + metered_fare
}

#[derive(Debug, sqlx::FromRow)]
pub struct Ride {
    pub id: Id<Ride>,
    pub user_id: Id<User>,
    pub chair_id: Option<Id<Chair>>,
    pub pickup_latitude: i32,
    pub pickup_longitude: i32,
    pub destination_latitude: i32,
    pub destination_longitude: i32,
    pub evaluation: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl Ride {
    pub fn pickup(&self) -> Coordinate {
        Coordinate {
            latitude: self.pickup_latitude,
            longitude: self.pickup_longitude,
        }
    }
    pub fn destination(&self) -> Coordinate {
        Coordinate {
            latitude: self.destination_latitude,
            longitude: self.destination_longitude,
        }
    }
    pub fn calc_sale(&self) -> i32 {
        calc_sale(self.pickup(), self.destination())
    }
}

impl Ride {
    pub fn pickup_coord(&self) -> Coordinate {
        Coordinate {
            latitude: self.pickup_latitude,
            longitude: self.pickup_longitude,
        }
    }
    pub fn destination_coord(&self) -> Coordinate {
        Coordinate {
            latitude: self.destination_latitude,
            longitude: self.destination_longitude,
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RideStatus {
    pub id: Id<RideStatus>,
    pub ride_id: Id<Ride>,
    pub status: RideStatusEnum,
    pub created_at: DateTime<Utc>,
    pub app_sent_at: Option<DateTime<Utc>>,
    pub chair_sent_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Owner {
    pub id: Id<Owner>,
    pub name: String,
    pub access_token: String,
    pub chair_register_token: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct Coupon {
    pub user_id: Id<User>,
    pub code: String,
    pub discount: i32,
    pub created_at: DateTime<Utc>,
    pub used_by: Option<Id<Ride>>,
}
