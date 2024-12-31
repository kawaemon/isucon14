use std::{marker::PhantomData, str::FromStr, sync::atomic::AtomicUsize};

use chrono::{DateTime, Utc};
use derivative::Derivative;
use sqlx::{mysql::MySqlValueRef, Database, MySql};
use thiserror::Error;

use crate::Coordinate;

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

#[derive(Derivative)]
#[derivative(
    Debug(bound = ""),
    Clone(bound = ""),
    Hash(bound = ""),
    PartialEq(bound = ""),
    Eq(bound = "")
)]
pub struct Id<T>(String, PhantomData<fn() -> T>);
impl<T> AsRef<str> for Id<T> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl<T> std::fmt::Display for Id<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl<T> From<&Id<T>> for reqwest::header::HeaderValue {
    fn from(val: &Id<T>) -> Self {
        reqwest::header::HeaderValue::from_str(&val.0).unwrap()
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
        Ok(Id::from_string(s))
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
        static ID: AtomicUsize = AtomicUsize::new(1);
        let id = ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Id(id.to_string(), PhantomData)
    }
    pub fn from_string(id: impl Into<String>) -> Self {
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
    pub fn calc_sale(&self) -> i32 {
        crate::calculate_fare(self.pickup_coord(), self.destination_coord())
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
