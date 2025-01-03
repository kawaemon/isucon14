use std::{hash::Hash, marker::PhantomData, str::FromStr, sync::LazyLock};

use chrono::{DateTime, Utc};
use dashmap::SharedValue;
use derivative::Derivative;
use sqlx::{mysql::MySqlValueRef, Database, MySql};
use thiserror::Error;

use crate::{ConcurrentHashMap, ConcurrentHashSet, Coordinate};

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
    Copy(bound = ""),
    Hash(bound = ""),
    PartialEq(bound = ""),
    Eq(bound = "")
)]
pub struct Id<T>(Symbol, PhantomData<fn() -> T>);
impl<T> From<&Id<T>> for reqwest::header::HeaderValue {
    fn from(val: &Id<T>) -> Self {
        reqwest::header::HeaderValue::from_str(val.resolve()).unwrap()
    }
}
impl<T> serde::Serialize for Id<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}
impl<'de, T> serde::Deserialize<'de> for Id<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::new_from(Symbol::deserialize(deserializer)?))
    }
}
impl<'r, T> sqlx::Encode<'r, MySql> for Id<T> {
    fn encode_by_ref(
        &self,
        buf: &mut <MySql as Database>::ArgumentBuffer<'r>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        self.resolve().encode_by_ref(buf)
    }
}
impl<'r, T> sqlx::Decode<'r, MySql> for Id<T> {
    fn decode(value: MySqlValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        Ok(Id::new_from(Symbol::decode(value)?))
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
        Self::new_from(Symbol::new_from(ulid::Ulid::new().to_string()))
    }
    pub fn new_from(s: Symbol) -> Self {
        Id(s, PhantomData)
    }
    pub fn resolve(&self) -> &'static str {
        self.0.resolve()
    }
}

static STRING_TABLE: LazyLock<ConcurrentHashMap<&'static str, u64 /* hash of str */>> =
    LazyLock::new(Default::default);

use std::borrow::Cow;

#[derive(Debug, Clone, Copy)]
pub struct Symbol(&'static str, u64 /* hash */);
impl PartialEq<Symbol> for Symbol {
    fn eq(&self, other: &Symbol) -> bool {
        std::ptr::eq(self.0, other.0)
    }
}
impl Eq for Symbol {}
impl Hash for Symbol {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.1.hash(state);
    }
}
impl Symbol {
    fn new_from_inner(s: Cow<'_, str>) -> Self {
        if let Some(st) = STRING_TABLE.get(s.as_ref()) {
            return Symbol(st.key(), *st.value());
        }

        let table: &ConcurrentHashMap<_, _> = &*STRING_TABLE;

        let hash = table.hash_usize(&s);
        let shard_key = table.determine_shard(hash);
        let mut shard = table.shards().get(shard_key).unwrap().write();
        let s = match shard.find_or_find_insert_slot(
            hash as u64,
            |&(ins, _)| ins == s,
            |(ins, _)| table.hash_usize(ins) as u64,
        ) {
            Ok(bucket) => unsafe { bucket.as_ref().0 },
            Err(slot) => {
                // TODO: use arena
                let stored: &'static str = String::leak(s.into_owned());
                unsafe {
                    shard.insert_in_slot(
                        hash as u64,
                        slot,
                        (stored, SharedValue::new(hash as u64)),
                    );
                }
                stored
            }
        };

        Self(s, hash as u64)
    }

    pub fn new_from_ref(s: &str) -> Self {
        Self::new_from_inner(s.into())
    }
    pub fn new_from(s: String) -> Self {
        Self::new_from_inner(s.into())
    }

    #[inline(always)]
    pub fn resolve(&self) -> &'static str {
        self.0
    }
}
impl serde::Serialize for Symbol {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.resolve().serialize(serializer)
    }
}
impl<'de> serde::Deserialize<'de> for Symbol {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = <String>::deserialize(deserializer)?;
        Ok(Self::new_from_ref(&s))
    }
}
impl sqlx::Type<sqlx::MySql> for Symbol {
    fn type_info() -> <sqlx::MySql as Database>::TypeInfo {
        <&str>::type_info()
    }
    fn compatible(ty: &<sqlx::MySql as Database>::TypeInfo) -> bool {
        <&str>::compatible(ty)
    }
}
impl<'r> sqlx::Encode<'r, MySql> for Symbol {
    fn encode_by_ref(
        &self,
        buf: &mut <MySql as Database>::ArgumentBuffer<'r>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        self.resolve().encode_by_ref(buf)
    }
}
impl<'r> sqlx::Decode<'r, MySql> for Symbol {
    fn decode(value: MySqlValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <&str as sqlx::Decode<MySql>>::decode(value)?;
        Ok(Self::new_from_ref(s))
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Chair {
    pub id: Id<Chair>,
    pub owner_id: Id<Owner>,
    pub name: Symbol,
    pub access_token: Symbol,
    pub model: Symbol,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ChairLocation {
    pub id: String,
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
    pub username: Symbol,
    pub firstname: Symbol,
    pub lastname: Symbol,
    pub date_of_birth: Symbol,
    pub access_token: Symbol,
    pub invitation_code: Symbol,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct PaymentToken {
    pub user_id: Id<User>,
    pub token: Symbol,
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
    pub name: Symbol,
    pub access_token: Symbol,
    pub chair_register_token: Symbol,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct Coupon {
    pub user_id: Id<User>,
    pub code: Symbol,
    pub discount: i32,
    pub created_at: DateTime<Utc>,
    pub used_by: Option<Id<Ride>>,
}
