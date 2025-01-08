use std::{hash::Hash, marker::PhantomData, str::FromStr, sync::LazyLock};

use chrono::{DateTime, Utc};
use dashmap::SharedValue;
use derivative::Derivative;
use sqlx::{mysql::MySqlValueRef, Database, MySql};
use std::borrow::Cow;
use thiserror::Error;

use crate::{fw::SerializeJson, ConcurrentHashSet, Coordinate};

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
        let s = <&str as sqlx::Decode<MySql>>::decode(value)?;
        let r = s.parse()?;
        Ok(r)
    }
}
impl sqlx::Type<sqlx::MySql> for RideStatusEnum {
    fn type_info() -> <sqlx::MySql as Database>::TypeInfo {
        String::type_info()
    }
    fn compatible(_ty: &<sqlx::MySql as Database>::TypeInfo) -> bool {
        true // enum の比較めんどくさい！ここでミスっても大したことない！ FromStr で Err 落ちするから大丈夫！多分！
    }
}
impl RideStatusEnum {
    #[inline(always)]
    fn as_str(&self) -> &str {
        match self {
            RideStatusEnum::Matching => "MATCHING",
            RideStatusEnum::Enroute => "ENROUTE",
            RideStatusEnum::Pickup => "PICKUP",
            RideStatusEnum::Carrying => "CARRYING",
            RideStatusEnum::Arrived => "ARRIVED",
            RideStatusEnum::Completed => "COMPLETED",
            RideStatusEnum::Canceled => "CANCELED",
        }
    }
}
impl SerializeJson for RideStatusEnum {
    fn size_est(&self) -> usize {
        "COMPLETED".len() + 2
    }
    fn ser(&self, buf: &mut String) {
        buf.push('"');
        buf.push_str(self.as_str());
        buf.push('"');
    }
}
impl std::fmt::Display for RideStatusEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
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
pub struct Id<T>(
    Symbol,
    #[derivative(Debug = "ignore")] PhantomData<fn() -> T>,
);
sqlx_forward_symbol!(impl<T> for Id<T>);
impl<T> SerializeJson for Id<T> {
    #[inline(always)]
    fn size_est(&self) -> usize {
        const { ulid::ULID_LEN + 2 }
    }
    #[inline(always)]
    fn ser(&self, buf: &mut String) {
        self.0.ser(buf);
    }
}
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
impl<T> Id<T> {
    pub fn new() -> Self {
        Self::new_from(Symbol::new_from(ulid::Ulid::new().to_string()))
    }
    fn from_symbol(s: Symbol) -> Self {
        Self(s, PhantomData)
    }
    pub fn new_from(s: Symbol) -> Self {
        Self(s, PhantomData)
    }
    pub fn resolve(&self) -> &'static str {
        self.0.resolve()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SymbolHasherBuilder;
impl std::hash::BuildHasher for SymbolHasherBuilder {
    type Hasher = SymbolHasher;
    fn build_hasher(&self) -> Self::Hasher {
        SymbolHasher::new()
    }
}

#[derive(Debug)]
pub struct SymbolHasher(Option<u64>);
impl SymbolHasher {
    fn new() -> Self {
        Self(None)
    }
    fn unimp(&self) -> ! {
        unimplemented!("expected single u64 hash only");
    }
}
#[rustfmt::skip]
impl std::hash::Hasher for SymbolHasher {
    fn write_u64(&mut self, i: u64) {
        if self.0.is_some() {
            self.unimp();
        }
        self.0 = Some(i);
    }
    fn finish(&self) -> u64 {
        let Some(&i) = self.0.as_ref() else {
            self.unimp();
        };
        i
    }
    fn write(&mut self, _bytes: &[u8]) { self.unimp(); }
    fn write_u8(&mut self, _i: u8) { self.unimp(); }
    fn write_u16(&mut self, _i: u16) { self.unimp(); }
    fn write_u32(&mut self, _i: u32) { self.unimp(); }
    // write_u64
    fn write_u128(&mut self, _i: u128) { self.unimp(); }
    fn write_usize(&mut self, _i: usize) { self.unimp(); }
    fn write_i8(&mut self, _i: i8) { self.unimp(); }
    fn write_i16(&mut self, _i: i16) { self.unimp(); }
    fn write_i32(&mut self, _i: i32) { self.unimp(); }
    fn write_i64(&mut self, _i: i64) { self.unimp(); }
    fn write_i128(&mut self, _i: i128) { self.unimp(); }
    fn write_isize(&mut self, _i: isize) { self.unimp(); }
}

#[derive(Derivative, Clone, Copy)]
#[derivative(Debug)]
pub struct Symbol(&'static str);
impl SerializeJson for Symbol {
    #[inline(always)]
    fn size_est(&self) -> usize {
        self.0.len() + 2
    }
    #[inline(always)]
    fn ser(&self, buf: &mut String) {
        buf.push('"');
        buf.push_str(self.resolve());
        buf.push('"');
    }
}
impl PartialEq<Symbol> for Symbol {
    fn eq(&self, other: &Symbol) -> bool {
        std::ptr::eq(self.0, other.0)
    }
}
impl Eq for Symbol {}
impl Hash for Symbol {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // ptr を hash 値として使いたいわけだけど、 align の問題で
        // ケツが常にキリのいい数字なので mod 取ったときにいい感じの分布にならず
        // bucket が被りまくって、結果として hash 競合しまくって ptr::eq が呼ばれまくっている
        // と予想して、スクランブルしていい感じにする。
        // https://faithandbrave.hateblo.jp/entry/20121022/1350887537
        let offset = 0x9e3779b9u64;
        let addr = self.0.as_ptr() as usize as u64;
        let addr = addr.wrapping_mul(offset).rotate_left(21);
        state.write_u64(addr);
    }
}
impl Symbol {
    fn new_from_inner(s: Cow<'_, str>) -> Self {
        static STRING_TABLE: LazyLock<ConcurrentHashSet<&'static str>> =
            LazyLock::new(Default::default);

        let table = &*STRING_TABLE;

        if let Some(st) = table.get(s.as_ref()) {
            return Symbol(st.key());
        }

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
                    shard.insert_in_slot(hash as u64, slot, (stored, SharedValue::new(())));
                }
                stored
            }
        };

        Self(s)
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
        let s = <Cow<'_, str>>::deserialize(deserializer)?;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct InvitationCode(Symbol);
sqlx_forward_symbol!(impl for InvitationCode);
impl SerializeJson for InvitationCode {
    #[inline(always)]
    fn size_est(&self) -> usize {
        const { 16 + 2 }
    }
    #[inline(always)]
    fn ser(&self, buf: &mut String) {
        self.0.ser(buf);
    }
}
impl InvitationCode {
    pub fn new() -> Self {
        InvitationCode(Symbol::new_from(crate::secure_random_str(8)))
    }
    fn from_symbol(s: Symbol) -> Self {
        Self(s)
    }
    pub fn gen_for_invited(&self) -> CouponCode {
        CouponCode(Symbol::new_from(format!("I{}", self.0.resolve())))
    }
    pub fn gen_for_reward(&self) -> CouponCode {
        CouponCode(Symbol::new_from(format!("R{}", ulid::Ulid::new())))
    }
    pub fn is_empty(&self) -> bool {
        let is_empty = self.0.resolve().is_empty();
        assert!(!is_empty);
        is_empty
    }
    pub fn as_symbol(&self) -> Symbol {
        self.0
    }
}

pub static COUPON_CP_NEW2024: LazyLock<CouponCode> =
    LazyLock::new(|| CouponCode(Symbol::new_from_ref("CP_NEW2024")));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CouponCode(Symbol);
sqlx_forward_symbol!(impl for CouponCode);
impl SerializeJson for CouponCode {
    #[inline(always)]
    fn size_est(&self) -> usize {
        const { 16 + 2 + 1 }
    }
    #[inline(always)]
    fn ser(&self, buf: &mut String) {
        self.0.ser(buf);
    }
}
impl CouponCode {
    fn from_symbol(s: Symbol) -> Self {
        Self(s)
    }
    pub fn is_empty(&self) -> bool {
        let is_empty = self.0.resolve().is_empty();
        assert!(!is_empty);
        is_empty
    }
    pub fn as_symbol(&self) -> Symbol {
        self.0
    }
}

macro_rules! sqlx_forward_symbol {
    (impl$(<$generic:ident>)? for $ty:ty) => {
        impl$(<$generic>)? sqlx::Type<sqlx::MySql> for $ty {
            fn type_info() -> <sqlx::MySql as Database>::TypeInfo {
                String::type_info()
            }
            fn compatible(ty: &<sqlx::MySql as Database>::TypeInfo) -> bool {
                String::compatible(ty)
            }
        }
        impl<'r, $($generic)?> sqlx::Encode<'r, MySql> for $ty {
            fn encode_by_ref(
                &self,
                buf: &mut <MySql as Database>::ArgumentBuffer<'r>,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                self.0.resolve().encode_by_ref(buf)
            }
        }
        impl<'r, $($generic)?> sqlx::Decode<'r, MySql> for $ty {
            fn decode(value: MySqlValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
                Ok(Self::from_symbol(Symbol::decode(value)?))
            }
        }
    };
}
use sqlx_forward_symbol;

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
    pub invitation_code: InvitationCode,
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
    pub code: CouponCode,
    pub discount: i32,
    pub created_at: DateTime<Utc>,
    pub used_by: Option<Id<Ride>>,
}
