use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::Mutex;

use crate::models::{Id, Ride, RideStatus, RideStatusEnum};

pub struct RideStatusInsert {
    id: Id<RideStatus>,
    ride_id: Id<Ride>,
    status: RideStatusEnum,
    created_at: DateTime<Utc>,
}

pub enum NotifiedType {
    App,
    Chair,
}

pub struct RideStatusUpdate {
    ty: NotifiedType,
    status_id: Id<RideStatus>,
    at: DateTime<Utc>,
}

struct ChangeSet {
    inserts: Mutex<Vec<RideStatusInsert>>,
    updates: Mutex<Vec<RideStatusUpdate>>,
}

pub struct Deferred {
    set: Arc<ChangeSet>,
}

impl Deferred {}
