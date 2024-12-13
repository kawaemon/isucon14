use std::sync::Arc;

use sqlx::{MySql, Pool};

use super::{cache_init::CacheInit, Repository};

#[allow(clippy::module_inception)]
mod ride;
mod status;

// 椅子が今目的（pickup, destination)についたかどうか
// 椅子に対する通知
// 椅子がpickupに向かっていることの通知
// 椅子が運んでいることの通知

pub type RideCache = Arc<RideCacheInner>;

#[derive(Debug)]
pub struct RideCacheInner {}

impl Repository {
    pub(super) async fn init_ride_cache(_pool: &Pool<MySql>, _init: &mut CacheInit) -> RideCache {
        Arc::new(RideCacheInner {})
    }
}
