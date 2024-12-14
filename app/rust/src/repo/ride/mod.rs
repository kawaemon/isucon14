use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use sqlx::{MySql, Pool};

use crate::models::{Id, Ride, RideStatusEnum};

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
pub struct RideCacheInner {
    latest_ride_stat: RwLock<HashMap<Id<Ride>, RideStatusEnum>>,
}

impl Repository {
    pub(super) async fn init_ride_cache(_pool: &Pool<MySql>, init: &mut CacheInit) -> RideCache {
        let mut latest_ride_stat = HashMap::new();

        init.ride_statuses.sort_unstable_by_key(|x| x.created_at);
        for stat in &init.ride_statuses {
            latest_ride_stat.insert(stat.ride_id.clone(), stat.status);
        }

        Arc::new(RideCacheInner {
            latest_ride_stat: RwLock::new(latest_ride_stat),
        })
    }
}
