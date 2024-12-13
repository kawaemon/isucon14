use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use chrono::Utc;

use crate::{
    app_handlers::ChairStats,
    models::{Chair, Id, Owner, Ride, RideStatus, RideStatusEnum},
};

use super::{cache_init::CacheInit, maybe_tx, Repository, Result, Tx};

pub type ChairCache = Arc<ChairCacheInner>;

type SharedChair = Arc<RwLock<Chair>>;

#[derive(Debug)]
pub struct ChairCacheInner {
    by_id: RwLock<HashMap<Id<Chair>, SharedChair>>,
    by_access_token: RwLock<HashMap<String, SharedChair>>,
    by_owner: RwLock<HashMap<Id<Owner>, Vec<SharedChair>>>,
}

impl ChairCacheInner {
    async fn push_chair(&self, c: Chair) {
        let shared = Arc::new(RwLock::new(c.clone()));
        let mut id = self.by_id.write().await;
        let mut ac = self.by_access_token.write().await;
        let mut ow = self.by_owner.write().await;

        id.insert(c.id, Arc::clone(&shared));
        ac.insert(c.access_token, Arc::clone(&shared));
        ow.entry(c.owner_id)
            .or_insert_with(Vec::new)
            .push(Arc::clone(&shared));
    }
}

impl Repository {
    pub(super) async fn init_chair_cache(init: &mut CacheInit) -> ChairCache {
        let mut bid = HashMap::new();
        let mut ac = HashMap::new();
        let mut owner = HashMap::new();
        for chair in &init.chairs {
            let c = Arc::new(RwLock::new(chair.clone()));
            bid.insert(chair.id.clone(), Arc::clone(&c));
            ac.insert(chair.access_token.clone(), Arc::clone(&c));
            owner
                .entry(chair.owner_id.clone())
                .or_insert_with(Vec::new)
                .push(Arc::clone(&c));
        }

        ChairCache::new(ChairCacheInner {
            by_id: RwLock::new(bid),
            by_access_token: RwLock::new(ac),
            by_owner: RwLock::new(owner),
        })
    }
}

// chairs
impl Repository {
    pub async fn chair_get_by_id(
        &self,
        _tx: impl Into<Option<&mut Tx>>,
        id: &Id<Chair>,
    ) -> Result<Option<Chair>> {
        let cache = self.chair_cache.by_id.read().await;
        let Some(entry) = cache.get(id) else {
            return Ok(None);
        };
        let entry = entry.read().await.clone();
        Ok(Some(entry))
    }

    pub async fn chair_get_by_access_token(&self, token: &str) -> Result<Option<Chair>> {
        let cache = self.chair_cache.by_access_token.read().await;
        let Some(entry) = cache.get(token) else {
            return Ok(None);
        };
        let entry = entry.read().await.clone();
        Ok(Some(entry))
    }

    pub async fn chair_get_by_owner(&self, owner: &Id<Owner>) -> Result<Vec<Chair>> {
        let cache = self.chair_cache.by_owner.read().await;
        let Some(entry) = cache.get(owner) else {
            return Ok(vec![]);
        };
        let mut res = vec![];
        for e in entry {
            res.push(e.read().await.clone());
        }
        Ok(res)
    }

    pub async fn chair_get_stats(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        id: &Id<Chair>,
    ) -> Result<ChairStats> {
        let mut tx = tx.into();

        let q = sqlx::query_as("SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC")
            .bind(id);
        let rides: Vec<Ride> = maybe_tx!(self, tx, q.fetch_all)?;

        let mut total_ride_count = 0;
        let mut total_evaluation = 0.0;
        for ride in rides {
            let q =
                sqlx::query_as("SELECT * FROM ride_statuses WHERE ride_id = ? ORDER BY created_at")
                    .bind(&ride.id);

            let ride_statuses: Vec<RideStatus> = maybe_tx!(self, tx, q.fetch_all)?;

            if !ride_statuses
                .iter()
                .any(|status| status.status == RideStatusEnum::Arrived)
            {
                continue;
            }
            if !ride_statuses
                .iter()
                .any(|status| (status.status == RideStatusEnum::Carrying))
            {
                continue;
            }
            let is_completed = ride_statuses
                .iter()
                .any(|status| status.status == RideStatusEnum::Completed);
            if !is_completed {
                continue;
            }

            total_ride_count += 1;
            total_evaluation += ride.evaluation.unwrap() as f64;
        }

        let total_evaluation_avg = if total_ride_count > 0 {
            total_evaluation / total_ride_count as f64
        } else {
            0.0
        };

        Ok(ChairStats {
            total_rides_count: total_ride_count,
            total_evaluation_avg,
        })
    }

    /// latest が completed になっていればよい
    pub async fn chair_get_completeds(&self, tx: impl Into<Option<&mut Tx>>) -> Result<Vec<Chair>> {
        let mut tx = tx.into();

        let q = sqlx::query_as("SELECT * FROM chairs");
        let chairs: Vec<Chair> = maybe_tx!(self, tx, q.fetch_all)?;

        let mut res = vec![];
        'chair: for chair in chairs {
            if !chair.is_active {
                continue;
            }

            let q =
                sqlx::query_as("SELECT * FROM rides WHERE chair_id = ? ORDER BY created_at DESC")
                    .bind(&chair.id);

            let rides: Vec<Ride> = maybe_tx!(self, tx, q.fetch_all)?;

            for ride in rides {
                let status = self.ride_status_latest(tx.as_deref_mut(), &ride.id).await?;
                if status != RideStatusEnum::Completed {
                    continue 'chair;
                }
            }

            res.push(chair);
        }

        Ok(res)
    }

    // writes

    pub async fn chair_add(
        &self,
        id: &Id<Chair>,
        owner: &Id<Owner>,
        name: &str,
        model: &str,
        is_active: bool,
        access_token: &str,
    ) -> Result<()> {
        let at = Utc::now();

        self.chair_cache
            .push_chair(Chair {
                id: id.clone(),
                owner_id: owner.clone(),
                name: name.to_owned(),
                access_token: access_token.to_owned(),
                model: model.to_owned(),
                is_active,
                created_at: at,
                updated_at: at,
            })
            .await;

        sqlx::query("INSERT INTO chairs (id, owner_id, name, model, is_active, access_token, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id)
            .bind(owner)
            .bind(name)
            .bind(model)
            .bind(is_active)
            .bind(access_token)
            .bind(at)
            .bind(at)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn chair_update_is_active(&self, id: &Id<Chair>, active: bool) -> Result<()> {
        {
            let mut cache = self.chair_cache.by_id.write().await;
            let mut entry = cache.get_mut(id).unwrap().write().await;
            entry.is_active = active;
        }

        sqlx::query("UPDATE chairs SET is_active = ? WHERE id = ?")
            .bind(active)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
