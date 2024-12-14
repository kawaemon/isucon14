use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use chrono::Utc;

use crate::{
    app_handlers::ChairStats,
    models::{Chair, Id, Owner},
};

use super::{cache_init::CacheInit, maybe_tx, Repository, Result, Tx};

pub type ChairCache = Arc<ChairCacheInner>;

type SharedChair = Arc<RwLock<Chair>>;

#[derive(Debug, Clone)]
struct ChairStat {
    total_evaluation: i32,
    total_rides: i32,
}
impl ChairStat {
    fn new() -> Self {
        Self {
            total_evaluation: 0,
            total_rides: 0,
        }
    }
    fn update(&mut self, eval: i32) {
        self.total_evaluation += eval;
        self.total_rides += 1;
    }
}

#[derive(Debug)]
pub struct ChairCacheInner {
    by_id: RwLock<HashMap<Id<Chair>, SharedChair>>,
    by_access_token: RwLock<HashMap<String, SharedChair>>,
    by_owner: RwLock<HashMap<Id<Owner>, Vec<SharedChair>>>,

    stats: RwLock<HashMap<Id<Chair>, ChairStat>>,
}

impl ChairCacheInner {
    pub async fn push_chair(&self, c: Chair) {
        let shared = Arc::new(RwLock::new(c.clone()));
        let mut id = self.by_id.write().await;
        let mut ac = self.by_access_token.write().await;
        let mut ow = self.by_owner.write().await;
        let mut st = self.stats.write().await;

        id.insert(c.id.clone(), Arc::clone(&shared));
        ac.insert(c.access_token, Arc::clone(&shared));
        ow.entry(c.owner_id)
            .or_insert_with(Vec::new)
            .push(Arc::clone(&shared));
        st.insert(c.id, ChairStat::new());
    }

    pub async fn on_eval(&self, chair_id: &Id<Chair>, eval: i32) {
        let mut cache = self.stats.write().await;
        cache.get_mut(chair_id).unwrap().update(eval);
    }
}

impl Repository {
    pub(super) fn init_chair_cache(init: &mut CacheInit) -> ChairCache {
        let mut bid = HashMap::new();
        let mut ac = HashMap::new();
        let mut owner = HashMap::new();
        let mut stats = HashMap::new();
        for chair in &init.chairs {
            let c = Arc::new(RwLock::new(chair.clone()));
            bid.insert(chair.id.clone(), Arc::clone(&c));
            ac.insert(chair.access_token.clone(), Arc::clone(&c));
            owner
                .entry(chair.owner_id.clone())
                .or_insert_with(Vec::new)
                .push(Arc::clone(&c));
            stats.insert(chair.id.clone(), ChairStat::new());
        }

        for s in &init.rides {
            if let Some(eval) = s.evaluation.as_ref() {
                let chair_id = s.chair_id.as_ref().unwrap();
                let stat = stats.get_mut(chair_id).unwrap();
                stat.update(*eval);
            }
        }

        ChairCache::new(ChairCacheInner {
            by_id: RwLock::new(bid),
            by_access_token: RwLock::new(ac),
            by_owner: RwLock::new(owner),
            stats: RwLock::new(stats),
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

    // COMPLETED なものを集める(1)
    pub async fn chair_get_stats(
        &self,
        _tx: impl Into<Option<&mut Tx>>,
        id: &Id<Chair>,
    ) -> Result<ChairStats> {
        let stat: ChairStat = {
            let cache = self.chair_cache.stats.read().await;
            cache.get(id).unwrap().clone()
        };

        let total_evaluation_avg = {
            if stat.total_rides > 0 {
                stat.total_evaluation as f64 / stat.total_rides as f64
            } else {
                0.0
            }
        };

        Ok(ChairStats {
            total_rides_count: stat.total_rides as i32,
            total_evaluation_avg,
        })
    }

    /// latest が completed になっていればよい
    pub async fn chair_get_completeds(&self, tx: impl Into<Option<&mut Tx>>) -> Result<Vec<Chair>> {
        let mut tx = tx.into();

        let q = sqlx::query_as("SELECT * FROM chairs");
        let chairs: Vec<Chair> = maybe_tx!(self, tx, q.fetch_all)?;

        let mut res = vec![];
        for chair in chairs {
            if !chair.is_active {
                continue;
            }

            let q = sqlx::query_scalar(
                "SELECT count(*) = 0 FROM rides WHERE chair_id = ? and evaluation is null",
            )
            .bind(&chair.id);

            let ok: bool = maybe_tx!(self, tx, q.fetch_one)?;
            if ok {
                res.push(chair);
            }
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
        self.ride_cache.on_chair_add(id).await;

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
        let now = Utc::now();
        {
            let mut cache = self.chair_cache.by_id.write().await;
            let mut entry = cache.get_mut(id).unwrap().write().await;
            entry.is_active = active;
            entry.updated_at = now;
        }

        sqlx::query("UPDATE chairs SET is_active = ?, updated_at = ? WHERE id = ?")
            .bind(active)
            .bind(now)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
