use crate::repo::dl::DlRwLock as RwLock;
use crate::FxHashMap as HashMap;
use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use sqlx::{MySql, Pool, QueryBuilder};

use crate::{
    app_handlers::ChairStats,
    models::{Chair, Id, Owner},
};

use super::{
    cache_init::CacheInit,
    deferred::{DeferrableMayUpdated, UpdatableDeferred},
    Repository, Result,
};

pub type ChairCache = Arc<ChairCacheInner>;

type SharedChair = Arc<ChairEntry>;
#[derive(Debug)]
pub struct ChairEntry {
    pub id: Id<Chair>,
    pub owner_id: Id<Owner>,
    pub name: String,
    pub access_token: String,
    pub model: String,
    pub created_at: DateTime<Utc>,

    pub is_active: RwLock<bool>,
    pub updated_at: RwLock<DateTime<Utc>>,

    pub stat: RwLock<ChairStat>,
}
impl ChairEntry {
    pub fn new(c: Chair) -> Self {
        ChairEntry {
            id: c.id.clone(),
            owner_id: c.owner_id.clone(),
            name: c.name.clone(),
            access_token: c.access_token.clone(),
            model: c.model.clone(),
            is_active: RwLock::new(c.is_active),
            created_at: c.created_at,
            updated_at: RwLock::new(c.updated_at),
            stat: RwLock::new(ChairStat::new()),
        }
    }
    pub async fn chair(&self) -> Chair {
        Chair {
            id: self.id.clone(),
            owner_id: self.owner_id.clone(),
            name: self.name.clone(),
            access_token: self.access_token.clone(),
            model: self.model.clone(),
            is_active: *self.is_active.read().await,
            created_at: self.created_at,
            updated_at: *self.updated_at.read().await,
        }
    }
    pub async fn set_active(&self, is_active: bool, now: DateTime<Utc>) {
        let mut a = self.is_active.write().await;
        let mut u = self.updated_at.write().await;
        *a = is_active;
        *u = now;
    }
}

#[derive(Debug, Clone)]
pub struct ChairStat {
    total_evaluation: i32,
    total_rides: i32,
    sales: BTreeMap<DateTime<Utc>, i32>,
}
impl ChairStat {
    fn new() -> Self {
        Self {
            total_evaluation: 0,
            total_rides: 0,
            sales: BTreeMap::new(),
        }
    }
    fn update(&mut self, eval: i32, sales: i32, at: DateTime<Utc>) {
        self.total_evaluation += eval;
        self.total_rides += 1;
        // 累積和を使えないことはないがライド数が大したことなさそうなのでこのままでいいや
        // let last = self.sales.last_entry().map(|x| *x.get()).unwrap_or(0);
        // self.sales.insert(at, last + sales);
        self.sales.insert(at, sales);
    }
    fn get_sales(&self, since: DateTime<Utc>, mut until: DateTime<Utc>) -> i32 {
        until += Duration::microseconds(999);
        self.sales.range(since..=until).map(|x| *x.1).sum()
    }
}

#[derive(Debug)]
pub struct ChairCacheInner {
    by_id: RwLock<HashMap<Id<Chair>, SharedChair>>,
    by_access_token: RwLock<HashMap<String, SharedChair>>,
    by_owner: RwLock<HashMap<Id<Owner>, Vec<SharedChair>>>,

    deferred: UpdatableDeferred<ChairDeferrable>,
}

pub struct ChairSalesStat {
    pub id: Id<Chair>,
    pub name: String,
    pub model: String,
    pub sales: i32,
}

impl Repository {
    pub async fn chair_sale_stats_by_owner(
        &self,
        owner: &Id<Owner>,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<ChairSalesStat>> {
        let cache = self.chair_cache.by_owner.read().await;
        let cache = cache.get(owner).unwrap();
        let mut res = vec![];
        for c in cache.iter() {
            let sales = c.stat.read().await.get_sales(since, until);
            res.push(ChairSalesStat {
                id: c.id.clone(),
                name: c.name.clone(),
                model: c.model.clone(),
                sales,
            });
        }
        Ok(res)
    }
}

impl ChairCacheInner {
    pub async fn push_chair(&self, c: Chair) {
        let shared = Arc::new(ChairEntry::new(c.clone()));
        let mut id = self.by_id.write().await;
        let mut ac = self.by_access_token.write().await;
        let mut ow = self.by_owner.write().await;

        id.insert(c.id.clone(), Arc::clone(&shared));
        ac.insert(c.access_token, Arc::clone(&shared));
        ow.entry(c.owner_id)
            .or_insert_with(Vec::new)
            .push(Arc::clone(&shared));
    }

    pub async fn on_eval(&self, chair_id: &Id<Chair>, eval: i32, sales: i32, at: DateTime<Utc>) {
        let cache = self.by_id.read().await;
        let chair = cache.get(chair_id).unwrap();
        let mut stat = chair.stat.write().await;
        stat.update(eval, sales, at);
    }
}

struct ChairCacheInit {
    by_id: HashMap<Id<Chair>, SharedChair>,
    by_access_token: HashMap<String, SharedChair>,
    by_owner: HashMap<Id<Owner>, Vec<SharedChair>>,
}
impl ChairCacheInit {
    async fn from_init(init: &mut CacheInit) -> Self {
        let mut bid = HashMap::default();
        let mut ac = HashMap::default();
        let mut owner = HashMap::default();
        for chair in &init.chairs {
            let c = Arc::new(ChairEntry::new(chair.clone()));
            bid.insert(chair.id.clone(), Arc::clone(&c));
            ac.insert(chair.access_token.clone(), Arc::clone(&c));
            owner
                .entry(chair.owner_id.clone())
                .or_insert_with(Vec::new)
                .push(Arc::clone(&c));
        }

        for s in &init.rides {
            if let Some(eval) = s.evaluation.as_ref() {
                let chair_id = s.chair_id.as_ref().unwrap();
                let chair = bid.get(chair_id).unwrap();
                chair
                    .stat
                    .write()
                    .await
                    .update(*eval, s.calc_sale(), s.updated_at);
            }
        }

        Self {
            by_id: bid,
            by_access_token: ac,
            by_owner: owner,
        }
    }
}

impl Repository {
    pub(super) async fn init_chair_cache(pool: &Pool<MySql>, init: &mut CacheInit) -> ChairCache {
        let init = ChairCacheInit::from_init(init).await;

        ChairCache::new(ChairCacheInner {
            by_id: RwLock::new(init.by_id),
            by_access_token: RwLock::new(init.by_access_token),
            by_owner: RwLock::new(init.by_owner),
            deferred: UpdatableDeferred::new(pool),
        })
    }
    pub(super) async fn reinit_chair_cache(&self, init: &mut CacheInit) {
        let init = ChairCacheInit::from_init(init).await;

        let ChairCacheInner {
            by_id,
            by_access_token,
            by_owner,
            deferred: _,
        } = &*self.chair_cache;
        let mut id = by_id.write().await;
        let mut ac = by_access_token.write().await;
        let mut ow = by_owner.write().await;

        *id = init.by_id;
        *ac = init.by_access_token;
        *ow = init.by_owner;
    }
}

// chairs
impl Repository {
    pub async fn chair_get_by_id(&self, id: &Id<Chair>) -> Result<Option<Chair>> {
        let cache = self.chair_cache.by_id.read().await;
        let Some(entry) = cache.get(id) else {
            return Ok(None);
        };
        Ok(Some(entry.chair().await))
    }

    pub async fn chair_get_by_access_token(&self, token: &str) -> Result<Option<Chair>> {
        let cache = self.chair_cache.by_access_token.read().await;
        let Some(entry) = cache.get(token) else {
            return Ok(None);
        };
        Ok(Some(entry.chair().await))
    }

    pub async fn chair_get_by_owner(&self, owner: &Id<Owner>) -> Result<Vec<Chair>> {
        let cache = self.chair_cache.by_owner.read().await;
        let Some(entry) = cache.get(owner) else {
            return Ok(vec![]);
        };
        let mut res = vec![];
        for e in entry {
            res.push(e.chair().await);
        }
        Ok(res)
    }

    // COMPLETED なものを集める(1)
    pub async fn chair_get_stats(&self, id: &Id<Chair>) -> Result<ChairStats> {
        let stat: ChairStat = {
            let cache = self.chair_cache.by_id.read().await;
            let chair = cache.get(id).unwrap();
            let s: ChairStat = chair.stat.read().await.clone();
            s
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
        let c = Chair {
            id: id.clone(),
            owner_id: owner.clone(),
            name: name.to_owned(),
            access_token: access_token.to_owned(),
            model: model.to_owned(),
            is_active,
            created_at: at,
            updated_at: at,
        };
        self.chair_cache.push_chair(c.clone()).await;
        self.chair_cache.deferred.insert(c).await;
        self.ride_cache.on_chair_add(id).await;

        Ok(())
    }

    pub async fn chair_update_is_active(&self, id: &Id<Chair>, active: bool) -> Result<()> {
        let now = Utc::now();
        {
            let cache = self.chair_cache.by_id.read().await;
            let entry = cache.get(id).unwrap();
            entry.set_active(active, now).await;

            if active {
                self.ride_cache.on_chair_status_change(id, false).await;
                self.push_free_chair(id).await;
            }
        }

        self.chair_cache
            .deferred
            .update(ChairUpdate {
                id: id.clone(),
                active,
                at: now,
            })
            .await;

        Ok(())
    }
}

#[derive(Debug)]
struct ChairUpdate {
    id: Id<Chair>,
    active: bool,
    at: DateTime<Utc>,
}

struct ChairDeferrable;
impl DeferrableMayUpdated for ChairDeferrable {
    const NAME: &str = "chairs";

    type Insert = Chair;
    type Update = ChairUpdate;
    type UpdateQuery = ChairUpdate;

    fn summarize(
        inserts: &mut [Self::Insert],
        updates: Vec<Self::Update>,
    ) -> Vec<Self::UpdateQuery> {
        let mut inserts = inserts
            .iter_mut()
            .map(|x| (x.id.clone(), x))
            .collect::<HashMap<_, _>>();
        let mut new_updates = HashMap::default();

        for u in updates {
            let Some(i) = inserts.get_mut(&u.id) else {
                let n = new_updates
                    .entry(u.id.clone())
                    .or_insert_with(|| ChairUpdate {
                        id: u.id.clone(),
                        active: u.active,
                        at: u.at,
                    });
                n.active = u.active;
                n.at = u.at;
                continue;
            };
            i.is_active = u.active;
            i.updated_at = u.at;
        }

        new_updates.into_values().collect()
    }

    async fn exec_insert(
        tx: &mut sqlx::Transaction<'static, sqlx::MySql>,
        inserts: &[Self::Insert],
    ) {
        let mut builder = QueryBuilder::new(
            "insert into chairs
                (id, owner_id, name, model, is_active, access_token, created_at, updated_at) ",
        );
        builder.push_values(inserts, |mut b, i| {
            b.push_bind(&i.id)
                .push_bind(&i.owner_id)
                .push_bind(&i.name)
                .push_bind(&i.model)
                .push_bind(i.is_active)
                .push_bind(&i.access_token)
                .push_bind(i.created_at)
                .push_bind(i.updated_at);
        });
        builder.build().execute(&mut **tx).await.unwrap();
    }

    async fn exec_update(
        tx: &mut sqlx::Transaction<'static, sqlx::MySql>,
        update: &Self::UpdateQuery,
    ) {
        sqlx::query("UPDATE chairs SET is_active = ?, updated_at = ? WHERE id = ?")
            .bind(update.active)
            .bind(update.at)
            .bind(&update.id)
            .execute(&mut **tx)
            .await
            .unwrap();
    }
}
