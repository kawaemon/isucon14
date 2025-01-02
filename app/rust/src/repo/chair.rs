use crate::dl::DlSyncRwLock;
use crate::models::Symbol;
use crate::{AtomicDateTime, HashMap};
use std::sync::atomic::AtomicBool;
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

#[derive(Debug, Clone)]
pub struct EffortlessChair {
    pub id: Id<Chair>,
    pub owner_id: Id<Owner>,
    pub name: Symbol,
    pub access_token: Symbol,
    pub model: Symbol,
    pub created_at: DateTime<Utc>,
}

type SharedChair = Arc<ChairEntry>;
#[derive(Debug)]
pub struct ChairEntry {
    pub id: Id<Chair>,
    pub owner_id: Id<Owner>,
    pub name: Symbol,
    pub access_token: Symbol,
    pub model: Symbol,
    pub created_at: DateTime<Utc>,

    pub is_active: AtomicBool,
    pub updated_at: AtomicDateTime,

    pub stat: DlSyncRwLock<ChairStat>,
}
impl ChairEntry {
    pub fn new(c: Chair) -> Self {
        ChairEntry {
            id: c.id,
            owner_id: c.owner_id,
            name: c.name,
            access_token: c.access_token,
            model: c.model,
            is_active: AtomicBool::new(c.is_active),
            created_at: c.created_at,
            updated_at: AtomicDateTime::new(c.updated_at),
            stat: DlSyncRwLock::new(ChairStat::new()),
        }
    }
    pub fn chair(&self) -> Chair {
        Chair {
            id: self.id,
            owner_id: self.owner_id,
            name: self.name,
            access_token: self.access_token,
            model: self.model,
            is_active: self.is_active.load(std::sync::atomic::Ordering::Relaxed),
            created_at: self.created_at,
            updated_at: self.updated_at.load(),
        }
    }
    pub fn chair_effortless(&self) -> EffortlessChair {
        EffortlessChair {
            id: self.id,
            owner_id: self.owner_id,
            name: self.name,
            access_token: self.access_token,
            model: self.model,
            created_at: self.created_at,
        }
    }
    pub fn set_active(&self, is_active: bool, now: DateTime<Utc>) {
        self.is_active
            .store(is_active, std::sync::atomic::Ordering::Relaxed);
        self.updated_at.store(now);
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
    pub by_id: DlSyncRwLock<HashMap<Id<Chair>, SharedChair>>,
    by_access_token: DlSyncRwLock<HashMap<Symbol, SharedChair>>,
    by_owner: DlSyncRwLock<HashMap<Id<Owner>, Vec<SharedChair>>>,

    deferred: UpdatableDeferred<ChairDeferrable>,
}

pub struct ChairSalesStat {
    pub id: Id<Chair>,
    pub name: Symbol,
    pub model: Symbol,
    pub sales: i32,
}

impl Repository {
    pub fn chair_sale_stats_by_owner(
        &self,
        owner: Id<Owner>,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<ChairSalesStat>> {
        let cache = self.chair_cache.by_owner.read();
        let cache = cache.get(&owner).unwrap();
        let mut res = vec![];
        for c in cache.iter() {
            let sales = c.stat.read().get_sales(since, until);
            res.push(ChairSalesStat {
                id: c.id,
                name: c.name,
                model: c.model,
                sales,
            });
        }
        Ok(res)
    }
}

impl ChairCacheInner {
    pub fn push_chair(&self, c: Chair) {
        let shared = Arc::new(ChairEntry::new(c.clone()));
        {
            let mut id = self.by_id.write();
            id.insert(c.id, Arc::clone(&shared));
        }
        {
            let mut ac = self.by_access_token.write();
            ac.insert(c.access_token, Arc::clone(&shared));
        }
        {
            let mut ow = self.by_owner.write();
            ow.entry(c.owner_id).or_default().push(Arc::clone(&shared));
        }
    }

    pub fn on_eval(&self, chair_id: Id<Chair>, eval: i32, sales: i32, at: DateTime<Utc>) {
        let cache = self.by_id.read();
        let chair = cache.get(&chair_id).unwrap();
        let mut stat = chair.stat.write();
        stat.update(eval, sales, at);
    }
}

struct ChairCacheInit {
    by_id: HashMap<Id<Chair>, SharedChair>,
    by_access_token: HashMap<Symbol, SharedChair>,
    by_owner: HashMap<Id<Owner>, Vec<SharedChair>>,
}
impl ChairCacheInit {
    fn from_init(init: &mut CacheInit) -> Self {
        let mut bid = HashMap::default();
        let mut ac = HashMap::default();
        let mut owner = HashMap::default();
        for chair in &init.chairs {
            let c = Arc::new(ChairEntry::new(chair.clone()));
            bid.insert(chair.id, Arc::clone(&c));
            ac.insert(chair.access_token, Arc::clone(&c));
            owner
                .entry(chair.owner_id)
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
    pub(super) fn init_chair_cache(pool: &Pool<MySql>, init: &mut CacheInit) -> ChairCache {
        let init = ChairCacheInit::from_init(init);

        ChairCache::new(ChairCacheInner {
            by_id: DlSyncRwLock::new(init.by_id),
            by_access_token: DlSyncRwLock::new(init.by_access_token),
            by_owner: DlSyncRwLock::new(init.by_owner),
            deferred: UpdatableDeferred::new(pool),
        })
    }
    pub(super) fn reinit_chair_cache(&self, init: &mut CacheInit) {
        let init = ChairCacheInit::from_init(init);

        let ChairCacheInner {
            by_id,
            by_access_token,
            by_owner,
            deferred: _,
        } = &*self.chair_cache;
        let mut id = by_id.write();
        let mut ac = by_access_token.write();
        let mut ow = by_owner.write();

        *id = init.by_id;
        *ac = init.by_access_token;
        *ow = init.by_owner;
    }
}

// chairs
impl Repository {
    pub fn chair_get_by_id_effortless(&self, id: Id<Chair>) -> Result<Option<EffortlessChair>> {
        let cache = self.chair_cache.by_id.read();
        let Some(entry) = cache.get(&id) else {
            return Ok(None);
        };
        Ok(Some(entry.chair_effortless()))
    }

    pub fn chair_get_by_access_token(&self, token: Symbol) -> Result<Option<EffortlessChair>> {
        let cache = self.chair_cache.by_access_token.read();
        let Some(entry) = cache.get(&token) else {
            return Ok(None);
        };
        Ok(Some(entry.chair_effortless()))
    }

    pub fn chair_get_by_owner(&self, owner: Id<Owner>) -> Result<Vec<Chair>> {
        let cache = self.chair_cache.by_owner.read();
        let Some(entry) = cache.get(&owner) else {
            return Ok(vec![]);
        };
        Ok(entry.iter().map(|x| x.chair()).collect())
    }

    // COMPLETED なものを集める(1)
    pub fn chair_get_stats(&self, id: Id<Chair>) -> Result<ChairStats> {
        let stat: ChairStat = {
            let cache = self.chair_cache.by_id.read();
            let chair = cache.get(&id).unwrap();
            let s: ChairStat = chair.stat.read().clone();
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
            total_rides_count: stat.total_rides,
            total_evaluation_avg,
        })
    }

    // writes

    pub fn chair_add(
        &self,
        id: Id<Chair>,
        owner: Id<Owner>,
        name: Symbol,
        model: Symbol,
        is_active: bool,
        access_token: Symbol,
    ) -> Result<()> {
        let at = Utc::now();
        let c = Chair {
            id,
            owner_id: owner,
            name: name.to_owned(),
            access_token: access_token.to_owned(),
            model: model.to_owned(),
            is_active,
            created_at: at,
            updated_at: at,
        };
        self.chair_cache.push_chair(c.clone());
        self.chair_cache.deferred.insert(c);
        self.ride_cache.on_chair_add(id);

        Ok(())
    }

    pub fn chair_update_is_active(&self, id: Id<Chair>, active: bool) -> Result<()> {
        let now = Utc::now();
        {
            let cache = self.chair_cache.by_id.read();
            let entry = cache.get(&id).unwrap();
            entry.set_active(active, now);
        }

        if active {
            self.ride_cache.on_chair_status_change(id, false);
            self.push_free_chair(id);
        }

        self.chair_cache.deferred.update(ChairUpdate {
            id,
            active,
            at: now,
        });

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
            .map(|x| (x.id, x))
            .collect::<HashMap<_, _>>();
        let mut new_updates = HashMap::default();

        for u in updates {
            let Some(i) = inserts.get_mut(&u.id) else {
                let n = new_updates.entry(u.id).or_insert_with(|| ChairUpdate {
                    id: u.id,
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
            b.push_bind(i.id)
                .push_bind(i.owner_id)
                .push_bind(i.name)
                .push_bind(i.model)
                .push_bind(i.is_active)
                .push_bind(i.access_token)
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
            .bind(update.id)
            .execute(&mut **tx)
            .await
            .unwrap();
    }
}
