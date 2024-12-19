use chrono::{DateTime, Utc};
use sqlx::{MySql, Pool, QueryBuilder};

use shared::DlRwLock as RwLock;
use shared::FxHashMap as HashMap;
use std::sync::Arc;

use crate::models::{Coupon, Id, Ride, User};

use super::{cache_init::CacheInit, Repository, Result};
use shared::deferred::{DeferrableMayUpdated, UpdatableDeferred};

pub type CouponCache = Arc<CouponCacheInner>;

#[derive(Debug)]
struct CouponEntry {
    user_id: Id<User>,
    code: String,
    discount: i32,
    created_at: DateTime<Utc>,
    used_by: RwLock<Option<Id<Ride>>>,
}
impl CouponEntry {
    fn new(c: &Coupon) -> Self {
        Self {
            user_id: c.user_id.clone(),
            code: c.code.clone(),
            discount: c.discount,
            created_at: c.created_at,
            used_by: RwLock::new(c.used_by.clone()),
        }
    }
    async fn coupon(&self) -> Coupon {
        Coupon {
            user_id: self.user_id.clone(),
            code: self.code.clone(),
            discount: self.discount,
            created_at: self.created_at,
            used_by: self.used_by.read().await.clone(),
        }
    }
}

type SharedCoupon = Arc<CouponEntry>;

#[derive(Debug)]
pub struct CouponCacheInner {
    by_code: RwLock<HashMap<String, RwLock<Vec<SharedCoupon>>>>,
    by_usedby: RwLock<HashMap<Id<Ride>, SharedCoupon>>,

    user_queue: RwLock<HashMap<Id<User>, RwLock<Vec<SharedCoupon>>>>,
    deferred: UpdatableDeferred<DeferrableCoupons>,
}

struct Init {
    by_code: HashMap<String, RwLock<Vec<SharedCoupon>>>,
    by_usedby: HashMap<Id<Ride>, SharedCoupon>,

    user_queue: HashMap<Id<User>, RwLock<Vec<SharedCoupon>>>,
}
impl Init {
    async fn from_init(init: &mut CacheInit) -> Self {
        init.coupon.sort_unstable_by_key(|x| x.created_at);

        let mut all = HashMap::default();
        let mut code = HashMap::default();
        let mut usedby = HashMap::default();

        for coupon in &init.coupon {
            let e = Arc::new(CouponEntry::new(coupon));
            all.insert(
                (coupon.user_id.clone(), coupon.code.clone()),
                Arc::clone(&e),
            );

            code.entry(coupon.code.clone())
                .or_insert_with(|| RwLock::new(Vec::new()))
                .write()
                .await
                .push(Arc::clone(&e));

            if let Some(u) = coupon.used_by.as_ref() {
                usedby.insert(u.clone(), Arc::clone(&e));
            }
        }

        let mut user_queue = HashMap::default();
        for coupon in &init.coupon {
            if coupon.used_by.is_some() {
                continue;
            }
            let e = all
                .get(&(coupon.user_id.clone(), coupon.code.clone()))
                .unwrap();
            user_queue
                .entry(coupon.user_id.clone())
                .or_insert_with(|| RwLock::new(Vec::new()))
                .write()
                .await
                .push(Arc::clone(e));
        }

        Self {
            by_code: code,
            by_usedby: usedby,
            user_queue,
        }
    }
}

impl Repository {
    pub(super) async fn init_coupon_cache(pool: &Pool<MySql>, init: &mut CacheInit) -> CouponCache {
        let init = Init::from_init(init).await;
        let cache = CouponCacheInner {
            by_code: RwLock::new(init.by_code),
            by_usedby: RwLock::new(init.by_usedby),
            user_queue: RwLock::new(init.user_queue),
            deferred: UpdatableDeferred::new(pool),
        };
        Arc::new(cache)
    }
    pub(super) async fn reinit_coupon_cache(&self, init: &mut CacheInit) {
        let init = Init::from_init(init).await;

        let CouponCacheInner {
            by_code,
            by_usedby,
            user_queue,
            deferred: _,
        } = &*self.coupon_cache;
        let mut c = by_code.write().await;
        let mut u = by_usedby.write().await;
        let mut q = user_queue.write().await;

        *c = init.by_code;
        *u = init.by_usedby;
        *q = init.user_queue;
    }
}

impl Repository {
    pub async fn coupon_get_count_by_code(&self, code: &str) -> Result<usize> {
        let cache = self.coupon_cache.by_code.read().await;
        let Some(c) = cache.get(code) else {
            return Ok(0);
        };
        let c = c.read().await;
        Ok(c.len())
    }
    pub async fn coupon_get_by_usedby(&self, ride: &Id<Ride>) -> Result<Option<Coupon>> {
        let cache = self.coupon_cache.by_usedby.read().await;
        let Some(c) = cache.get(ride) else {
            return Ok(None);
        };
        Ok(Some(c.coupon().await))
    }
    pub async fn coupon_get_unused_order_by_created_at(
        &self,
        user_id: &Id<User>,
    ) -> Result<Vec<Coupon>> {
        let cache = self.coupon_cache.user_queue.read().await;
        let Some(c) = cache.get(user_id) else {
            return Ok(vec![]);
        };
        let mut res = vec![];
        for c in c.read().await.iter() {
            res.push(c.coupon().await);
        }
        Ok(res)
    }

    // writes

    pub async fn coupon_add(&self, user: &Id<User>, code: &str, amount: i32) -> Result<()> {
        let c = Coupon {
            user_id: user.clone(),
            code: code.to_owned(),
            discount: amount,
            created_at: Utc::now(),
            used_by: None,
        };
        {
            let e = Arc::new(CouponEntry::new(&c));
            let mut code_cache = self.coupon_cache.by_code.write().await;
            let mut user_cache = self.coupon_cache.user_queue.write().await;
            code_cache
                .entry(code.to_owned())
                .or_insert_with(|| RwLock::new(Vec::new()))
                .write()
                .await
                .push(Arc::clone(&e));
            user_cache
                .entry(user.clone())
                .or_insert_with(|| RwLock::new(Vec::new()))
                .write()
                .await
                .push(Arc::clone(&e));
        }

        self.coupon_cache.deferred.insert(c).await;
        Ok(())
    }

    pub async fn coupon_set_used(
        &self,
        user: &Id<User>,
        code: &str,
        ride: &Id<Ride>,
    ) -> Result<()> {
        {
            let user_cache = self.coupon_cache.user_queue.read().await;
            let user_cache = user_cache.get(user).unwrap();
            let mut user_cache = user_cache.write().await;
            let pos = user_cache.iter().position(|x| x.code == code).unwrap();
            let entry = user_cache.remove(pos);

            let mut cache = self.coupon_cache.by_usedby.write().await;
            let res = cache.insert(ride.clone(), entry);
            assert!(res.is_none());
        }

        self.coupon_cache
            .deferred
            .update(CouponUpdate {
                user_id: user.clone(),
                code: code.to_owned(),
                used_by: ride.clone(),
            })
            .await;
        Ok(())
    }
}

#[derive(Debug)]
struct CouponUpdate {
    user_id: Id<User>,
    code: String,
    used_by: Id<Ride>,
}

struct DeferrableCoupons;
impl DeferrableMayUpdated for DeferrableCoupons {
    const NAME: &str = "coupons";

    type Insert = Coupon;
    type Update = CouponUpdate;
    type UpdateQuery = CouponUpdate;

    fn summarize(
        inserts: &mut [Self::Insert],
        updates: Vec<Self::Update>,
    ) -> Vec<Self::UpdateQuery> {
        let mut inserts = inserts
            .iter_mut()
            .map(|x| ((x.user_id.clone(), x.code.clone()), x))
            .collect::<HashMap<_, _>>();
        let mut new_updates: Vec<CouponUpdate> = vec![];

        for u in updates {
            let Some(i) = inserts.get_mut(&(u.user_id.clone(), u.code.clone())) else {
                new_updates.push(u);
                continue;
            };
            assert!(i.used_by.is_none());
            i.used_by = Some(u.used_by);
        }

        new_updates
    }

    async fn exec_insert(
        tx: &mut sqlx::Transaction<'static, sqlx::MySql>,
        inserts: &[Self::Insert],
    ) {
        let mut builder =
            QueryBuilder::new("insert into coupons(user_id, code, discount, created_at, used_by) ");

        builder.push_values(inserts, |mut b, i| {
            b.push_bind(&i.user_id)
                .push_bind(&i.code)
                .push_bind(i.discount)
                .push_bind(i.created_at)
                .push_bind(&i.user_id);
        });

        builder.build().execute(&mut **tx).await.unwrap();
    }

    async fn exec_update(
        tx: &mut sqlx::Transaction<'static, sqlx::MySql>,
        update: &Self::UpdateQuery,
    ) {
        sqlx::query("UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = ?")
            .bind(&update.user_id)
            .bind(&update.user_id)
            .bind(&update.code)
            .execute(&mut **tx)
            .await
            .unwrap();
    }
}
