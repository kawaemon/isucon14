use chrono::{DateTime, Utc};
use sqlx::{MySql, Pool, QueryBuilder};

use crate::dl::DlSyncRwLock;
use crate::{ConcurrentHashMap, ConcurrentSymbolMap, HashMap};
use std::sync::Arc;

use crate::models::{Coupon, CouponCode, Id, Ride, User};

use super::{
    cache_init::CacheInit,
    deferred::{DeferrableMayUpdated, UpdatableDeferred},
    Repository, Result,
};

pub type CouponCache = Arc<CouponCacheInner>;

#[derive(Debug)]
struct CouponEntry {
    user_id: Id<User>,
    code: CouponCode,
    discount: i32,
    created_at: DateTime<Utc>,
    used_by: DlSyncRwLock<Option<Id<Ride>>>,
}
impl CouponEntry {
    fn new(c: &Coupon) -> Self {
        Self {
            user_id: c.user_id,
            code: c.code,
            discount: c.discount,
            created_at: c.created_at,
            used_by: DlSyncRwLock::new(c.used_by),
        }
    }
    fn coupon(&self) -> Coupon {
        Coupon {
            user_id: self.user_id,
            code: self.code,
            discount: self.discount,
            created_at: self.created_at,
            used_by: *self.used_by.read(),
        }
    }
}

type SharedCoupon = Arc<CouponEntry>;

#[derive(Debug)]
pub struct CouponCacheInner {
    by_code: DlSyncRwLock<ConcurrentSymbolMap<CouponCode, DlSyncRwLock<Vec<SharedCoupon>>>>,
    by_usedby: DlSyncRwLock<ConcurrentSymbolMap<Id<Ride>, SharedCoupon>>,
    user_queue: DlSyncRwLock<ConcurrentSymbolMap<Id<User>, DlSyncRwLock<Vec<SharedCoupon>>>>,

    deferred: UpdatableDeferred<DeferrableCoupons>,
}

struct Init {
    by_code: ConcurrentSymbolMap<CouponCode, DlSyncRwLock<Vec<SharedCoupon>>>,
    by_usedby: ConcurrentSymbolMap<Id<Ride>, SharedCoupon>,

    user_queue: ConcurrentSymbolMap<Id<User>, DlSyncRwLock<Vec<SharedCoupon>>>,
}
impl Init {
    fn from_init(init: &mut CacheInit) -> Self {
        init.coupon.sort_unstable_by_key(|x| x.created_at);

        let all = ConcurrentHashMap::default();
        let code = ConcurrentSymbolMap::default();
        let usedby = ConcurrentSymbolMap::default();

        for coupon in &init.coupon {
            let e = Arc::new(CouponEntry::new(coupon));
            all.insert((coupon.user_id, coupon.code), Arc::clone(&e));

            code.entry(coupon.code)
                .or_insert_with(|| DlSyncRwLock::new(Vec::new()))
                .write()
                .push(Arc::clone(&e));

            if let Some(u) = coupon.used_by {
                usedby.insert(u, Arc::clone(&e));
            }
        }

        let user_queue = ConcurrentSymbolMap::default();
        for coupon in &init.coupon {
            if coupon.used_by.is_some() {
                continue;
            }
            let e = all.get(&(coupon.user_id, coupon.code)).unwrap();
            user_queue
                .entry(coupon.user_id)
                .or_insert_with(|| DlSyncRwLock::new(Vec::new()))
                .write()
                .push(Arc::clone(&*e));
        }

        Self {
            by_code: code,
            by_usedby: usedby,
            user_queue,
        }
    }
}

impl Repository {
    pub(super) fn init_coupon_cache(pool: &Pool<MySql>, init: &mut CacheInit) -> CouponCache {
        let init = Init::from_init(init);
        let cache = CouponCacheInner {
            by_code: DlSyncRwLock::new(init.by_code),
            by_usedby: DlSyncRwLock::new(init.by_usedby),
            user_queue: DlSyncRwLock::new(init.user_queue),
            deferred: UpdatableDeferred::new(pool),
        };
        Arc::new(cache)
    }
    pub(super) fn reinit_coupon_cache(&self, init: &mut CacheInit) {
        let init = Init::from_init(init);

        let CouponCacheInner {
            by_code,
            by_usedby,
            user_queue,
            deferred: _,
        } = &*self.coupon_cache;
        let mut c = by_code.write();
        let mut u = by_usedby.write();
        let mut q = user_queue.write();

        *c = init.by_code;
        *u = init.by_usedby;
        *q = init.user_queue;
    }
}

impl Repository {
    pub fn coupon_get_count_by_code(&self, code: CouponCode) -> Result<usize> {
        let cache = self.coupon_cache.by_code.read();
        let Some(c) = cache.get(&code) else {
            return Ok(0);
        };
        let c = c.read();
        Ok(c.len())
    }
    pub fn coupon_get_by_usedby(&self, ride: Id<Ride>) -> Result<Option<Coupon>> {
        let cache = self.coupon_cache.by_usedby.read();
        let Some(c) = cache.get(&ride) else {
            return Ok(None);
        };
        Ok(Some(c.coupon()))
    }
    pub fn coupon_get_unused_order_by_created_at(&self, user_id: Id<User>) -> Result<Vec<Coupon>> {
        let cache = self.coupon_cache.user_queue.read();
        let Some(c) = cache.get(&user_id) else {
            return Ok(vec![]);
        };
        let c = c.read();
        Ok(c.iter().map(|x| x.coupon()).collect())
    }

    // writes

    pub fn coupon_add(&self, user: Id<User>, code: CouponCode, amount: i32) -> Result<()> {
        let c = Coupon {
            user_id: user,
            code: code.to_owned(),
            discount: amount,
            created_at: Utc::now(),
            used_by: None,
        };
        let e = Arc::new(CouponEntry::new(&c));
        {
            let code_cache = self.coupon_cache.by_code.read();
            code_cache
                .entry(code.to_owned())
                .or_insert_with(|| DlSyncRwLock::new(Vec::new()))
                .write()
                .push(Arc::clone(&e));
        }
        {
            let user_cache = self.coupon_cache.user_queue.read();
            user_cache
                .entry(user)
                .or_insert_with(|| DlSyncRwLock::new(Vec::new()))
                .write()
                .push(Arc::clone(&e));
        }

        self.coupon_cache.deferred.insert(c);
        Ok(())
    }

    pub fn coupon_set_used(&self, user: Id<User>, code: CouponCode, ride: Id<Ride>) -> Result<()> {
        {
            let user_cache = self.coupon_cache.user_queue.read();
            let user_cache = user_cache.get(&user).unwrap();

            let entry = {
                let mut user_cache = user_cache.write();
                let pos = user_cache.iter().position(|x| x.code == code).unwrap();
                user_cache.remove(pos)
            };

            let cache = self.coupon_cache.by_usedby.read();
            let res = cache.insert(ride, entry);
            assert!(res.is_none());
        }

        self.coupon_cache.deferred.update(CouponUpdate {
            user_id: user,
            code,
            used_by: ride,
        });
        Ok(())
    }
}

#[derive(Debug)]
struct CouponUpdate {
    user_id: Id<User>,
    code: CouponCode,
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
            .map(|x| ((x.user_id, x.code), x))
            .collect::<HashMap<_, _>>();
        let mut new_updates: Vec<CouponUpdate> = vec![];

        for u in updates {
            let Some(i) = inserts.get_mut(&(u.user_id, u.code)) else {
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
            b.push_bind(i.user_id)
                .push_bind(i.code.as_symbol())
                .push_bind(i.discount)
                .push_bind(i.created_at)
                .push_bind(i.user_id);
        });

        builder.build().execute(&mut **tx).await.unwrap();
    }

    async fn exec_update(
        tx: &mut sqlx::Transaction<'static, sqlx::MySql>,
        update: &Self::UpdateQuery,
    ) {
        sqlx::query("UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = ?")
            .bind(update.user_id)
            .bind(update.user_id)
            .bind(update.code.as_symbol())
            .execute(&mut **tx)
            .await
            .unwrap();
    }
}
