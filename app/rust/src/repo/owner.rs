use crate::{dl::DlSyncRwLock, ConcurrentHashMap};
use std::sync::Arc;

use chrono::Utc;
use sqlx::{MySql, Pool, QueryBuilder};

use crate::models::{Id, Owner};

use super::{
    cache_init::CacheInit,
    deferred::{DeferrableSimple, SimpleDeferred},
    Repository, Result,
};

pub type OwnerCache = Arc<OwnerCacheInner>;
type SharedOwner = Arc<Owner>;

#[derive(Debug)]
pub struct OwnerCacheInner {
    by_id: Arc<DlSyncRwLock<ConcurrentHashMap<Id<Owner>, SharedOwner>>>,
    by_token: Arc<DlSyncRwLock<ConcurrentHashMap<String, SharedOwner>>>,
    by_crt: Arc<DlSyncRwLock<ConcurrentHashMap<String, SharedOwner>>>,
    deferred: SimpleDeferred<OwnerDeferrable>,
}

impl OwnerCacheInner {
    fn push(&self, u: Owner) {
        let s = Arc::new(u.clone());

        self.by_id.read().insert(u.id, Arc::clone(&s));
        self.by_token.read().insert(u.access_token, Arc::clone(&s));
        self.by_crt
            .read()
            .insert(u.chair_register_token, Arc::clone(&s));
    }
}

struct OwnerCacheInit {
    by_id: ConcurrentHashMap<Id<Owner>, SharedOwner>,
    by_token: ConcurrentHashMap<String, SharedOwner>,
    by_crt: ConcurrentHashMap<String, SharedOwner>,
}
impl OwnerCacheInit {
    fn from_init(init: &mut CacheInit) -> Self {
        let id = ConcurrentHashMap::default();
        let t = ConcurrentHashMap::default();
        let crt = ConcurrentHashMap::default();
        for owner in &init.owners {
            let owner = Arc::new(owner.clone());
            id.insert(owner.id.clone(), Arc::clone(&owner));
            t.insert(owner.access_token.clone(), Arc::clone(&owner));
            crt.insert(owner.chair_register_token.clone(), Arc::clone(&owner));
        }
        Self {
            by_id: id,
            by_token: t,
            by_crt: crt,
        }
    }
}

impl Repository {
    pub(super) fn init_owner_cache(init: &mut CacheInit, pool: &Pool<MySql>) -> OwnerCache {
        let init = OwnerCacheInit::from_init(init);
        Arc::new(OwnerCacheInner {
            by_id: Arc::new(DlSyncRwLock::new(init.by_id)),
            by_token: Arc::new(DlSyncRwLock::new(init.by_token)),
            by_crt: Arc::new(DlSyncRwLock::new(init.by_crt)),
            deferred: SimpleDeferred::new(pool),
        })
    }
    pub(super) fn reinit_owner_cache(&self, init: &mut CacheInit) {
        let init = OwnerCacheInit::from_init(init);

        let OwnerCacheInner {
            by_id,
            by_token,
            by_crt,
            deferred: _,
        } = &*self.owner_cache;
        let mut id = by_id.write();
        let mut t = by_token.write();
        let mut c = by_crt.write();

        *id = init.by_id;
        *t = init.by_token;
        *c = init.by_crt;
    }
}

impl Repository {
    pub fn owner_get_by_access_token(&self, token: &str) -> Result<Option<Owner>> {
        let cache = self.owner_cache.by_token.read();
        let Some(entry) = cache.get(token) else {
            return Ok(None);
        };
        Ok(Some(Owner::clone(&*entry)))
    }
    pub fn owner_get_by_id(&self, id: &Id<Owner>) -> Result<Option<Owner>> {
        let cache = self.owner_cache.by_id.read();
        let Some(entry) = cache.get(id) else {
            return Ok(None);
        };
        Ok(Some(Owner::clone(&*entry)))
    }
    pub fn owner_get_by_chair_register_token(&self, crt: &str) -> Result<Option<Owner>> {
        let cache = self.owner_cache.by_crt.read();
        let Some(entry) = cache.get(crt) else {
            return Ok(None);
        };
        Ok(Some(Owner::clone(&*entry)))
    }

    // write

    pub fn owner_add(
        &self,
        id: &Id<Owner>,
        name: &str,
        token: &str,
        chair_reg_token: &str,
    ) -> Result<()> {
        let now = Utc::now();
        let o = Owner {
            id: id.clone(),
            name: name.to_owned(),
            access_token: token.to_owned(),
            chair_register_token: chair_reg_token.to_owned(),
            created_at: now,
            updated_at: now,
        };

        self.owner_cache.push(o.clone());
        self.owner_cache.deferred.insert(o);

        Ok(())
    }
}

struct OwnerDeferrable;

impl DeferrableSimple for OwnerDeferrable {
    const NAME: &str = "owners";
    type Insert = Owner;

    async fn exec_insert(
        tx: &mut sqlx::Transaction<'static, sqlx::MySql>,
        inserts: &[Self::Insert],
    ) {
        let mut builder = QueryBuilder::new("
            INSERT INTO owners (id, name, access_token, chair_register_token, created_at, updated_at)
        ");

        builder.push_values(inserts, |mut b, i| {
            b.push_bind(&i.id)
                .push_bind(&i.name)
                .push_bind(&i.access_token)
                .push_bind(&i.chair_register_token)
                .push_bind(i.created_at)
                .push_bind(i.updated_at);
        });

        builder.build().execute(&mut **tx).await.unwrap();
    }
}
