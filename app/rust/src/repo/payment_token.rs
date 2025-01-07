use sqlx::{MySql, Pool, QueryBuilder, Transaction};

use crate::ConcurrentSymbolMap;
use crate::{dl::DlSyncRwLock, models::Symbol};
use std::sync::Arc;

use crate::models::{Id, User};

use super::{
    cache_init::CacheInit,
    deferred::{DeferrableSimple, SimpleDeferred},
    Repository, Result,
};

pub type PtCache = Arc<PtCacheInner>;

#[derive(Debug)]
pub struct PtCacheInner {
    cache: DlSyncRwLock<ConcurrentSymbolMap<Id<User>, Symbol>>,
    deferred: SimpleDeferred<PaymentTokenDeferrable>,
}

pub type PtCacheInit = ConcurrentSymbolMap<Id<User>, Symbol>;

fn init(init: &mut CacheInit) -> PtCacheInit {
    let res = ConcurrentSymbolMap::default();
    for t in &init.pt {
        res.insert(t.user_id, t.token);
    }
    res
}

impl Repository {
    pub fn init_pt_cache(i: &mut CacheInit, pool: &Pool<MySql>) -> PtCache {
        Arc::new(PtCacheInner {
            cache: DlSyncRwLock::new(init(i)),
            deferred: SimpleDeferred::new(pool),
        })
    }
    pub fn reinit_pt_cache(&self, i: &mut CacheInit) {
        *self.pt_cache.cache.write() = init(i);
    }
}

impl Repository {
    pub fn payment_token_get(&self, user: Id<User>) -> Result<Option<Symbol>> {
        let cache = self.pt_cache.cache.read();
        Ok(cache.get(&user).map(|x| *x))
    }

    pub fn payment_token_add(&self, user: Id<User>, token: Symbol) -> Result<()> {
        self.pt_cache.cache.write().insert(user, token.to_owned());
        self.pt_cache.deferred.insert(TokenInsert {
            id: user,
            token: token.to_owned(),
        });
        Ok(())
    }
}

#[derive(Debug)]
pub struct TokenInsert {
    id: Id<User>,
    token: Symbol,
}

pub struct PaymentTokenDeferrable;
impl DeferrableSimple for PaymentTokenDeferrable {
    const NAME: &str = "payment_tokens";

    type Insert = TokenInsert;

    async fn exec_insert(tx: &mut Transaction<'static, MySql>, inserts: &[Self::Insert]) {
        let mut builder = QueryBuilder::new("INSERT INTO payment_tokens (user_id, token) ");
        builder.push_values(inserts, |mut b, i| {
            b.push_bind(i.id).push_bind(i.token);
        });
        builder.build().execute(&mut **tx).await.unwrap();
    }
}
