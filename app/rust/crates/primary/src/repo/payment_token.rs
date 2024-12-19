use sqlx::{MySql, Pool, QueryBuilder};

use shared::DlRwLock as RwLock;
use shared::FxHashMap as HashMap;
use std::sync::Arc;

use crate::models::{Id, User};

use super::{cache_init::CacheInit, Repository, Result};
use shared::deferred::{DeferrableSimple, SimpleDeferred};

pub type PtCache = Arc<PtCacheInner>;

#[derive(Debug)]
pub struct PtCacheInner {
    cache: RwLock<HashMap<Id<User>, String>>,
    deferred: SimpleDeferred<PaymentTokenDeferrable>,
}

pub type PtCacheInit = HashMap<Id<User>, String>;

fn init(init: &mut CacheInit) -> PtCacheInit {
    let mut res = HashMap::default();
    for t in &init.pt {
        res.insert(t.user_id.clone(), t.token.clone());
    }
    res
}

impl Repository {
    pub fn init_pt_cache(i: &mut CacheInit, pool: &Pool<MySql>) -> PtCache {
        Arc::new(PtCacheInner {
            cache: RwLock::new(init(i)),
            deferred: SimpleDeferred::new(pool),
        })
    }
    pub async fn reinit_pt_cache(&self, i: &mut CacheInit) {
        *self.pt_cache.cache.write().await = init(i);
    }
}

impl Repository {
    pub async fn payment_token_get(&self, user: &Id<User>) -> Result<Option<String>> {
        let cache = self.pt_cache.cache.read().await;
        Ok(cache.get(user).cloned())
    }

    pub async fn payment_token_add(&self, user: &Id<User>, token: &str) -> Result<()> {
        self.pt_cache
            .cache
            .write()
            .await
            .insert(user.clone(), token.to_owned());
        self.pt_cache
            .deferred
            .insert(TokenInsert {
                id: user.clone(),
                token: token.to_owned(),
            })
            .await;
        Ok(())
    }
}

#[derive(Debug)]
pub struct TokenInsert {
    id: Id<User>,
    token: String,
}

pub struct PaymentTokenDeferrable;
impl DeferrableSimple for PaymentTokenDeferrable {
    const NAME: &str = "payment_tokens";

    type Insert = TokenInsert;

    async fn exec_insert(tx: &Pool<MySql>, inserts: &[Self::Insert]) {
        let mut builder = QueryBuilder::new("INSERT INTO payment_tokens (user_id, token) ");
        builder.push_values(inserts, |mut b, i| {
            b.push_bind(&i.id).push_bind(&i.token);
        });
        builder.build().execute(tx).await.unwrap();
    }
}
