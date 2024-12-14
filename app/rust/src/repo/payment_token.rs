use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use crate::models::{Id, User};

use super::{cache_init::CacheInit, Repository, Result, Tx};

pub type PtCache = Arc<RwLock<HashMap<Id<User>, String>>>;
pub type PtCacheInit = HashMap<Id<User>, String>;

fn init(init: &mut CacheInit) -> PtCacheInit {
    let mut res = HashMap::new();
    for t in &init.pt {
        res.insert(t.user_id.clone(), t.token.clone());
    }
    res
}

impl Repository {
    pub fn init_pt_cache(i: &mut CacheInit) -> PtCache {
        Arc::new(RwLock::new(init(i)))
    }
    pub async fn reinit_pt_cache(&self, i: &mut CacheInit) {
        *self.pt_cache.write().await = init(i);
    }
}

impl Repository {
    pub async fn payment_token_get(
        &self,
        _tx: impl Into<Option<&mut Tx>>,
        user: &Id<User>,
    ) -> Result<Option<String>> {
        let cache = self.pt_cache.read().await;
        Ok(cache.get(user).cloned())
    }

    pub async fn payment_token_add(&self, user: &Id<User>, token: &str) -> Result<()> {
        self.pt_cache
            .write()
            .await
            .insert(user.clone(), token.to_owned());
        sqlx::query("INSERT INTO payment_tokens (user_id, token) VALUES (?, ?)")
            .bind(user)
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
