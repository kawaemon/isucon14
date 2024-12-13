use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use crate::models::{Id, User};

use super::{cache_init::CacheInit, maybe_tx, Repository, Result, Tx};

pub type PtCache = Arc<RwLock<HashMap<Id<User>, String>>>;

impl Repository {
    pub fn init_pt_cache(init: &mut CacheInit) -> PtCache {
        let mut res = HashMap::new();
        for t in &init.pt {
            res.insert(t.user_id.clone(), t.token.clone());
        }
        Arc::new(RwLock::new(res))
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
