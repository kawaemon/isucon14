use std::{collections::HashMap, sync::Arc};

use chrono::Utc;
use tokio::sync::RwLock;

use crate::models::{Id, Owner};

use super::{cache_init::CacheInit, Repository, Result, Tx};

pub type OwnerCache = Arc<OwnerCacheInner>;
type SharedOwner = Arc<Owner>;

#[derive(Debug)]
pub struct OwnerCacheInner {
    by_id: Arc<RwLock<HashMap<Id<Owner>, SharedOwner>>>,
    by_token: Arc<RwLock<HashMap<String, SharedOwner>>>,
}

impl OwnerCacheInner {
    async fn push(&self, u: Owner) {
        let s = Arc::new(u.clone());

        let mut id = self.by_id.write().await;
        let mut t = self.by_token.write().await;
        id.insert(u.id, Arc::clone(&s));
        t.insert(u.access_token, Arc::clone(&s));
    }
}

struct OwnerCacheInit {
    by_id: HashMap<Id<Owner>, SharedOwner>,
    by_token: HashMap<String, SharedOwner>,
}
impl OwnerCacheInit {
    fn from_init(init: &mut CacheInit) -> Self {
        let mut id = HashMap::new();
        let mut t = HashMap::new();
        for owner in &init.owners {
            let owner = Arc::new(owner.clone());
            id.insert(owner.id.clone(), Arc::clone(&owner));
            t.insert(owner.access_token.clone(), Arc::clone(&owner));
        }
        Self {
            by_id: id,
            by_token: t,
        }
    }
}

impl Repository {
    pub(super) fn init_owner_cache(init: &mut CacheInit) -> OwnerCache {
        let init = OwnerCacheInit::from_init(init);
        Arc::new(OwnerCacheInner {
            by_id: Arc::new(RwLock::new(init.by_id)),
            by_token: Arc::new(RwLock::new(init.by_token)),
        })
    }
    pub(super) async fn reinit_owner_cache(&self, init: &mut CacheInit) {
        let init = OwnerCacheInit::from_init(init);

        let OwnerCacheInner { by_id, by_token } = &*self.owner_cache;
        let mut id = by_id.write().await;
        let mut t = by_token.write().await;

        *id = init.by_id;
        *t = init.by_token;
    }
}

impl Repository {
    pub async fn owner_get_by_access_token(&self, token: &str) -> Result<Option<Owner>> {
        let cache = self.owner_cache.by_token.read().await;
        let Some(entry) = cache.get(token) else {
            return Ok(None);
        };
        Ok(Some(Owner::clone(entry)))
    }
    pub async fn owner_get_by_id(
        &self,
        _tx: impl Into<Option<&mut Tx>>,
        id: &Id<Owner>,
    ) -> Result<Option<Owner>> {
        let cache = self.owner_cache.by_id.read().await;
        let Some(entry) = cache.get(id) else {
            return Ok(None);
        };
        Ok(Some(Owner::clone(entry)))
    }

    // write

    pub async fn owner_add(
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
            created_at: now,
            updated_at: now,
        };

        self.owner_cache.push(o).await;

        sqlx::query(
            "INSERT INTO owners (id, name, access_token, chair_register_token, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(name)
        .bind(token)
        .bind(chair_reg_token)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
