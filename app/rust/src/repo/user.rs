use std::{collections::HashMap, sync::Arc};

use chrono::Utc;
use tokio::sync::RwLock;

use crate::models::{Id, User};

use super::{cache_init::CacheInit, maybe_tx, Repository, Result, Tx};

pub type UserCache = Arc<UserCacheInner>;
type SharedUser = Arc<User>;

#[derive(Debug)]
pub struct UserCacheInner {
    by_id: Arc<RwLock<HashMap<Id<User>, SharedUser>>>,
    by_token: Arc<RwLock<HashMap<String, SharedUser>>>,
}

impl UserCacheInner {
    async fn push(&self, u: User) {
        let s = Arc::new(u.clone());

        let mut id = self.by_id.write().await;
        let mut t = self.by_token.write().await;
        id.insert(u.id, Arc::clone(&s));
        t.insert(u.access_token, Arc::clone(&s));
    }
}

impl Repository {
    pub(super) async fn init_user_cache(init: &mut CacheInit) -> UserCache {
        let mut id = HashMap::new();
        let mut t = HashMap::new();
        for user in &init.users {
            let user = Arc::new(user.clone());
            id.insert(user.id.clone(), Arc::clone(&user));
            t.insert(user.access_token.clone(), Arc::clone(&user));
        }
        Arc::new(UserCacheInner {
            by_id: Arc::new(RwLock::new(id)),
            by_token: Arc::new(RwLock::new(t)),
        })
    }
}

impl Repository {
    pub async fn user_get_by_acess_token(&self, token: &str) -> Result<Option<User>> {
        let cache = self.user_cache.by_token.read().await;
        let Some(entry) = cache.get(token) else {
            return Ok(None);
        };
        Ok(Some(User::clone(entry)))
    }
    pub async fn user_get_by_id(&self, id: &Id<User>) -> Result<Option<User>> {
        let cache = self.user_cache.by_id.read().await;
        let Some(entry) = cache.get(id) else {
            return Ok(None);
        };
        Ok(Some(User::clone(entry)))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn user_add(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        id: &Id<User>,
        username: &str,
        first: &str,
        last: &str,
        dob: &str,
        token: &str,
        inv_code: &str,
    ) -> Result<()> {
        let mut tx = tx.into();
        let now = Utc::now();

        let u = User {
            id: id.clone(),
            username: username.to_owned(),
            firstname: first.to_owned(),
            lastname: last.to_owned(),
            date_of_birth: dob.to_owned(),
            access_token: token.to_owned(),
            created_at: now,
            updated_at: now,
        };
        self.user_cache.push(u).await;

        let q = sqlx::query("INSERT INTO users (id, username, firstname, lastname, date_of_birth, access_token, invitation_code, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id)
            .bind(username)
            .bind(first)
            .bind(last)
            .bind(dob)
            .bind(token)
            .bind(inv_code)
            .bind(now)
            .bind(now);
        maybe_tx!(self, tx, q.execute)?;
        Ok(())
    }
}
