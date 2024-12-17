use crate::FxHashMap as HashMap;
use std::sync::Arc;

use crate::repo::dl::DlRwLock as RwLock;
use chrono::Utc;

use crate::models::{Id, User};

use super::{cache_init::CacheInit, Repository, Result};

pub type UserCache = Arc<UserCacheInner>;
type SharedUser = Arc<User>;

#[derive(Debug)]
pub struct UserCacheInner {
    by_id: Arc<RwLock<HashMap<Id<User>, SharedUser>>>,
    by_token: Arc<RwLock<HashMap<String, SharedUser>>>,
    by_inv_code: Arc<RwLock<HashMap<String, SharedUser>>>,
}

impl UserCacheInner {
    async fn push(&self, u: User) {
        let s = Arc::new(u.clone());

        let mut id = self.by_id.write().await;
        let mut t = self.by_token.write().await;
        let mut inv = self.by_inv_code.write().await;
        id.insert(u.id, Arc::clone(&s));
        t.insert(u.access_token, Arc::clone(&s));
        inv.insert(u.invitation_code, Arc::clone(&s));
    }
}

pub struct UserCacheInit {
    by_id: HashMap<Id<User>, SharedUser>,
    by_token: HashMap<String, SharedUser>,
    by_inv_code: HashMap<String, SharedUser>,
}
impl UserCacheInit {
    fn from_init(init: &mut CacheInit) -> Self {
        let mut id = HashMap::default();
        let mut t = HashMap::default();
        let mut inv = HashMap::default();
        for user in &init.users {
            let user = Arc::new(user.clone());
            id.insert(user.id.clone(), Arc::clone(&user));
            t.insert(user.access_token.clone(), Arc::clone(&user));
            inv.insert(user.invitation_code.clone(), Arc::clone(&user));
        }
        Self {
            by_id: id,
            by_token: t,
            by_inv_code: inv,
        }
    }
}

impl Repository {
    pub(super) fn init_user_cache(init: &mut CacheInit) -> UserCache {
        let init = UserCacheInit::from_init(init);

        Arc::new(UserCacheInner {
            by_id: Arc::new(RwLock::new(init.by_id)),
            by_token: Arc::new(RwLock::new(init.by_token)),
            by_inv_code: Arc::new(RwLock::new(init.by_inv_code)),
        })
    }
    pub(super) async fn reinit_user_cache(&self, init: &mut CacheInit) {
        let init = UserCacheInit::from_init(init);

        let UserCacheInner {
            by_id,
            by_token,
            by_inv_code,
        } = &*self.user_cache;
        let mut id = by_id.write().await;
        let mut t = by_token.write().await;
        let mut inv = by_inv_code.write().await;

        *id = init.by_id;
        *t = init.by_token;
        *inv = init.by_inv_code;
    }
}

impl Repository {
    pub async fn user_get_by_access_token(&self, token: &str) -> Result<Option<User>> {
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
    pub async fn user_get_by_inv_code(&self, code: &str) -> Result<Option<User>> {
        let cache = self.user_cache.by_inv_code.read().await;
        let Some(entry) = cache.get(code) else {
            return Ok(None);
        };
        Ok(Some(User::clone(entry)))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn user_add(
        &self,
        id: &Id<User>,
        username: &str,
        first: &str,
        last: &str,
        dob: &str,
        token: &str,
        inv_code: &str,
    ) -> Result<()> {
        let now = Utc::now();

        let u = User {
            id: id.clone(),
            username: username.to_owned(),
            firstname: first.to_owned(),
            lastname: last.to_owned(),
            date_of_birth: dob.to_owned(),
            access_token: token.to_owned(),
            invitation_code: inv_code.to_owned(),
            created_at: now,
            updated_at: now,
        };
        self.user_cache.push(u).await;
        self.ride_cache.on_user_add(id).await;

        sqlx::query("INSERT INTO users (id, username, firstname, lastname, date_of_birth, access_token, invitation_code, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id)
            .bind(username)
            .bind(first)
            .bind(last)
            .bind(dob)
            .bind(token)
            .bind(inv_code)
            .bind(now)
            .bind(now)
            .execute(&self.pool).await?;
        Ok(())
    }
}
