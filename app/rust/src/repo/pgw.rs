use sqlx::{MySql, Pool};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::{Repository, Result, Tx};

pub type PgwCache = Arc<RwLock<String>>;

async fn init(pool: &Pool<MySql>) -> String {
    sqlx::query_scalar("SELECT value FROM settings WHERE name = 'payment_gateway_url'")
        .fetch_one(pool)
        .await
        .unwrap()
}

impl Repository {
    pub(super) async fn init_pgw_cache(pool: &Pool<MySql>) -> PgwCache {
        Arc::new(RwLock::new(init(pool).await))
    }
    pub(super) async fn reinit_pgw_cache(&self, pool: &Pool<MySql>) {
        *self.pgw_cache.write().await = init(pool).await;
    }
}

impl Repository {
    pub async fn pgw_set(&self, s: &str) -> Result<()> {
        *self.pgw_cache.write().await = s.to_owned();
        Ok(())
    }

    pub async fn pgw_get(&self, _tx: impl Into<Option<&mut Tx>>) -> Result<String> {
        Ok(self.pgw_cache.read().await.clone())
    }
}
