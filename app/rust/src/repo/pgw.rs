use sqlx::{MySql, Pool};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::{Repository, Result, Tx};

pub type PgwCache = Arc<RwLock<String>>;

impl Repository {
    pub async fn init_pgw_cache(pool: &Pool<MySql>) -> PgwCache {
        let now: String =
            sqlx::query_scalar("SELECT value FROM settings WHERE name = 'payment_gateway_url'")
                .fetch_one(pool)
                .await
                .unwrap();
        Arc::new(RwLock::new(now))
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
