use crate::dl::DlSyncRwLock;
use sqlx::{MySql, Pool};
use std::sync::Arc;

use super::{Repository, Result};

pub type PgwCache = Arc<DlSyncRwLock<String>>;

async fn init(pool: &Pool<MySql>) -> String {
    sqlx::query_scalar("SELECT value FROM settings WHERE name = 'payment_gateway_url'")
        .fetch_one(pool)
        .await
        .unwrap()
}

impl Repository {
    pub(super) async fn init_pgw_cache(pool: &Pool<MySql>) -> PgwCache {
        Arc::new(DlSyncRwLock::new(init(pool).await))
    }
    pub(super) async fn reinit_pgw_cache(&self, pool: &Pool<MySql>) {
        *self.pgw_cache.write() = init(pool).await;
    }
}

impl Repository {
    pub async fn pgw_set(&self, s: &str) -> Result<()> {
        *self.pgw_cache.write() = s.to_owned();
        sqlx::query("UPDATE settings SET value = ? WHERE name = 'payment_gateway_url'")
            .bind(s)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub fn pgw_get(&self) -> Result<String> {
        Ok(self.pgw_cache.read().clone())
    }
}
