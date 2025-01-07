use crate::{dl::DlSyncRwLock, models::Symbol};
use reqwest::Url;
use sqlx::{MySql, Pool};
use std::sync::Arc;

use super::{Repository, Result};

pub type PgwCache = Arc<DlSyncRwLock<Url>>;

fn prepare_url(s: &str) -> Url {
    let mut url = Url::parse(s).unwrap();
    url.set_path("payments");
    url
}

async fn init(pool: &Pool<MySql>) -> Url {
    let s: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE name = 'payment_gateway_url'")
            .fetch_one(pool)
            .await
            .unwrap();
    prepare_url(&s)
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
    pub async fn pgw_set(&self, s: Symbol) -> Result<()> {
        sqlx::query("UPDATE settings SET value = ? WHERE name = 'payment_gateway_url'")
            .bind(s)
            .execute(&self.pool)
            .await?;
        *self.pgw_cache.write() = prepare_url(s.resolve());
        Ok(())
    }

    pub fn pgw_get(&self) -> Result<Url> {
        Ok(self.pgw_cache.read().clone())
    }
}
