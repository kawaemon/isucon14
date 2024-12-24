use crate::FxHashMap as HashMap;
use std::sync::Arc;

use sqlx::{MySql, Pool};

use crate::dl::DlRwLock as RwLock;

use super::Repository;

pub type ChairModelCache = Arc<Inner>;

#[derive(Debug)]
pub struct Inner {
    pub speed: RwLock<HashMap<String, i32>>,
}

#[derive(Debug)]
struct Init {
    cache: HashMap<String, i32>,
}
impl Init {
    async fn fetch(pool: &Pool<MySql>) -> Self {
        #[derive(sqlx::FromRow)]
        struct Query {
            name: String,
            speed: i32,
        }
        let res: Vec<Query> = sqlx::query_as("select * from chair_models")
            .fetch_all(pool)
            .await
            .unwrap();
        Self {
            cache: res.into_iter().map(|x| (x.name, x.speed)).collect(),
        }
    }
}

impl Repository {
    pub(super) async fn init_chair_model_cache(pool: &Pool<MySql>) -> ChairModelCache {
        let init = Init::fetch(pool).await;
        Arc::new(Inner {
            speed: RwLock::new(init.cache),
        })
    }
    pub(super) async fn reinit_chair_model_cache(&self) {
        let init = Init::fetch(&self.pool).await;
        *self.chair_model_cache.speed.write().await = init.cache;
    }
}
