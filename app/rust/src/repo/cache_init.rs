use sqlx::{MySql, Pool};

use crate::models::{Chair, ChairLocation};

pub struct CacheInit {
    pub chairs: Vec<Chair>,
    pub locations: Vec<ChairLocation>,
}

impl CacheInit {
    pub async fn load(pool: &Pool<MySql>) -> Self {
        Self {
            chairs: sqlx::query_as("select * from chairs")
                .fetch_all(pool)
                .await
                .unwrap(),
            locations: sqlx::query_as("select * from chair_locations")
                .fetch_all(pool)
                .await
                .unwrap(),
        }
    }
}
