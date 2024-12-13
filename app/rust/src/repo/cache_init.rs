use sqlx::{MySql, Pool};

use crate::models::ChairLocation;

pub struct CacheInit {
    pub locations: Vec<ChairLocation>,
}

impl CacheInit {
    pub async fn load(pool: &Pool<MySql>) -> Self {
        Self {
            locations: sqlx::query_as("select * from chair_locations")
                .fetch_all(pool)
                .await
                .unwrap(),
        }
    }
}
