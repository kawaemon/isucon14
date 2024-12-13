use sqlx::{MySql, Pool};

use crate::models::{Chair, ChairLocation, Owner, User};

pub struct CacheInit {
    pub users: Vec<User>,
    pub owners: Vec<Owner>,
    pub chairs: Vec<Chair>,
    pub locations: Vec<ChairLocation>,
}

impl CacheInit {
    pub async fn load(pool: &Pool<MySql>) -> Self {
        Self {
            users: sqlx::query_as("select * from users")
                .fetch_all(pool)
                .await
                .unwrap(),
            owners: sqlx::query_as("select * from owners")
                .fetch_all(pool)
                .await
                .unwrap(),
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
