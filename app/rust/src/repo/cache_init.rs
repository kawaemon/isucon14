use sqlx::{MySql, Pool};

use crate::models::{Chair, ChairLocation, Coupon, Owner, PaymentToken, Ride, RideStatus, User};

pub struct CacheInit {
    pub users: Vec<User>,
    pub owners: Vec<Owner>,
    pub chairs: Vec<Chair>,
    pub rides: Vec<Ride>,
    pub ride_statuses: Vec<RideStatus>,
    pub locations: Vec<ChairLocation>,
    pub pt: Vec<PaymentToken>,
    pub coupon: Vec<Coupon>,
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
            rides: sqlx::query_as("select * from rides")
                .fetch_all(pool)
                .await
                .unwrap(),
            ride_statuses: sqlx::query_as("select * from ride_statuses")
                .fetch_all(pool)
                .await
                .unwrap(),
            locations: sqlx::query_as("select * from chair_locations")
                .fetch_all(pool)
                .await
                .unwrap(),
            pt: sqlx::query_as("select * from payment_tokens")
                .fetch_all(pool)
                .await
                .unwrap(),
            coupon: sqlx::query_as("select * from coupons")
                .fetch_all(pool)
                .await
                .unwrap(),
        }
    }
}
