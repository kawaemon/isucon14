pub mod cache_init;
pub mod chair;
pub mod coupon;
pub mod deferred;
pub mod model;
pub mod owner;
pub mod payment_token;
pub mod pgw;
pub mod ride;
pub mod user;

use cache_init::CacheInit;
use chair::ChairCache;
use coupon::CouponCache;
use model::ChairModelCache;
use owner::OwnerCache;
use payment_token::PtCache;
use pgw::PgwCache;
use ride::RideCache;
use sqlx::{MySql, Pool, Transaction};
use user::UserCache;

use crate::Error;

pub type Tx = Transaction<'static, MySql>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Repository {
    pool: Pool<MySql>,

    user_cache: UserCache,
    owner_cache: OwnerCache,
    chair_cache: ChairCache,
    chair_model_cache: ChairModelCache,
    pub ride_cache: RideCache,
    pgw_cache: PgwCache,
    pt_cache: PtCache,
    coupon_cache: CouponCache,
}

impl Repository {
    pub async fn new(pool: &Pool<MySql>) -> Self {
        let mut init = CacheInit::load(pool).await;
        let chair_cache = Self::init_chair_cache(pool, &mut init);

        let r = Self {
            pool: pool.clone(),

            user_cache: Self::init_user_cache(&mut init, pool),
            owner_cache: Self::init_owner_cache(&mut init, pool),
            ride_cache: Self::init_ride_cache(&mut init, pool, &chair_cache),
            chair_model_cache: Self::init_chair_model_cache(pool).await,
            chair_cache,
            pgw_cache: Self::init_pgw_cache(pool).await,
            pt_cache: Self::init_pt_cache(&mut init, pool),
            coupon_cache: Self::init_coupon_cache(pool, &mut init),
        };

        tracing::info!("cache initialized");

        r
    }

    pub async fn reinit(&self) {
        let mut init = CacheInit::load(&self.pool).await;

        self.reinit_user_cache(&mut init);
        self.reinit_owner_cache(&mut init);
        self.reinit_chair_cache(&mut init);
        self.reinit_ride_cache(&mut init, &self.chair_cache);
        self.reinit_chair_model_cache().await;
        self.reinit_pgw_cache(&self.pool).await;
        self.reinit_pt_cache(&mut init);
        self.reinit_coupon_cache(&mut init);

        tracing::info!("cache re-initialized");
    }
}
