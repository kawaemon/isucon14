mod cache_init;
mod chair;
mod location;
mod owner;
mod payment_token;
mod pgw;
mod ride;
mod user;

use cache_init::CacheInit;
use chair::ChairCache;
use location::ChairLocationCache;
use owner::OwnerCache;
use payment_token::PtCache;
use pgw::PgwCache;
use ride::RideCache;
use sqlx::{MySql, Pool, Transaction};
use user::UserCache;

use crate::Error;

macro_rules! maybe_tx {
    ($self:expr, $tx:expr, $query:ident.$method:ident) => {{
        if let Some(tx) = $tx.as_mut() {
            let tx: &mut Tx = *tx;
            $query.$method(&mut **tx).await
        } else {
            $query.$method(&$self.pool).await
        }
    }};
}
use maybe_tx;

pub type Tx = Transaction<'static, MySql>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Repository {
    pool: Pool<MySql>,

    user_cache: UserCache,
    owner_cache: OwnerCache,
    chair_cache: ChairCache,
    chair_location_cache: ChairLocationCache,
    ride_cache: RideCache,
    pgw_cache: PgwCache,
    pt_cache: PtCache,
}

impl Repository {
    pub async fn new(pool: &Pool<MySql>) -> Self {
        let mut init = CacheInit::load(pool).await;

        Self {
            pool: pool.clone(),

            user_cache: Self::init_user_cache(&mut init),
            owner_cache: Self::init_owner_cache(&mut init),
            chair_cache: Self::init_chair_cache(&mut init),
            ride_cache: Self::init_ride_cache(pool, &mut init).await,
            chair_location_cache: Self::init_chair_location_cache(pool, &mut init).await,
            pgw_cache: Self::init_pgw_cache(pool).await,
            pt_cache: Self::init_pt_cache(&mut init),
        }
    }
}
