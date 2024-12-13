mod cache_init;
mod chair;
mod location;
mod owner;
mod payment_token;
mod pgw;
mod user;

use cache_init::CacheInit;
use chair::ChairCache;
use chrono::{DateTime, Utc};
use location::ChairLocationCache;
use owner::OwnerCache;
use payment_token::PtCache;
use pgw::PgwCache;
use sqlx::{MySql, Pool, Transaction};
use user::UserCache;

use crate::{
    models::{Chair, Id, Ride, RideStatus, RideStatusEnum, User},
    Coordinate, Error,
};

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
            chair_location_cache: Self::init_chair_location_cache(pool, &mut init).await,
            pgw_cache: Self::init_pgw_cache(pool).await,
            pt_cache: Self::init_pt_cache(&mut init),
        }
    }
}

// rides
impl Repository {
    pub async fn rides_user_ongoing(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        user: &Id<User>,
    ) -> Result<bool> {
        let mut tx = tx.into();

        let q = sqlx::query_as("SELECT * FROM rides WHERE user_id = ?").bind(user);

        let rides: Vec<Ride> = maybe_tx!(self, tx, q.fetch_all)?;

        for ride in rides {
            let status = self.ride_status_latest(tx.as_deref_mut(), &ride.id).await?;
            if status != RideStatusEnum::Completed {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub async fn rides_waiting_for_match(&self) -> Result<Vec<Ride>> {
        let t = sqlx::query_as("select * from rides where chair_id is null order by created_at")
            .fetch_all(&self.pool)
            .await?;
        Ok(t)
    }

    pub async fn rides_get_assigned(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        chair_id: &Id<Chair>,
    ) -> Result<Option<(Ride, RideStatusEnum)>> {
        let mut tx = tx.into();
        let q = sqlx::query_as(
            "SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(chair_id);

        let Some(ride): Option<Ride> = maybe_tx!(self, tx, q.fetch_optional)? else {
            return Ok(None);
        };

        let status = self.ride_status_latest(tx, &ride.id).await?;
        Ok(Some((ride, status)))
    }

    // writes

    pub async fn rides_new(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        id: &Id<Ride>,
        user: &Id<User>,
        pickup: Coordinate,
        dest: Coordinate,
    ) -> Result<()> {
        let mut tx = tx.into();

        let q = sqlx::query("INSERT INTO rides (id, user_id, pickup_latitude, pickup_longitude, destination_latitude, destination_longitude) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(id)
            .bind(user)
            .bind(pickup.latitude)
            .bind(pickup.longitude)
            .bind(dest.latitude)
            .bind(dest.longitude);

        maybe_tx!(self, tx, q.execute)?;
        Ok(())
    }

    pub async fn rides_assign(&self, ride_id: &Id<Ride>, chair_id: &Id<Chair>) -> Result<()> {
        sqlx::query("update rides set chair_id = ? where id = ?")
            .bind(chair_id)
            .bind(ride_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn rides_set_evaluation(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        id: &Id<Ride>,
        eval: i32,
    ) -> Result<DateTime<Utc>> {
        let now = Utc::now();
        let mut tx = tx.into();

        let q = sqlx::query("UPDATE rides SET evaluation = ?, updated_at = ? WHERE id = ?")
            .bind(eval)
            .bind(now)
            .bind(id);

        maybe_tx!(self, tx, q.execute)?;

        Ok(now)
    }
}

// ride_status
impl Repository {
    pub async fn ride_status_latest(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        ride_id: &Id<Ride>,
    ) -> Result<RideStatusEnum> {
        let mut tx = tx.into();
        let q = sqlx::query_scalar(
            "SELECT status FROM ride_statuses WHERE ride_id = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(ride_id);

        let s = maybe_tx!(self, tx, q.fetch_one)?;

        Ok(s)
    }

    // writes

    pub async fn ride_status_update(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        ride_id: &Id<Ride>,
        status: RideStatusEnum,
    ) -> Result<()> {
        let mut tx = tx.into();
        let q = sqlx::query("INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)")
            .bind(Id::<RideStatus>::new())
            .bind(ride_id)
            .bind(status);

        maybe_tx!(self, tx, q.execute)?;

        Ok(())
    }

    pub async fn ride_status_chair_notified(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        status_id: &Id<RideStatus>,
    ) -> Result<()> {
        let mut tx = tx.into();
        let q = sqlx::query(
            "UPDATE ride_statuses SET chair_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?",
        )
        .bind(status_id);

        maybe_tx!(self, tx, q.execute)?;

        Ok(())
    }

    pub async fn ride_status_app_notified(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        status_id: &Id<RideStatus>,
    ) -> Result<()> {
        let mut tx = tx.into();
        let q =
            sqlx::query("UPDATE ride_statuses SET app_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?")
                .bind(status_id);

        maybe_tx!(self, tx, q.execute)?;

        Ok(())
    }
}
