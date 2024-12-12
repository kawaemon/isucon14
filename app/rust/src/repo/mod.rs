mod location;

use chrono::{DateTime, Utc};
use sqlx::{MySql, Pool, Transaction};

use crate::{
    models::{Chair, ChairLocation, Id, Owner, Ride, RideStatus, RideStatusEnum, User},
    owner_handlers::MysqlDecimal,
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
}

impl Repository {
    pub fn new(pool: &Pool<MySql>) -> Self {
        Self { pool: pool.clone() }
    }
}

// users
impl Repository {
    pub async fn user_get_by_acess_token(&self, token: &str) -> Result<Option<User>> {
        let t = sqlx::query_as("SELECT * FROM users WHERE access_token = ?")
            .bind(token)
            .fetch_optional(&self.pool)
            .await?;
        Ok(t)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn user_add(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        id: &Id<User>,
        username: &str,
        first: &str,
        last: &str,
        dob: &str,
        token: &str,
        inv_code: &str,
    ) -> Result<()> {
        let mut tx = tx.into();
        let q = sqlx::query("INSERT INTO users (id, username, firstname, lastname, date_of_birth, access_token, invitation_code) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(id)
            .bind(username)
            .bind(first)
            .bind(last)
            .bind(dob)
            .bind(token)
            .bind(inv_code);
        maybe_tx!(self, tx, q.execute)?;
        Ok(())
    }
}

// payment_token
impl Repository {
    pub async fn payment_token_get(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        user: &Id<User>,
    ) -> Result<Option<String>> {
        let mut tx = tx.into();
        let q = sqlx::query_scalar("SELECT token FROM payment_tokens WHERE user_id = ?").bind(user);
        Ok(maybe_tx!(self, tx, q.fetch_optional)?)
    }

    pub async fn payment_token_add(&self, user: &Id<User>, token: &str) -> Result<()> {
        sqlx::query("INSERT INTO payment_tokens (user_id, token) VALUES (?, ?)")
            .bind(user)
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

// owners
impl Repository {
    pub async fn owner_get_by_acess_token(&self, token: &str) -> Result<Option<Owner>> {
        let t = sqlx::query_as("SELECT * FROM owners WHERE access_token = ?")
            .bind(token)
            .fetch_optional(&self.pool)
            .await?;
        Ok(t)
    }

    // write

    pub async fn owner_add(
        &self,
        id: &Id<Owner>,
        name: &str,
        token: &str,
        chair_reg_token: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO owners (id, name, access_token, chair_register_token) VALUES (?, ?, ?, ?)",
        )
        .bind(id)
        .bind(name)
        .bind(token)
        .bind(chair_reg_token)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

// chairs
impl Repository {
    pub async fn chair_get_by_acess_token(&self, token: &str) -> Result<Option<Chair>> {
        let t = sqlx::query_as("SELECT * FROM chairs WHERE access_token = ?")
            .bind(token)
            .fetch_optional(&self.pool)
            .await?;
        Ok(t)
    }

    pub async fn chair_get_by_owner(&self, owner: &Id<Owner>) -> Result<Vec<Chair>> {
        let t = sqlx::query_as("SELECT * FROM chairs WHERE owner_id = ?")
            .bind(owner)
            .fetch_all(&self.pool)
            .await?;
        Ok(t)
    }

    /// latest が completed になっていればよい
    pub async fn chair_get_completeds(&self, tx: impl Into<Option<&mut Tx>>) -> Result<Vec<Chair>> {
        let mut tx = tx.into();

        let q = sqlx::query_as("SELECT * FROM chairs");
        let chairs: Vec<Chair> = maybe_tx!(self, tx, q.fetch_all)?;

        let mut res = vec![];
        'chair: for chair in chairs {
            if !chair.is_active {
                continue;
            }

            let q =
                sqlx::query_as("SELECT * FROM rides WHERE chair_id = ? ORDER BY created_at DESC")
                    .bind(&chair.id);

            let rides: Vec<Ride> = maybe_tx!(self, tx, q.fetch_all)?;

            for ride in rides {
                let status = self.ride_status_latest(tx.as_deref_mut(), &ride.id).await?;
                if status != RideStatusEnum::Completed {
                    continue 'chair;
                }
            }

            res.push(chair);
        }

        Ok(res)
    }

    // writes

    pub async fn chair_add(
        &self,
        id: &Id<Chair>,
        owner: &Id<Owner>,
        name: &str,
        model: &str,
        is_active: bool,
        access_token: &str,
    ) -> Result<()> {
        sqlx::query("INSERT INTO chairs (id, owner_id, name, model, is_active, access_token) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(id)
            .bind(owner)
            .bind(name)
            .bind(model)
            .bind(is_active)
            .bind(access_token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn chair_update_is_active(&self, id: &Id<Chair>, active: bool) -> Result<()> {
        sqlx::query("UPDATE chairs SET is_active = ? WHERE id = ?")
            .bind(active)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
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

impl Repository {
    pub async fn pgw_set(&self, s: &str) -> Result<()> {
        sqlx::query("UPDATE settings SET value = ? WHERE name = 'payment_gateway_url'")
            .bind(s)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn pgw_get(&self, tx: impl Into<Option<&mut Tx>>) -> Result<String> {
        let mut tx = tx.into();
        let q = sqlx::query_scalar("SELECT value FROM settings WHERE name = 'payment_gateway_url'");
        Ok(maybe_tx!(self, tx, q.fetch_one)?)
    }
}
