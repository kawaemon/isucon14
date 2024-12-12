use sqlx::{MySql, Pool, Transaction};

use crate::{
    models::{Chair, Id, Owner, Ride, RideStatus, RideStatusEnum, User},
    Error,
};

macro_rules! maybe_tx {
    ($self:expr, $tx:expr, $query:ident.$method:ident) => {
        if let Some(tx) = $tx.into() {
            $query.$method(&mut **tx).await
        } else {
            $query.$method(&$self.pool).await
        }
    };
}

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
}

// ride_status
impl Repository {
    pub async fn ride_status_update(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        ride_id: &Id<Ride>,
        status: RideStatusEnum,
    ) -> Result<()> {
        let q = sqlx::query("INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)")
            .bind(Id::<RideStatus>::new())
            .bind(ride_id)
            .bind(status);

        maybe_tx!(self, tx, q.execute)?;

        Ok(())
    }
}
