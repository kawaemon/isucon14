use sqlx::{MySql, Pool, Transaction};

use crate::{
    models::{Id, Ride, RideStatus, RideStatusEnum},
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

#[derive(Debug)]
pub struct Repository {
    pool: Pool<MySql>,
}

impl Repository {
    pub fn new(pool: &Pool<MySql>) -> Self {
        Self { pool: pool.clone() }
    }
}

impl Repository {
    pub async fn ride_status_update(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        ride_id: &Id<Ride>,
        status: RideStatusEnum,
    ) -> Result<(), Error> {
        let q = sqlx::query("INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)")
            .bind(Id::<RideStatus>::new())
            .bind(ride_id)
            .bind(status);

        maybe_tx!(self, tx, q.execute)?;

        Ok(())
    }
}
