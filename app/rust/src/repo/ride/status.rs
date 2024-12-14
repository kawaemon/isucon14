use crate::models::{Chair, Id, Ride, RideStatus, RideStatusEnum, User};
use crate::repo::{maybe_tx, Repository, Result, Tx};

use super::NotificationBody;

// ride_status
impl Repository {
    pub async fn ride_status_latest(
        &self,
        _tx: impl Into<Option<&mut Tx>>,
        ride_id: &Id<Ride>,
    ) -> Result<RideStatusEnum> {
        let cache = self.ride_cache.latest_ride_stat.read().await;
        Ok(*cache.get(ride_id).unwrap())
    }

    // writes

    pub async fn ride_status_update(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        ride_id: &Id<Ride>,
        user_id: &Id<User>,
        chair_id: Option<&Id<Chair>>,
        status: RideStatusEnum,
    ) -> Result<()> {
        let mut tx = tx.into();
        let status_id = Id::<RideStatus>::new();
        let q = sqlx::query("INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)")
            .bind(&status_id)
            .bind(ride_id)
            .bind(status);

        maybe_tx!(self, tx, q.execute)?;

        {
            let mut cache = self.ride_cache.latest_ride_stat.write().await;
            cache.insert(ride_id.clone(), status);
        }
        let b = NotificationBody {
            ride_id: ride_id.clone(),
            ride_status_id: status_id.clone(),
            status,
        };
        {
            let mut t = self.ride_cache.user_notification.write().await;
            t.get_mut(user_id).unwrap().push(b.clone(), false);
        }
        if let Some(c) = chair_id {
            let mut t = self.ride_cache.chair_notification.write().await;
            t.get_mut(c).unwrap().push(b.clone(), false);
        }
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
