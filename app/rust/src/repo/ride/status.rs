use std::sync::Arc;

use crate::models::{Id, Ride, RideStatus, RideStatusEnum};
use crate::repo::{maybe_tx, Repository, Result, Tx};

use super::NotificationBody;

// ride_status
impl Repository {
    pub async fn ride_status_latest(
        &self,
        _tx: impl Into<Option<&mut Tx>>,
        ride_id: &Id<Ride>,
    ) -> Result<RideStatusEnum> {
        let cache = self.ride_cache.ride_cache.read().await;
        let ride = cache.get(ride_id).unwrap();
        let s = ride.latest_status.read().await;
        Ok(*s)
    }

    // writes

    pub async fn ride_status_update(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        ride_id: &Id<Ride>,
        status: RideStatusEnum,
    ) -> Result<()> {
        let mut tx = tx.into();
        let status_id = Id::<RideStatus>::new();
        let q = sqlx::query("INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)")
            .bind(&status_id)
            .bind(ride_id)
            .bind(status);

        maybe_tx!(self, tx, q.execute)?;

        let b = NotificationBody {
            ride_id: ride_id.clone(),
            ride_status_id: status_id.clone(),
            status,
        };

        let ride = {
            let ride_cache = self.ride_cache.ride_cache.read().await;
            let ride = ride_cache.get(ride_id).unwrap();
            Arc::clone(ride)
        };

        *ride.latest_status.write().await = status;

        {
            let mut user = self.ride_cache.user_notification.write().await;
            let mark_sent = user.get_mut(&ride.user_id).unwrap().push(b.clone(), false);
            if mark_sent {
                self.ride_status_app_notified(tx.as_deref_mut(), &status_id)
                    .await?;
            }
        }

        let chair_id = {
            let ref_ = ride.chair_id.read().await;
            ref_.clone()
        };

        if let Some(c) = chair_id {
            {
                let mut chair = self.ride_cache.chair_notification.write().await;
                let mark_sent = chair.get_mut(&c).unwrap().push(b.clone(), false);
                if mark_sent {
                    self.ride_status_chair_notified(tx, &status_id).await?;
                }
            }

            if status == RideStatusEnum::Completed {
                self.ride_cache.on_chair_status_change(&c, false).await;
            }

            let mut movement_cache = self.ride_cache.chair_movement_cache.write().await;
            match status {
                RideStatusEnum::Matching => {}
                RideStatusEnum::Enroute => {
                    movement_cache.insert(c.clone(), Arc::clone(&ride));
                }
                RideStatusEnum::Pickup => {
                    movement_cache.remove(&c).unwrap();
                }
                RideStatusEnum::Carrying => {
                    movement_cache.insert(c.clone(), Arc::clone(&ride));
                }
                RideStatusEnum::Arrived => {
                    movement_cache.remove(&c).unwrap();
                }
                RideStatusEnum::Completed => {}
                RideStatusEnum::Canceled => unreachable!(), // 使われてないよね？
            }
        }

        if status == RideStatusEnum::Matching {
            let mut waiting_rides = self.ride_cache.waiting_rides.lock().await; // DEADLOCK HERE
            waiting_rides.push_back(Arc::clone(&ride));
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
