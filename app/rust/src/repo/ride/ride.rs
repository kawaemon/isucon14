use crate::models::{Chair, Id, Ride, RideStatus, RideStatusEnum, User};
use crate::repo::dl::DlRwLock as RwLock;
use crate::repo::{maybe_tx, Repository, Result, Tx};
use crate::Coordinate;
use chrono::{DateTime, Utc};
use std::sync::Arc;

use super::{NotificationBody, RideEntry};

// rides
impl Repository {
    pub async fn ride_get(
        &self,
        _tx: impl Into<Option<&mut Tx>>,
        id: &Id<Ride>,
    ) -> Result<Option<Ride>> {
        let cache = self.ride_cache.ride_cache.read().await;
        let Some(e) = cache.get(id) else {
            return Ok(None);
        };
        Ok(Some(e.ride().await))
    }

    // COMPLETED
    pub async fn rides_user_ongoing(&self, user: &Id<User>) -> Result<bool> {
        let cache = self.ride_cache.ride_cache.read().await;
        for ride in cache.values() {
            if &ride.user_id != user {
                continue;
            }
            if *ride.latest_status.read().await != RideStatusEnum::Completed {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn rides_get_assigned(
        &self,
        _tx: impl Into<Option<&mut Tx>>,
        chair_id: &Id<Chair>,
    ) -> Result<Option<(Ride, RideStatusEnum)>> {
        let cache = self.ride_cache.chair_movement_cache.read().await;
        let Some(m) = cache.get(chair_id) else {
            return Ok(None);
        };
        let ride = m.ride().await;
        let status = *m.latest_status.read().await;
        Ok(Some((ride, status)))
    }

    pub async fn rides_count_by_user(&self, id: &Id<User>) -> Result<usize> {
        let r: i32 = sqlx::query_scalar("SELECT count(*) FROM rides WHERE user_id = ?")
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        Ok(r as usize)
    }

    // writes

    pub async fn rides_new_and_set_matching(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        id: &Id<Ride>,
        user: &Id<User>,
        pickup: Coordinate,
        dest: Coordinate,
    ) -> Result<()> {
        let mut tx = tx.into();
        let now = Utc::now();

        let q = sqlx::query("INSERT INTO rides (id, user_id, pickup_latitude, pickup_longitude, destination_latitude, destination_longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id)
            .bind(user)
            .bind(pickup.latitude)
            .bind(pickup.longitude)
            .bind(dest.latitude)
            .bind(dest.longitude)
            .bind(now)
            .bind(now);
        maybe_tx!(self, tx, q.execute)?;

        {
            let r = Arc::new(RideEntry {
                id: id.clone(),
                user_id: user.clone(),
                pickup,
                destination: dest,
                created_at: now,
                chair_id: RwLock::new(None),
                evaluation: RwLock::new(None),
                updated_at: RwLock::new(now),
                latest_status: RwLock::new(RideStatusEnum::Matching),
            });
            let mut cache = self.ride_cache.ride_cache.write().await;
            cache.insert(id.clone(), Arc::clone(&r));
        }

        self.ride_status_update(tx, id, RideStatusEnum::Matching)
            .await?;

        Ok(())
    }

    pub async fn rides_assign(
        &self,
        ride_id: &Id<Ride>,
        status_id: &Id<RideStatus>,
        chair_id: &Id<Chair>,
    ) -> Result<()> {
        let now = Utc::now();
        {
            let mut cache = self.ride_cache.ride_cache.write().await;
            let e = cache.get_mut(ride_id).unwrap();
            e.set_chair_id(chair_id, now).await;
        }
        {
            self.ride_cache.on_chair_status_change(chair_id, true).await;
        }
        sqlx::query("update rides set chair_id = ?, updated_at = ? where id = ?")
            .bind(chair_id)
            .bind(now)
            .bind(ride_id)
            .execute(&self.pool)
            .await?;
        let b = NotificationBody {
            ride_id: ride_id.clone(),
            ride_status_id: status_id.clone(),
            status: RideStatusEnum::Matching,
        };
        {
            let mark_sent = {
                let cache = self.ride_cache.chair_notification.read().await;
                cache.get(chair_id).unwrap().push(b, false).await
            };
            if mark_sent {
                self.ride_status_chair_notified(status_id).await.unwrap();
            }
        }
        Ok(())
    }

    pub async fn rides_set_evaluation(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        id: &Id<Ride>,
        chair_id: &Id<Chair>,
        eval: i32,
    ) -> Result<DateTime<Utc>> {
        let now = Utc::now();
        let mut tx = tx.into();

        let q = sqlx::query("UPDATE rides SET evaluation = ?, updated_at = ? WHERE id = ?")
            .bind(eval)
            .bind(now)
            .bind(id);

        maybe_tx!(self, tx, q.execute)?;

        let sales = {
            let mut cache = self.ride_cache.ride_cache.write().await;
            let ride = cache.get_mut(id).unwrap();
            ride.set_evaluation(eval, now).await;
            ride.ride().await.calc_sale()
        };

        self.chair_cache.on_eval(chair_id, eval, sales, now).await;

        Ok(now)
    }
}
