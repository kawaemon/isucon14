use std::sync::Arc;
use std::time::Duration;

use crate::models::{Chair, Id, Ride, RideStatus, RideStatusEnum, User};
use crate::repo::{maybe_tx, Repository, Result, Tx};
use crate::Coordinate;
use chrono::{DateTime, Utc};

use super::NotificationBody;

// rides
impl Repository {
    pub async fn ride_get(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        id: &Id<Ride>,
    ) -> Result<Option<Ride>> {
        let mut tx = tx.into();
        let q = sqlx::query_as("select * from rides where id = ?").bind(id);
        Ok(maybe_tx!(self, tx, q.fetch_optional)?)
    }

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
        let statuses: Vec<RideStatus> =
            sqlx::query_as("select * from ride_statuses where ride_id = ?")
                .bind(ride_id)
                .fetch_all(&self.pool)
                .await?;
        assert!(statuses.len() == 1);
        let status = &statuses[0];
        assert!(status.status == RideStatusEnum::Matching);
        let b = NotificationBody {
            ride_id: ride_id.clone(),
            ride_status_id: status.id.clone(),
            status: RideStatusEnum::Matching,
        };
        {
            let mut cache = self.ride_cache.chair_notification.write().await;
            cache.get_mut(chair_id).unwrap().push(b, false);
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

        // 更新が早すぎて警告が出る
        {
            let chair_cache = Arc::clone(&self.chair_cache);
            let chair_id = chair_id.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                chair_cache.on_eval(&chair_id, eval).await;
            });
        }

        Ok(now)
    }
}
