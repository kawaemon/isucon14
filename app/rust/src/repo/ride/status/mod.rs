pub mod deferred;
use chrono::Utc;
pub use deferred::Deferred;
use deferred::{NotifiedType, RideStatusInsert, RideStatusUpdate};

use std::sync::Arc;

use crate::models::{Id, Ride, RideStatus, RideStatusEnum};
use crate::repo::{Repository, Result, Tx};

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
        _tx: impl Into<Option<&mut Tx>>,
        ride_id: &Id<Ride>,
        status: RideStatusEnum,
    ) -> Result<()> {
        let status_id = Id::<RideStatus>::new();

        self.ride_cache
            .deferred
            .insert(RideStatusInsert {
                id: status_id.clone(),
                ride_id: ride_id.clone(),
                status,
                created_at: Utc::now(),
            })
            .await;

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
            let mark_sent = {
                let user = self.ride_cache.user_notification.read().await;
                user.get(&ride.user_id)
                    .unwrap()
                    .push(b.clone(), false)
                    .await
            };
            if mark_sent {
                self.ride_status_app_notified(&status_id).await?;
            }
        }

        let chair_id = {
            let ref_ = ride.chair_id.read().await;
            ref_.clone()
        };

        if let Some(c) = chair_id {
            {
                let mark_sent = {
                    let chair = self.ride_cache.chair_notification.read().await;
                    chair.get(&c).unwrap().push(b.clone(), false).await
                };
                if mark_sent {
                    self.ride_status_chair_notified(&status_id).await?;
                }
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
            let mut waiting_rides = self.ride_cache.waiting_rides.lock().await;
            waiting_rides.push_back((Arc::clone(&ride), status_id));
        }

        Ok(())
    }

    pub async fn ride_status_chair_notified(&self, status_id: &Id<RideStatus>) -> Result<()> {
        self.ride_cache
            .deferred
            .update(RideStatusUpdate {
                ty: NotifiedType::Chair,
                status_id: status_id.clone(),
                at: Utc::now(),
            })
            .await;
        Ok(())
    }

    pub async fn ride_status_app_notified(&self, status_id: &Id<RideStatus>) -> Result<()> {
        self.ride_cache
            .deferred
            .update(RideStatusUpdate {
                ty: NotifiedType::App,
                status_id: status_id.clone(),
                at: Utc::now(),
            })
            .await;

        Ok(())
    }
}
