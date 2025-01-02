pub mod deferred;
use chrono::Utc;
use deferred::{NotifiedType, RideStatusUpdate};

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::models::{Id, Ride, RideStatus, RideStatusEnum};
use crate::repo::{Repository, Result};

use super::NotificationBody;

// ride_status
impl Repository {
    pub fn ride_status_latest(&self, ride_id: Id<Ride>) -> Result<RideStatusEnum> {
        let cache = self.ride_cache.ride_cache.read();
        let ride = cache.get(&ride_id).unwrap();
        let s = ride.latest_status.read();
        Ok(*s)
    }

    // writes

    pub fn ride_status_update(&self, ride_id: Id<Ride>, status: RideStatusEnum) -> Result<()> {
        let status_id = Id::<RideStatus>::new();

        self.ride_cache.ride_status_deferred.insert(RideStatus {
            id: status_id,
            ride_id,
            status,
            created_at: Utc::now(),
            app_sent_at: None,
            chair_sent_at: None,
        });

        let b = NotificationBody {
            ride_id,
            ride_status_id: status_id,
            status,
        };

        let ride = {
            let ride_cache = self.ride_cache.ride_cache.read();
            let ride = ride_cache.get(&ride_id).unwrap();
            Arc::clone(&*ride)
        };

        ride.set_latest_ride_status(status);

        let mark_sent = {
            let user = self.ride_cache.user_notification.read();
            let user = user.get(&ride.user_id).unwrap();
            user.push(b.clone(), false)
        };
        if mark_sent {
            self.ride_status_app_notified(status_id);
        }

        let chair_id = { *ride.chair_id.read() };

        if let Some(c) = chair_id {
            let mark_sent = {
                let chair = self.ride_cache.chair_notification.read();
                let chair = chair.get(&c).unwrap();
                chair.push(b, false)
            };
            if mark_sent {
                self.ride_status_chair_notified(status_id);
            }

            match status {
                RideStatusEnum::Matching => {
                    self.ride_cache
                        .user_has_ride
                        .read()
                        .entry(ride.user_id)
                        .or_insert_with(|| AtomicBool::new(true))
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
                RideStatusEnum::Enroute => {
                    self.chair_set_movement(c, ride.pickup, RideStatusEnum::Pickup, ride.id);
                }
                RideStatusEnum::Pickup => {}
                RideStatusEnum::Carrying => {
                    self.chair_set_movement(c, ride.destination, RideStatusEnum::Arrived, ride.id);
                }
                RideStatusEnum::Arrived => {}
                RideStatusEnum::Completed => {
                    self.ride_cache
                        .user_has_ride
                        .read()
                        .entry(ride.user_id)
                        .or_insert_with(|| AtomicBool::new(false))
                        .store(false, std::sync::atomic::Ordering::Relaxed);
                }
                RideStatusEnum::Canceled => unreachable!(), // 使われてないよね？
            }
        }

        if status == RideStatusEnum::Matching {
            let mut waiting_rides = self.ride_cache.waiting_rides.lock();
            waiting_rides.push_back((Arc::clone(&ride), status_id));
        }

        Ok(())
    }

    pub fn ride_status_chair_notified(&self, status_id: Id<RideStatus>) {
        self.ride_cache
            .ride_status_deferred
            .update(RideStatusUpdate {
                ty: NotifiedType::Chair,
                status_id,
                at: Utc::now(),
            });
    }

    pub fn ride_status_app_notified(&self, status_id: Id<RideStatus>) {
        self.ride_cache
            .ride_status_deferred
            .update(RideStatusUpdate {
                ty: NotifiedType::App,
                status_id,
                at: Utc::now(),
            });
    }
}
