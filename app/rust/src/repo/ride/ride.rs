use crate::dl::DlSyncRwLock;
use crate::models::{Chair, Id, Ride, RideStatus, RideStatusEnum, User};
use crate::repo::deferred::DeferrableMayUpdated;
use crate::repo::{Repository, Result};
use crate::Coordinate;
use crate::HashMap;
use chrono::{DateTime, Utc};
use sqlx::QueryBuilder;
use std::sync::Arc;

use super::{NotificationBody, RideEntry};

// rides
impl Repository {
    pub fn ride_get(&self, id: Id<Ride>) -> Result<Option<Ride>> {
        let cache = self.ride_cache.ride_cache.read();
        let Some(e) = cache.get(&id) else {
            return Ok(None);
        };
        Ok(Some(e.ride()))
    }

    // COMPLETED
    pub fn rides_user_ongoing(&self, user: Id<User>) -> Result<bool> {
        let s = self
            .ride_cache
            .user_has_ride
            .read()
            .get(&user)
            .map(|x| x.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(false);
        Ok(s)
    }

    pub fn rides_by_user(&self, id: Id<User>) -> Result<Vec<Ride>> {
        let mut res = vec![];
        let cache = self.ride_cache.user_rides.read();
        let cache = cache.get(&id).unwrap();
        let cache = cache.read();
        for c in cache.iter() {
            res.push(c.ride());
        }
        res.reverse();
        Ok(res)
    }

    pub fn rides_count_by_user(&self, id: Id<User>) -> Result<usize> {
        let cache = self.ride_cache.user_rides.read();
        let len = cache.get(&id).unwrap().read().len();
        Ok(len)
    }

    // writes

    pub fn rides_new_and_set_matching(
        &self,
        id: Id<Ride>,
        user: Id<User>,
        pickup: Coordinate,
        dest: Coordinate,
    ) -> Result<()> {
        let now = Utc::now();

        self.ride_cache.ride_deferred.insert(Ride {
            id,
            user_id: user,
            chair_id: None,
            pickup_latitude: pickup.latitude,
            pickup_longitude: pickup.longitude,
            destination_latitude: dest.latitude,
            destination_longitude: dest.longitude,
            evaluation: None,
            created_at: now,
            updated_at: now,
        });

        {
            let r = Arc::new(RideEntry {
                id,
                user_id: user,
                pickup,
                destination: dest,
                created_at: now,
                chair_id: DlSyncRwLock::new(None),
                evaluation: DlSyncRwLock::new(None),
                updated_at: DlSyncRwLock::new(now),
                latest_status: DlSyncRwLock::new(RideStatusEnum::Matching),
            });
            let cache = self.ride_cache.ride_cache.read();
            cache.insert(id, Arc::clone(&r));
            let cache = self.ride_cache.user_rides.read();
            let cache = cache.get(&user).unwrap();
            let mut cache = cache.write();
            cache.push(Arc::clone(&r));
        }

        self.ride_status_update(id, RideStatusEnum::Matching)?;

        Ok(())
    }

    pub fn rides_assign(
        &self,
        ride_id: Id<Ride>,
        status_id: Id<RideStatus>,
        chair_id: Id<Chair>,
    ) -> Result<()> {
        let now = Utc::now();
        {
            let cache = self.ride_cache.ride_cache.read();
            let e = cache.get_mut(&ride_id).unwrap();
            e.set_chair_id(chair_id, now);
        }

        self.on_chair_went_on_duty(chair_id);

        self.ride_cache.ride_deferred.update(RideUpdate {
            id: ride_id,
            updated_at: now,
            content: RideUpdateContent::Assign { chair_id },
        });

        let b = NotificationBody {
            ride_id,
            ride_status_id: status_id,
            status: RideStatusEnum::Matching,
        };
        {
            let mark_sent = {
                let cache = self.ride_cache.chair_notification.read();
                let cache = cache.get(&chair_id).unwrap();
                cache.push(b, false)
            };
            if mark_sent {
                self.ride_status_chair_notified(status_id);
            }
        }
        Ok(())
    }

    pub fn rides_set_evaluation(
        &self,
        id: Id<Ride>,
        chair_id: Id<Chair>,
        eval: i32,
    ) -> Result<DateTime<Utc>> {
        let now = Utc::now();

        self.ride_cache.ride_deferred.update(RideUpdate {
            id,
            updated_at: now,
            content: RideUpdateContent::Eval { eval },
        });

        let sales = {
            let cache = self.ride_cache.ride_cache.read();
            let ride = cache.get(&id).unwrap();
            ride.set_evaluation(eval, now);
            crate::calculate_fare(ride.pickup, ride.destination)
        };

        self.chair_cache.on_eval(chair_id, eval, sales, now);

        Ok(now)
    }
}

#[derive(Debug)]
pub struct RideUpdate {
    id: Id<Ride>,
    updated_at: DateTime<Utc>,
    content: RideUpdateContent,
}
#[derive(Debug)]
pub enum RideUpdateContent {
    Assign { chair_id: Id<Chair> },
    Eval { eval: i32 },
}

#[derive(Debug)]
pub struct RideUpdateQuery {
    id: Id<Ride>,
    chair_id: Option<Id<Chair>>,
    eval: Option<i32>,
    updated_at: DateTime<Utc>,
}

pub struct RideDeferred;
impl DeferrableMayUpdated for RideDeferred {
    const NAME: &str = "rides";

    type Insert = Ride;
    type Update = RideUpdate;
    type UpdateQuery = RideUpdateQuery;

    fn summarize(
        inserts: &mut [Self::Insert],
        updates: Vec<Self::Update>,
    ) -> Vec<Self::UpdateQuery> {
        let mut inserts = inserts
            .iter_mut()
            .map(|x| (x.id, x))
            .collect::<HashMap<_, _>>();
        let mut new_updates = HashMap::default();

        for u in updates {
            let Some(i) = inserts.get_mut(&u.id) else {
                let r = new_updates.entry(u.id).or_insert_with(|| RideUpdateQuery {
                    id: u.id,
                    chair_id: None,
                    eval: None,
                    updated_at: u.updated_at,
                });
                r.updated_at = u.updated_at;
                match u.content {
                    RideUpdateContent::Assign { chair_id } => r.chair_id = Some(chair_id),
                    RideUpdateContent::Eval { eval } => r.eval = Some(eval),
                }
                continue;
            };

            i.updated_at = u.updated_at;
            match u.content {
                RideUpdateContent::Assign { chair_id } => i.chair_id = Some(chair_id),
                RideUpdateContent::Eval { eval } => i.evaluation = Some(eval),
            }
        }

        new_updates.into_values().collect()
    }

    async fn exec_insert(
        tx: &mut sqlx::Transaction<'static, sqlx::MySql>,
        inserts: &[Self::Insert],
    ) {
        let mut builder = QueryBuilder::new("
            INSERT INTO rides (
                id, user_id, pickup_latitude, pickup_longitude, destination_latitude, destination_longitude, created_at,
                updated_at, chair_id, evaluation
            )");
        builder.push_values(inserts, |mut b, i| {
            b.push_bind(i.id)
                .push_bind(i.user_id)
                .push_bind(i.pickup_latitude)
                .push_bind(i.pickup_longitude)
                .push_bind(i.destination_latitude)
                .push_bind(i.destination_longitude)
                .push_bind(i.created_at)
                .push_bind(i.updated_at)
                .push_bind(i.chair_id)
                .push_bind(i.evaluation);
        });
        builder.build().execute(&mut **tx).await.unwrap();
    }

    async fn exec_update(
        tx: &mut sqlx::Transaction<'static, sqlx::MySql>,
        update: &Self::UpdateQuery,
    ) {
        let mut builder = QueryBuilder::new("update rides set updated_at = ");
        builder.push_bind(update.updated_at);
        builder.push(", ");

        let mut need_comma = false;
        if let Some(c) = update.chair_id.as_ref() {
            builder.push("chair_id = ");
            builder.push_bind(c);
            need_comma = true
        }
        if let Some(e) = update.eval.as_ref() {
            if need_comma {
                builder.push(", ");
            }
            builder.push("evaluation = ");
            builder.push_bind(e);
        }
        builder.push(" where id = ");
        builder.push_bind(update.id);
        builder.build().execute(&mut **tx).await.unwrap();
    }
}
