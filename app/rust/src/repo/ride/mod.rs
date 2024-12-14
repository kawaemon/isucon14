use chrono::{DateTime, Utc};
use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::Arc,
};
use tokio::sync::RwLock;

use sqlx::{MySql, Pool};

use crate::{
    models::{Chair, Id, Ride, RideStatus, RideStatusEnum, User},
    Coordinate,
};

use super::{cache_init::CacheInit, Repository, Result};

#[allow(clippy::module_inception)]
mod ride;
mod status;

pub type RideCache = Arc<RideCacheInner>;

#[derive(Debug)]
pub struct RideCacheInner {
    ride_cache: RwLock<HashMap<Id<Ride>, Arc<RideEntry>>>,
    latest_ride_stat: RwLock<HashMap<Id<Ride>, RideStatusEnum>>,

    user_notification: RwLock<HashMap<Id<User>, NotificationQueue>>,
    chair_notification: RwLock<HashMap<Id<Chair>, NotificationQueue>>,
}

impl Repository {
    pub(super) async fn init_ride_cache(_pool: &Pool<MySql>, init: &mut CacheInit) -> RideCache {
        init.rides.sort_unstable_by_key(|x| x.created_at);
        init.ride_statuses.sort_unstable_by_key(|x| x.created_at);

        let mut statuses = HashMap::new();
        for status in &init.ride_statuses {
            statuses
                .entry(status.ride_id.clone())
                .or_insert_with(Vec::new)
                .push(status.clone());
        }

        //
        // status cache
        //
        let mut latest_ride_stat = HashMap::new();
        for stat in &init.ride_statuses {
            latest_ride_stat.insert(stat.ride_id.clone(), stat.status);
        }

        //
        // user_notification
        //
        let mut user_notification = HashMap::new();
        for user in &init.users {
            user_notification.insert(user.id.clone(), NotificationQueue::new());
        }
        for ride in &init.rides {
            let queue = user_notification.get_mut(&ride.user_id).unwrap();
            let st = statuses.get(&ride.id).unwrap();
            for status in st.iter() {
                let b = NotificationBody {
                    ride_id: ride.id.clone(),
                    ride_status_id: status.id.clone(),
                    status: status.status,
                };
                queue.push(b, status.app_sent_at.is_some());
            }
        }

        //
        // chair_notification
        //
        let mut chair_notification = HashMap::new();
        for chair in &init.chairs {
            chair_notification.insert(chair.id.clone(), NotificationQueue::new());
        }
        for status in &init.ride_statuses {
            if status.status != RideStatusEnum::Enroute {
                continue;
            }
            let ride = init.rides.iter().find(|x| x.id == status.ride_id).unwrap();
            let queue = chair_notification
                .get_mut(ride.chair_id.as_ref().unwrap())
                .unwrap();
            for status in statuses.get(&status.ride_id).unwrap().iter() {
                let b = NotificationBody {
                    ride_id: ride.id.clone(),
                    ride_status_id: status.id.clone(),
                    status: status.status,
                };
                queue.push(b, status.chair_sent_at.is_some());
            }
        }

        let mut rides = HashMap::new();
        for ride in &init.rides {
            rides.insert(
                ride.id.clone(),
                Arc::new(RideEntry {
                    id: ride.id.clone(),
                    user_id: ride.user_id.clone(),
                    pickup: ride.pickup_coord(),
                    destination: ride.destination_coord(),
                    created_at: ride.created_at,
                    chair_id: RwLock::new(ride.chair_id.clone()),
                    evaluation: RwLock::new(ride.evaluation.clone()),
                    updated_at: RwLock::new(ride.updated_at),
                }),
            );
        }

        Arc::new(RideCacheInner {
            latest_ride_stat: RwLock::new(latest_ride_stat),
            user_notification: RwLock::new(user_notification),
            chair_notification: RwLock::new(chair_notification),
            ride_cache: RwLock::new(rides),
        })
    }
}

impl RideCacheInner {
    pub async fn on_user_add(&self, id: &Id<User>) {
        self.user_notification
            .write()
            .await
            .insert(id.clone(), NotificationQueue::new());
    }
    pub async fn on_chair_add(&self, id: &Id<Chair>) {
        self.chair_notification
            .write()
            .await
            .insert(id.clone(), NotificationQueue::new());
    }
}

impl Repository {
    pub async fn chair_get_next_notification(
        &self,
        id: &Id<Chair>,
    ) -> Result<Option<NotificationBody>> {
        let next = {
            let mut cache = self.ride_cache.chair_notification.write().await;
            let queue = cache.get_mut(id).unwrap();
            let Some(next) = queue.get_next() else {
                return Ok(None);
            };
            next
        };
        if !next.sent {
            self.ride_status_chair_notified(None, &next.body.ride_status_id)
                .await?;
        }
        Ok(Some(next.body))
    }

    pub async fn app_get_next_notification(
        &self,
        id: &Id<User>,
    ) -> Result<Option<NotificationBody>> {
        let next = {
            let mut cache = self.ride_cache.user_notification.write().await;
            let queue = cache.get_mut(id).unwrap();
            let Some(next) = queue.get_next() else {
                return Ok(None);
            };
            next
        };
        if !next.sent {
            self.ride_status_app_notified(None, &next.body.ride_status_id)
                .await?;
        }
        Ok(Some(next.body))
    }
}

pub struct NotificationQueue {
    last_sent: Option<NotificationBody>,
    queue: VecDeque<NotificationBody>,
}
impl std::fmt::Debug for NotificationQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            return f
                .debug_struct("NotificationQueue")
                .field("last_sent", &self.last_sent)
                .field("queue", &self.queue)
                .finish();
        }
        if !self.queue.is_empty() {
            write!(f, "{} notifications in queue", self.queue.len())
        } else if self.last_sent.is_some() {
            write!(f, "all notifications were sent")
        } else {
            write!(f, "have no notification")
        }
    }
}

impl NotificationQueue {
    fn new() -> Self {
        Self {
            last_sent: None,
            queue: VecDeque::new(),
        }
    }
    pub fn push(&mut self, b: NotificationBody, sent: bool) {
        if sent {
            if !self.queue.is_empty() {
                tracing::warn!("bug? sent notification after not-sent one; discarding queue");
                self.queue.clear();
            }
            self.last_sent = Some(b);
            return;
        }
        self.last_sent = None;
        self.queue.push_back(b);
    }

    pub fn get_next(&mut self) -> Option<NotificationEntry> {
        if self.queue.is_empty() {
            let last = self.last_sent.clone();
            return last.map(|body| NotificationEntry { sent: true, body });
        }
        let e = self.queue.pop_front().unwrap();
        self.last_sent = Some(e.clone());
        Some(NotificationEntry {
            sent: false,
            body: e,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NotificationEntry {
    sent: bool,
    body: NotificationBody,
}

#[derive(Debug, Clone)]
pub struct NotificationBody {
    pub ride_id: Id<Ride>,
    pub ride_status_id: Id<RideStatus>,
    pub status: RideStatusEnum,
}

#[derive(Debug)]
pub struct RideEntry {
    id: Id<Ride>,
    user_id: Id<User>,
    pickup: Coordinate,
    destination: Coordinate,
    created_at: DateTime<Utc>,

    chair_id: RwLock<Option<Id<Chair>>>,
    evaluation: RwLock<Option<i32>>,
    updated_at: RwLock<DateTime<Utc>>,
}
impl RideEntry {
    pub async fn ride(&self) -> Ride {
        Ride {
            id: self.id.clone(),
            user_id: self.user_id.clone(),
            chair_id: self.chair_id.read().await.clone(),
            pickup_latitude: self.pickup.latitude,
            pickup_longitude: self.pickup.longitude,
            destination_latitude: self.destination.latitude,
            destination_longitude: self.destination.longitude,
            evaluation: *self.evaluation.read().await,
            created_at: self.created_at,
            updated_at: *self.updated_at.read().await,
        }
    }
    pub async fn set_chair_id(&self, chair_id: &Id<Chair>, now: DateTime<Utc>) {
        let mut c = self.chair_id.write().await;
        let mut u = self.updated_at.write().await;
        *c = Some(chair_id.clone());
        *u = now;
    }
    pub async fn set_evaluation(&self, eval: i32, now: DateTime<Utc>) {
        let mut e = self.evaluation.write().await;
        let mut u = self.updated_at.write().await;
        *e = Some(eval);
        *u = now;
    }
}
