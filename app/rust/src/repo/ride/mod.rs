use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use tokio::sync::RwLock;

use sqlx::{MySql, Pool};

use crate::models::{Chair, Id, Ride, RideStatus, RideStatusEnum, User};

use super::{cache_init::CacheInit, Repository, Result};

#[allow(clippy::module_inception)]
mod ride;
mod status;

pub type RideCache = Arc<RideCacheInner>;

#[derive(Debug)]
pub struct RideCacheInner {
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

        println!();
        println!();
        println!("### users ###");
        for (u, n) in user_notification.iter() {
            println!("{u:?}: {n:?}");
        }
        println!();
        println!();
        println!("### chairs ###");
        for (c, n) in chair_notification.iter() {
            println!("{c:?}: {n:?}");
        }

        Arc::new(RideCacheInner {
            latest_ride_stat: RwLock::new(latest_ride_stat),
            user_notification: RwLock::new(user_notification),
            chair_notification: RwLock::new(chair_notification),
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
        let mut cache = self.ride_cache.chair_notification.write().await;
        let queue = cache.get_mut(id).unwrap();
        let Some(next) = queue.get_next() else {
            return Ok(None);
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
        let mut cache = self.ride_cache.user_notification.write().await;
        let queue = cache.get_mut(id).unwrap();
        let Some(next) = queue.get_next() else {
            return Ok(None);
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
