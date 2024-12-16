use crate::app_handlers::AppGetNearbyChairsResponseChair;
use crate::repo::dl::DlMutex as Mutex;
use crate::repo::dl::DlRwLock as RwLock;
use chrono::{DateTime, Utc};
use std::collections::HashSet;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use crate::{
    models::{Chair, Id, Ride, RideStatus, RideStatusEnum, User},
    Coordinate,
};

use super::{cache_init::CacheInit, chair::ChairCache, Repository, Result};

#[allow(clippy::module_inception)]
mod ride;
mod status;

pub type RideCache = Arc<RideCacheInner>;

#[derive(Debug)]
pub struct RideCacheInner {
    ride_cache: RwLock<HashMap<Id<Ride>, Arc<RideEntry>>>,

    waiting_rides: Mutex<VecDeque<Arc<RideEntry>>>,
    free_chairs_lv1: Mutex<HashSet<Id<Chair>>>,
    free_chairs_lv2: Mutex<Vec<Id<Chair>>>,

    chair_movement_cache: RwLock<HashMap<Id<Chair>, Arc<RideEntry>>>,

    user_notification: RwLock<HashMap<Id<User>, NotificationQueue>>,
    chair_notification: RwLock<HashMap<Id<Chair>, NotificationQueue>>,
}

struct RideCacheInit {
    ride_cache: HashMap<Id<Ride>, Arc<RideEntry>>,

    waiting_rides: VecDeque<Arc<RideEntry>>,
    free_chairs_lv1: HashSet<Id<Chair>>,
    free_chairs_lv2: Vec<Id<Chair>>,

    chair_movement_cache: HashMap<Id<Chair>, Arc<RideEntry>>,

    user_notification: HashMap<Id<User>, NotificationQueue>,
    chair_notification: HashMap<Id<Chair>, NotificationQueue>,
}
impl RideCacheInit {
    async fn from_init(init: &mut CacheInit) -> Self {
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
                let mark_sent = queue.push(b, status.app_sent_at.is_some()).await;
                assert!(!mark_sent);
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
                let mark_sent = queue.push(b, status.chair_sent_at.is_some()).await;
                assert!(!mark_sent);
            }
        }

        //
        // rides
        //
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
                    evaluation: RwLock::new(ride.evaluation),
                    updated_at: RwLock::new(ride.updated_at),
                    latest_status: RwLock::new(*latest_ride_stat.get(&ride.id).unwrap()),
                }),
            );
        }

        //
        // chair_movement_cache
        //
        let mut chair_movement_cache = HashMap::new();
        for ride in &init.rides {
            let Some(chair_id) = ride.chair_id.as_ref() else {
                continue;
            };
            let ride = rides.get(&ride.id).unwrap();
            for status in statuses.get(&ride.id).unwrap() {
                match status.status {
                    RideStatusEnum::Matching => {}
                    RideStatusEnum::Enroute => {
                        chair_movement_cache.insert(chair_id.clone(), Arc::clone(ride));
                    }
                    RideStatusEnum::Pickup => {
                        chair_movement_cache.remove(chair_id).unwrap();
                    }
                    RideStatusEnum::Carrying => {
                        chair_movement_cache.insert(chair_id.clone(), Arc::clone(ride));
                    }
                    RideStatusEnum::Arrived => {
                        chair_movement_cache.remove(chair_id).unwrap();
                    }
                    RideStatusEnum::Completed => {}
                    RideStatusEnum::Canceled => unreachable!(), // 使われてないよね？
                }
            }
        }

        //
        // waiting_rides
        //
        let mut waiting_rides = VecDeque::new();
        for ride in &init.rides {
            if ride.chair_id.is_none() {
                let ride = rides.get(&ride.id).unwrap();
                waiting_rides.push_back(Arc::clone(ride));
            }
        }

        //
        // free_chairs
        //
        let mut free_chairs_lv1 = HashSet::new();
        let mut free_chairs_lv2 = Vec::new();
        for chair in &init.chairs {
            let n = chair_notification.get(&chair.id).unwrap();
            let n = n.0.lock().await;
            // active かつ 送信済みかそうでないかに関わらず最後に作成された通知が Completed であればいい
            let mut lv1 = chair.is_active;
            if !n.queue.is_empty() {
                lv1 &= n.queue.back().unwrap().status == RideStatusEnum::Completed
            } else if n.last_sent.is_some() {
                lv1 &= n
                    .last_sent
                    .as_ref()
                    .is_some_and(|x| x.status == RideStatusEnum::Completed);
            } else if n.queue.is_empty() && n.last_sent.is_none() {
            } else {
                unreachable!()
            }
            if lv1 {
                free_chairs_lv1.insert(chair.id.clone());
            }

            // active かつ最後の通知が Completed でそれが送信済みであればいい
            let lv2 = chair.is_active
                && n.queue.is_empty()
                && n.last_sent
                    .as_ref()
                    .is_none_or(|x| x.status == RideStatusEnum::Completed);
            if lv2 {
                free_chairs_lv2.push(chair.id.clone());
            }
        }

        Self {
            ride_cache: rides,
            user_notification,
            chair_notification,
            waiting_rides,
            free_chairs_lv1,
            free_chairs_lv2,
            chair_movement_cache,
        }
    }
}

impl Repository {
    pub(super) async fn init_ride_cache(
        init: &mut CacheInit,
        _chair_cache: &ChairCache,
    ) -> RideCache {
        let init = RideCacheInit::from_init(init).await;
        Arc::new(RideCacheInner {
            user_notification: RwLock::new(init.user_notification),
            chair_notification: RwLock::new(init.chair_notification),
            ride_cache: RwLock::new(init.ride_cache),
            waiting_rides: Mutex::new(init.waiting_rides),
            free_chairs_lv1: Mutex::new(init.free_chairs_lv1),
            free_chairs_lv2: Mutex::new(init.free_chairs_lv2),
            chair_movement_cache: RwLock::new(init.chair_movement_cache),
        })
    }
    pub(super) async fn reinit_ride_cache(&self, init: &mut CacheInit) {
        let init = RideCacheInit::from_init(init).await;

        let RideCacheInner {
            ride_cache,
            user_notification,
            chair_notification,
            waiting_rides,
            free_chairs_lv1,
            free_chairs_lv2,
            chair_movement_cache,
        } = &*self.ride_cache;

        let mut r = ride_cache.write().await;
        let mut u = user_notification.write().await;
        let mut c = chair_notification.write().await;
        let mut f1 = free_chairs_lv1.lock().await;
        let mut f2 = free_chairs_lv2.lock().await;
        let mut w = waiting_rides.lock().await;
        let mut cm = chair_movement_cache.write().await;

        *r = init.ride_cache;
        *u = init.user_notification;
        *c = init.chair_notification;
        *f1 = init.free_chairs_lv1;
        *f2 = init.free_chairs_lv2;
        *w = init.waiting_rides;
        *cm = init.chair_movement_cache;
    }
}

// notification の送信後
impl Repository {
    /// latest が completed になっていればよい
    pub async fn chair_huifhiubher(
        &self,
        coord: Coordinate,
        dist: i32,
    ) -> Result<Vec<AppGetNearbyChairsResponseChair>> {
        let cache = self.ride_cache.free_chairs_lv1.lock().await;
        let mut res = vec![];
        for chair in cache.iter() {
            let Some(chair_coord) = self.chair_location_get_latest(chair).await? else {
                continue;
            };
            if coord.distance(chair_coord) <= dist {
                let chair = self.chair_get_by_id(None, chair).await?.unwrap();
                res.push(AppGetNearbyChairsResponseChair {
                    id: chair.id,
                    name: chair.name,
                    model: chair.model,
                    current_coordinate: chair_coord,
                });
            }
        }
        Ok(res)
    }
}

impl Repository {
    pub async fn push_free_chair(&self, id: &Id<Chair>) {
        let len = {
            let mut cache = self.ride_cache.free_chairs_lv2.lock().await;
            cache.push(id.clone());
            cache.len()
        };
        if len >= 10 {
            let me = self.clone();
            tokio::spawn(async move {
                me.do_matching().await;
            });
        }
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
    pub async fn on_chair_status_change(&self, id: &Id<Chair>, on_duty: bool) {
        let mut cache = self.free_chairs_lv1.lock().await;
        if on_duty {
            cache.remove(id);
        } else {
            cache.insert(id.clone());
        }
    }
}

pub struct NotificationTransceiver {
    /// received notification must be sent (W)
    pub notification_rx: tokio::sync::broadcast::Receiver<Option<NotificationBody>>,
}

impl Repository {
    pub async fn chair_get_next_notification_sse(
        &self,
        id: &Id<Chair>,
    ) -> Result<NotificationTransceiver> {
        let mut cache = self.ride_cache.chair_notification.write().await;
        let queue = cache.get_mut(id).unwrap();

        let (tx, rx) = tokio::sync::broadcast::channel(8);

        let next = queue.get_next().await;
        tx.send(next.clone().map(|x| x.body)).unwrap();
        let mut send = 1;

        if let Some(mut next) = next {
            while !next.sent {
                self.ride_status_chair_notified(None, &next.body.ride_status_id)
                    .await?;
                next = queue.get_next().await.unwrap();
                tx.send(Some(next.body.clone())).unwrap();
                send += 1;
            }
        }

        tracing::debug!("chair sse beginning, sent to sync={send}");

        queue.0.lock().await.tx = Some(tx);

        Ok(NotificationTransceiver {
            notification_rx: rx,
        })
    }

    pub async fn user_get_next_notification_sse(
        &self,
        id: &Id<User>,
    ) -> Result<NotificationTransceiver> {
        let mut cache = self.ride_cache.user_notification.write().await;
        let queue = cache.get_mut(id).unwrap();

        let (tx, rx) = tokio::sync::broadcast::channel(8);

        let next = queue.get_next().await;
        tx.send(next.clone().map(|x| x.body)).unwrap();
        let mut send = 1;

        if let Some(mut next) = next {
            while !next.sent {
                self.ride_status_app_notified(None, &next.body.ride_status_id)
                    .await?;
                next = queue.get_next().await.unwrap();
                tx.send(Some(next.body.clone())).unwrap();
                send += 1;
            }
        }

        tracing::debug!("app sse beginning, sent to sync={send}");

        queue.0.lock().await.tx = Some(tx);

        Ok(NotificationTransceiver {
            notification_rx: rx,
        })
    }
}

#[derive(Debug)]
pub struct NotificationQueue(Mutex<NotificationQueueInner>);
impl NotificationQueue {
    fn new() -> Self {
        Self(Mutex::new(NotificationQueueInner::new()))
    }
    pub async fn push(&self, b: NotificationBody, sent: bool) -> bool {
        self.0.lock().await.push(b, sent)
    }
    pub async fn get_next(&mut self) -> Option<NotificationEntry> {
        self.0.lock().await.get_next()
    }
}

pub struct NotificationQueueInner {
    last_sent: Option<NotificationBody>,
    queue: VecDeque<NotificationBody>,

    tx: Option<tokio::sync::broadcast::Sender<Option<NotificationBody>>>,
}
impl std::fmt::Debug for NotificationQueueInner {
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

impl NotificationQueueInner {
    fn new() -> Self {
        Self {
            last_sent: None,
            queue: VecDeque::new(),
            tx: None,
        }
    }

    #[must_use = "if true returned, you must set sent flag"]
    pub fn push(&mut self, b: NotificationBody, sent: bool) -> bool {
        if sent {
            if self.tx.is_some() {
                tracing::warn!("bug: sent notification pushed after tx registered");
            }
            if !self.queue.is_empty() {
                tracing::warn!("bug?: sent notification after not-sent one; discarding queue");
                self.queue.clear();
            }
            self.last_sent = Some(b);
            return false;
        }
        if let Some(tx) = self.tx.as_ref() {
            assert!(self.queue.is_empty());
            let sent = tx.send(Some(b.clone())).is_ok();
            if sent {
                tracing::debug!("queue: sse sent");
                self.last_sent = Some(b);
                return true;
            }
            tracing::debug!("notification queue tx dropped");
            self.tx = None;
        }
        self.last_sent = None;
        self.queue.push_back(b);
        false
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
    latest_status: RwLock<RideStatusEnum>,
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
    pub async fn set_latest_ride_status(&self, status: RideStatusEnum) {
        *self.latest_status.write().await = status;
    }
}

impl Repository {
    pub async fn do_matching(&self) {
        let (pairs, waiting, free) = {
            let mut waiting_rides = self.ride_cache.waiting_rides.lock().await;
            let waiting_rides_len = waiting_rides.len();
            let mut free_chairs = self.ride_cache.free_chairs_lv2.lock().await;
            let free_chairs_len = free_chairs.len();

            let mut chair_pos = HashMap::new();
            for chair in free_chairs.iter() {
                let loc = self.chair_location_get_latest(chair).await.unwrap();
                chair_pos.insert(chair.clone(), loc);
            }

            let matches = waiting_rides.len().min(free_chairs.len());

            struct Pair {
                chair_id: Id<Chair>,
                ride_id: Id<Ride>,
            }
            let mut pairs = vec![];

            for ride in waiting_rides.drain(0..matches) {
                let best = free_chairs
                    .iter()
                    .min_by_key(|cid| {
                        chair_pos
                            .get(cid)
                            .unwrap()
                            .map(|x| ride.pickup.distance(x))
                            .unwrap_or(9999999)
                    })
                    .unwrap()
                    .clone();
                let pos = free_chairs.iter().position(|x| x == &best).unwrap();
                free_chairs.swap_remove(pos);
                pairs.push(Pair {
                    chair_id: best,
                    ride_id: ride.id.clone(),
                });
            }
            (pairs, waiting_rides_len, free_chairs_len)
        };

        let mut fut = vec![];

        let pairs_len = pairs.len();
        for pair in pairs {
            let me = self.clone();
            let d = tokio::spawn(async move {
                me.rides_assign(&pair.ride_id, &pair.chair_id)
                    .await
                    .unwrap();
            });
            fut.push(d);
        }

        futures::future::join_all(fut).await;

        tracing::info!("waiting = {waiting:3}, free = {free:3}, matches = {pairs_len:3}",);
    }
}
