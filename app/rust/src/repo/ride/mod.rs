use crate::app_handlers::AppGetNearbyChairsResponseChair;
use crate::dl::DlMutex as Mutex;
use crate::dl::DlRwLock as RwLock;
use crate::FxHashMap as HashMap;
use crate::FxHashSet as HashSet;
use chrono::{DateTime, Utc};
use ride::RideDeferred;
use sqlx::MySql;
use sqlx::Pool;
use status::deferred::RideStatusDeferrable;
use std::{collections::VecDeque, sync::Arc};

use crate::{
    models::{Chair, Id, Ride, RideStatus, RideStatusEnum, User},
    Coordinate,
};

use super::deferred::UpdatableDeferred;
use super::{cache_init::CacheInit, Repository, Result};

#[allow(clippy::module_inception)]
mod ride;
mod status;

pub type RideCache = Arc<RideCacheInner>;

#[derive(Debug)]
pub struct RideCacheInner {
    ride_cache: RwLock<HashMap<Id<Ride>, Arc<RideEntry>>>,
    #[allow(clippy::type_complexity)]
    user_rides: RwLock<HashMap<Id<User>, RwLock<Vec<Arc<RideEntry>>>>>,

    waiting_rides: Mutex<VecDeque<(Arc<RideEntry>, Id<RideStatus>)>>,
    free_chairs_lv1: RwLock<HashSet<Id<Chair>>>,
    free_chairs_lv2: Mutex<HashSet<Id<Chair>>>,

    user_notification: RwLock<HashMap<Id<User>, NotificationQueue>>,
    chair_notification: RwLock<HashMap<Id<Chair>, NotificationQueue>>,

    ride_deferred: UpdatableDeferred<RideDeferred>,
    ride_status_deferred: UpdatableDeferred<RideStatusDeferrable>,

    matching_lock: Mutex<()>,
}

struct RideCacheInit {
    ride_cache: HashMap<Id<Ride>, Arc<RideEntry>>,
    user_rides: HashMap<Id<User>, RwLock<Vec<Arc<RideEntry>>>>,

    waiting_rides: VecDeque<(Arc<RideEntry>, Id<RideStatus>)>,
    free_chairs_lv1: HashSet<Id<Chair>>,
    free_chairs_lv2: HashSet<Id<Chair>>,

    user_notification: HashMap<Id<User>, NotificationQueue>,
    chair_notification: HashMap<Id<Chair>, NotificationQueue>,
}
impl RideCacheInit {
    async fn from_init(init: &mut CacheInit) -> Self {
        init.rides.sort_unstable_by_key(|x| x.created_at);
        init.ride_statuses.sort_unstable_by_key(|x| x.created_at);

        let mut statuses = HashMap::default();
        for status in &init.ride_statuses {
            statuses
                .entry(status.ride_id.clone())
                .or_insert_with(Vec::new)
                .push(status.clone());
        }

        //
        // status cache
        //
        let mut latest_ride_stat = HashMap::default();
        for stat in &init.ride_statuses {
            latest_ride_stat.insert(stat.ride_id.clone(), stat.status);
        }

        //
        // user_notification
        //
        let mut user_notification = HashMap::default();
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
        let mut chair_notification = HashMap::default();
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
        let mut rides = HashMap::default();
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
        let mut chair_movement_cache = HashMap::default();
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
                let status = statuses.get(&ride.id).unwrap();
                assert_eq!(status.len(), 1);
                assert_eq!(status[0].status, RideStatusEnum::Matching);
                let ride = rides.get(&ride.id).unwrap();
                waiting_rides.push_back((Arc::clone(ride), status[0].id.clone()));
            }
        }

        //
        // free_chairs
        //
        let mut free_chairs_lv1 = HashSet::default();
        let mut free_chairs_lv2 = HashSet::default();
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
                free_chairs_lv2.insert(chair.id.clone());
            }
        }

        //
        // user_rides
        //
        let mut user_rides = HashMap::default();
        for user in &init.users {
            user_rides.insert(user.id.clone(), RwLock::new(Vec::new()));
        }
        for ride in &init.rides {
            let r = Arc::clone(rides.get(&ride.id).unwrap());
            let c = user_rides.get(&ride.user_id).unwrap();
            c.write().await.push(r);
        }

        Self {
            ride_cache: rides,
            user_notification,
            chair_notification,
            waiting_rides,
            free_chairs_lv1,
            free_chairs_lv2,
            user_rides,
        }
    }
}

impl Repository {
    pub(super) async fn init_ride_cache(init: &mut CacheInit, pool: &Pool<MySql>) -> RideCache {
        let init = RideCacheInit::from_init(init).await;
        Arc::new(RideCacheInner {
            user_notification: RwLock::new(init.user_notification),
            chair_notification: RwLock::new(init.chair_notification),
            ride_cache: RwLock::new(init.ride_cache),
            waiting_rides: Mutex::new(init.waiting_rides),
            free_chairs_lv1: RwLock::new(init.free_chairs_lv1),
            free_chairs_lv2: Mutex::new(init.free_chairs_lv2),
            user_rides: RwLock::new(init.user_rides),
            ride_deferred: UpdatableDeferred::new(pool),
            ride_status_deferred: UpdatableDeferred::new(pool),
            matching_lock: Mutex::new(()),
        })
    }
    pub(super) async fn reinit_ride_cache(&self, init: &mut CacheInit) {
        let init = RideCacheInit::from_init(init).await;

        let RideCacheInner {
            ride_cache,
            user_rides,
            user_notification,
            chair_notification,
            waiting_rides,
            free_chairs_lv1,
            free_chairs_lv2,
            ride_deferred: _,
            ride_status_deferred: _,
            matching_lock: _,
        } = &*self.ride_cache;

        let mut r = ride_cache.write().await;
        let mut u = user_notification.write().await;
        let mut c = chair_notification.write().await;
        let mut f1 = free_chairs_lv1.write().await;
        let mut f2 = free_chairs_lv2.lock().await;
        let mut w = waiting_rides.lock().await;
        let mut ur = user_rides.write().await;

        *r = init.ride_cache;
        *u = init.user_notification;
        *c = init.chair_notification;
        *f1 = init.free_chairs_lv1;
        *f2 = init.free_chairs_lv2;
        *w = init.waiting_rides;
        *ur = init.user_rides;
    }
}

impl Repository {
    pub async fn chair_huifhiubher(
        &self,
        coord: Coordinate,
        dist: i32,
    ) -> Result<Vec<AppGetNearbyChairsResponseChair>> {
        let cache = self.ride_cache.free_chairs_lv1.read().await;
        let mut res = vec![];
        for chair in cache.iter() {
            let Some(chair_coord) = self.chair_location_get_latest(chair).await? else {
                continue;
            };
            if coord.distance(chair_coord) <= dist {
                let chair = self.chair_get_by_id_effortless(None, chair).await?.unwrap();
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
            cache.insert(id.clone());
            cache.len()
        };
        if len == 10 {
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
        self.user_rides
            .write()
            .await
            .insert(id.clone(), RwLock::new(vec![]));
    }
    pub async fn on_chair_add(&self, id: &Id<Chair>) {
        self.chair_notification
            .write()
            .await
            .insert(id.clone(), NotificationQueue::new());
    }
    pub async fn on_chair_status_change(&self, id: &Id<Chair>, on_duty: bool) {
        let mut cache = self.free_chairs_lv1.write().await;
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
        let cache = self.ride_cache.chair_notification.read().await;
        let queue = cache.get(id).unwrap();

        let (tx, rx) = tokio::sync::broadcast::channel(8);

        let next = queue.get_next().await;
        tx.send(next.clone().map(|x| x.body)).unwrap();
        let mut send = 1;

        if let Some(mut next) = next {
            while !next.sent {
                self.ride_status_chair_notified(&next.body.ride_status_id)
                    .await?;
                next = queue.get_next().await.unwrap();
                tx.send(Some(next.body.clone())).unwrap();
                send += 1;
            }
        }

        // 嘘、notification inner がそれを勝手にやっているはず
        // if queue.last_sent.status == Completed { push_to_v2 }

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
        let cache = self.ride_cache.user_notification.read().await;
        let queue = cache.get(id).unwrap();

        let (tx, rx) = tokio::sync::broadcast::channel(8);

        let next = queue.get_next().await;
        tx.send(next.clone().map(|x| x.body)).unwrap();
        let mut send = 1;

        if let Some(mut next) = next {
            while !next.sent {
                self.ride_status_app_notified(&next.body.ride_status_id)
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
    pub async fn get_next(&self) -> Option<NotificationEntry> {
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
            let _lock = self.ride_cache.matching_lock.lock().await;

            let mut waiting_rides = self.ride_cache.waiting_rides.lock().await;
            let waiting_rides_len = waiting_rides.len();
            let mut free_chairs = self.ride_cache.free_chairs_lv2.lock().await;
            let free_chairs_len = free_chairs.len();
            let chair_speed_cache = self.chair_model_cache.speed.read().await;

            // stupid
            let mut chair_pos = HashMap::default();
            for chair in free_chairs.iter() {
                let loc = self.chair_location_get_latest(chair).await.unwrap();
                chair_pos.insert(chair.clone(), loc);
            }
            let mut chair_speed = HashMap::default();
            for chair in free_chairs.iter() {
                let c = self
                    .chair_get_by_id_effortless(None, chair)
                    .await
                    .unwrap()
                    .unwrap();
                let speed: i32 = *chair_speed_cache.get(&c.model).unwrap();
                chair_speed.insert(chair.clone(), speed);
            }

            let matches = waiting_rides.len().min(free_chairs.len());

            struct Pair {
                chair_id: Id<Chair>,
                ride_id: Id<Ride>,
                status_id: Id<RideStatus>,
            }
            let mut pairs = vec![];

            for (ride, status_id) in waiting_rides.drain(0..matches) {
                let best = free_chairs
                    .iter()
                    .min_by_key(|cid| {
                        let Some(chair_pos) = chair_pos.get(cid).unwrap() else {
                            return 99999999;
                        };
                        let travel_distance = chair_pos.distance(ride.pickup);
                        // + ride.pickup.distance(ride.destination);
                        let speed = chair_speed.get(cid).unwrap();
                        travel_distance / speed
                    })
                    .unwrap()
                    .clone();
                free_chairs.remove(&best);
                pairs.push(Pair {
                    chair_id: best,
                    ride_id: ride.id.clone(),
                    status_id,
                });
            }
            (pairs, waiting_rides_len, free_chairs_len)
        };

        let mut fut = vec![];

        let pairs_len = pairs.len();
        for pair in pairs {
            let me = self.clone();
            let d = tokio::spawn(async move {
                me.rides_assign(&pair.ride_id, &pair.status_id, &pair.chair_id)
                    .await
                    .unwrap();
            });
            fut.push(d);
        }

        futures::future::join_all(fut).await;

        tracing::debug!("waiting = {waiting:3}, free = {free:3}, matches = {pairs_len:3}",);
    }
}
