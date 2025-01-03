use crate::app_handlers::AppGetNearbyChairsResponseChair;
use crate::dl::DlSyncMutex;
use crate::dl::DlSyncRwLock;
use crate::ConcurrentHashMap;
use crate::ConcurrentHashSet;
use crate::HashMap;
use chrono::{DateTime, Utc};
use ride::RideDeferred;
use sqlx::MySql;
use sqlx::Pool;
use status::deferred::RideStatusDeferrable;
use std::sync::atomic::AtomicBool;
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
    ride_cache: DlSyncRwLock<ConcurrentHashMap<Id<Ride>, Arc<RideEntry>>>,
    user_rides: DlSyncRwLock<ConcurrentHashMap<Id<User>, DlSyncRwLock<Vec<Arc<RideEntry>>>>>,

    waiting_rides: DlSyncMutex<VecDeque<(Arc<RideEntry>, Id<RideStatus>)>>,
    free_chairs_lv1: DlSyncRwLock<ConcurrentHashSet<Id<Chair>>>,
    free_chairs_lv2: DlSyncMutex<Vec<Id<Chair>>>,

    user_has_ride: DlSyncRwLock<ConcurrentHashMap<Id<User>, AtomicBool>>,

    user_notification: DlSyncRwLock<ConcurrentHashMap<Id<User>, NotificationQueue>>,
    chair_notification: DlSyncRwLock<ConcurrentHashMap<Id<Chair>, NotificationQueue>>,

    ride_deferred: UpdatableDeferred<RideDeferred>,
    ride_status_deferred: UpdatableDeferred<RideStatusDeferrable>,

    matching_lock: DlSyncMutex<()>,
}

struct RideCacheInit {
    ride_cache: ConcurrentHashMap<Id<Ride>, Arc<RideEntry>>,
    user_rides: ConcurrentHashMap<Id<User>, DlSyncRwLock<Vec<Arc<RideEntry>>>>,

    waiting_rides: VecDeque<(Arc<RideEntry>, Id<RideStatus>)>,
    free_chairs_lv1: ConcurrentHashSet<Id<Chair>>,
    free_chairs_lv2: Vec<Id<Chair>>,

    user_has_ride: ConcurrentHashMap<Id<User>, AtomicBool>,

    user_notification: ConcurrentHashMap<Id<User>, NotificationQueue>,
    chair_notification: ConcurrentHashMap<Id<Chair>, NotificationQueue>,
}
impl RideCacheInit {
    fn from_init(init: &mut CacheInit) -> Self {
        init.rides.sort_unstable_by_key(|x| x.created_at);
        init.ride_statuses.sort_unstable_by_key(|x| x.created_at);

        let statuses = ConcurrentHashMap::default();
        for status in &init.ride_statuses {
            statuses
                .entry(status.ride_id)
                .or_insert_with(Vec::new)
                .push(status.clone());
        }

        //
        // status cache
        //
        let latest_ride_stat = ConcurrentHashMap::default();
        for stat in &init.ride_statuses {
            latest_ride_stat.insert(stat.ride_id, stat.status);
        }

        //
        // user_notification
        //
        let user_notification = ConcurrentHashMap::default();
        for user in &init.users {
            user_notification.insert(user.id, NotificationQueue::new());
        }
        for ride in &init.rides {
            let queue = user_notification.get_mut(&ride.user_id).unwrap();
            let st = statuses.get(&ride.id).unwrap();
            for status in st.iter() {
                let b = NotificationBody {
                    ride_id: ride.id,
                    ride_status_id: status.id,
                    status: status.status,
                };
                let mark_sent = queue.push(b, status.app_sent_at.is_some());
                assert!(!mark_sent);
            }
        }

        //
        // chair_notification
        //
        let chair_notification = ConcurrentHashMap::default();
        for chair in &init.chairs {
            chair_notification.insert(chair.id, NotificationQueue::new());
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
                    ride_id: ride.id,
                    ride_status_id: status.id,
                    status: status.status,
                };
                let mark_sent = queue.push(b, status.chair_sent_at.is_some());
                assert!(!mark_sent);
            }
        }

        //
        // rides
        //
        let rides = ConcurrentHashMap::default();
        for ride in &init.rides {
            rides.insert(
                ride.id,
                Arc::new(RideEntry {
                    id: ride.id,
                    user_id: ride.user_id,
                    pickup: ride.pickup_coord(),
                    destination: ride.destination_coord(),
                    created_at: ride.created_at,
                    chair_id: DlSyncRwLock::new(ride.chair_id),
                    evaluation: DlSyncRwLock::new(ride.evaluation),
                    updated_at: DlSyncRwLock::new(ride.updated_at),
                    latest_status: DlSyncRwLock::new(*latest_ride_stat.get(&ride.id).unwrap()),
                }),
            );
        }

        //
        // chair_movement_cache
        //
        let mut chair_movement_cache = HashMap::default();
        let user_has_ride = ConcurrentHashMap::default();
        for ride in &init.rides {
            let chair_id = ride.chair_id.as_ref();
            let ride = rides.get(&ride.id).unwrap();
            for status in statuses.get(&ride.id).unwrap().iter() {
                match status.status {
                    RideStatusEnum::Matching => {
                        user_has_ride
                            .entry(ride.user_id)
                            .or_insert_with(|| AtomicBool::new(true))
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                    RideStatusEnum::Enroute => {
                        chair_movement_cache.insert(chair_id.unwrap(), Arc::clone(&*ride));
                    }
                    RideStatusEnum::Pickup => {
                        chair_movement_cache.remove(chair_id.unwrap()).unwrap();
                    }
                    RideStatusEnum::Carrying => {
                        chair_movement_cache.insert(chair_id.unwrap(), Arc::clone(&*ride));
                    }
                    RideStatusEnum::Arrived => {
                        chair_movement_cache.remove(chair_id.unwrap()).unwrap();
                    }
                    RideStatusEnum::Completed => {
                        user_has_ride
                            .get(&ride.user_id)
                            .unwrap()
                            .store(false, std::sync::atomic::Ordering::Relaxed);
                    }
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
                waiting_rides.push_back((Arc::clone(&*ride), status[0].id));
            }
        }

        //
        // free_chairs
        //
        let free_chairs_lv1 = ConcurrentHashSet::default();
        let mut free_chairs_lv2 = Vec::default();
        for chair in &init.chairs {
            let n = chair_notification.get(&chair.id).unwrap();
            let n = n.0.lock();
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
                free_chairs_lv1.insert(chair.id);
            }

            // active かつ最後の通知が Completed でそれが送信済みであればいい
            let lv2 = chair.is_active
                && n.queue.is_empty()
                && n.last_sent
                    .as_ref()
                    .is_none_or(|x| x.status == RideStatusEnum::Completed);
            if lv2 {
                free_chairs_lv2.push(chair.id);
            }
        }

        //
        // user_rides
        //
        let user_rides = ConcurrentHashMap::default();
        for user in &init.users {
            user_rides.insert(user.id, DlSyncRwLock::new(Vec::new()));
        }
        for ride in &init.rides {
            let r = Arc::clone(&*rides.get(&ride.id).unwrap());
            let c = user_rides.get(&ride.user_id).unwrap();
            c.write().push(r);
        }

        Self {
            ride_cache: rides,
            user_notification,
            chair_notification,
            waiting_rides,
            free_chairs_lv1,
            free_chairs_lv2,
            user_rides,
            user_has_ride,
        }
    }
}

impl Repository {
    pub(super) fn init_ride_cache(init: &mut CacheInit, pool: &Pool<MySql>) -> RideCache {
        let init = RideCacheInit::from_init(init);
        Arc::new(RideCacheInner {
            user_notification: DlSyncRwLock::new(init.user_notification),
            chair_notification: DlSyncRwLock::new(init.chair_notification),
            ride_cache: DlSyncRwLock::new(init.ride_cache),
            waiting_rides: DlSyncMutex::new(init.waiting_rides),
            free_chairs_lv1: DlSyncRwLock::new(init.free_chairs_lv1),
            free_chairs_lv2: DlSyncMutex::new(init.free_chairs_lv2),
            user_rides: DlSyncRwLock::new(init.user_rides),
            ride_deferred: UpdatableDeferred::new(pool),
            ride_status_deferred: UpdatableDeferred::new(pool),
            matching_lock: DlSyncMutex::new(()),
            user_has_ride: DlSyncRwLock::new(init.user_has_ride),
        })
    }
    pub(super) fn reinit_ride_cache(&self, init: &mut CacheInit) {
        let init = RideCacheInit::from_init(init);

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
            user_has_ride,
        } = &*self.ride_cache;

        let mut r = ride_cache.write();
        let mut u = user_notification.write();
        let mut c = chair_notification.write();
        let mut f2 = free_chairs_lv2.lock();
        let mut w = waiting_rides.lock();
        let mut ur = user_rides.write();

        *r = init.ride_cache;
        *u = init.user_notification;
        *c = init.chair_notification;
        *f2 = init.free_chairs_lv2;
        *w = init.waiting_rides;
        *ur = init.user_rides;
        *free_chairs_lv1.write() = init.free_chairs_lv1;
        *user_has_ride.write() = init.user_has_ride;
    }
}

impl Repository {
    pub fn chair_huifhiubher(
        &self,
        coord: Coordinate,
        dist: i32,
    ) -> Result<Vec<AppGetNearbyChairsResponseChair>> {
        let free_chair_cache = self.ride_cache.free_chairs_lv1.read();
        let loc_cache = self.chair_location_cache.cache.read();
        let chair_cache = self.chair_cache.by_id.read();

        let mut res = vec![];
        for chair in free_chair_cache.iter() {
            let Some(chair_coord) = loc_cache.get(&*chair) else {
                continue;
            };
            let chair_coord = chair_coord.latest();
            if chair_coord.distance(coord) <= dist {
                let chair = chair_cache.get(&*chair).unwrap();
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

crate::conf_env!(static MATCHING_CHAIR_THRESHOLD: usize = {
    from: "MATCHING_CHAIR_THRESHOLD",
    default: "100",
});

impl Repository {
    pub fn push_free_chair(&self, id: Id<Chair>) {
        let len = {
            let mut cache = self.ride_cache.free_chairs_lv2.lock();
            cache.push(id);
            cache.len()
        };
        if len == *MATCHING_CHAIR_THRESHOLD {
            let me = self.clone();
            tokio::spawn(async move {
                me.do_matching();
            });
        }
    }
}

impl RideCacheInner {
    pub fn on_user_add(&self, id: Id<User>) {
        self.user_notification
            .read()
            .insert(id, NotificationQueue::new());
        self.user_rides.read().insert(id, DlSyncRwLock::new(vec![]));
    }
    pub fn on_chair_add(&self, id: Id<Chair>) {
        self.chair_notification
            .read()
            .insert(id, NotificationQueue::new());
    }
    pub fn on_chair_status_change(&self, id: Id<Chair>, on_duty: bool) {
        let cache = self.free_chairs_lv1.read();
        if on_duty {
            cache.remove(&id);
        } else {
            cache.insert(id);
        }
    }
}

pub struct NotificationTransceiver {
    /// received notification must be sent (W)
    pub notification_rx: tokio::sync::broadcast::Receiver<Option<NotificationBody>>,
}

impl Repository {
    pub fn chair_get_next_notification_sse(
        &self,
        id: Id<Chair>,
    ) -> Result<NotificationTransceiver> {
        let cache = self.ride_cache.chair_notification.read();
        let queue = cache.get(&id).unwrap();

        let (tx, rx) = tokio::sync::broadcast::channel(8);

        let next = queue.get_next();
        tx.send(next.clone().map(|x| x.body)).unwrap();

        if let Some(mut next) = next {
            while !next.sent {
                self.ride_status_chair_notified(next.body.ride_status_id);
                next = queue.get_next().unwrap();
                tx.send(Some(next.body.clone())).unwrap();
            }
        }

        queue.0.lock().tx = Some(tx);

        Ok(NotificationTransceiver {
            notification_rx: rx,
        })
    }

    pub fn user_get_next_notification_sse(&self, id: Id<User>) -> Result<NotificationTransceiver> {
        let cache = self.ride_cache.user_notification.read();
        let queue = cache.get(&id).unwrap();

        let (tx, rx) = tokio::sync::broadcast::channel(8);

        let next = queue.get_next();
        tx.send(next.clone().map(|x| x.body)).unwrap();

        if let Some(mut next) = next {
            while !next.sent {
                self.ride_status_app_notified(next.body.ride_status_id);
                next = queue.get_next().unwrap();
                tx.send(Some(next.body.clone())).unwrap();
            }
        }

        queue.0.lock().tx = Some(tx);

        Ok(NotificationTransceiver {
            notification_rx: rx,
        })
    }
}

#[derive(Debug)]
pub struct NotificationQueue(DlSyncMutex<NotificationQueueInner>);
impl NotificationQueue {
    fn new() -> Self {
        Self(DlSyncMutex::new(NotificationQueueInner::new()))
    }
    pub fn push(&self, b: NotificationBody, sent: bool) -> bool {
        self.0.lock().push(b, sent)
    }
    pub fn get_next(&self) -> Option<NotificationEntry> {
        self.0.lock().get_next()
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

    #[must_use = "if returned true, you must set sent flag"]
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
                self.last_sent = Some(b);
                return true;
            }
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

    chair_id: DlSyncRwLock<Option<Id<Chair>>>,
    evaluation: DlSyncRwLock<Option<i32>>,
    updated_at: DlSyncRwLock<DateTime<Utc>>,
    latest_status: DlSyncRwLock<RideStatusEnum>,
}
impl RideEntry {
    pub fn ride(&self) -> Ride {
        Ride {
            id: self.id,
            user_id: self.user_id,
            chair_id: *self.chair_id.read(),
            pickup_latitude: self.pickup.latitude,
            pickup_longitude: self.pickup.longitude,
            destination_latitude: self.destination.latitude,
            destination_longitude: self.destination.longitude,
            evaluation: *self.evaluation.read(),
            created_at: self.created_at,
            updated_at: *self.updated_at.read(),
        }
    }
    pub fn set_chair_id(&self, chair_id: Id<Chair>, now: DateTime<Utc>) {
        *self.chair_id.write() = Some(chair_id);
        *self.updated_at.write() = now;
    }
    pub fn set_evaluation(&self, eval: i32, now: DateTime<Utc>) {
        *self.evaluation.write() = Some(eval);
        *self.updated_at.write() = now;
    }
    pub fn set_latest_ride_status(&self, status: RideStatusEnum) {
        *self.latest_status.write() = status;
    }
}

impl Repository {
    pub fn do_matching(&self) {
        let (pairs, waiting, free, (s1, s2)) = {
            let _lock = self.ride_cache.matching_lock.lock();

            let free_chairs = {
                let mut c = self.ride_cache.free_chairs_lv2.lock();
                std::mem::take(&mut *c)
            };

            let free_chairs_len = free_chairs.len();

            let (waiting_rides_len, waiting_rides) = {
                let mut l = self.ride_cache.waiting_rides.lock();
                (l.len(), std::mem::take(&mut *l))
            };
            let chair_speed_cache = self.chair_model_cache.speed.read();

            struct AvailableChair {
                speed: i32,
                chair: Id<Chair>,
                coord: Coordinate,
            }

            let mut avail = vec![];

            // stupid
            for chair in free_chairs {
                let Some(loc) = self.chair_location_get_latest(chair).unwrap() else {
                    continue;
                };
                let chair = self.chair_get_by_id_effortless(chair).unwrap().unwrap();
                let speed: i32 = *chair_speed_cache.get(&chair.model).unwrap();
                avail.push(AvailableChair {
                    speed,
                    chair: chair.id,
                    coord: loc,
                });
            }

            struct Pair {
                chair_id: Id<Chair>,
                ride_id: Id<Ride>,
                status_id: Id<RideStatus>,
            }
            let mut pairs = vec![];
            let mut redu = vec![];
            let mut stage1 = 0;

            for (ride, status_id) in waiting_rides {
                let Some((best_index, best)) = avail
                    .iter()
                    .enumerate()
                    .filter(|(_i, x)| x.coord.distance(ride.pickup) < 10 * x.speed)
                    .max_by_key(|(_i, x)| x.coord.distance(ride.pickup))
                else {
                    redu.push((ride, status_id));
                    continue;
                };
                let chair = best.chair;
                avail.remove(best_index);
                pairs.push(Pair {
                    chair_id: chair,
                    ride_id: ride.id,
                    status_id,
                });
                stage1 += 1;
            }

            let mut redu2 = vec![];

            let mut stage2 = 0;
            for (ride, status_id) in redu {
                let Some((best_index, best)) = avail
                    .iter()
                    .enumerate()
                    // しょうがないので最も利益を生むライドにアサイン・・・と行きたいところだが
                    // さっさと初期のライドは終了して、評判で次のライドを爆発的に増やしたいのでサービス
                    // .filter(|(_i, x)| x.coord.distance(ride.pickup) < 10 * x.speed)
                    .min_by_key(|(_i, x)| x.coord.distance(ride.pickup))
                else {
                    redu2.push((ride, status_id));
                    continue;
                };
                let chair = best.chair;
                avail.remove(best_index);
                pairs.push(Pair {
                    chair_id: chair,
                    ride_id: ride.id,
                    status_id,
                });
                stage2 += 1;
            }

            if !redu2.is_empty() {
                let mut c = self.ride_cache.waiting_rides.lock();
                c.extend(redu2);
            }
            if !avail.is_empty() {
                let mut c = self.ride_cache.free_chairs_lv2.lock();
                c.extend(avail.into_iter().map(|x| x.chair));
            }

            (pairs, waiting_rides_len, free_chairs_len, (stage1, stage2))
        };

        let pairs_len = pairs.len();
        for pair in pairs {
            self.rides_assign(pair.ride_id, pair.status_id, pair.chair_id)
                .unwrap();
        }

        tracing::info!(
            "waiting={waiting:3}, free={free:3}, matches={pairs_len:3}, stage1={s1}, stage2={s2}",
        );
    }
}
