use crate::app_handlers::AppGetNearbyChairsResponseChair;
use crate::dl::DlSyncMutex;
use crate::dl::DlSyncRwLock;
use crate::ConcurrentHashMap;
use crate::ConcurrentSymbolMap;
use crate::HashMap;
use chrono::TimeDelta;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use pathfinding::matrix::Matrix;
use ride::RideDeferred;
use sqlx::MySql;
use sqlx::Pool;
use status::deferred::RideStatusDeferrable;
use std::sync::atomic::AtomicBool;
use std::sync::LazyLock;
use std::time::Duration;
use std::time::Instant;
use std::{collections::VecDeque, sync::Arc};

use crate::{
    models::{Chair, Id, Ride, RideStatus, RideStatusEnum, User},
    Coordinate,
};

use super::chair::ChairCache;
use super::chair::ChairEntry;
use super::deferred::UpdatableDeferred;
use super::{cache_init::CacheInit, Repository, Result};

#[allow(clippy::module_inception)]
mod ride;
mod status;

pub type RideCache = Arc<RideCacheInner>;

#[derive(Debug)]
pub struct RideCacheInner {
    ride_cache: DlSyncRwLock<ConcurrentSymbolMap<Id<Ride>, Arc<RideEntry>>>,
    user_rides: DlSyncRwLock<ConcurrentSymbolMap<Id<User>, DlSyncRwLock<Vec<Arc<RideEntry>>>>>,

    waiting_rides: DlSyncMutex<Vec<(Arc<RideEntry>, Id<RideStatus>)>>,
    free_chairs_lv1: DlSyncRwLock<ConcurrentHashMap<Id<Chair>, Arc<ChairEntry>>>,
    free_chairs_lv2: DlSyncMutex<Vec<Id<Chair>>>,

    user_has_ride: DlSyncRwLock<ConcurrentSymbolMap<Id<User>, AtomicBool>>,

    user_notification: DlSyncRwLock<ConcurrentSymbolMap<Id<User>, NotificationQueue>>,
    chair_notification: DlSyncRwLock<ConcurrentSymbolMap<Id<Chair>, NotificationQueue>>,

    ride_deferred: UpdatableDeferred<RideDeferred>,
    ride_status_deferred: UpdatableDeferred<RideStatusDeferrable>,

    matching_lock: DlSyncMutex<()>,
}

struct RideCacheInit {
    ride_cache: ConcurrentSymbolMap<Id<Ride>, Arc<RideEntry>>,
    user_rides: ConcurrentSymbolMap<Id<User>, DlSyncRwLock<Vec<Arc<RideEntry>>>>,

    waiting_rides: Vec<(Arc<RideEntry>, Id<RideStatus>)>,
    free_chairs_lv1: ConcurrentHashMap<Id<Chair>, Arc<ChairEntry>>,
    free_chairs_lv2: Vec<Id<Chair>>,

    user_has_ride: ConcurrentSymbolMap<Id<User>, AtomicBool>,

    user_notification: ConcurrentSymbolMap<Id<User>, NotificationQueue>,
    chair_notification: ConcurrentSymbolMap<Id<Chair>, NotificationQueue>,
}
impl RideCacheInit {
    fn from_init(init: &mut CacheInit, chair_cache: &ChairCache) -> Self {
        init.rides.sort_unstable_by_key(|x| x.created_at);
        init.ride_statuses.sort_unstable_by_key(|x| x.created_at);

        let statuses = ConcurrentSymbolMap::default();
        for status in &init.ride_statuses {
            statuses
                .entry(status.ride_id)
                .or_insert_with(Vec::new)
                .push(status.clone());
        }

        //
        // status cache
        //
        let latest_ride_stat = ConcurrentSymbolMap::default();
        for stat in &init.ride_statuses {
            latest_ride_stat.insert(stat.ride_id, stat.status);
        }

        //
        // user_notification
        //
        let user_notification = ConcurrentSymbolMap::default();
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
        let chair_notification = ConcurrentSymbolMap::default();
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
        let rides = ConcurrentSymbolMap::default();
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
        let user_has_ride = ConcurrentSymbolMap::default();
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
        let mut waiting_rides = Vec::new();
        for ride in &init.rides {
            if ride.chair_id.is_none() {
                let status = statuses.get(&ride.id).unwrap();
                assert_eq!(status.len(), 1);
                assert_eq!(status[0].status, RideStatusEnum::Matching);
                let ride = rides.get(&ride.id).unwrap();
                waiting_rides.push((Arc::clone(&*ride), status[0].id));
            }
        }

        //
        // free_chairs
        //
        let free_chairs_lv1 = ConcurrentHashMap::default();
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
                let chair_cache = chair_cache.by_id.read();
                let chair_entry = chair_cache.get(&chair.id).unwrap();
                free_chairs_lv1.insert(chair.id, Arc::clone(chair_entry));
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
        let user_rides = ConcurrentSymbolMap::default();
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
    pub(super) fn init_ride_cache(
        init: &mut CacheInit,
        pool: &Pool<MySql>,
        chair_cache: &ChairCache,
    ) -> RideCache {
        let init = RideCacheInit::from_init(init, chair_cache);
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
    pub(super) fn reinit_ride_cache(&self, init: &mut CacheInit, chair_cache: &ChairCache) {
        let init = RideCacheInit::from_init(init, chair_cache);

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
    pub fn chair_nearby(
        &self,
        coord: Coordinate,
        distance: i32,
    ) -> Result<Vec<AppGetNearbyChairsResponseChair>> {
        let free_chair_cache = self.ride_cache.free_chairs_lv1.read();

        let mut res = Vec::with_capacity(free_chair_cache.len());
        for chair in free_chair_cache.iter() {
            let Some(chair_coord) = chair.loc.latest() else {
                continue;
            };
            if chair_coord.distance(coord) <= distance {
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
    pub fn push_free_chair(&self, id: Id<Chair>) {
        let mut cache = self.ride_cache.free_chairs_lv2.lock();
        cache.push(id);
    }

    pub fn on_chair_became_free(&self, chair: Id<Chair>) {
        let chair_cache = self.chair_cache.by_id.read();
        let chair_entry = chair_cache.get(&chair).unwrap();
        let cache = self.ride_cache.free_chairs_lv1.read();
        cache.insert(chair, chair_entry.clone());
    }
    pub fn on_chair_went_on_duty(&self, id: Id<Chair>) {
        let cache = self.ride_cache.free_chairs_lv1.read();
        cache.remove(&id);
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

#[derive(Clone)]
struct AvailableChair {
    speed: i32,
    chair: Id<Chair>,
    coord: Coordinate,
}

struct Pair {
    chair_id: Id<Chair>,
    ride_id: Id<Ride>,
    status_id: Id<RideStatus>,
    score: i32,
}
impl std::fmt::Debug for Pair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "chair{:?} <-{}-> ride{:?}",
            self.chair_id, self.score, self.ride_id
        )
    }
}

struct MatchingLimiter {
    history: VecDeque<(Instant, usize)>,
}
impl MatchingLimiter {
    fn new() -> Self {
        Self {
            history: VecDeque::new(),
        }
    }
    fn push(&mut self, count: usize) {
        self.history
            .push_back((Instant::now() + Self::ONESEC, count));
    }
    const ONESEC: Duration = Duration::from_secs(1);
    fn remain(&mut self) -> usize {
        let now = Instant::now();
        while let Some(&(expires, _)) = self.history.front() {
            if expires > now {
                break;
            }
            self.history.pop_front();
        }

        crate::conf_env! {
            static MATCH_LIMIT_PER_SEC: usize = {
                from: "MATCHING_LIMIT_PER_SEC",
                default: "500",
            }
        }
        let limit = *MATCH_LIMIT_PER_SEC;
        let count: usize = self.history.iter().map(|&(_at, count)| count).sum();
        limit.saturating_sub(count)
    }
    fn until_next_release(&self) -> Option<Duration> {
        let &(expires, _) = self.history.front()?;
        let diff = expires - Instant::now();
        if diff.is_zero() {
            return Some(Duration::from_millis(5));
        }
        Some(diff)
    }
}

// これを最小化する
crate::conf_env! {
    static SCORE_TARGET: i32 = {
        from: "SCORE_TARGET",
        default: "125",
    }
}
#[inline(always)]
fn score_target(chair: &AvailableChair, ride: &RideEntry, target: i32) -> i32 {
    score(chair, ride).abs_diff(target) as i32
}
#[inline(always)]
fn score(chair: &AvailableChair, ride: &RideEntry) -> i32 {
    let pickup_distance = chair.coord.distance(ride.pickup);
    let distance = pickup_distance * 10 + ride.pickup.distance(ride.destination);
    distance / chair.speed
}

type Workers = Vec<AvailableChair>;
type Jobs = Vec<(Arc<RideEntry>, Id<RideStatus>)>;

fn solve_pathfinding_isekai_joucho(
    workers: Workers,
    jobs: Jobs,
) -> (Workers, Jobs, Vec<Pair>, i32, bool) {
    // region を分けてその中でマッチングを行う
    // region 分けた方が多分 O(n^3)の n が減って計算早くなると思う

    const REGION_A: Coordinate = Coordinate {
        latitude: 0,
        longitude: 0,
    };
    const REGION_B: Coordinate = Coordinate {
        latitude: 300,
        longitude: 300,
    };

    let mut split_workers = (vec![], vec![]);
    for w in workers {
        if w.coord.distance(REGION_A) > w.coord.distance(REGION_B) {
            split_workers.1.push(w);
        } else {
            split_workers.0.push(w);
        }
    }

    let mut split_jobs = (vec![], vec![]);
    for j in jobs {
        if j.0.pickup.distance(REGION_A) > j.0.pickup.distance(REGION_B) {
            split_jobs.1.push(j);
        } else {
            split_jobs.0.push(j);
        }
    }

    let (mut rw, mut rj, mut p, score, lim) =
        solve_pathfinding_kaf(split_workers.0, split_jobs.0, true);
    let (rw2, rj2, p2, score2, lim2) = solve_pathfinding_kaf(split_workers.1, split_jobs.1, false);

    rw.extend(rw2);
    rj.extend(rj2);
    p.extend(p2);

    (rw, rj, p, score + score2, lim || lim2)
}

static LIMITER: LazyLock<Mutex<MatchingLimiter>> =
    LazyLock::new(|| Mutex::new(MatchingLimiter::new()));

fn solve_pathfinding_kaf(
    workers: Workers,
    jobs: Jobs,
    use_half: bool,
) -> (Workers, Jobs, Vec<Pair>, i32, bool) {
    if workers.is_empty() || jobs.is_empty() {
        return (workers, jobs, vec![], 0, false);
    }

    let jobs_len = jobs.len();
    let workers_len = workers.len();

    // 緊急のやつだけ先にやる
    let mut urgent_jobs = vec![];
    let mut ok_jobs = vec![];
    let now = Utc::now();

    // const ONE_TICK_DURATION: Duration = Duration::from_millis(30);
    // const SCORE1_TICK_COUNT: usize = 100

    crate::conf_env!(static URGENT_THRESHOLD_MS: i64 = {
        from: "URGENT_THRESHOLD_MS",
        default: "20000",
    });
    let urgent_threshold = TimeDelta::milliseconds(*URGENT_THRESHOLD_MS);

    for task in jobs {
        let elapsed = now - task.0.created_at;
        if elapsed > urgent_threshold {
            urgent_jobs.push(task);
        } else {
            ok_jobs.push(task);
        }
    }

    let urgent_len = urgent_jobs.len();
    let ok_len = ok_jobs.len();

    // 逃すとゲームオーバーなので urgent は何がなんでもマッチングする
    let (mut remain_workers, rj, p, score) = solve_pathfinding_rim(workers, urgent_jobs);

    ok_jobs.extend(rj);

    let remain = {
        let r = LIMITER.lock().remain();
        if use_half {
            r / 2
        } else {
            r
        }
    };

    // urgent でないものは limiter の制限を受ける
    // work と job の少ない方のどちらかが全数マッチングされるから、
    // 小さい方を limiter の値まで絞る。
    let matches = remain_workers.len().min(ok_jobs.len());

    let mut rw3 = vec![];
    let mut rj3 = vec![];

    let mut hit_limiter = true;
    let limiter_log;
    if matches <= remain {
        limiter_log = "nohit";
        hit_limiter = false;
    } else if ok_jobs.len() > remain_workers.len() {
        // job の方が多いので worker を limiter に合わせる。
        // これが圧倒的に多いはず
        limiter_log = "-worker";
        rw3.extend(remain_workers.drain(remain..));
    } else if ok_jobs.len() <= remain_workers.len() {
        // worker の方が多いので jobs を limiter に合わせる
        // なかなかないはず。。
        limiter_log = "-job";
        rj3.extend(ok_jobs.drain(remain..));
    } else {
        unreachable!()
    }

    let (mut rw2, mut rj2, mut p2, score2) = solve_pathfinding_rim(remain_workers, ok_jobs);

    rj2.extend(rj3);
    rw2.extend(rw3);
    p2.extend(p);

    let pairs_len = p2.len();

    tracing::debug!(
        "a={use_half:5}: jobs={jobs_len:5}, workers={workers_len:4}, limiter_remain={remain:4}({limiter_log:7>}), ok:urg={ok_len:5}:{urgent_len:3}, pairs={pairs_len}",
    );
    LIMITER.lock().push(p2.len());

    (rw2, rj2, p2, score + score2, hit_limiter)
}

fn solve_pathfinding_rim(mut workers: Workers, mut jobs: Jobs) -> (Workers, Jobs, Vec<Pair>, i32) {
    if workers.is_empty() || jobs.is_empty() {
        return (workers, jobs, vec![], 0);
    }

    // 横に長いのは OK なので横に椅子(worker)をおく
    // 椅子のほうが多い分にはassignment問題として適切
    // タスクの方が多い場合、キューの最初からを優先して配置する

    // 椅子の方が多い場合全てのタスクにマッチング可能

    // 大抵の場合タスクの方が多い
    // タスクの全体最適を取りたいが、長時間マッチングされない場合が存在する
    // とんでもなくマッチングされなかったライドを必ず拾う仕組みがいる？
    // ↑ _花譜で解決済み

    let target = *SCORE_TARGET;
    let mut weights = Matrix::from_fn(jobs.len(), workers.len(), |(y, x)| {
        let task = &jobs[y];
        let worker = &workers[x];
        score_target(worker, &task.0, target)
    });

    let mut transposed = false;
    if jobs.len() > workers.len() {
        weights.transpose();
        transposed = true;
    }
    let (reported_sum, pairs) = pathfinding::kuhn_munkres::kuhn_munkres_min(&weights);

    let mut worker_used = vec![false; workers.len()];
    let mut task_used = vec![false; jobs.len()];
    let mut res = vec![];
    let mut target_sum = 0;
    let mut sum = 0;
    for i in 0..pairs.len() {
        let (task_i, worker_i) = {
            if transposed {
                (pairs[i], i)
            } else {
                (i, pairs[i])
            }
        };

        let task = &jobs[task_i];
        task_used[task_i] = true;

        let worker = &workers[worker_i];
        worker_used[worker_i] = true;

        let score = score(worker, &task.0);
        target_sum += score.abs_diff(target) as i32;
        sum += score;

        res.push(Pair {
            chair_id: worker.chair,
            ride_id: task.0.id,
            status_id: task.1,
            score,
        });
    }

    let mut redu_workers = vec![];
    let mut redu_jobs = vec![];
    for i in (0..workers.len()).rev() {
        if !worker_used[i] {
            redu_workers.push(workers.remove(i));
        }
    }
    for i in (0..jobs.len()).rev() {
        if !task_used[i] {
            redu_jobs.push(jobs.remove(i));
        }
    }

    assert_eq!(target_sum, reported_sum, "wrong score calculcation");
    (redu_workers, redu_jobs, res, sum)
}

impl Repository {
    pub fn do_matching(&self) -> Option<Duration> {
        let (pairs, waiting, free, retry) = {
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

            let (avail, redu2, pairs, retry) = {
                let (a, b, c, pf_score, retry) =
                    solve_pathfinding_isekai_joucho(avail, waiting_rides);
                let pairs = c.len();
                let avg = pf_score.checked_div(pairs as i32).unwrap_or(0);
                let target = *SCORE_TARGET;
                tracing::debug!("pairs={pairs:3}, avg={avg:5}, target={target:5}");
                (a, b, c, retry)
            };

            if !redu2.is_empty() {
                let mut c = self.ride_cache.waiting_rides.lock();
                c.extend(redu2);
            }
            if !avail.is_empty() {
                let mut c = self.ride_cache.free_chairs_lv2.lock();
                c.extend(avail.into_iter().map(|x| x.chair));
            }

            (pairs, waiting_rides_len, free_chairs_len, retry)
        };

        let pairs_len = pairs.len();
        for pair in pairs {
            self.rides_assign(pair.ride_id, pair.status_id, pair.chair_id)
                .unwrap();
        }

        tracing::debug!("waiting={waiting:6}, free={free:3}, matches={pairs_len:3}, retry={retry}");
        tracing::debug!("");

        (pairs_len > 100).then_some(Duration::from_millis(60))
        // if retry {
        //     LIMITER.lock().until_next_release()
        // } else {
        //     None
        // }
    }
}
