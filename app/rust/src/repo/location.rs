use crate::{
    dl::DlSyncRwLock,
    models::{Ride, RideStatusEnum},
    HashMap,
};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::{MySql, Pool, Transaction};

use crate::{
    models::{Chair, ChairLocation, Id},
    Coordinate,
};

use super::{
    cache_init::CacheInit,
    deferred::{DeferrableSimple, SimpleDeferred},
    Repository, Result,
};

pub type ChairLocationCache = Arc<ChairLocationCacheInner>;

#[derive(Debug)]
pub struct ChairLocationCacheInner {
    cache: DlSyncRwLock<HashMap<Id<Chair>, Entry>>,
    deferred: SimpleDeferred<ChairLocationDeferrable>,
}

struct ChairLocationDeferrable;
impl DeferrableSimple for ChairLocationDeferrable {
    const NAME: &str = "chair_locations";

    type Insert = ChairLocation;

    async fn exec_insert(tx: &mut Transaction<'static, MySql>, inserts: &[Self::Insert]) {
        let mut query = sqlx::QueryBuilder::new(
            "insert into chair_locations(id, chair_id, latitude, longitude, created_at) ",
        );
        query.push_values(inserts, |mut b, e: &ChairLocation| {
            b.push_bind(&e.id)
                .push_bind(&e.chair_id)
                .push_bind(e.latitude)
                .push_bind(e.longitude)
                .push_bind(e.created_at);
        });
        query.build().execute(&mut **tx).await.unwrap();
    }
}

#[derive(Debug)]
struct Entry(DlSyncRwLock<EntryInner>);
impl Entry {
    fn new(coord: Coordinate, at: DateTime<Utc>) -> Self {
        Self(DlSyncRwLock::new(EntryInner::new(coord, at)))
    }
    fn update(&self, coord: Coordinate, at: DateTime<Utc>) -> Option<(Id<Ride>, RideStatusEnum)> {
        self.0.write().update(coord, at)
    }
    fn set_movement(&self, coord: Coordinate, next: RideStatusEnum, ride: Id<Ride>) {
        self.0.write().set_movement(coord, next, ride);
    }
    fn latest(&self) -> Coordinate {
        self.0.read().latest_coord
    }
    fn get_total(&self) -> (i64, DateTime<Utc>) {
        let e = self.0.read();
        (e.total, e.updated_at)
    }
    fn clear_movement(&self) {
        self.0.write().clear_movement();
    }
}

#[derive(Debug)]
struct EntryInner {
    latest_coord: Coordinate,
    updated_at: DateTime<Utc>,
    total: i64,
    movement: Option<(Coordinate, RideStatusEnum /* next */, Id<Ride>)>,
}

impl EntryInner {
    fn new(coord: Coordinate, at: DateTime<Utc>) -> Self {
        Self {
            latest_coord: coord,
            updated_at: at,
            total: 0,
            movement: None,
        }
    }
    fn update(
        &mut self,
        coord: Coordinate,
        at: DateTime<Utc>,
    ) -> Option<(Id<Ride>, RideStatusEnum)> {
        self.total += self.latest_coord.distance(coord) as i64;
        self.latest_coord = coord;
        self.updated_at = at;

        if self.movement.as_ref().is_some_and(|x| x.0 == coord) {
            let (_c, r, i) = self.movement.take().unwrap();
            return Some((i, r));
        }

        None
    }
    fn set_movement(&mut self, coord: Coordinate, next: RideStatusEnum, ride: Id<Ride>) {
        assert!(self.movement.is_none());
        self.movement = Some((coord, next, ride));
    }
    fn clear_movement(&mut self) {
        self.movement = None;
    }
}

struct ChairLocationCacheInit {
    cache: HashMap<Id<Chair>, Entry>,
}
impl ChairLocationCacheInit {
    fn from_init(init: &mut CacheInit) -> ChairLocationCacheInit {
        init.locations.sort_unstable_by_key(|x| x.created_at);

        let mut res: HashMap<Id<Chair>, Entry> = HashMap::default();
        for loc in &init.locations {
            if let Some(c) = res.get_mut(&loc.chair_id) {
                c.update(loc.coord(), loc.created_at);
            } else {
                res.insert(
                    loc.chair_id.clone(),
                    Entry::new(loc.coord(), loc.created_at),
                );
            }
        }

        init.rides.sort_unstable_by_key(|x| x.created_at);
        init.ride_statuses.sort_unstable_by_key(|x| x.created_at);

        let mut statuses = HashMap::default();
        for status in &init.ride_statuses {
            statuses
                .entry(status.ride_id.clone())
                .or_insert_with(Vec::new)
                .push(status.clone());
        }

        for ride in &init.rides {
            let Some(chair_id) = ride.chair_id.as_ref() else {
                continue;
            };
            for status in statuses.get(&ride.id).unwrap() {
                match status.status {
                    RideStatusEnum::Matching => {}
                    RideStatusEnum::Enroute => {
                        let loc_entry = res.get_mut(chair_id).unwrap();
                        loc_entry.set_movement(
                            ride.pickup_coord(),
                            RideStatusEnum::Pickup,
                            ride.id.clone(),
                        );
                    }
                    RideStatusEnum::Pickup => {
                        let loc_entry = res.get_mut(chair_id).unwrap();
                        loc_entry.clear_movement();
                    }
                    RideStatusEnum::Carrying => {
                        let loc_entry = res.get_mut(chair_id).unwrap();
                        loc_entry.set_movement(
                            ride.destination_coord(),
                            RideStatusEnum::Arrived,
                            ride.id.clone(),
                        );
                    }
                    RideStatusEnum::Arrived => {
                        let loc_entry = res.get_mut(chair_id).unwrap();
                        loc_entry.clear_movement();
                    }
                    RideStatusEnum::Completed => {}
                    RideStatusEnum::Canceled => unreachable!(), // 使われてないよね？
                }
            }
        }

        ChairLocationCacheInit { cache: res }
    }
}

impl Repository {
    pub(super) fn init_chair_location_cache(
        pool: &Pool<MySql>,
        init: &mut CacheInit,
    ) -> ChairLocationCache {
        let init = ChairLocationCacheInit::from_init(init);

        Arc::new(ChairLocationCacheInner {
            cache: DlSyncRwLock::new(init.cache),
            deferred: SimpleDeferred::new(pool),
        })
    }

    pub(super) fn reinit_chair_location_cache(&self, init: &mut CacheInit) {
        let init = ChairLocationCacheInit::from_init(init);
        *self.chair_location_cache.cache.write() = init.cache;
    }
}

impl Repository {
    pub fn chair_location_get_latest(&self, id: &Id<Chair>) -> Result<Option<Coordinate>> {
        let cache = self.chair_location_cache.cache.read();
        let Some(cache) = cache.get(id) else {
            return Ok(None);
        };
        Ok(Some(cache.latest()))
    }

    pub fn chair_total_distance(
        &self,
        chair_id: &Id<Chair>,
    ) -> Result<Option<(i64, DateTime<Utc>)>> {
        let cache = self.chair_location_cache.cache.read();
        let Some(cache) = cache.get(chair_id) else {
            return Ok(None);
        };
        Ok(Some(cache.get_total()))
    }

    pub fn chair_set_movement(
        &self,
        chair_id: &Id<Chair>,
        coord: Coordinate,
        next: RideStatusEnum,
        ride: &Id<Ride>,
    ) {
        let cache = self.chair_location_cache.cache.read();
        let cache = cache.get(chair_id).unwrap();
        cache.set_movement(coord, next, ride.clone());
    }

    pub fn chair_location_update(
        &self,
        chair_id: &Id<Chair>,
        coord: Coordinate,
    ) -> Result<DateTime<Utc>> {
        let created_at = Utc::now();

        let c = ChairLocation {
            id: Id::new(),
            chair_id: chair_id.clone(),
            latitude: coord.latitude,
            longitude: coord.longitude,
            created_at,
        };

        self.chair_location_cache.deferred.insert(c);

        {
            let (hit, update) = 'upd: {
                if let Some(c) = self.chair_location_cache.cache.read().get(chair_id) {
                    break 'upd (true, c.update(coord, created_at));
                }
                (false, None)
            };

            if let Some((ride, status)) = update {
                self.ride_status_update(&ride, status).unwrap();
            }

            if hit {
                return Ok(created_at);
            }
        }

        let mut cache = self.chair_location_cache.cache.write();
        cache.insert(chair_id.clone(), Entry::new(coord, created_at));

        Ok(created_at)
    }
}
