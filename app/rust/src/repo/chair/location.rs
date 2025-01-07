use crate::{
    dl::DlSyncRwLock,
    models::{Ride, RideStatusEnum},
    HashMap,
};

use chrono::{DateTime, Utc};
use sqlx::{MySql, Transaction};

use crate::{
    models::{Chair, ChairLocation, Id},
    Coordinate,
};

use crate::repo::{cache_init::CacheInit, deferred::DeferrableSimple, Repository, Result};

pub(super) struct ChairLocationDeferrable;
impl DeferrableSimple for ChairLocationDeferrable {
    const NAME: &str = "chair_locations";

    type Insert = ChairLocation;

    async fn exec_insert(tx: &mut Transaction<'static, MySql>, inserts: &[Self::Insert]) {
        let mut query = sqlx::QueryBuilder::new(
            "insert into chair_locations(id, chair_id, latitude, longitude, created_at) ",
        );
        query.push_values(inserts, |mut b, e: &ChairLocation| {
            b.push_bind(&e.id)
                .push_bind(e.chair_id)
                .push_bind(e.latitude)
                .push_bind(e.longitude)
                .push_bind(e.created_at);
        });
        query.build().execute(&mut **tx).await.unwrap();
    }
}

#[derive(Debug)]
pub struct LocationCache {
    inner: DlSyncRwLock<Option<EntryInner>>,
}
impl LocationCache {
    pub fn new() -> Self {
        Self {
            inner: DlSyncRwLock::new(None),
        }
    }
    pub fn update(
        &self,
        coord: Coordinate,
        at: DateTime<Utc>,
    ) -> Option<(Id<Ride>, RideStatusEnum)> {
        let mut cache = self.inner.write();
        match cache.as_mut() {
            Some(d) => d.update(coord, at),
            None => {
                *cache = Some(EntryInner::new(coord, at));
                None
            }
        }
    }
    pub fn set_movement(&self, coord: Coordinate, next: RideStatusEnum, ride: Id<Ride>) {
        let mut c = self.inner.write();
        c.as_mut().unwrap().set_movement(coord, next, ride);
    }
    pub fn latest(&self) -> Option<Coordinate> {
        Some(self.inner.read().as_ref()?.latest_coord)
    }
    pub fn get_total(&self) -> Option<(i64, DateTime<Utc>)> {
        let e = self.inner.read();
        let e = e.as_ref()?;
        Some((e.total, e.updated_at))
    }
    pub fn clear_movement(&self) {
        self.inner.write().as_mut().unwrap().clear_movement();
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

pub(super) struct ChairLocationCacheInit {
    pub cache: HashMap<Id<Chair>, LocationCache>,
}
impl ChairLocationCacheInit {
    pub(super) fn from_init(init: &mut CacheInit) -> ChairLocationCacheInit {
        init.locations.sort_unstable_by_key(|x| x.created_at);

        let mut res: HashMap<Id<Chair>, LocationCache> = HashMap::default();
        for loc in &init.locations {
            if let Some(c) = res.get_mut(&loc.chair_id) {
                c.update(loc.coord(), loc.created_at);
            } else {
                let c = LocationCache::new();
                c.update(loc.coord(), loc.created_at);
                res.insert(loc.chair_id, c);
            }
        }

        init.rides.sort_unstable_by_key(|x| x.created_at);
        init.ride_statuses.sort_unstable_by_key(|x| x.created_at);

        let mut statuses = HashMap::default();
        for status in &init.ride_statuses {
            statuses
                .entry(status.ride_id)
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
                            ride.id,
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
                            ride.id,
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
    pub fn chair_location_get_latest(&self, id: Id<Chair>) -> Result<Option<Coordinate>> {
        let cache = self.chair_cache.by_id.read();
        let Some(cache) = cache.get(&id) else {
            return Ok(None);
        };
        Ok(cache.loc.latest())
    }

    pub fn chair_total_distance(
        &self,
        chair_id: Id<Chair>,
    ) -> Result<Option<(i64, DateTime<Utc>)>> {
        let cache = self.chair_cache.by_id.read();
        let Some(cache) = cache.get(&chair_id) else {
            return Ok(None);
        };
        Ok(cache.loc.get_total())
    }

    pub fn chair_set_movement(
        &self,
        chair_id: Id<Chair>,
        coord: Coordinate,
        next: RideStatusEnum,
        ride: Id<Ride>,
    ) {
        let cache = self.chair_cache.by_id.read();
        let cache = cache.get(&chair_id).unwrap();
        cache.loc.set_movement(coord, next, ride);
    }

    pub fn chair_location_update(
        &self,
        chair_id: Id<Chair>,
        coord: Coordinate,
    ) -> Result<DateTime<Utc>> {
        let created_at = Utc::now();

        let c = ChairLocation {
            id: ulid::Ulid::new().to_string(),
            chair_id,
            latitude: coord.latitude,
            longitude: coord.longitude,
            created_at,
        };

        self.chair_cache.location_deferred.insert(c);

        let update = {
            if let Some(c) = self.chair_cache.by_id.read().get(&chair_id) {
                c.loc.update(coord, created_at)
            } else {
                None
            }
        };

        if let Some((ride, status)) = update {
            self.ride_status_update(ride, status).unwrap();
        }

        Ok(created_at)
    }
}
