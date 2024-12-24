use crate::{
    models::{Ride, RideStatusEnum},
    FxHashMap as HashMap,
};
use std::sync::Arc;

use crate::dl::DlRwLock as RwLock;
use chrono::{DateTime, Utc};
use sqlx::{MySql, Pool};

use crate::{
    models::{Chair, ChairLocation, Id},
    Coordinate,
};

use super::{
    cache_init::CacheInit,
    deferred::{DeferrableSimple, SimpleDeferred},
    Repository, Result, Tx,
};

pub type ChairLocationCache = Arc<ChairLocationCacheInner>;

#[derive(Debug)]
pub struct ChairLocationCacheInner {
    cache: RwLock<HashMap<Id<Chair>, Entry>>,
    deferred: SimpleDeferred<ChairLocationDeferrable>,
}

struct ChairLocationDeferrable;
impl DeferrableSimple for ChairLocationDeferrable {
    const NAME: &str = "chair_locations";

    type Insert = ChairLocation;

    async fn exec_insert(tx: &Pool<MySql>, inserts: &[Self::Insert]) {
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
        query.build().execute(tx).await.unwrap();
    }
}

#[derive(Debug)]
struct Entry(RwLock<EntryInner>);
impl Entry {
    fn new(coord: Coordinate, at: DateTime<Utc>) -> Self {
        Self(RwLock::new(EntryInner::new(coord, at)))
    }
    async fn update(
        &self,
        coord: Coordinate,
        at: DateTime<Utc>,
    ) -> Option<(Id<Ride>, RideStatusEnum)> {
        self.0.write().await.update(coord, at)
    }
    async fn set_movement(&self, coord: Coordinate, next: RideStatusEnum, ride: Id<Ride>) {
        self.0.write().await.set_movement(coord, next, ride);
    }
    async fn latest(&self) -> Coordinate {
        self.0.read().await.latest_coord
    }
    async fn get_total(&self) -> (i64, DateTime<Utc>) {
        let e = self.0.read().await;
        (e.total, e.updated_at)
    }
    async fn clear_movement(&self) {
        self.0.write().await.clear_movement();
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
    async fn from_init(init: &mut CacheInit) -> ChairLocationCacheInit {
        init.locations.sort_unstable_by_key(|x| x.created_at);

        let mut res: HashMap<Id<Chair>, Entry> = HashMap::default();
        for loc in &init.locations {
            if let Some(c) = res.get_mut(&loc.chair_id) {
                c.update(loc.coord(), loc.created_at).await;
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
                        loc_entry
                            .set_movement(
                                ride.pickup_coord(),
                                RideStatusEnum::Pickup,
                                ride.id.clone(),
                            )
                            .await;
                    }
                    RideStatusEnum::Pickup => {
                        let loc_entry = res.get_mut(chair_id).unwrap();
                        loc_entry.clear_movement().await;
                    }
                    RideStatusEnum::Carrying => {
                        let loc_entry = res.get_mut(chair_id).unwrap();
                        loc_entry
                            .set_movement(
                                ride.destination_coord(),
                                RideStatusEnum::Arrived,
                                ride.id.clone(),
                            )
                            .await;
                    }
                    RideStatusEnum::Arrived => {
                        let loc_entry = res.get_mut(chair_id).unwrap();
                        loc_entry.clear_movement().await;
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
    pub(super) async fn init_chair_location_cache(
        pool: &Pool<MySql>,
        init: &mut CacheInit,
    ) -> ChairLocationCache {
        let init = ChairLocationCacheInit::from_init(init).await;

        Arc::new(ChairLocationCacheInner {
            cache: RwLock::new(init.cache),
            deferred: SimpleDeferred::new(pool),
        })
    }

    pub(super) async fn reinit_chair_location_cache(
        &self,
        _pool: &Pool<MySql>,
        init: &mut CacheInit,
    ) {
        let init = ChairLocationCacheInit::from_init(init).await;
        *self.chair_location_cache.cache.write().await = init.cache;
    }
}

impl Repository {
    pub async fn chair_location_get_latest(&self, id: &Id<Chair>) -> Result<Option<Coordinate>> {
        let cache = self.chair_location_cache.cache.read().await;
        let Some(cache) = cache.get(id) else {
            return Ok(None);
        };
        Ok(Some(cache.latest().await))
    }

    pub async fn chair_total_distance(
        &self,
        chair_id: &Id<Chair>,
    ) -> Result<Option<(i64, DateTime<Utc>)>> {
        let cache = self.chair_location_cache.cache.read().await;
        let Some(cache) = cache.get(chair_id) else {
            return Ok(None);
        };
        Ok(Some(cache.get_total().await))
    }

    pub async fn chair_set_movement(
        &self,
        chair_id: &Id<Chair>,
        coord: Coordinate,
        next: RideStatusEnum,
        ride: &Id<Ride>,
    ) {
        let cache = self.chair_location_cache.cache.read().await;
        let cache = cache.get(chair_id).unwrap();
        cache.set_movement(coord, next, ride.clone()).await;
    }

    pub async fn chair_location_update(
        &self,
        _tx: impl Into<Option<&mut Tx>>,
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

        self.chair_location_cache.deferred.insert(c).await;

        {
            {
                let cache = self.chair_location_cache.cache.read().await;
                if let Some(c) = cache.get(chair_id) {
                    let update = c.update(coord, created_at).await;
                    drop(cache);

                    if let Some((ride, status)) = update {
                        self.ride_status_update(None, &ride, status).await.unwrap();
                    }

                    return Ok(created_at);
                }
            }
        }

        let mut cache = self.chair_location_cache.cache.write().await;
        cache.insert(chair_id.clone(), Entry::new(coord, created_at));

        Ok(created_at)
    }
}
