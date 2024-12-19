use chrono::{DateTime, Utc};
use shared::DlRwLock as RwLock;
use shared::{
    deferred::{DeferrableSimple, SimpleDeferred},
    models::{Chair, ChairLocation, Coordinate, Id, Ride, RideStatus, RideStatusEnum},
    ws::coordinate::CoordResponseGet,
    FxHashMap as HashMap,
};
use sqlx::{MySql, Pool};

#[derive(Debug)]
pub struct ChairRepository {
    pool: Pool<MySql>,
    chairs: RwLock<HashMap<String, Id<Chair>>>,
    locations: RwLock<HashMap<Id<Chair>, Entry>>,
    deferred: SimpleDeferred<ChairLocationDeferrable>,
}

impl ChairRepository {
    pub async fn new(pool: &Pool<MySql>) -> Self {
        let init = ChairLocationCacheInit::from_init(pool).await;
        Self {
            pool: pool.clone(),
            chairs: RwLock::new(init.chairs),
            locations: RwLock::new(init.location),
            deferred: SimpleDeferred::new(pool),
        }
    }
    pub async fn reinit(&self) {
        let init = ChairLocationCacheInit::from_init(&self.pool).await;

        let ChairLocationCacheInit { chairs, location } = init;

        let mut c = self.chairs.write().await;
        let mut l = self.locations.write().await;

        *c = chairs;
        *l = location;
    }

    pub async fn chair_add(&self, id: &Id<Chair>, token: &str) {
        let mut cache = self.chairs.write().await;
        cache.insert(token.to_owned(), id.clone());
    }
    pub async fn chair_get_by_access_token(&self, token: &str) -> Option<Id<Chair>> {
        self.chairs.read().await.get(token).cloned()
    }
    pub async fn chair_get_bulk(&self, ids: &[Id<Chair>]) -> HashMap<Id<Chair>, CoordResponseGet> {
        let locs = self.locations.read().await;
        let mut res = HashMap::default();
        for id in ids {
            let Some(entry) = locs.get(id) else {
                continue;
            };
            let entry = entry.0.read().await;
            let g = CoordResponseGet {
                latest: entry.latest_coord,
                total_distance: entry.total,
                latest_updated_at: entry.updated_at,
            };
            res.insert(id.clone(), g);
        }
        res
    }

    pub async fn chair_set_movement(
        &self,
        id: &Id<Chair>,
        ride: Id<Ride>,
        coord: Coordinate,
        state: RideStatusEnum,
    ) {
        let cache = self.locations.read().await;
        let entry = cache.get(id).unwrap();
        let mut entry = entry.0.write().await;
        entry.set_movement(ride, coord, state);
    }

    pub async fn chair_location_update(
        &self,
        chair: &Id<Chair>,
        coord: Coordinate,
    ) -> (DateTime<Utc>, Option<(Id<Ride>, RideStatusEnum)>) {
        let now = Utc::now();

        self.deferred
            .insert(ChairLocation {
                id: Id::new(),
                chair_id: chair.clone(),
                latitude: coord.latitude,
                longitude: coord.longitude,
                created_at: now,
            })
            .await;

        {
            let cache = self.locations.read().await;
            if let Some(e) = cache.get(chair) {
                let mut entry = e.0.write().await;
                let res = entry.update(coord, now);
                return (now, res);
            }
        }

        let mut cache = self.locations.write().await;
        cache.insert(chair.clone(), Entry::new(coord, now));
        (now, None)
    }
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
}

#[derive(Debug)]
struct EntryInner {
    destination: Option<(Id<Ride>, Coordinate, RideStatusEnum)>,
    latest_coord: Coordinate,
    updated_at: DateTime<Utc>,
    total: i64,
}

impl EntryInner {
    fn new(coord: Coordinate, at: DateTime<Utc>) -> Self {
        Self {
            destination: None,
            latest_coord: coord,
            updated_at: at,
            total: 0,
        }
    }
    fn set_movement(&mut self, ride: Id<Ride>, coord: Coordinate, new_state: RideStatusEnum) {
        self.destination = Some((ride, coord, new_state));
    }
    fn update(
        &mut self,
        coord: Coordinate,
        at: DateTime<Utc>,
    ) -> Option<(Id<Ride>, RideStatusEnum)> {
        self.total += self.latest_coord.distance(coord) as i64;
        self.latest_coord = coord;
        self.updated_at = at;
        if let Some((_ride, dest, _status)) = self.destination.as_ref() {
            if *dest == coord {
                let (ride, _dest, status) = self.destination.take().unwrap();
                return Some((ride, status));
            }
        }
        None
    }
}

struct ChairLocationCacheInit {
    chairs: HashMap<String, Id<Chair>>,
    location: HashMap<Id<Chair>, Entry>,
}
impl ChairLocationCacheInit {
    async fn from_init(pool: &Pool<MySql>) -> ChairLocationCacheInit {
        let mut locations: Vec<ChairLocation> = sqlx::query_as("select * from chair_locations")
            .fetch_all(pool)
            .await
            .unwrap();
        let chair_records: Vec<Chair> = sqlx::query_as("select * from chairs")
            .fetch_all(pool)
            .await
            .unwrap();
        let rides: Vec<Ride> = sqlx::query_as("select * from rides")
            .fetch_all(pool)
            .await
            .unwrap();
        let ride_statuses: Vec<RideStatus> = sqlx::query_as("select * from ride_statuses")
            .fetch_all(pool)
            .await
            .unwrap();
        let mut statuses = HashMap::default();
        for status in &ride_statuses {
            statuses
                .entry(status.ride_id.clone())
                .or_insert_with(Vec::new)
                .push(status.clone());
        }

        locations.sort_unstable_by_key(|x| x.created_at);

        let mut chairs: HashMap<String, Id<Chair>> = HashMap::default();
        for chair in &chair_records {
            chairs.insert(chair.access_token.clone(), chair.id.clone());
        }

        let mut location: HashMap<Id<Chair>, Entry> = HashMap::default();
        for loc in &locations {
            if let Some(c) = location.get_mut(&loc.chair_id) {
                c.0.write().await.update(loc.coord(), loc.created_at);
            } else {
                location.insert(
                    loc.chair_id.clone(),
                    Entry::new(loc.coord(), loc.created_at),
                );
            }
        }

        for ride in &rides {
            let Some(chair_id) = ride.chair_id.as_ref() else {
                continue;
            };
            let mut e = location.get(chair_id).unwrap().0.write().await;
            for status in statuses.get(&ride.id).unwrap() {
                match status.status {
                    RideStatusEnum::Matching => {}
                    RideStatusEnum::Enroute => {
                        e.set_movement(ride.id.clone(), ride.pickup(), RideStatusEnum::Pickup);
                    }
                    RideStatusEnum::Pickup => {
                        e.destination = None;
                    }
                    RideStatusEnum::Carrying => {
                        e.set_movement(
                            ride.id.clone(),
                            ride.destination(),
                            RideStatusEnum::Arrived,
                        );
                    }
                    RideStatusEnum::Arrived => {
                        e.destination = None;
                    }
                    RideStatusEnum::Completed => {}
                    RideStatusEnum::Canceled => unreachable!(), // 使われてないよね？
                }
            }
        }

        ChairLocationCacheInit { location, chairs }
    }
}
