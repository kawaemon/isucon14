use shared::FxHashMap as HashMap;
use std::sync::Arc;

use crate::repo::dl::DlRwLock as RwLock;
use chrono::{DateTime, Utc};
use sqlx::{MySql, Pool};

use crate::models::{Chair, ChairLocation, Coordinate, Id};

use super::{cache_init::CacheInit, Repository, Result};
use shared::deferred::{DeferrableSimple, SimpleDeferred};

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
    async fn update(&self, coord: Coordinate, at: DateTime<Utc>) {
        self.0.write().await.update(coord, at);
    }
}

#[derive(Debug)]
struct EntryInner {
    latest_coord: Coordinate,
    updated_at: DateTime<Utc>,
    total: i64,
}

impl EntryInner {
    fn new(coord: Coordinate, at: DateTime<Utc>) -> Self {
        Self {
            latest_coord: coord,
            updated_at: at,
            total: 0,
        }
    }
    fn update(&mut self, coord: Coordinate, at: DateTime<Utc>) {
        self.total += self.latest_coord.distance(coord) as i64;
        self.latest_coord = coord;
        self.updated_at = at;
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
        let cache = cache.0.read().await;
        Ok(Some(cache.latest_coord))
    }

    pub async fn chair_total_distance(
        &self,
        chair_id: &Id<Chair>,
    ) -> Result<Option<(i64, DateTime<Utc>)>> {
        let cache = self.chair_location_cache.cache.read().await;
        let Some(cache) = cache.get(chair_id) else {
            return Ok(None);
        };
        let cache = cache.0.read().await;
        Ok(Some((cache.total, cache.updated_at)))
    }

    pub async fn chair_location_update(
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

        self.chair_location_cache.deferred.insert(c).await;

        {
            let cache = self.chair_location_cache.cache.read().await;
            if let Some(c) = cache.get(chair_id) {
                c.update(coord, created_at).await;
                return Ok(created_at);
            }
        }

        let mut cache = self.chair_location_cache.cache.write().await;
        cache.insert(chair_id.clone(), Entry::new(coord, created_at));

        Ok(created_at)
    }
}
