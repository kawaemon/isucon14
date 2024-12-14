use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use sqlx::{MySql, Pool};
use tokio::sync::{Mutex, RwLock};

use crate::{
    models::{Chair, ChairLocation, Id},
    Coordinate,
};

use super::{cache_init::CacheInit, Repository, Result, Tx};

pub type ChairLocationCache = Arc<ChairLocationCacheInner>;

#[derive(Debug)]
pub struct ChairLocationCacheInner {
    cache: RwLock<HashMap<Id<Chair>, Entry>>,
    deferred: Deferred,
}

const THRESHOLD: usize = 500;

#[derive(Debug)]
struct Deferred {
    queue: Arc<Mutex<Vec<ChairLocation>>>,
    on_update: tokio::sync::mpsc::UnboundedSender<()>,
}
impl Deferred {
    async fn new(pool: &Pool<MySql>) -> Self {
        let pool = pool.clone();
        let queue = Arc::new(Mutex::new(vec![]));

        let mqueue = Arc::clone(&queue);

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                let sleep = tokio::time::sleep(Duration::from_millis(2000));
                tokio::pin!(sleep);
                let frx = rx.recv();
                tokio::pin!(frx);

                let mut thistime = vec![];
                {
                    tokio::select! {
                        _ = &mut sleep => { }
                        _ = &mut frx => { }
                    }
                    let mut lock = mqueue.lock().await;
                    let range = 0..lock.len().min(THRESHOLD);
                    thistime.extend(lock.drain(range));
                }

                if thistime.is_empty() {
                    continue;
                }
                let mut query = sqlx::QueryBuilder::new(
                    "insert into chair_locations(id, chair_id, latitude, longitude, created_at) ",
                );
                query.push_values(thistime.iter(), |mut b, e: &ChairLocation| {
                    b.push_bind(&e.id)
                        .push_bind(&e.chair_id)
                        .push_bind(e.latitude)
                        .push_bind(e.longitude)
                        .push_bind(e.created_at);
                });

                let t = Instant::now();
                query.build().execute(&pool).await.unwrap();
                let e = t.elapsed().as_millis();
                tracing::debug!("pushed {} locations in {e} ms", thistime.len());
            }
        });

        Deferred {
            queue,
            on_update: tx,
        }
    }

    async fn push(&self, c: ChairLocation) {
        let mut queue = self.queue.lock().await;
        queue.push(c);
        if queue.len() == THRESHOLD {
            self.on_update.send(()).unwrap();
        }
    }
}

#[derive(Debug)]
struct Entry {
    latest_coord: Coordinate,
    updated_at: DateTime<Utc>,
    total: i64,
}

impl Entry {
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
    fn from_init(init: &mut CacheInit) -> ChairLocationCacheInit {
        init.locations.sort_unstable_by_key(|x| x.created_at);

        let mut res: HashMap<Id<Chair>, Entry> = HashMap::new();
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

        ChairLocationCacheInit { cache: res }
    }
}

impl Repository {
    pub(super) async fn init_chair_location_cache(
        pool: &Pool<MySql>,
        init: &mut CacheInit,
    ) -> ChairLocationCache {
        let init = ChairLocationCacheInit::from_init(init);

        Arc::new(ChairLocationCacheInner {
            cache: RwLock::new(init.cache),
            deferred: Deferred::new(pool).await,
        })
    }

    pub(super) async fn reinit_chair_location_cache(
        &self,
        _pool: &Pool<MySql>,
        init: &mut CacheInit,
    ) {
        let init = ChairLocationCacheInit::from_init(init);
        *self.chair_location_cache.cache.write().await = init.cache;
    }
}

impl Repository {
    pub async fn chair_location_get_latest(
        &self,
        _tx: impl Into<Option<&mut Tx>>,
        id: &Id<Chair>,
    ) -> Result<Option<Coordinate>> {
        let cache = self.chair_location_cache.cache.read().await;
        Ok(cache.get(id).map(|x| x.latest_coord))
    }

    pub async fn chair_total_distance(
        &self,
        chair_id: &Id<Chair>,
    ) -> Result<Option<(i64, DateTime<Utc>)>> {
        let cache = self.chair_location_cache.cache.read().await;
        Ok(cache.get(chair_id).map(|x| (x.total, x.updated_at)))
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

        self.chair_location_cache.deferred.push(c).await;
        {
            let mut cache = self.chair_location_cache.cache.write().await;
            if let Some(c) = cache.get_mut(chair_id) {
                c.update(coord, created_at);
            } else {
                cache.insert(chair_id.clone(), Entry::new(coord, created_at));
            }
        }

        Ok(created_at)
    }
}
