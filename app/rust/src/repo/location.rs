use std::{collections::HashMap, sync::Arc, time::Duration};

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

#[derive(Debug)]
struct Deferred {
    queue: Arc<Mutex<Vec<ChairLocation>>>,
    on_update: tokio::sync::mpsc::UnboundedSender<()>,
}
impl Deferred {
    async fn new(pool: &Pool<MySql>) -> Self {
        let pool = pool.clone();
        let queue = Arc::new(Mutex::new(vec![]));

        let mqueue = queue.clone();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                let threshold = 500;
                let sleep = tokio::time::sleep(Duration::from_millis(500));
                tokio::pin!(sleep);
                let frx = rx.recv();
                tokio::pin!(frx);

                tokio::select! {
                    _ = &mut sleep => { }
                    _ = &mut frx => {
                        if mqueue.lock().await.len() < threshold {
                            continue;
                        }
                    }
                }

                let mut queue = mqueue.lock().await;
                if queue.is_empty() {
                    continue;
                }

                let mut query = sqlx::QueryBuilder::new(
                    "insert into chair_locations(id, chair_id, latitude, longitude, created_at) ",
                );
                query.push_values(queue.iter(), |mut b, e: &ChairLocation| {
                    b.push_bind(&e.id)
                        .push_bind(&e.chair_id)
                        .push_bind(e.latitude)
                        .push_bind(e.longitude)
                        .push_bind(e.created_at);
                });
                query.build().execute(&pool).await.unwrap();

                tracing::info!("pushed {} locations", queue.len());
                queue.clear();
            }
        });

        Deferred {
            queue,
            on_update: tx,
        }
    }

    async fn push(&self, c: ChairLocation) {
        self.queue.lock().await.push(c);
        self.on_update.send(()).unwrap();
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

impl Repository {
    pub(super) async fn init_chair_location_cache(
        pool: &Pool<MySql>,
        init: &mut CacheInit,
    ) -> ChairLocationCache {
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

        Arc::new(ChairLocationCacheInner {
            cache: RwLock::new(res),
            deferred: Deferred::new(pool).await,
        })
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
