use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use sqlx::{MySql, Pool, QueryBuilder};
use tokio::sync::Mutex;

use crate::models::{Id, Ride, RideStatus, RideStatusEnum};

#[derive(Debug)]
pub struct RideStatusInsert {
    pub id: Id<RideStatus>,
    pub ride_id: Id<Ride>,
    pub status: RideStatusEnum,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug)]
pub enum NotifiedType {
    App,
    Chair,
}

#[derive(Debug)]
pub struct RideStatusUpdate {
    pub ty: NotifiedType,
    pub status_id: Id<RideStatus>,
    pub at: DateTime<Utc>,
}

#[derive(Debug)]
struct ChangeSet {
    inserts: Vec<RideStatusInsert>,
    updates: Vec<RideStatusUpdate>,
}

#[derive(Debug, Clone)]
pub struct Deferred {
    set: Arc<Mutex<ChangeSet>>,
}

impl Deferred {
    pub fn new(pool: &Pool<MySql>) -> Self {
        let set = Arc::new(Mutex::new(ChangeSet {
            inserts: vec![],
            updates: vec![],
        }));
        Self::spawn_comitter(&set, pool);
        Self { set }
    }
    pub async fn insert(&self, i: RideStatusInsert) {
        let mut set = self.set.lock().await;
        set.inserts.push(i);
    }
    pub async fn update(&self, u: RideStatusUpdate) {
        let mut set = self.set.lock().await;
        set.updates.push(u);
    }
    fn spawn_comitter(set: &Arc<Mutex<ChangeSet>>, pool: &Pool<MySql>) {
        let pool = pool.clone();
        let set = Arc::clone(set);
        tokio::spawn(async move {
            loop {
                Self::commit(&set, &pool).await;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }
    async fn commit(set: &Arc<Mutex<ChangeSet>>, pool: &Pool<MySql>) {
        let begin = Instant::now();
        let (inserts, updates) = {
            let mut set = set.lock().await;
            let inserts = std::mem::take(&mut set.inserts);
            let updates = std::mem::take(&mut set.updates);
            (inserts, updates)
        };
        let inserts_len = inserts.len();
        let updates_count_old = updates.len();

        let mut inserts = inserts
            .into_iter()
            .map(|x| {
                (
                    x.id.clone(),
                    RideStatus {
                        id: x.id,
                        ride_id: x.ride_id,
                        status: x.status,
                        created_at: x.created_at,
                        app_sent_at: None,
                        chair_sent_at: None,
                    },
                )
            })
            .collect::<HashMap<_, _>>();

        #[derive(Default)]
        struct UpdateQuery {
            app: Option<DateTime<Utc>>,
            chair: Option<DateTime<Utc>>,
        }

        let mut actual_updates: HashMap<Id<RideStatus>, UpdateQuery> = HashMap::new();
        for u in updates {
            let Some(i) = inserts.get_mut(&u.status_id) else {
                let r = actual_updates.entry(u.status_id).or_default();
                match u.ty {
                    NotifiedType::App => r.app = Some(u.at),
                    NotifiedType::Chair => r.chair = Some(u.at),
                }
                continue;
            };
            match u.ty {
                NotifiedType::App => i.app_sent_at = Some(u.at),
                NotifiedType::Chair => i.chair_sent_at = Some(u.at),
            }
        }

        let updates_count_new = actual_updates.len();

        if inserts.is_empty() && actual_updates.is_empty() {
            return;
        }

        let summarize_took = begin.elapsed().as_millis();
        let begin = Instant::now();

        let mut tx = pool.begin().await.unwrap();

        if !inserts.is_empty() {
            let mut insert_query = QueryBuilder::new(
                "insert into ride_statuses
                    (id, ride_id, status, created_at, app_sent_at, chair_sent_at)",
            );

            insert_query.push_values(inserts.values(), |mut b, i| {
                b.push_bind(&i.id)
                    .push_bind(&i.ride_id)
                    .push_bind(i.status)
                    .push_bind(i.created_at)
                    .push_bind(i.app_sent_at)
                    .push_bind(i.chair_sent_at);
            });

            insert_query.build().execute(&mut *tx).await.unwrap();
        }

        let inserts_took = begin.elapsed().as_millis();
        let begin = Instant::now();

        if !actual_updates.is_empty() {
            for (id, update) in actual_updates {
                let mut update_query = QueryBuilder::new("update ride_statuses set");
                let mut need_sepa = false;
                if let Some(app) = update.app {
                    update_query.push(" app_sent_at = ");
                    update_query.push_bind(app);
                    need_sepa = true
                }
                if let Some(app) = update.chair {
                    if need_sepa {
                        update_query.push(",");
                    }
                    update_query.push(" chair_sent_at = ");
                    update_query.push_bind(app);
                }
                update_query.push(" where id = ");
                update_query.push_bind(id);
                update_query.build().execute(&mut *tx).await.unwrap();
            }
        }

        let updates_took = begin.elapsed().as_millis();
        let begin = Instant::now();
        tx.commit().await.unwrap();
        let commit_took = begin.elapsed().as_millis();

        tracing::info!(
            "{inserts_len} inserts and {updates_count_old}=>{updates_count_new} updates"
        );
        tracing::info!("prep={summarize_took}ms, inserts={inserts_took}ms, updates={updates_took}ms, commit={commit_took}ms");
    }
}
