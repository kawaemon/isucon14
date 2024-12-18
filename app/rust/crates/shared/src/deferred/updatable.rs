use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use derivative::Derivative;
use sqlx::{MySql, Pool, Transaction};
use tokio::sync::Mutex;

pub trait DeferrableMayUpdated: 'static {
    const NAME: &str;

    type Insert: std::fmt::Debug + Send + 'static;
    type Update: std::fmt::Debug + Send + 'static;
    type UpdateQuery: std::fmt::Debug + Send + 'static;

    fn summarize(
        inserts: &mut [Self::Insert],
        updates: Vec<Self::Update>,
    ) -> Vec<Self::UpdateQuery>;

    fn exec_insert(
        tx: &mut Transaction<'static, MySql>,
        inserts: &[Self::Insert],
    ) -> impl Future<Output = ()> + Send;
    fn exec_update(
        tx: &mut Transaction<'static, MySql>,
        update: &Self::UpdateQuery,
    ) -> impl Future<Output = ()> + Send;
}

#[derive(Derivative)]
#[derivative(Debug(bound = ""))]
struct ChangeSet<D: DeferrableMayUpdated> {
    inserts: Vec<D::Insert>,
    updates: Vec<D::Update>,
}

#[derive(Derivative)]
#[derivative(Debug(bound = ""), Clone(bound = ""))]
pub struct UpdatableDeferred<D: DeferrableMayUpdated> {
    set: Arc<Mutex<ChangeSet<D>>>,
}

impl<D: DeferrableMayUpdated> UpdatableDeferred<D> {
    pub fn new(pool: &Pool<MySql>) -> Self {
        let set = Arc::new(Mutex::new(ChangeSet {
            inserts: vec![],
            updates: vec![],
        }));
        Self::spawn_committer(&set, pool);
        Self { set }
    }
    pub async fn insert(&self, i: D::Insert) {
        let mut set = self.set.lock().await;
        set.inserts.push(i);
    }
    pub async fn update(&self, u: D::Update) {
        let mut set = self.set.lock().await;
        set.updates.push(u);
    }
    fn spawn_committer(set: &Arc<Mutex<ChangeSet<D>>>, pool: &Pool<MySql>) {
        let pool = pool.clone();
        let set = Arc::clone(set);
        tokio::spawn(async move {
            loop {
                Self::commit(&set, &pool).await;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }
    async fn commit(set: &Arc<Mutex<ChangeSet<D>>>, pool: &Pool<MySql>) {
        let begin = Instant::now();
        let (mut inserts, updates) = {
            let mut set = set.lock().await;
            let inserts = std::mem::take(&mut set.inserts);
            let updates = std::mem::take(&mut set.updates);
            (inserts, updates)
        };
        let inserts_len = inserts.len();
        let updates_count_old = updates.len();

        let actual_updates = D::summarize(&mut inserts, updates);

        let updates_count_new = actual_updates.len();

        if inserts.is_empty() && actual_updates.is_empty() {
            return;
        }

        let summarize_took = begin.elapsed().as_millis();
        let begin = Instant::now();

        let mut tx = pool.begin().await.unwrap();

        if !inserts.is_empty() {
            D::exec_insert(&mut tx, &inserts).await;
        }

        let inserts_took = begin.elapsed().as_millis();
        let begin = Instant::now();

        if !actual_updates.is_empty() {
            for update in actual_updates {
                D::exec_update(&mut tx, &update).await;
            }
        }

        let updates_took = begin.elapsed().as_millis();
        let begin = Instant::now();
        tx.commit().await.unwrap();
        let commit_took = begin.elapsed().as_millis();

        let name = D::NAME;
        tracing::debug!(
            "{name}: {inserts_len} inserts and {updates_count_old}=>{updates_count_new} updates",
        );
        tracing::debug!("{name}: prep={summarize_took}ms, inserts={inserts_took}ms, updates={updates_took}ms, commit={commit_took}ms");
    }
}
