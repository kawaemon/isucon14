use std::{future::Future, time::Duration};

// use std::time::Instant;

use derivative::Derivative;
use sqlx::{MySql, Pool, Transaction};
use tokio::sync::mpsc;

pub trait DeferrableMayUpdated: 'static {
    const NAME: &str;

    type Insert: std::fmt::Debug + Send + Sync + 'static;
    type Update: std::fmt::Debug + Send + Sync + 'static;
    type UpdateQuery: std::fmt::Debug + Send + Sync + 'static;

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
    insert_tx: mpsc::UnboundedSender<D::Insert>,
    update_tx: mpsc::UnboundedSender<D::Update>,
}

impl<D: DeferrableMayUpdated> UpdatableDeferred<D> {
    pub fn new(pool: &Pool<MySql>) -> Self {
        let insert = mpsc::unbounded_channel();
        let update = mpsc::unbounded_channel();
        Self::spawn_committer(pool, insert.1, update.1);
        Self {
            insert_tx: insert.0,
            update_tx: update.0,
        }
    }
    pub fn insert(&self, i: D::Insert) {
        self.insert_tx.send(i).unwrap();
    }
    pub fn update(&self, u: D::Update) {
        self.update_tx.send(u).unwrap();
    }
    fn spawn_committer(
        pool: &Pool<MySql>,
        mut insert_rx: mpsc::UnboundedReceiver<D::Insert>,
        mut update_rx: mpsc::UnboundedReceiver<D::Update>,
    ) {
        let pool = pool.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;

                let mut set = ChangeSet {
                    inserts: vec![],
                    updates: vec![],
                };

                insert_rx.recv_many(&mut set.inserts, 99999).await;
                update_rx.recv_many(&mut set.updates, 99999).await;

                Self::commit(set, &pool).await;
            }
        });
    }
    async fn commit(set: ChangeSet<D>, pool: &Pool<MySql>) {
        // let begin = Instant::now();
        let mut inserts = set.inserts;
        // let inserts_len = inserts.len();
        let updates = set.updates;
        // let updates_count_old = updates.len();

        let actual_updates = D::summarize(&mut inserts, updates);

        // let updates_count_new = actual_updates.len();

        if inserts.is_empty() && actual_updates.is_empty() {
            return;
        }

        // let summarize_took = begin.elapsed().as_millis();
        // let begin = Instant::now();

        let mut tx = pool.begin().await.unwrap();

        if !inserts.is_empty() {
            for i in inserts.chunks(500) {
                D::exec_insert(&mut tx, i).await;
            }
        }

        // let inserts_took = begin.elapsed().as_millis();
        // let begin = Instant::now();

        if !actual_updates.is_empty() {
            for update in actual_updates {
                D::exec_update(&mut tx, &update).await;
            }
        }

        // let updates_took = begin.elapsed().as_millis();
        // let begin = Instant::now();
        tx.commit().await.unwrap();
        // let commit_took = begin.elapsed().as_millis();

        // let name = D::NAME;
        // tracing::debug!(
        //     "{name}: {inserts_len} inserts and {updates_count_old}=>{updates_count_new} updates",
        // );
        // tracing::debug!("{name}: prep={summarize_took}ms, inserts={inserts_took}ms, updates={updates_took}ms, commit={commit_took}ms");
    }
}
