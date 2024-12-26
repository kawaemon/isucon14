use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use derivative::Derivative;
use sqlx::{MySql, Pool, Transaction};
use tokio::sync::Mutex;

pub trait DeferrableSimple: 'static {
    const NAME: &str;

    type Insert: std::fmt::Debug + Send + Sync + 'static;

    fn exec_insert(
        tx: &mut Transaction<'static, MySql>,
        inserts: &[Self::Insert],
    ) -> impl Future<Output = ()> + Send;
}

const COMMIT_THRESHOLD: usize = 500;

#[derive(Derivative)]
#[derivative(Debug(bound = ""))]
struct ChangeSet<D: DeferrableSimple> {
    inserts: Vec<D::Insert>,
}

#[derive(Derivative)]
#[derivative(Debug(bound = ""), Clone(bound = ""))]
pub struct SimpleDeferred<D: DeferrableSimple> {
    set: Arc<Mutex<ChangeSet<D>>>,
    tx: tokio::sync::mpsc::UnboundedSender<()>,
}

impl<D: DeferrableSimple> SimpleDeferred<D> {
    pub fn new(pool: &Pool<MySql>) -> Self {
        let set = Arc::new(Mutex::new(ChangeSet { inserts: vec![] }));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self::spawn_committer(&set, pool, rx);
        Self { set, tx }
    }
    pub async fn insert(&self, i: D::Insert) {
        let len = {
            let mut set = self.set.lock().await;
            set.inserts.push(i);
            set.inserts.len()
        };
        if len == COMMIT_THRESHOLD {
            self.tx.send(()).unwrap();
        }
    }
    fn spawn_committer(
        set: &Arc<Mutex<ChangeSet<D>>>,
        pool: &Pool<MySql>,
        mut do_commit: tokio::sync::mpsc::UnboundedReceiver<()>,
    ) {
        let pool = pool.clone();
        let set = Arc::clone(set);
        tokio::spawn(async move {
            loop {
                let sleep = tokio::time::sleep(Duration::from_millis(500));
                tokio::pin!(sleep);
                let frx = do_commit.recv();
                tokio::pin!(frx);

                tokio::select! {
                    _ = &mut sleep => {}
                    _ = &mut frx => {}
                }

                Self::commit(&set, &pool).await;
            }
        });
    }
    async fn commit(set: &Arc<Mutex<ChangeSet<D>>>, pool: &Pool<MySql>) {
        let begin = Instant::now();
        let inserts = {
            let mut inserts = vec![];
            let mut set = set.lock().await;
            inserts.append(&mut set.inserts);
            inserts
        };

        let inserts_len = inserts.len();
        if inserts.is_empty() {
            return;
        }

        let prep_took = begin.elapsed().as_millis();

        let begin = Instant::now();
        let mut tx = pool.begin().await.unwrap();
        for i in inserts.chunks(500) {
            D::exec_insert(&mut tx, i).await;
        }
        tx.commit().await.unwrap();
        let took = begin.elapsed().as_millis();

        let name = D::NAME;
        tracing::debug!("{name}: {inserts_len} inserts, prep={prep_took}ms, insert={took}ms");
    }
}
