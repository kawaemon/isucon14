use std::{
    future::Future,
    time::{Duration, Instant},
};

use derivative::Derivative;
use sqlx::{MySql, Pool, Transaction};

pub trait DeferrableSimple: 'static {
    const NAME: &str;

    type Insert: std::fmt::Debug + Send + Sync + 'static;

    fn exec_insert(
        tx: &mut Transaction<'static, MySql>,
        inserts: &[Self::Insert],
    ) -> impl Future<Output = ()> + Send;
}

#[derive(Derivative)]
#[derivative(Debug(bound = ""), Clone(bound = ""))]
pub struct SimpleDeferred<D: DeferrableSimple> {
    tx: tokio::sync::mpsc::UnboundedSender<D::Insert>,
}

impl<D: DeferrableSimple> SimpleDeferred<D> {
    pub fn new(pool: &Pool<MySql>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self::spawn_committer(pool, rx);
        Self { tx }
    }
    pub fn insert(&self, i: D::Insert) {
        self.tx.send(i).unwrap();
    }
    fn spawn_committer(
        pool: &Pool<MySql>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<D::Insert>,
    ) {
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut buffer = vec![];
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;

                rx.recv_many(&mut buffer, 9999999).await;
                if buffer.is_empty() {
                    continue;
                }

                Self::commit(&buffer, &pool).await;
                buffer.clear();
            }
        });
    }
    async fn commit(inserts: &[D::Insert], pool: &Pool<MySql>) {
        let begin = Instant::now();

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
