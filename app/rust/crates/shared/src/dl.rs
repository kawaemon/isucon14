use std::{
    future::Future,
    ops::{Deref, DerefMut},
    time::{Duration, Instant},
};

use sha2::Sha256;
use tokio::sync::{Mutex, RwLock};

#[derive(Debug)]
pub struct DlRwLock<T>(RwLock<T>);

impl<T> DlRwLock<T> {
    pub fn new(v: T) -> Self {
        Self(RwLock::new(v))
    }
    pub async fn read(&self) -> impl Deref<Target = T> + '_ {
        ReadTracked::new(with_timeout(self.0.read()).await)
    }
    pub async fn read_notrack(&self) -> impl Deref<Target = T> + '_ {
        with_timeout(self.0.read()).await
    }
    pub async fn write(&self) -> impl DerefMut<Target = T> + '_ {
        with_timeout(self.0.write()).await
    }
}

struct ReadTracked<D> {
    inner: D,
    at: Instant,
}
impl<T, D: Deref<Target = T>> Deref for ReadTracked<D> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}
impl<T> ReadTracked<T> {
    fn new(i: T) -> Self {
        Self {
            inner: i,
            at: Instant::now(),
        }
    }
}
impl<T> Drop for ReadTracked<T> {
    fn drop(&mut self) {
        let elap = self.at.elapsed();
        if elap.as_millis() > 500 {
            let bt = backtrace::Backtrace::new();
            let bt = format!("{bt:?}");
            tracing::warn!("lock held {}ms at\n{bt}", elap.as_millis());
        }
    }
}

#[derive(Debug)]
pub struct DlMutex<T>(Mutex<T>);

impl<T> DlMutex<T> {
    pub fn new(v: T) -> Self {
        Self(Mutex::new(v))
    }
    pub async fn lock(&self) -> impl DerefMut<Target = T> + '_ {
        with_timeout(self.0.lock()).await
    }
}

async fn with_timeout<T>(fut: impl Future<Output = T>) -> T {
    let Ok(o) = tokio::time::timeout(Duration::from_millis(1000), fut).await else {
        let bt = backtrace::Backtrace::new();
        let bt = format!("{bt:?}");

        use sha2::Digest;
        let mut hasher = Sha256::new();
        hasher.update(&bt);
        let result = hasher.finalize();
        let hex = format!("{:x}", result);
        tokio::fs::create_dir_all("./dl").await.unwrap();
        tokio::fs::write(&format!("./dl/{hex}.txt"), &bt)
            .await
            .unwrap();

        panic!("lock cannot acquired");
    };

    o
}
