use std::{
    future::Future,
    ops::{Deref, DerefMut},
    time::Duration,
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
        with_timeout(self.0.read()).await
    }
    pub async fn write(&self) -> impl DerefMut<Target = T> + '_ {
        with_timeout(self.0.write()).await
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
    fut.await
}

#[cfg(jier)]
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
