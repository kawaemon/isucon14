#[cfg(not(feature = "dl"))]
pub use dl_off::*;

#[cfg(feature = "dl")]
pub use dl_on::*;

use std::ops::{Deref, DerefMut};

type AsyncRwLock<T> = tokio::sync::RwLock<T>;
type AsyncMutex<T> = tokio::sync::Mutex<T>;

#[allow(unused)]
#[cfg(not(feature = "parking_lot"))]
type SyncRwLock<T> = std::sync::RwLock<T>;
#[allow(unused)]
#[cfg(not(feature = "parking_lot"))]
type SyncMutex<T> = std::sync::Mutex<T>;

#[cfg(feature = "parking_lot")]
type SyncRwLock<T> = parking_lot::RwLock<T>;
#[cfg(feature = "parking_lot")]
type SyncMutex<T> = parking_lot::Mutex<T>;

#[cfg(not(feature = "dl"))]
mod dl_off {
    use super::*;

    #[derive(Debug)]
    pub struct DlSyncRwLock<T>(SyncRwLock<T>);
    impl<T> DlSyncRwLock<T> {
        pub fn new(v: T) -> Self {
            Self(SyncRwLock::new(v))
        }
        pub fn read(&self) -> impl Deref<Target = T> + '_ {
            #[cfg(feature = "parking_lot")]
            return self.0.read();
            #[cfg(not(feature = "parking_lot"))]
            return self.0.read().unwrap();
        }
        pub fn write(&self) -> impl DerefMut<Target = T> + '_ {
            #[cfg(feature = "parking_lot")]
            return self.0.write();
            #[cfg(not(feature = "parking_lot"))]
            return self.0.write().unwrap();
        }
    }
    #[derive(Debug)]
    pub struct DlRwLock<T>(AsyncRwLock<T>);
    impl<T> DlRwLock<T> {
        pub fn new(v: T) -> Self {
            Self(AsyncRwLock::new(v))
        }
        pub async fn read(&self) -> impl Deref<Target = T> + '_ {
            self.0.read().await
        }
        pub async fn write(&self) -> impl DerefMut<Target = T> + '_ {
            self.0.write().await
        }
    }

    #[derive(Debug)]
    pub struct DlSyncMutex<T>(SyncMutex<T>);
    impl<T> DlSyncMutex<T> {
        pub fn new(v: T) -> Self {
            Self(SyncMutex::new(v))
        }
        pub async fn lock(&self) -> impl DerefMut<Target = T> + '_ {
            #[cfg(feature = "parking_lot")]
            return self.0.lock();
            #[cfg(not(feature = "parking_lot"))]
            return self.0.lock().unwrap();
        }
    }
    #[derive(Debug)]
    pub struct DlMutex<T>(AsyncMutex<T>);
    impl<T> DlMutex<T> {
        pub fn new(v: T) -> Self {
            Self(AsyncMutex::new(v))
        }
        pub async fn lock(&self) -> impl DerefMut<Target = T> + '_ {
            self.0.lock().await
        }
    }
}

#[cfg(feature = "dl")]
mod dl_on {
    use super::*;
    use core::fmt;
    use std::{
        future::Future,
        time::{Duration, Instant},
    };

    use backtrace::{BacktraceFmt, BacktraceFrame, BytesOrWideString};
    use chrono::Utc;
    use sha2::Sha256;

    #[derive(Debug)]
    pub struct DlSyncRwLock<T>(parking_lot::RwLock<T>);
    impl<T> DlSyncRwLock<T> {
        pub fn new(v: T) -> Self {
            Self(parking_lot::RwLock::new(v))
        }
        pub fn read(&self) -> impl Deref<Target = T> + '_ {
            TrackedRef::new(with_timeout_sync(|t| self.0.try_read_until(t)))
        }
        pub fn write(&self) -> impl DerefMut<Target = T> + '_ {
            TrackedRef::new(with_timeout_sync(|t| self.0.try_write_until(t)))
        }
    }
    #[derive(Debug)]
    pub struct DlRwLock<T>(AsyncRwLock<T>);
    impl<T> DlRwLock<T> {
        pub fn new(v: T) -> Self {
            Self(tokio::sync::RwLock::new(v))
        }
        pub async fn read(&self) -> impl Deref<Target = T> + '_ {
            TrackedRef::new(with_timeout(self.0.read()).await)
        }
        pub async fn write(&self) -> impl DerefMut<Target = T> + '_ {
            TrackedRef::new(with_timeout(self.0.write()).await)
        }
    }

    #[derive(Debug)]
    pub struct DlSyncMutex<T>(parking_lot::Mutex<T>);
    impl<T> DlSyncMutex<T> {
        pub fn new(v: T) -> Self {
            Self(parking_lot::Mutex::new(v))
        }
        pub async fn lock(&self) -> impl DerefMut<Target = T> + '_ {
            TrackedRef::new(with_timeout_sync(|t| self.0.try_lock_until(t)))
        }
    }
    #[derive(Debug)]
    pub struct DlMutex<T>(AsyncMutex<T>);
    impl<T> DlMutex<T> {
        pub fn new(v: T) -> Self {
            Self(tokio::sync::Mutex::new(v))
        }
        pub async fn lock(&self) -> impl DerefMut<Target = T> + '_ {
            TrackedRef::new(with_timeout(self.0.lock()).await)
        }
    }

    crate::conf_env!(static DL_WARN_LOCKHOLD: u64 = {
        from: "DL_WARN_LOCKHOLD",
        default: "1000",
    });

    struct TrackedRef<T>(T, Instant);
    impl<T> TrackedRef<T> {
        fn new(v: T) -> Self {
            Self(v, Instant::now())
        }
    }
    impl<T> Drop for TrackedRef<T> {
        fn drop(&mut self) {
            let e = self.1.elapsed();
            if e > Duration::from_millis(*DL_WARN_LOCKHOLD) {
                let bt = get_bt();
                let e = e.as_millis();
                tracing::warn!("lock held {e}ms in:\n{bt}")
            }
        }
    }
    impl<D: Deref> Deref for TrackedRef<D> {
        type Target = D::Target;
        fn deref(&self) -> &Self::Target {
            self.0.deref()
        }
    }
    impl<D: DerefMut> DerefMut for TrackedRef<D> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.0.deref_mut()
        }
    }

    crate::conf_env!(static DL_TIMEOUT_MS: u64 = {
        from: "DL_TIMEOUT_MS",
        default: "10000",
    });

    pub fn with_timeout_sync<T>(f: impl FnOnce(Instant) -> Option<T>) -> T {
        let ins = Instant::now() + Duration::from_millis(*DL_TIMEOUT_MS);
        let Some(o) = f(ins) else {
            let (name, bt) = gen_bt_file();
            std::fs::create_dir_all("./dl").unwrap();
            std::fs::write(format!("./dl/{name}"), &bt).unwrap();
            panic!("lock cannot acquired\n{bt}");
        };
        o
    }

    pub async fn with_timeout<T>(fut: impl Future<Output = T>) -> T {
        let dur = Duration::from_millis(*DL_TIMEOUT_MS);
        let Ok(o) = tokio::time::timeout(dur, fut).await else {
            let (name, bt) = gen_bt_file();
            tokio::fs::create_dir_all("./dl").await.unwrap();
            tokio::fs::write(format!("./dl/{name}"), &bt).await.unwrap();
            panic!("lock cannot acquired\n{bt}");
        };
        o
    }

    /// (name, content)
    fn gen_bt_file() -> (String, String) {
        let bt = get_bt();
        use sha2::Digest;
        let mut hasher = Sha256::new();
        hasher.update(&bt);
        let result = hasher.finalize();
        let t = Utc::now().timestamp();
        let hex = format!("{t}-{:16x}.txt", result);
        (hex, bt)
    }

    fn get_bt() -> String {
        let bt = backtrace::Backtrace::new();

        let mut frames = vec![vec![]];
        for frame in bt.frames() {
            let syms = frame.symbols();
            let useful = syms
                .iter()
                .flat_map(|x| x.name())
                .map(|x| format!("{}", x))
                .any(|x| {
                    if x.contains("dl::") {
                        return false;
                    }
                    x.contains(env!("CARGO_CRATE_NAME"))
                });
            if !useful {
                if !frames.last().unwrap().is_empty() {
                    frames.push(vec![]);
                }
                continue;
            }
            frames.last_mut().unwrap().push(frame);
        }

        if frames.is_empty() {
            return String::new();
        }

        let mut buf = String::new();
        let len = frames.len();
        for (i, frames) in frames.into_iter().enumerate() {
            let cb = CustomDebug(frames);
            buf.push_str(&format!("{cb:?}"));
            if i != len - 1 {
                buf.push_str("--- collapsed ---\n");
            }
        }

        return buf;

        struct CustomDebug<'a>(Vec<&'a BacktraceFrame>);
        impl std::fmt::Debug for CustomDebug<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let mut pp = |fmt: &mut fmt::Formatter<'_>, path: BytesOrWideString<'_>| {
                    fmt::Display::fmt(&path, fmt)
                };
                let mut f = BacktraceFmt::new(f, backtrace::PrintFmt::Short, &mut pp);
                f.add_context()?;
                for frame in &self.0 {
                    f.frame().backtrace_frame(frame)?;
                }
                f.finish()
            }
        }
    }
}
