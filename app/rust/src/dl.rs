use core::fmt;
use std::{
    future::Future,
    ops::{Deref, DerefMut},
    sync::LazyLock,
    time::{Duration, Instant},
};

use backtrace::{BacktraceFmt, BacktraceFrame, BytesOrWideString};
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
        TrackedRef::new(with_timeout(self.0.write()).await)
    }
}

#[derive(Debug)]
pub struct DlMutex<T>(Mutex<T>);

impl<T> DlMutex<T> {
    pub fn new(v: T) -> Self {
        Self(Mutex::new(v))
    }
    pub async fn lock(&self) -> impl DerefMut<Target = T> + '_ {
        TrackedRef::new(with_timeout(self.0.lock()).await)
    }
}

static DL_WARN_LOCKHOLD: LazyLock<Duration> = LazyLock::new(|| {
    let ms = std::env::var("DL_WARN_LOCKHOLD")
        .unwrap_or_else(|_| "1000".to_owned())
        .parse()
        .expect("invalid DL_WARN_LOCKHOLD");
    Duration::from_millis(ms)
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
        if e > *DL_WARN_LOCKHOLD {
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

static DL_TIMEOUT: LazyLock<Duration> = LazyLock::new(|| {
    let ms = std::env::var("DL_TIMEOUT_MS")
        .unwrap_or_else(|_| "10000".to_owned())
        .parse()
        .expect("invalid DL_TIMEOUT_MS");
    Duration::from_millis(ms)
});

async fn with_timeout<T>(fut: impl Future<Output = T>) -> T {
    let Ok(o) = tokio::time::timeout(*DL_TIMEOUT, fut).await else {
        let bt = get_bt();
        use sha2::Digest;
        let mut hasher = Sha256::new();
        hasher.update(&bt);
        let result = hasher.finalize();
        let hex = format!("{:x}", result);
        tokio::fs::create_dir_all("./dl").await.unwrap();
        tokio::fs::write(&format!("./dl/{hex}.txt"), &bt)
            .await
            .unwrap();

        panic!("lock cannot acquired\n{bt}");
    };

    o
}

#[track_caller]
fn get_bt() -> String {
    let bt = backtrace::Backtrace::new();

    let mut frames = vec![vec![]];
    for frame in bt.frames() {
        let syms = frame.symbols();
        let useful = syms
            .iter()
            .flat_map(|x| x.name().and_then(|x| x.as_str()))
            .any(|x| x.contains(env!("CARGO_CRATE_NAME")));
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
