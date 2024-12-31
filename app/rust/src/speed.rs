use std::{cmp::Reverse, sync::Arc, time::Duration};

use crate::{dl::DlMutex as Mutex, HashMap as HashMap};

type Registry = Arc<Mutex<HashMap<String, SpeedStatisticsEntry>>>;

#[derive(Debug, Clone)]
pub struct SpeedStatistics {
    pub m: Registry,
}
#[derive(Debug, Default)]
pub struct SpeedStatisticsEntry {
    pub total_duration: Duration,
    pub count: i32,
}

impl SpeedStatistics {
    pub fn new() -> Self {
        let m = Arc::new(Mutex::new(HashMap::default()));
        tokio::spawn({
            let m = Arc::clone(&m);
            async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    Self::show(&m).await;
                }
            }
        });
        Self { m }
    }

    pub async fn show(m: &Registry) {
        let speed = { std::mem::take(&mut *m.lock().await) };

        let mut speed = speed.into_iter().collect::<Vec<_>>();
        speed.sort_unstable_by_key(|(_p, e): &(String, SpeedStatisticsEntry)| {
            Reverse(e.total_duration.as_millis() / e.count as u128)
        });
        for (path, e) in speed {
            let avg = e.total_duration.as_millis() / e.count as u128;
            let total = e.total_duration.as_millis();
            tracing::info!(
                "{path:40} {:6} requests took {avg:4}ms avg {total:6}ms total",
                e.count
            );
        }
    }

    pub async fn on_request(&self, key: &str, dur: Duration) {
        let mut speed = self.m.lock().await;
        let entry = speed.entry(key.to_owned()).or_default();
        entry.count += 1;
        entry.total_duration += dur;
    }
}
