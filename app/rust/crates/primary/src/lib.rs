pub use shared::models::{self, Error};
use std::sync::Arc;

use repo::Repository;
use shared::{
    ws::{
        pgw::{PgwRequest, PgwResponse},
        WsSystem, WsSystemHandler,
    },
    FxHashMap as HashMap,
};
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct AppState {
    pub pool: sqlx::MySqlPool,
    pub repo: Arc<Repository>,
    pub pgw: WsSystem<PgwSystemHandler>,
    pub speed: SpeedStatistics,
    pub client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub struct PgwSystemHandler;
impl WsSystemHandler for PgwSystemHandler {
    type Request = PgwRequest;
    type Response = PgwResponse;
    type Notification = ();
    type NotificationResponse = ();

    async fn handle(&self, _: Self::Notification) -> Self::NotificationResponse {
        unreachable!()
    }
}

#[derive(Debug, Clone)]
pub struct SpeedStatistics {
    pub m: Arc<Mutex<HashMap<String, SpeedStatisticsEntry>>>,
}
#[derive(Debug, Default)]
pub struct SpeedStatisticsEntry {
    pub total_duration: Duration,
    pub count: i32,
}

pub fn secure_random_str(b: usize) -> String {
    use rand::RngCore as _;
    let mut buf = vec![0; b];
    let mut rng = rand::thread_rng();
    rng.fill_bytes(&mut buf);
    hex::encode(&buf)
}

pub mod app_handlers;
pub mod chair_handlers;
pub mod internal_handlers;
pub mod middlewares;
pub mod owner_handlers;
pub mod repo;
