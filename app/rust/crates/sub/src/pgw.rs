use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::{Duration, Instant},
};

use axum::{
    extract::{ws::WebSocket, State, WebSocketUpgrade},
    response::IntoResponse,
};
use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use shared::ws::{
    from_axum,
    pgw::{PgwRequest, PgwResponse},
    WsSystem, WsSystemHandler,
};
use tokio::sync::Semaphore;

use crate::AppState;

#[derive(Debug, Clone)]
pub struct PgwState {
    semaphore: Arc<Semaphore>,
    client: Client,
    stats: Arc<Stats>,
}
impl PgwState {
    pub fn new(c: usize) -> Self {
        PgwState {
            semaphore: Arc::new(Semaphore::new(c)),
            client: Client::new(),
            stats: Stats::new(),
        }
    }
}

pub fn pgw_routes() -> axum::Router<AppState> {
    axum::Router::new().route("/private/pgw", axum::routing::get(endpoint))
}

#[derive(Debug)]
struct Stats {
    tries: AtomicUsize,
    success: AtomicUsize,
    failure: AtomicUsize,
    process_ms: AtomicUsize,
}
impl Stats {
    fn new() -> Arc<Self> {
        let stats = Arc::new(Self {
            tries: AtomicUsize::new(0),
            success: AtomicUsize::new(0),
            failure: AtomicUsize::new(0),
            process_ms: AtomicUsize::new(0),
        });

        tokio::spawn({
            let stats = Arc::clone(&stats);
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(5000)).await;
                    stats.report();
                }
            }
        });

        stats
    }
    fn report(&self) {
        let success = self.success.swap(0, std::sync::atomic::Ordering::Relaxed);
        let failure = self.failure.swap(0, std::sync::atomic::Ordering::Relaxed);
        let tries = self.tries.swap(0, std::sync::atomic::Ordering::Relaxed);
        let process_ms = self
            .process_ms
            .swap(0, std::sync::atomic::Ordering::Relaxed);
        let total = success + failure;
        let ratio = (success as f64 / total as f64 * 100.0) as usize;
        tracing::info!("ok={ratio:3}%, total={total:5}, ok={success:5}, fail={failure:5}");

        let ratio = total as f64 / tries as f64;
        let avg_time = (process_ms as f64 / tries as f64) as usize;
        tracing::info!("tries={tries:5}, req/try={ratio:3.2}, avg={avg_time:5}ms");
    }
    fn on_req(&self, code: StatusCode) {
        if code.is_success() {
            &self.success
        } else {
            &self.failure
        }
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    fn on_end(&self, duration: Duration) {
        self.tries
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.process_ms.fetch_add(
            duration.as_millis() as usize,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

async fn endpoint(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|sock| handle(sock, state))
}

async fn handle(stream: WebSocket, state: AppState) {
    let (tx, rx) = stream.split();
    let (tx, rx) = from_axum(tx, rx);
    let (tx, rx) = (Box::pin(tx), Box::pin(rx));
    let (_sys, ex_wait) = WsSystem::new_wait_for_recv(SystemHandler { state }, tx, rx);
    ex_wait.await.unwrap();
}

#[derive(Debug, Clone)]
struct SystemHandler {
    state: AppState,
}

impl WsSystemHandler for SystemHandler {
    type Request = ();
    type Response = ();
    type Notification = PgwRequest;
    type NotificationResponse = PgwResponse;

    async fn handle(&self, req: Self::Notification) -> Self::NotificationResponse {
        let work: Work = req.into();
        work.doit(&self.state).await;
        PgwResponse
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Work {
    url: String,
    token: String,
    amount: i32,
    desired_count: usize,
}
impl From<PgwRequest> for Work {
    fn from(value: PgwRequest) -> Self {
        Work {
            url: value.url,
            token: value.token,
            amount: value.amount,
            desired_count: value.desired_count,
        }
    }
}

impl Work {
    async fn doit(&self, AppState { pgw: state, .. }: &AppState) {
        let _ = state.semaphore.acquire().await.unwrap();
        let begin = Instant::now();
        let url = format!("{}/payments", &self.url);

        for _ in 0..1000 {
            #[derive(serde::Serialize)]
            struct Req {
                amount: i32,
            }

            let req = Req {
                amount: self.amount,
            };
            let req = state.client.post(&url).bearer_auth(&self.token).json(&req);
            let Ok(res) = req.send().await else {
                continue;
            };

            let status = res.status();
            state.stats.on_req(status);

            if status == reqwest::StatusCode::NO_CONTENT {
                state.stats.on_end(begin.elapsed());
                return;
            }

            // エラーが返ってきても成功している場合があるので、社内決済マイクロサービスに問い合わせ
            let Ok(r) = state.client.get(&url).bearer_auth(&self.token).send().await else {
                continue;
            };

            let status = r.status();
            state.stats.on_req(status);

            if status != reqwest::StatusCode::OK {
                continue;
            }

            #[derive(Deserialize)]
            struct Res {}
            let Ok(r): Result<Vec<Res>, _> = r.json().await else {
                continue;
            };
            let payment_len = r.len();

            if self.desired_count == payment_len {
                state.stats.on_end(begin.elapsed());
                return;
            }
        }

        panic!("failed, {self:#?}");
    }
}
