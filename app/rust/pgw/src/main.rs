use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use futures_util::lock::Mutex;
use futures_util::SinkExt;
use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

#[tokio::main]
async fn main() {
    run().await;
}

async fn run() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info,tower_http=debug,axum::rejection=trace");
    }
    tracing_subscriber::fmt::init();

    let stats = Arc::new(Stats::new());
    {
        let stats = Arc::clone(&stats);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(5000)).await;
                stats.report();
            }
        });
    }

    let c = std::env::var("CONCURRENCY")
        .unwrap_or("30".to_owned())
        .parse()
        .unwrap();

    tracing::info!("concurrency = {c}");

    let state = AppState {
        semaphore: Arc::new(Semaphore::new(c)),
        client: Client::new(),
        stats: Arc::clone(&stats),
    };

    let app = axum::Router::new()
        .route("/ws", axum::routing::get(endpoint))
        .with_state(state)
        .layer(tower_http::trace::TraceLayer::new_for_http());

    axum::serve(TcpListener::bind(("0.0.0.0", 4444)).await.unwrap(), app)
        .await
        .unwrap();
}

#[derive(Debug)]
struct Stats {
    tries: AtomicUsize,
    success: AtomicUsize,
    failure: AtomicUsize,
    process_ms: AtomicUsize,
}
impl Stats {
    fn new() -> Self {
        Self {
            tries: AtomicUsize::new(0),
            success: AtomicUsize::new(0),
            failure: AtomicUsize::new(0),
            process_ms: AtomicUsize::new(0),
        }
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

#[derive(Debug, Clone)]
struct AppState {
    semaphore: Arc<Semaphore>,
    client: Client,
    stats: Arc<Stats>,
}

async fn endpoint(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|sock| handle(sock, state))
}

async fn handle(stream: WebSocket, state: AppState) {
    let (tx, mut rx) = stream.split();
    let tx = Arc::new(Mutex::new(tx));

    while let Some(Ok(msg)) = rx.next().await {
        match msg {
            Message::Binary(_) => {}
            Message::Close(_) => break,
            Message::Ping(_) | Message::Pong(_) => {}

            Message::Text(req) => {
                let work: Work = match serde_json::from_str(&req) {
                    Ok(a) => a,
                    Err(e) => {
                        tracing::warn!("invalid request {e:?}");
                        continue;
                    }
                };

                let tx = Arc::clone(&tx);
                let state = state.clone();
                tokio::spawn(async move {
                    work.doit(state).await;
                    #[derive(Serialize)]
                    struct Res<'a> {
                        work_id: &'a str,
                    }
                    let res = Res {
                        work_id: &work.work_id,
                    };
                    let res = serde_json::to_string(&res).unwrap();
                    tx.lock().await.send(Message::Text(res)).await.unwrap();
                });
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Work {
    work_id: String,

    url: String,
    token: String,
    amount: i32,
    desired_count: usize,
}

impl Work {
    async fn doit(&self, state: AppState) {
        let _ = state.semaphore.acquire().await.unwrap();
        let begin = Instant::now();
        let url = &self.url;

        for _ in 0..1000 {
            #[derive(serde::Serialize)]
            struct Req {
                amount: i32,
            }
            let Ok(res) = state
                .client
                .post(format!("{url}/payments"))
                .bearer_auth(&self.token)
                .json(&Req {
                    amount: self.amount,
                })
                .send()
                .await
            else {
                continue;
            };

            let status = res.status();
            state.stats.on_req(status);

            if status == reqwest::StatusCode::NO_CONTENT {
                state.stats.on_end(begin.elapsed());
                return;
            }

            // エラーが返ってきても成功している場合があるので、社内決済マイクロサービスに問い合わせ
            let Ok(r) = state
                .client
                .get(format!("{url}/payments"))
                .bearer_auth(&self.token)
                .send()
                .await
            else {
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
