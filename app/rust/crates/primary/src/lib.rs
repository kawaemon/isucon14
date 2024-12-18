use std::sync::{atomic::AtomicUsize, Arc};

use axum::{http::StatusCode, response::Response};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use models::Id;
use repo::Repository;
use shared::FxHashMap as HashMap;
use std::time::Duration;
use tokio::{
    net::TcpStream,
    sync::{oneshot, Mutex},
};
use tokio_tungstenite::{
    tungstenite::{Message, Utf8Bytes},
    MaybeTlsStream, WebSocketStream,
};

#[derive(Debug, Clone)]
pub struct AppState {
    pub pool: sqlx::MySqlPool,
    pub repo: Arc<Repository>,
    pub pgw: PaymentGateway,
    pub speed: SpeedStatistics,
    pub client: reqwest::Client,
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

#[derive(Debug, Clone)]
pub struct PaymentGateway {
    #[allow(clippy::type_complexity)]
    conns: Arc<Vec<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>>,
    jobs: Arc<Mutex<HashMap<Id<PaymentGateway>, oneshot::Sender<()>>>>,
    count: Arc<AtomicUsize>,
}

impl PaymentGateway {
    async fn recv_routine(
        mut rx: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        url: &str,
        jobs: Arc<Mutex<HashMap<Id<PaymentGateway>, oneshot::Sender<()>>>>,
    ) {
        while let Some(Ok(msg)) = rx.next().await {
            let msg = match msg {
                Message::Text(msg) => msg,
                Message::Binary(_)
                | Message::Ping(_)
                | Message::Pong(_)
                | Message::Close(_)
                | Message::Frame(_) => continue,
            };

            #[derive(serde::Deserialize)]
            struct Res {
                work_id: Id<PaymentGateway>,
            }
            let res: Res = match serde_json::from_str(&msg) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("unknown message received from {url}: {e:?}");
                    continue;
                }
            };
            jobs.lock()
                .await
                .remove(&res.work_id)
                .unwrap()
                .send(())
                .unwrap();
        }
        tracing::error!("connection for {url} ended");
    }

    pub async fn new(urls: &[impl AsRef<str>]) -> Self {
        let jobs = Arc::new(Mutex::new(HashMap::default()));

        let mut txs = vec![];
        for url in urls.iter() {
            let (socket, _res) = tokio_tungstenite::connect_async(url.as_ref())
                .await
                .unwrap();
            let (tx, rx) = socket.split();
            txs.push(Mutex::new(tx));

            let jobs = Arc::clone(&jobs);
            let url = url.as_ref().to_owned();
            tokio::spawn(async move {
                Self::recv_routine(rx, &url, jobs).await;
            });
        }

        Self {
            conns: Arc::new(txs),
            jobs,
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn enqueue(&self, url: &str, token: &str, amount: i32, desired_payment_count: usize) {
        #[derive(serde::Serialize)]
        struct Req<'a> {
            work_id: Id<PaymentGateway>,
            url: &'a str,
            token: &'a str,
            amount: i32,
            desired_count: usize,
        }

        let r = Req {
            work_id: Id::new(),
            url,
            token,
            amount,
            desired_count: desired_payment_count,
        };

        let (tx, rx) = oneshot::channel();

        {
            self.jobs.lock().await.insert(r.work_id.clone(), tx);
        }

        let c = self
            .count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let target = c % self.conns.len();

        let msg = serde_json::to_string(&r).unwrap();

        {
            self.conns[target]
                .lock()
                .await
                .send(Message::Text(Utf8Bytes::from(msg)))
                .await
                .unwrap();
        }

        rx.await.unwrap();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SQLx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("failed to initialize: stdout={stdout} stderr={stderr}")]
    Initialize { stdout: String, stderr: String },
    #[error("{0}")]
    PaymentGateway(#[from] crate::payment_gateway::PaymentGatewayError),
    #[error("{0}")]
    BadRequest(&'static str),
    #[error("{0}")]
    Unauthorized(&'static str),
    #[error("{0}")]
    NotFound(&'static str),
    #[error("{0}")]
    Conflict(&'static str),
}
impl axum::response::IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::PaymentGateway(_) => StatusCode::BAD_GATEWAY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        #[derive(Debug, serde::Serialize)]
        struct ErrorBody {
            message: String,
        }
        let message = self.to_string();
        tracing::error!("{message}");

        (status, axum::Json(ErrorBody { message })).into_response()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy, serde::Serialize, serde::Deserialize)]
pub struct Coordinate {
    pub latitude: i32,
    pub longitude: i32,
}
impl Coordinate {
    pub fn distance(&self, other: Coordinate) -> i32 {
        (self.latitude.abs_diff(other.latitude) + self.longitude.abs_diff(other.longitude)) as i32
    }
}

pub fn secure_random_str(b: usize) -> String {
    use rand::RngCore as _;
    let mut buf = vec![0; b];
    let mut rng = rand::thread_rng();
    rng.fill_bytes(&mut buf);
    hex::encode(&buf)
}

const INITIAL_FARE: i32 = 500;
const FARE_PER_DISTANCE: i32 = 100;

pub fn calculate_fare(pickup: Coordinate, dest: Coordinate) -> i32 {
    let metered_fare = FARE_PER_DISTANCE * pickup.distance(dest);
    INITIAL_FARE + metered_fare
}

pub mod app_handlers;
pub mod chair_handlers;
pub mod internal_handlers;
pub mod middlewares;
pub mod models;
pub mod owner_handlers;
pub mod payment_gateway;
pub mod repo;
