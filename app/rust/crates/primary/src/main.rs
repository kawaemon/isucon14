use axum::extract::State;
use futures::StreamExt;
use isuride::repo::Repository;
use isuride::{internal_handlers::spawn_matching_thread, AppState, Error};
use isuride::{Config, PgwSystemHandler, SpeedStatistics};
use shared::pool::get_pool;
use shared::ws::WsSystem;
use shared::FxHashMap as HashMap;
use std::cmp::Reverse;
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let v = std::env::var_os("V").is_some();
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var(
            "RUST_LOG",
            if v {
                "info,tower_http=debug,axum::rejection=trace"
            } else {
                "info,axum::rejection=trace"
            },
        );
    }
    tracing_subscriber::fmt::init();

    let pool = get_pool().await;

    let sub = std::env::var("SUB_SERVER").unwrap_or("ws://localhost:4444".to_owned());

    let repo = Arc::new(Repository::new(&pool, &format!("{sub}/private/coordinate")).await);
    let pgw = {
        let url = &format!("{sub}/private/pgw");
        let (stream, _res) = tokio_tungstenite::connect_async(url).await.unwrap();
        let (tx, rx) = stream.split();
        WsSystem::new(PgwSystemHandler, tx, rx)
    };
    let speed = SpeedStatistics {
        m: Arc::new(Mutex::new(HashMap::default())),
    };
    {
        let speed = speed.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;

                let speed = { std::mem::take(&mut *speed.m.lock().await) };

                let mut speed = speed.into_iter().collect::<Vec<_>>();
                speed.sort_unstable_by_key(|(_p, e)| {
                    Reverse(e.total_duration.as_millis() / e.count as u128)
                });
                for (path, e) in speed {
                    let avg = e.total_duration.as_millis() / e.count as u128;
                    tracing::info!("{path:50} {:6} requests took {:4}ms avg", e.count, avg);
                }
            }
        });
    }
    let client = reqwest::Client::builder()
        .tcp_keepalive(Duration::from_secs(10))
        .build()
        .unwrap();

    let app_state = AppState {
        pool,
        repo,
        pgw,
        speed,
        client,
        config: Config::new(),
    };

    spawn_matching_thread(app_state.clone());

    let app = axum::Router::new()
        .route("/api/initialize", axum::routing::post(post_initialize))
        .merge(isuride::app_handlers::app_routes(app_state.clone()))
        .merge(isuride::owner_handlers::owner_routes(app_state.clone()))
        .merge(isuride::chair_handlers::chair_routes(app_state.clone()))
        .merge(isuride::internal_handlers::internal_routes())
        .with_state(app_state.clone())
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .layer(axum::middleware::from_fn_with_state(
            app_state,
            isuride::middlewares::log_slow_requests,
        ));

    let tcp_listener =
        if let Some(std_listener) = listenfd::ListenFd::from_env().take_tcp_listener(0)? {
            TcpListener::from_std(std_listener)?
        } else {
            TcpListener::bind(&SocketAddr::from(([0, 0, 0, 0], 3000))).await?
        };
    axum::serve(tcp_listener, app).await?;

    Ok(())
}

#[derive(Debug, serde::Deserialize)]
struct PostInitializeRequest {
    payment_server: String,
}

#[derive(Debug, serde::Serialize)]
struct PostInitializeResponse {
    language: &'static str,
}

async fn post_initialize(
    State(AppState { repo, .. }): State<AppState>,
    axum::Json(req): axum::Json<PostInitializeRequest>,
) -> Result<axum::Json<PostInitializeResponse>, Error> {
    let status = tokio::process::Command::new("../sql/init.sh")
        .stdout(Stdio::inherit())
        .stdin(Stdio::inherit())
        .status()
        .await?;
    if !status.success() {
        return Err(Error::Initialize {
            stdout: String::new(),
            stderr: String::new(),
        });
    }

    repo.reinit().await;
    repo.pgw_set(&req.payment_server).await?;

    Ok(axum::Json(PostInitializeResponse { language: "rust" }))
}
