use axum::extract::State;
use isuride::payment_gateway::PaymentGatewayRestricter;
use isuride::repo::Repository;
use isuride::speed::SpeedStatistics;
use isuride::{internal_handlers::spawn_matching_thread, AppState, Error};
use isuride::{AppStateInner, NotificationStatistics};
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

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

    let host = std::env::var("ISUCON_DB_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("ISUCON_DB_PORT")
        .map(|port_str| {
            port_str.parse().expect(
                "failed to convert DB port number from ISUCON_DB_PORT environment variable into u16",
            )
        })
        .unwrap_or(3306);
    let user = std::env::var("ISUCON_DB_USER").unwrap_or_else(|_| "isucon".to_owned());
    let password = std::env::var("ISUCON_DB_PASSWORD").unwrap_or_else(|_| "isucon".to_owned());
    let dbname = std::env::var("ISUCON_DB_NAME").unwrap_or_else(|_| "isuride".to_owned());

    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(128)
        .min_connections(128)
        .connect_with(
            sqlx::mysql::MySqlConnectOptions::default()
                .host(&host)
                .port(port)
                .username(&user)
                .password(&password)
                .database(&dbname),
        )
        .await?;

    let repo = Arc::new(Repository::new(&pool).await);
    let pgw = PaymentGatewayRestricter::new();
    let speed = SpeedStatistics::new();
    let client = reqwest::Client::builder()
        .tcp_keepalive(Duration::from_secs(10))
        .build()
        .unwrap();

    let app_state = Arc::new(AppStateInner {
        pool,
        repo,
        pgw,
        speed,
        client,
        chair_notification_stat: NotificationStatistics::new(),
        user_notification_stat: NotificationStatistics::new(),
    });

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
            TcpListener::bind(&SocketAddr::from(([0, 0, 0, 0], 8080))).await?
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

#[axum::debug_handler]
async fn post_initialize(
    State(state): State<AppState>,
    axum::Json(req): axum::Json<PostInitializeRequest>,
) -> Result<axum::Json<PostInitializeResponse>, Error> {
    let status = tokio::process::Command::new("../sql/init.sh")
        .stdout(Stdio::inherit())
        .status()
        .await?;
    if !status.success() {
        return Err(Error::Initialize {
            stdout: String::new(),
            stderr: String::new(),
        });
    }

    state.repo.reinit().await;
    state.repo.pgw_set(&req.payment_server).await?;

    Ok(axum::Json(PostInitializeResponse { language: "rust" }))
}
