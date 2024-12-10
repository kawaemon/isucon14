use axum::extract::State;
use isuride::{internal_handlers::do_matching, AppCache, AppDeferred, AppQueue, AppState, Error};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info,tower_http=debug,axum::rejection=trace");
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
        .max_connections(72)
        .connect_with(
            sqlx::mysql::MySqlConnectOptions::default()
                .host(&host)
                .port(port)
                .username(&user)
                .password(&password)
                .database(&dbname),
        )
        .await?;

    let app_state = AppState {
        cache: Arc::new(AppCache::new(&pool).await),
        deferred: Arc::new(AppDeferred::new()),
        queue: Arc::new(AppQueue::new(&pool).await),
        pool,
    };

    {
        let pool = app_state.pool.clone();
        let def = app_state.deferred.clone();
        tokio::spawn(async move {
            loop {
                def.sync(&pool).await;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }

    {
        let state = app_state.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = do_matching(state.clone()).await {
                    tracing::error!("matching failed: {e:?}; continuing anyway");
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
    }

    let app = axum::Router::new()
        .route("/api/initialize", axum::routing::post(post_initialize))
        .merge(isuride::app_handlers::app_routes(app_state.clone()))
        .merge(isuride::owner_handlers::owner_routes(app_state.clone()))
        .merge(isuride::chair_handlers::chair_routes(app_state.clone()))
        .merge(isuride::internal_handlers::internal_routes())
        .with_state(app_state)
        .layer(tower_http::trace::TraceLayer::new_for_http());

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

async fn post_initialize(
    State(AppState {
        pool, cache, queue, ..
    }): State<AppState>,
    axum::Json(req): axum::Json<PostInitializeRequest>,
) -> Result<axum::Json<PostInitializeResponse>, Error> {
    let output = tokio::process::Command::new("../sql/init.sh")
        .output()
        .await?;
    if !output.status.success() {
        return Err(Error::Initialize {
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }

    sqlx::query("UPDATE settings SET value = ? WHERE name = 'payment_gateway_url'")
        .bind(req.payment_server)
        .execute(&pool)
        .await?;

    let AppCache {
        chair_location,
        ride_status_cache,
        chair_ride_cache,
        chair_auth_cache: chair_cache,
        user_auth_cache: user_cache,
    } = AppCache::new(&pool).await;
    *cache.chair_location.write().await = chair_location.into_inner();
    *cache.chair_ride_cache.write().await = chair_ride_cache.into_inner();
    *cache.ride_status_cache.write().await = ride_status_cache.into_inner();
    *cache.chair_auth_cache.write().await = chair_cache.into_inner();
    *cache.user_auth_cache.write().await = user_cache.into_inner();
    let AppQueue {
        chair_notification_queue,
    } = AppQueue::new(&pool).await;
    *queue.chair_notification_queue.lock().await = chair_notification_queue.into_inner();

    tokio::spawn(async move {
        tracing::info!("try to request collection to pprotein");
        if let Err(e) = reqwest::get("http://192.168.0.13:9000/api/group/collect").await {
            tracing::warn!("failed to communicate with pprotein: {e:#?}");
        }
        tracing::info!("try to request collection to pprotein: done");
    });

    Ok(axum::Json(PostInitializeResponse { language: "rust" }))
}
