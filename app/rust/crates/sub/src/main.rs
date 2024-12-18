mod coordinate;
mod pgw;

use coordinate::{coordinate_routes, ChairState};
use pgw::{pgw_routes, PgwState};
use shared::pool::get_pool;
use sqlx::{MySql, Pool};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    run().await;
}

async fn run() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info,tower_http=debug,axum::rejection=trace");
    }
    tracing_subscriber::fmt::init();

    let c = std::env::var("CONCURRENCY")
        .unwrap_or("30".to_owned())
        .parse()
        .unwrap();

    tracing::info!("concurrency = {c}");

    let pool = get_pool().await;
    let state = AppState::new(c, &pool).await;

    let app = axum::Router::new()
        .merge(pgw_routes())
        .merge(coordinate_routes(&state))
        .with_state(state)
        .layer(tower_http::trace::TraceLayer::new_for_http());

    axum::serve(TcpListener::bind(("0.0.0.0", 4444)).await.unwrap(), app)
        .await
        .unwrap();
}

#[derive(Debug, Clone)]
struct AppState {
    pgw: PgwState,
    chair: ChairState,
}
impl AppState {
    async fn new(c: usize, pool: &Pool<MySql>) -> Self {
        AppState {
            pgw: PgwState::new(c),
            chair: ChairState::new(pool).await,
        }
    }
}
