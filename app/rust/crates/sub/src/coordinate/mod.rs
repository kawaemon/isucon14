use std::sync::Arc;

use axum::{
    extract::{ws::WebSocket, Request, State, WebSocketUpgrade},
    middleware::Next,
    response::{IntoResponse, Response},
};
use axum_extra::extract::CookieJar;
use futures_util::StreamExt;
use shared::DlRwLock as RwLock;
use shared::{
    models::{Chair, Coordinate, Error, Id},
    ws::{
        coordinate::{CoordNotification, CoordNotificationResponse, CoordRequest, CoordResponse},
        from_axum, WsSystem, WsSystemHandler,
    },
};
use sqlx::{MySql, Pool};

use crate::AppState;

pub fn coordinate_routes(state: &AppState) -> axum::Router<AppState> {
    let public = axum::Router::new()
        .route("/public/coordinate", axum::routing::post(pub_coordinate))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            chair_auth_middleware,
        ));
    let private = axum::Router::new().route("/private/coordinate", axum::routing::get(ws_endpoint));
    public.merge(private)
}

pub async fn chair_auth_middleware(
    State(AppState {
        chair: ChairState { repo, .. },
        ..
    }): State<AppState>,
    jar: CookieJar,
    mut req: Request,
    next: Next,
) -> Result<Response, Error> {
    let Some(c) = jar.get("chair_session") else {
        return Err(Error::Unauthorized("chair_session cookie is required"));
    };
    let access_token = c.value();
    let Some(chair): Option<Id<Chair>> = repo.chair_get_by_access_token(access_token).await else {
        return Err(Error::Unauthorized("invalid access token"));
    };

    req.extensions_mut().insert(chair);

    Ok(next.run(req).await)
}

#[derive(Debug, serde::Serialize)]
struct ChairPostCoordinateResponse {
    recorded_at: i64,
}

async fn pub_coordinate(
    State(AppState { chair: state, .. }): State<AppState>,
    axum::Extension(chair): axum::Extension<Id<Chair>>,
    axum::Json(req): axum::Json<Coordinate>,
) -> Result<axum::Json<ChairPostCoordinateResponse>, Error> {
    let (created_at, new_status) = state.repo.chair_location_update(&chair, req).await;

    if let Some(status) = new_status {
        // しゃーない、まあコネクション貼るのは最初だけなので問題ないはず
        let tx = state.tx.read().await;
        let Some(tx) = tx.as_ref() else {
            panic!("no connection to primary server");
        };
        let n = CoordNotification::AtDestination {
            chair: chair.clone(),
            status,
        };
        let res = tx.enqueue(n).await;
        assert!(matches!(res, CoordNotificationResponse::AtDestination));
    }

    Ok(axum::Json(ChairPostCoordinateResponse {
        recorded_at: created_at.timestamp_millis(),
    }))
}

mod repo;

#[derive(Debug, Clone)]
pub struct ChairState {
    repo: Arc<repo::ChairRepository>,
    tx: Arc<RwLock<Option<WsSystem<SystemHandler>>>>,
}

impl ChairState {
    pub async fn new(pool: &Pool<MySql>) -> Self {
        Self {
            repo: Arc::new(repo::ChairRepository::new(pool).await),
            tx: Arc::new(RwLock::new(None)),
        }
    }
}

async fn ws_endpoint(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|sock| handle(sock, state))
}

async fn handle(stream: WebSocket, state: AppState) {
    let (tx, rx) = stream.split();
    let (tx, rx) = from_axum(tx, rx);
    let (tx, rx) = (Box::pin(tx), Box::pin(rx));

    let hd = SystemHandler {
        repo: Arc::clone(&state.chair.repo),
    };

    let (sys, ex_wait) = WsSystem::new_wait_for_recv(hd, tx, rx);
    {
        let mut tx = state.chair.tx.write().await;
        assert!(
            tx.is_none(),
            "multiple connection to primary server attempted"
        );
        *tx = Some(sys);
    }

    ex_wait.await.unwrap();
}

#[derive(Clone)]
struct SystemHandler {
    repo: Arc<repo::ChairRepository>,
}
impl WsSystemHandler for SystemHandler {
    type Request = CoordNotification;
    type Response = CoordNotificationResponse;

    type Notification = CoordRequest;
    type NotificationResponse = CoordResponse;

    async fn handle(&self, req: Self::Notification) -> CoordResponse {
        match req {
            CoordRequest::ReInit => {
                self.repo.reinit().await;
                CoordResponse::Reinit
            }

            CoordRequest::NewChair { id, token } => {
                self.repo.chair_add(&id, &token).await;
                CoordResponse::NewChair
            }

            CoordRequest::ChairMovement {
                chair,
                dest,
                new_state,
            } => {
                self.repo.chair_set_movement(&chair, dest, new_state).await;
                CoordResponse::ChairMovement
            }

            CoordRequest::Get(ids) => {
                let res = self.repo.chair_get_bulk(&ids).await;
                CoordResponse::Get(res)
            }
        }
    }
}
