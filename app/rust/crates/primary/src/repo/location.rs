use futures::StreamExt;
use shared::{
    models::{Ride, RideStatusEnum},
    ws::{
        coordinate::{CoordNotification, CoordNotificationResponse, CoordRequest, CoordResponse},
        WsSystem, WsSystemHandler,
    },
    FxHashMap as HashMap,
};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use shared::DlRwLock as RwLock;
use sqlx::{MySql, Pool};

use crate::models::{Chair, Coordinate, Id};

use super::{cache_init::CacheInit, Repository, Result};

pub type ChairLocationCache = Arc<ChairLocationCacheInner>;

#[derive(Debug, Clone)]
struct Handler {
    repo: Arc<RwLock<Option<Repository>>>, // 相互依存 T T
}

impl WsSystemHandler for Handler {
    type Request = CoordRequest;
    type Response = CoordResponse;
    type Notification = CoordNotification;
    type NotificationResponse = CoordNotificationResponse;

    async fn handle(&self, req: CoordNotification) -> Self::NotificationResponse {
        match req {
            CoordNotification::AtDestination {
                ride,
                chair: _,
                status,
            } => {
                let repo = self.repo.read_notrack().await;
                let Some(s) = repo.as_ref() else {
                    panic!("repository post init not called");
                };
                s.ride_status_update(&ride, status).await.unwrap();
                CoordNotificationResponse::AtDestination
            }
        }
    }
}

#[derive(Debug)]
pub struct ChairLocationCacheInner {
    reporef: Arc<RwLock<Option<Repository>>>,
    ws: WsSystem<Handler>,
}

impl Repository {
    pub(super) async fn init_chair_location_cache(path: &str) -> ChairLocationCache {
        let (stream, _res) = tokio_tungstenite::connect_async(path).await.unwrap();
        let (tx, rx) = stream.split();

        let reporef = Arc::new(RwLock::new(None));
        let h = Handler {
            repo: Arc::clone(&reporef),
        };
        let ws = WsSystem::new(h, tx, rx);

        Arc::new(ChairLocationCacheInner { reporef, ws })
    }

    pub(super) async fn chair_location_post_init(&self) {
        *self.chair_location_cache.reporef.write().await = Some(self.clone());
    }

    pub(super) async fn reinit_chair_location_cache(
        &self,
        _pool: &Pool<MySql>,
        _init: &mut CacheInit,
    ) {
        let res = self
            .chair_location_cache
            .ws
            .enqueue(CoordRequest::ReInit)
            .await;
        assert!(matches!(res, CoordResponse::Reinit));
    }
}

impl Repository {
    pub async fn chair_add_to_gw(&self, chair: &Id<Chair>, token: &str) {
        let res = self
            .chair_location_cache
            .ws
            .enqueue(CoordRequest::NewChair {
                id: chair.clone(),
                token: token.to_owned(),
            })
            .await;
        assert!(matches!(res, CoordResponse::NewChair));
    }
    pub async fn chair_movement_register(
        &self,
        chair: &Id<Chair>,
        ride: &Id<Ride>,
        target: Coordinate,
        next: RideStatusEnum,
    ) {
        let res = self
            .chair_location_cache
            .ws
            .enqueue(CoordRequest::ChairMovement {
                ride: ride.clone(),
                chair: chair.clone(),
                dest: target,
                new_state: next,
            })
            .await;
        assert!(matches!(res, CoordResponse::ChairMovement));
    }

    pub async fn chair_location_get_latest(
        &self,
        id: &[Id<Chair>],
    ) -> Result<HashMap<Id<Chair>, Coordinate>> {
        let res = self
            .chair_location_cache
            .ws
            .enqueue(CoordRequest::Get(id.to_vec()))
            .await;
        match res {
            CoordResponse::Get(r) => {
                let r = r.into_iter().map(|(k, v)| (k, v.latest)).collect();
                Ok(r)
            }
            _ => panic!("unknown response"),
        }
    }

    pub async fn chair_total_distance(
        &self,
        id: &[Id<Chair>],
    ) -> Result<HashMap<Id<Chair>, (i64, DateTime<Utc>)>> {
        let res = self
            .chair_location_cache
            .ws
            .enqueue(CoordRequest::Get(id.to_vec()))
            .await;
        match res {
            CoordResponse::Get(r) => {
                let r = r
                    .into_iter()
                    .map(|(k, v)| (k, (v.total_distance, v.latest_updated_at)))
                    .collect();
                Ok(r)
            }
            _ => panic!("unknown response"),
        }
    }
}
