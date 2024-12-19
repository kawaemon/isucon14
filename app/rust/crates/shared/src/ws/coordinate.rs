use chrono::{DateTime, Utc};

use crate::models::{Chair, Coordinate, RideStatusEnum};

use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoordRequest {
    ReInit,
    NewChair {
        id: Id<Chair>,
        token: String,
    },
    ChairMovement {
        chair: Id<Chair>,
        dest: Coordinate,
        new_state: RideStatusEnum,
    },
    Get(Vec<Id<Chair>>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoordResponse {
    Reinit,
    NewChair,
    ChairMovement,
    Get(HashMap<Id<Chair>, CoordResponseGet>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordResponseGet {
    pub latest: Coordinate,
    pub latest_updated_at: DateTime<Utc>,
    pub total_distance: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoordNotification {
    AtDestination {
        chair: Id<Chair>,
        status: RideStatusEnum,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoordNotificationResponse {
    AtDestination,
}
