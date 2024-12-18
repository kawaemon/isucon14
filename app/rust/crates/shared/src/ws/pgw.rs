use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgwRequest {
    pub url: String,
    pub token: String,
    pub amount: i32,
    pub desired_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgwResponse;
