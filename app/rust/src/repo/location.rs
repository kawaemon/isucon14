use chrono::{DateTime, Utc};

use crate::{
    models::{Chair, ChairLocation, Id},
    owner_handlers::MysqlDecimal,
    Coordinate,
};

use super::{maybe_tx, Repository, Result, Tx};

impl Repository {
    pub async fn chair_location_get_latest(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        id: &Id<Chair>,
    ) -> Result<Option<Coordinate>> {
        let mut tx = tx.into();

        let q = sqlx::query_as(
            "SELECT * FROM chair_locations WHERE chair_id = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(id);
        let Some(coord): Option<ChairLocation> = maybe_tx!(self, tx, q.fetch_optional)? else {
            return Ok(None);
        };

        Ok(Some(coord.coord()))
    }

    pub async fn chair_total_distance(
        &self,
        chair_id: &Id<Chair>,
    ) -> Result<Option<(i64, DateTime<Utc>)>> {
        #[derive(sqlx::FromRow)]
        struct QueryRes {
            total_distance: MysqlDecimal,
            total_distance_updated_at: DateTime<Utc>,
        }

        let r: Option<QueryRes> = sqlx::query_as(r#"
            SELECT
                chair_id,
                SUM(IFNULL(distance, 0)) AS total_distance,
                MAX(created_at)          AS total_distance_updated_at
            FROM (
                SELECT
                    chair_id,
                    created_at,
                    ABS(latitude - LAG(latitude) OVER (PARTITION BY chair_id ORDER BY created_at)) +
                    ABS(longitude - LAG(longitude) OVER (PARTITION BY chair_id ORDER BY created_at)) AS distance
                FROM chair_locations
                where chair_id = ?
            ) tmp
            GROUP BY chair_id
        "#)
            .bind(chair_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(r.map(|x| (x.total_distance.0, x.total_distance_updated_at)))
    }

    pub async fn chair_location_update(
        &self,
        tx: impl Into<Option<&mut Tx>>,
        chair_id: &Id<Chair>,
        coord: Coordinate,
    ) -> Result<DateTime<Utc>> {
        let mut tx = tx.into();
        let created_at = Utc::now();

        let q = sqlx::query(
            "INSERT INTO chair_locations (id, chair_id, latitude, longitude, created_at) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(Id::<ChairLocation>::new())
        .bind(chair_id)
        .bind(coord.latitude)
        .bind(coord.longitude)
        .bind(created_at);

        maybe_tx!(self, tx, q.execute)?;

        Ok(created_at)
    }
}
