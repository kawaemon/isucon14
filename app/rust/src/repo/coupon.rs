use crate::models::{Coupon, Id, Ride, User};

use super::{Repository, Result};

impl Repository {
    pub async fn coupon_get_by_code(&self, code: &str) -> Result<Vec<Coupon>> {
        let r = sqlx::query_as("SELECT * FROM coupons WHERE code = ?")
            .bind(code)
            .fetch_all(&self.pool)
            .await?;
        Ok(r)
    }
    pub async fn coupon_get_by_usedby(&self, ride: &Id<Ride>) -> Result<Option<Coupon>> {
        let r = sqlx::query_as("SELECT * FROM coupons WHERE used_by = ?")
            .bind(ride)
            .fetch_optional(&self.pool)
            .await?;
        Ok(r)
    }
    pub async fn coupon_get_unused_order_by_created_at(
        &self,
        user_id: &Id<User>,
    ) -> Result<Vec<Coupon>> {
        let r = sqlx::query_as(
            "select *
            from coupons
            where user_id = ? and used_by is null
            order by created_at",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(r)
    }

    // writes

    pub async fn coupon_add(&self, user: &Id<User>, code: &str, amount: i32) -> Result<()> {
        sqlx::query("INSERT INTO coupons (user_id, code, discount) VALUES (?, ?, ?)")
            .bind(user)
            .bind(code)
            .bind(amount)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn coupon_set_used(
        &self,
        user: &Id<User>,
        code: &str,
        ride: &Id<Ride>,
    ) -> Result<()> {
        sqlx::query("UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = ?")
            .bind(ride)
            .bind(user)
            .bind(code)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
