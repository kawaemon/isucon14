use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum_extra::extract::cookie::Cookie;
use axum_extra::extract::CookieJar;
use chrono::{DateTime, NaiveDate, Utc};

use crate::models::{Chair, Id, Owner, Ride};
use crate::{AppState, Error};

pub fn owner_routes(app_state: AppState) -> axum::Router<AppState> {
    let routes =
        axum::Router::new().route("/api/owner/owners", axum::routing::post(owner_post_owners));

    let authed_routes = axum::Router::new()
        .route("/api/owner/sales", axum::routing::get(owner_get_sales))
        .route("/api/owner/chairs", axum::routing::get(owner_get_chairs))
        .route_layer(axum::middleware::from_fn_with_state(
            app_state.clone(),
            crate::middlewares::owner_auth_middleware,
        ));

    routes.merge(authed_routes)
}

#[derive(Debug, serde::Deserialize)]
struct OwnerPostOwnersRequest {
    name: String,
}

#[derive(Debug, serde::Serialize)]
struct OwnerPostOwnersResponse {
    id: Id<Owner>,
    chair_register_token: String,
}

async fn owner_post_owners(
    State(AppState { repo, .. }): State<AppState>,
    jar: CookieJar,
    axum::Json(req): axum::Json<OwnerPostOwnersRequest>,
) -> Result<(CookieJar, (StatusCode, axum::Json<OwnerPostOwnersResponse>)), Error> {
    let owner_id = Id::new();
    let access_token = crate::secure_random_str(32);
    let chair_register_token = crate::secure_random_str(32);

    repo.owner_add(&owner_id, &req.name, &access_token, &chair_register_token)
        .await?;

    let jar = jar.add(Cookie::build(("owner_session", access_token)).path("/"));

    Ok((
        jar,
        (
            StatusCode::CREATED,
            axum::Json(OwnerPostOwnersResponse {
                id: owner_id,
                chair_register_token,
            }),
        ),
    ))
}

#[derive(Debug, serde::Serialize)]
struct ChairSales {
    id: Id<Chair>,
    name: String,
    sales: i32,
}

#[derive(Debug, serde::Serialize)]
struct ModelSales {
    model: String,
    sales: i32,
}

#[derive(Debug, serde::Serialize)]
struct OwnerGetSalesResponse {
    total_sales: i32,
    chairs: Vec<ChairSales>,
    models: Vec<ModelSales>,
}

#[derive(Debug, serde::Deserialize)]
struct GetOwnerSalesQuery {
    since: Option<i64>,
    until: Option<i64>,
}

async fn owner_get_sales(
    State(AppState { pool, repo, .. }): State<AppState>,
    axum::Extension(owner): axum::Extension<Owner>,
    Query(query): Query<GetOwnerSalesQuery>,
) -> Result<axum::Json<OwnerGetSalesResponse>, Error> {
    let since = if let Some(since) = query.since {
        DateTime::from_timestamp_millis(since).unwrap()
    } else {
        DateTime::from_timestamp_millis(0).unwrap()
    };
    let until = if let Some(until) = query.until {
        DateTime::from_timestamp_millis(until).unwrap()
    } else {
        DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(9999, 12, 31)
                .unwrap()
                .and_hms_opt(23, 59, 59)
                .unwrap(),
            Utc,
        )
    };

    let chairs: Vec<Chair> = repo.chair_get_by_owner(&owner.id).await?;

    let mut res = OwnerGetSalesResponse {
        total_sales: 0,
        chairs: Vec::with_capacity(chairs.len()),
        models: Vec::new(),
    };

    let mut model_sales_by_model = HashMap::new();

    for chair in chairs {
        let reqs: Vec<Ride> = sqlx::query_as(
            "SELECT rides.*
            FROM rides
            JOIN ride_statuses ON rides.id = ride_statuses.ride_id
            WHERE
                chair_id = ?
            AND status = 'COMPLETED'
            AND updated_at BETWEEN ? AND ? + INTERVAL 999 MICROSECOND",
        )
        .bind(&chair.id)
        .bind(since)
        .bind(until)
        .fetch_all(&pool)
        .await?;

        let sales = reqs.iter().map(|x| x.calc_sale()).sum();
        res.total_sales += sales;

        res.chairs.push(ChairSales {
            id: chair.id,
            name: chair.name,
            sales,
        });

        *model_sales_by_model.entry(chair.model).or_insert(0) += sales;
    }

    for (model, sales) in model_sales_by_model {
        res.models.push(ModelSales { model, sales });
    }

    Ok(axum::Json(res))
}

/// MySQL で COUNT()、SUM() 等を使って DECIMAL 型の値になったものを i64 に変換するための構造体。
#[derive(Debug)]
pub struct MysqlDecimal(pub i64);
impl sqlx::Decode<'_, sqlx::MySql> for MysqlDecimal {
    fn decode(
        value: sqlx::mysql::MySqlValueRef,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        use sqlx::{Type as _, ValueRef as _};

        let type_info = value.type_info();
        if i64::compatible(&type_info) {
            i64::decode(value).map(Self)
        } else if u64::compatible(&type_info) {
            let n = u64::decode(value)?.try_into()?;
            Ok(Self(n))
        } else if sqlx::types::Decimal::compatible(&type_info) {
            use num_traits::ToPrimitive as _;
            let n = sqlx::types::Decimal::decode(value)?
                .to_i64()
                .expect("failed to convert DECIMAL type to i64");
            Ok(Self(n))
        } else {
            panic!("MysqlDecimal is used with unknown type: {type_info:?}");
        }
    }
}
impl sqlx::Type<sqlx::MySql> for MysqlDecimal {
    fn type_info() -> sqlx::mysql::MySqlTypeInfo {
        i64::type_info()
    }

    fn compatible(ty: &sqlx::mysql::MySqlTypeInfo) -> bool {
        i64::compatible(ty) || u64::compatible(ty) || sqlx::types::Decimal::compatible(ty)
    }
}
impl From<MysqlDecimal> for i64 {
    fn from(value: MysqlDecimal) -> Self {
        value.0
    }
}

#[derive(Debug, serde::Serialize)]
struct OwnerGetChairResponse {
    chairs: Vec<OwnerGetChairResponseChair>,
}

#[derive(Debug, serde::Serialize)]
struct OwnerGetChairResponseChair {
    id: Id<Chair>,
    name: String,
    model: String,
    active: bool,
    registered_at: i64,
    total_distance: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_distance_updated_at: Option<i64>,
}

async fn owner_get_chairs(
    State(AppState { repo, .. }): State<AppState>,
    axum::Extension(owner): axum::Extension<Owner>,
) -> Result<axum::Json<OwnerGetChairResponse>, Error> {
    let chairs = repo.chair_get_by_owner(&owner.id).await?;

    let mut res = vec![];
    for chair in chairs {
        let (total_distance, total_distance_updated_at) = repo
            .chair_total_distance(&chair.id)
            .await?
            .map(|x| (x.0, Some(x.1.timestamp_millis())))
            .unwrap_or((0, None));

        res.push(OwnerGetChairResponseChair {
            id: chair.id,
            name: chair.name,
            model: chair.model,
            active: chair.is_active,
            registered_at: chair.created_at.timestamp_millis(),
            total_distance,
            total_distance_updated_at,
        })
    }

    Ok(axum::Json(OwnerGetChairResponse { chairs: res }))
}
