use crate::fw::{Controller, SerializeJson};
use crate::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use cookie::Cookie;
use hyper::StatusCode;

use crate::models::{Chair, Id, Owner, Symbol};
use crate::Error;

pub async fn owner_post_owners(
    c: &mut Controller,
) -> Result<(StatusCode, impl SerializeJson), Error> {
    #[derive(serde::Deserialize)]
    struct Req {
        name: Symbol,
    }

    let req: Req = c.body().await?;

    let owner_id = Id::new();
    let access_token = Symbol::new_from(crate::secure_random_str(8));
    let chair_register_token = Symbol::new_from(crate::secure_random_str(8));

    c.state()
        .repo
        .owner_add(owner_id, req.name, access_token, chair_register_token)?;

    c.cookie_add(Cookie::build(("owner_session", access_token.resolve())).path("/"));

    #[derive(serde::Serialize, macros::SerializeJson)]
    struct Res {
        id: Id<Owner>,
        chair_register_token: Symbol,
    }
    Ok((
        StatusCode::CREATED,
        Res {
            id: owner_id,
            chair_register_token,
        },
    ))
}

pub fn owner_get_sales(
    c: &mut Controller,
    since: Option<i64>,
    until: Option<i64>,
) -> Result<impl SerializeJson, Error> {
    let owner = c.auth_owner()?;
    let since = if let Some(since) = since {
        DateTime::from_timestamp_millis(since).unwrap()
    } else {
        DateTime::from_timestamp_millis(0).unwrap()
    };
    let until = if let Some(until) = until {
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

    #[derive(Debug, serde::Serialize, macros::SerializeJson)]
    struct Res {
        total_sales: i32,
        chairs: Vec<ChairSales>,
        models: Vec<ModelSales>,
    }
    #[derive(Debug, serde::Serialize, macros::SerializeJson)]
    pub struct ChairSales {
        pub id: Id<Chair>,
        pub name: Symbol,
        pub sales: i32,
    }
    #[derive(Debug, serde::Serialize, macros::SerializeJson)]
    struct ModelSales {
        model: Symbol,
        sales: i32,
    }

    let mut res = Res {
        total_sales: 0,
        chairs: Vec::new(),
        models: Vec::new(),
    };

    let mut model_sales_by_model = HashMap::default();

    for chair in c
        .state()
        .repo
        .chair_sale_stats_by_owner(owner.id, since, until)?
    {
        res.total_sales += chair.sales;
        *model_sales_by_model.entry(chair.model).or_insert(0) += chair.sales;
        res.chairs.push(ChairSales {
            id: chair.id,
            name: chair.name,
            sales: chair.sales,
        });
    }

    for (model, sales) in model_sales_by_model {
        res.models.push(ModelSales { model, sales });
    }

    Ok(res)
}

pub fn owner_get_chairs(c: &mut Controller) -> Result<impl SerializeJson, Error> {
    let owner = c.auth_owner()?;
    let state = c.state();
    let chairs = state.repo.chair_get_by_owner(owner.id)?;

    #[derive(Debug, serde::Serialize, macros::SerializeJson)]
    struct Res {
        chairs: Vec<ResChair>,
    }
    #[derive(Debug, serde::Serialize, macros::SerializeJson)]
    struct ResChair {
        id: Id<Chair>,
        name: Symbol,
        model: Symbol,
        active: bool,
        registered_at: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serjson(skip_if_none)]
        total_distance_updated_at: Option<i64>,
        total_distance: i64,
    }

    let mut res = vec![];
    for chair in chairs {
        let (total_distance, total_distance_updated_at) = state
            .repo
            .chair_total_distance(chair.id)?
            .map(|x| (x.0, Some(x.1.timestamp_millis())))
            .unwrap_or((0, None));

        res.push(ResChair {
            id: chair.id,
            name: chair.name,
            model: chair.model,
            active: chair.is_active,
            registered_at: chair.created_at.timestamp_millis(),
            total_distance,
            total_distance_updated_at,
        })
    }

    Ok(Res { chairs: res })
}
