use crate::FxHashMap as HashMap;

use chrono::{DateTime, Utc};
use sqlx::{MySql, QueryBuilder};

use crate::{
    models::{Id, RideStatus},
    repo::deferred::Deferrable,
};

#[derive(Debug)]
pub enum NotifiedType {
    App,
    Chair,
}

#[derive(Debug)]
pub struct RideStatusUpdate {
    pub ty: NotifiedType,
    pub status_id: Id<RideStatus>,
    pub at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct UpdateQuery {
    id: Id<RideStatus>,
    app: Option<DateTime<Utc>>,
    chair: Option<DateTime<Utc>>,
}

pub struct RideStatusDeferrable;
impl Deferrable for RideStatusDeferrable {
    const NAME: &str = "ride_statuses";

    type Insert = RideStatus;
    type Update = RideStatusUpdate;

    type UpdateQuery = UpdateQuery;

    fn summarize(
        inserts: &mut [Self::Insert],
        updates: Vec<Self::Update>,
    ) -> Vec<Self::UpdateQuery> {
        let mut inserts = inserts
            .iter_mut()
            .map(|x| (x.id.clone(), x))
            .collect::<HashMap<_, _>>();
        let mut actual_updates: HashMap<Id<RideStatus>, UpdateQuery> = HashMap::default();
        for u in updates {
            let Some(i) = inserts.get_mut(&u.status_id) else {
                let r = actual_updates
                    .entry(u.status_id.clone())
                    .or_insert_with(|| UpdateQuery {
                        id: u.status_id.clone(),
                        app: None,
                        chair: None,
                    });
                match u.ty {
                    NotifiedType::App => r.app = Some(u.at),
                    NotifiedType::Chair => r.chair = Some(u.at),
                }
                continue;
            };
            match u.ty {
                NotifiedType::App => i.app_sent_at = Some(u.at),
                NotifiedType::Chair => i.chair_sent_at = Some(u.at),
            }
        }
        actual_updates.into_values().collect()
    }

    async fn exec_insert(tx: &mut sqlx::Transaction<'static, MySql>, inserts: &[Self::Insert]) {
        let mut insert_query = QueryBuilder::new(
            "insert into ride_statuses
                    (id, ride_id, status, created_at, app_sent_at, chair_sent_at)",
        );

        insert_query.push_values(inserts, |mut b, i| {
            b.push_bind(&i.id)
                .push_bind(&i.ride_id)
                .push_bind(i.status)
                .push_bind(i.created_at)
                .push_bind(i.app_sent_at)
                .push_bind(i.chair_sent_at);
        });

        insert_query.build().execute(&mut **tx).await.unwrap();
    }

    async fn exec_update(tx: &mut sqlx::Transaction<'static, MySql>, update: &Self::UpdateQuery) {
        let mut update_query = QueryBuilder::new("update ride_statuses set");
        let mut need_sepa = false;
        if let Some(app) = update.app {
            update_query.push(" app_sent_at = ");
            update_query.push_bind(app);
            need_sepa = true
        }
        if let Some(app) = update.chair {
            if need_sepa {
                update_query.push(",");
            }
            update_query.push(" chair_sent_at = ");
            update_query.push_bind(app);
        }
        update_query.push(" where id = ");
        update_query.push_bind(&update.id);
        update_query.build().execute(&mut **tx).await.unwrap();
    }
}
