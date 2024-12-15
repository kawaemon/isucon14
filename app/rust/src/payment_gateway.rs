use sqlx::{MySql, Pool};
use tokio::sync::Semaphore;

use crate::models::{Id, User};
use crate::Error;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum PaymentGatewayError {
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("unexpected number of payments: {ride_count} != {payment_count}.")]
    UnexpectedNumberOfPayments {
        ride_count: usize,
        payment_count: usize,
    },
    #[error("[GET /payments] unexpected status code ({0})")]
    GetPayment(reqwest::StatusCode),
}

#[derive(Debug, serde::Serialize)]
pub struct PaymentGatewayPostPaymentRequest {
    pub amount: i32,
}

#[derive(Debug, serde::Deserialize)]
struct PaymentGatewayGetPaymentsResponseOne {
    amount: i32,
    status: String,
}

pub trait PostPaymentCallback<'a> {
    type Output: Future<Output = Result<i32, Error>>;

    fn call(&self, tx: &'a Pool<MySql>, user_id: &'a Id<User>) -> Self::Output;
}
impl<'a, F, Fut> PostPaymentCallback<'a> for F
where
    F: Fn(&'a Pool<MySql>, &'a Id<User>) -> Fut,
    Fut: Future<Output = Result<i32, Error>>,
{
    type Output = Fut;
    fn call(&self, tx: &'a Pool<MySql>, user_id: &'a Id<User>) -> Fut {
        self(tx, user_id)
    }
}

const CONCURRENCY: usize = 10;
const RETRY_LIMIT: usize = 20;

#[derive(Debug, Clone)]
pub struct PaymentGatewayRestricter {
    sema: Arc<Semaphore>,
}
impl PaymentGatewayRestricter {
    pub fn new() -> Self {
        Self {
            sema: Arc::new(Semaphore::new(CONCURRENCY)),
        }
    }
}

pub async fn request_payment_gateway_post_payment<F>(
    pgw: &PaymentGatewayRestricter,
    payment_gateway_url: &str,
    token: &str,
    param: &PaymentGatewayPostPaymentRequest,
    tx: &Pool<MySql>,
    user_id: &Id<User>,
    retrieve_rides_count: F,
) -> Result<(), Error>
where
    F: for<'a> PostPaymentCallback<'a>,
{
    // 失敗したらとりあえずリトライ
    // FIXME: 社内決済マイクロサービスのインフラに異常が発生していて、同時にたくさんリクエストすると変なことになる可能性あり

    let _permit = pgw.sema.acquire().await.unwrap();
    tracing::debug!("permit acquired; remain = {}", pgw.sema.available_permits());

    let mut retry = 0;
    loop {
        let result: Result<(), Error> = async {
            let res = reqwest::Client::new()
                .post(format!("{payment_gateway_url}/payments"))
                .bearer_auth(token)
                .json(param)
                .send()
                .await
                .map_err(PaymentGatewayError::Reqwest)?;

            if res.status() != reqwest::StatusCode::NO_CONTENT {
                // エラーが返ってきても成功している場合があるので、社内決済マイクロサービスに問い合わせ
                let get_res = reqwest::Client::new()
                    .get(format!("{payment_gateway_url}/payments"))
                    .bearer_auth(token)
                    .send()
                    .await
                    .map_err(PaymentGatewayError::Reqwest)?;

                // GET /payments は障害と関係なく200が返るので、200以外は回復不能なエラーとする
                if get_res.status() != reqwest::StatusCode::OK {
                    return Err(PaymentGatewayError::GetPayment(get_res.status()).into());
                }
                let payments: Vec<PaymentGatewayGetPaymentsResponseOne> =
                    get_res.json().await.map_err(PaymentGatewayError::Reqwest)?;

                let rides = retrieve_rides_count.call(tx, user_id).await?;

                if rides as usize != payments.len() {
                    return Err(PaymentGatewayError::UnexpectedNumberOfPayments {
                        ride_count: rides as usize,
                        payment_count: payments.len(),
                    }
                    .into());
                }
            }
            Ok(())
        }
        .await;

        if result.is_err() {
            if retry >= RETRY_LIMIT {
                tracing::error!("pgw request failed: retrying limit reached");
                break;
            }
            retry += 1;
            // tracing::warn!("pgw request failed: retrying [{}/{RETRY_LIMIT}]", retry + 1);
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        break;
    }

    Ok(())
}
