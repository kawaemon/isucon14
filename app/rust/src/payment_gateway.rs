use crate::models::{Id, User};
use crate::Error;
use std::future::Future;

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

    fn call(&self, tx: &'a mut sqlx::MySqlConnection, user_id: &'a Id<User>) -> Self::Output;
}
impl<'a, F, Fut> PostPaymentCallback<'a> for F
where
    F: Fn(&'a mut sqlx::MySqlConnection, &'a Id<User>) -> Fut,
    Fut: Future<Output = Result<i32, Error>>,
{
    type Output = Fut;
    fn call(&self, tx: &'a mut sqlx::MySqlConnection, user_id: &'a Id<User>) -> Fut {
        self(tx, user_id)
    }
}

const RETRY_COUNT: usize = 5;

pub async fn request_payment_gateway_post_payment<F>(
    payment_gateway_url: &str,
    token: &str,
    param: &PaymentGatewayPostPaymentRequest,
    tx: &mut sqlx::MySqlConnection,
    user_id: &Id<User>,
    retrieve_rides_count: F,
) -> Result<(), Error>
where
    F: for<'a> PostPaymentCallback<'a>,
{
    // 失敗したらとりあえずリトライ
    // FIXME: 社内決済マイクロサービスのインフラに異常が発生していて、同時にたくさんリクエストすると変なことになる可能性あり
    let mut retry = 0;

    loop {
        let result = async {
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

        if let Err(err) = result {
            tracing::warn!("pgw request failed: retrying [{}/{RETRY_COUNT}]", retry + 1);
            if retry < RETRY_COUNT {
                retry += 1;
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                continue;
            }
            return Err(err);
        }
        break;
    }

    Ok(())
}
