use reqwest::Url;

use crate::{
    models::{Id, Symbol},
    Error,
};

#[derive(Debug, thiserror::Error)]
pub enum PaymentGatewayError {
    #[error("invalid status code")]
    Status,
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
struct PaymentGatewayGetPaymentsResponseOne {}

const RETRY_LIMIT: usize = 1000;

pub async fn request_payment_gateway_post_payment(
    client: &reqwest::Client,
    payment_gateway_url: Url,
    token: Symbol,
    param: &PaymentGatewayPostPaymentRequest,
) -> Result<(), Error> {
    let key = Id::<()>::new();

    let token = token.resolve();

    let req = client
        .post(payment_gateway_url)
        .bearer_auth(token)
        .header("Idempotency-Key", &key)
        .json(param);

    let mut retry = 0;
    loop {
        let result: Result<(), Error> = async {
            let req = req.try_clone().unwrap();
            let res = req.send().await.map_err(PaymentGatewayError::Reqwest)?;

            let status = res.status();
            if status != reqwest::StatusCode::NO_CONTENT {
                return Err(PaymentGatewayError::Status)?;
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
            // tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        }
        break;
    }

    Ok(())
}
