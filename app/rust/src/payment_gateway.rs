use reqwest::StatusCode;
use tokio::sync::Semaphore;

use crate::Error;
use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

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
struct PaymentGatewayGetPaymentsResponseOne {}

const RETRY_LIMIT: usize = 1000;

#[derive(Debug, Clone)]
pub struct PaymentGatewayRestricter {
    sema: Arc<Semaphore>,
    tries: Arc<AtomicUsize>,
    success: Arc<AtomicUsize>,
    failure: Arc<AtomicUsize>,
}
impl PaymentGatewayRestricter {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let conc = std::env::var("CONCURRENCY")
            .unwrap_or("30".to_owned())
            .parse()
            .unwrap();

        let tries = Arc::new(AtomicUsize::new(0));
        let success = Arc::new(AtomicUsize::new(0));
        let failure = Arc::new(AtomicUsize::new(0));

        {
            let tries = Arc::clone(&tries);
            let (success, failure) = (Arc::clone(&success), Arc::clone(&failure));
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(5000)).await;
                    let success = success.swap(0, std::sync::atomic::Ordering::Relaxed);
                    let failure = failure.swap(0, std::sync::atomic::Ordering::Relaxed);
                    let tries = tries.swap(0, std::sync::atomic::Ordering::Relaxed);
                    let total = success + failure;
                    let ratio = (success as f64 / total as f64 * 100.0) as usize;
                    tracing::info!(
                        "pgw: ok={ratio:3}%, total={total:5}, ok={success:5}, fail={failure:5}"
                    );

                    let ratio = total as f64 / tries as f64;
                    tracing::info!("pgw: tries={tries:5}, req/try={ratio:3.2}");
                }
            });
        }

        Self {
            sema: Arc::new(Semaphore::new(conc)),
            success,
            failure,
            tries,
        }
    }

    pub fn on_begin(&self) {
        self.tries
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn on_req(&self, code: StatusCode) {
        if code.is_success() {
            &self.success
        } else {
            &self.failure
        }
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

async fn get_payment_history(
    pgw: &PaymentGatewayRestricter,
    client: &reqwest::Client,
    gw: &str,
    token: &str,
) -> Result<usize, Error> {
    let r = client
        .get(format!("{gw}/payments"))
        .bearer_auth(token)
        .send()
        .await
        .map_err(PaymentGatewayError::Reqwest)?;

    let status = r.status();
    pgw.on_req(status);

    if status != reqwest::StatusCode::OK {
        return Err(PaymentGatewayError::GetPayment(r.status()).into());
    }
    let r: Vec<PaymentGatewayGetPaymentsResponseOne> =
        r.json().await.map_err(PaymentGatewayError::Reqwest)?;
    Ok(r.len())
}

pub async fn request_payment_gateway_post_payment(
    client: &reqwest::Client,
    pgw: &PaymentGatewayRestricter,
    payment_gateway_url: &str,
    token: &str,
    param: &PaymentGatewayPostPaymentRequest,
    desired_ride_count: usize,
) -> Result<(), Error> {
    // 失敗したらとりあえずリトライ
    // FIXME: 社内決済マイクロサービスのインフラに異常が発生していて、同時にたくさんリクエストすると変なことになる可能性あり

    let _permit = pgw.sema.acquire().await.unwrap();
    tracing::debug!("permit acquired; remain = {}", pgw.sema.available_permits());

    pgw.on_begin();

    let mut retry = 0;
    loop {
        let result: Result<(), Error> = async {
            let res = client
                .post(format!("{payment_gateway_url}/payments"))
                .bearer_auth(token)
                .json(param)
                .send()
                .await
                .map_err(PaymentGatewayError::Reqwest)?;

            let status = res.status();
            pgw.on_req(status);

            if status != reqwest::StatusCode::NO_CONTENT {
                // エラーが返ってきても成功している場合があるので、社内決済マイクロサービスに問い合わせ
                let payment_len =
                    get_payment_history(pgw, client, payment_gateway_url, token).await?;

                if desired_ride_count != payment_len {
                    return Err(PaymentGatewayError::UnexpectedNumberOfPayments {
                        ride_count: desired_ride_count,
                        payment_count: payment_len,
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
            // tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        }
        break;
    }

    Ok(())
}
