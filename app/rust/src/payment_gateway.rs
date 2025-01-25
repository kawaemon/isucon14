use std::{collections::VecDeque, sync::LazyLock, time::Duration};

use bytes::Bytes;
use http_body_util::Full;
use hyper::{client::conn::http1::SendRequest, header, Method, Request, StatusCode, Version};
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use reqwest::Url;
use tokio::net::TcpStream;

use crate::{
    fw::format_json,
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

#[derive(Debug, serde::Serialize, macros::SerializeJson)]
pub struct PaymentGatewayPostPaymentRequest {
    pub amount: i32,
}

#[derive(Debug, serde::Deserialize)]
struct PaymentGatewayGetPaymentsResponseOne {}

const RETRY_LIMIT: usize = 1000;

type Sender = SendRequest<Full<Bytes>>;

static CONNECTIONS: LazyLock<Mutex<VecDeque<Sender>>> = LazyLock::new(Default::default);

async fn get_con(url: &Url) -> Result<Sender, Error> {
    let pool = { CONNECTIONS.lock().pop_front() };
    let s = match pool {
        Some(c) => c,
        None => {
            let host = format!(
                "{}:{}",
                url.host_str().unwrap(),
                url.port_or_known_default().unwrap()
            );
            let st = TcpStream::connect(host).await?;
            let st = TokioIo::new(st);

            let (sender, conn) = hyper::client::conn::http1::handshake(st).await?;
            tokio::spawn(async move {
                let e = conn.await;
            });

            sender
        }
    };
    Ok(s)
}

pub async fn request_payment_gateway_post_payment(
    _client: &reqwest::Client,
    url: Url,
    token: Symbol,
    param: &PaymentGatewayPostPaymentRequest,
) -> Result<(), Error> {
    let key = Id::<()>::new();

    let token = token.resolve();

    let req = Request::builder()
        .uri(url.as_str())
        .version(Version::HTTP_11)
        .method(Method::POST)
        .header(header::HOST, "")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header("Idempotency-Key", &key)
        .body(Full::new(Bytes::from(format_json(param))))
        .unwrap();

    let mut retry = 0;
    loop {
        let mut con = get_con(&url).await?;

        let result: Result<(), Error> = async {
            let res = con.send_request(req.clone()).await?;

            let status = res.status();
            if status != StatusCode::NO_CONTENT {
                return Err(PaymentGatewayError::Status)?;
            }

            Ok(())
        }
        .await;

        tokio::spawn(async move {
            let r = con.ready().await;
            if r.is_ok() {
                CONNECTIONS.lock().push_back(con);
            }
        });

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
