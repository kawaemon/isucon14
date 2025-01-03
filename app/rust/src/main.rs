use bytes::Bytes;
use http_body_util::{Either, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{header, Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use isuride::models::Symbol;
use isuride::repo::deferred::COMMIT_CHAN;
use isuride::repo::Repository;
#[cfg(feature = "speed")]
use isuride::speed::SpeedStatistics;
use isuride::AppStateInner;
use isuride::{internal_handlers::spawn_matching_thread, AppState, Error};
use serde::Serialize;
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

use isuride::fw::{Controller, SseBody};
use isuride::models::Id;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let v = std::env::var_os("V").is_some();
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var(
            "RUST_LOG",
            if v {
                "info,tower_http=debug,axum::rejection=trace"
            } else {
                "info,axum::rejection=trace"
            },
        );
    }
    tracing_subscriber::fmt::init();

    let host = std::env::var("ISUCON_DB_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("ISUCON_DB_PORT")
        .map(|port_str| {
            port_str.parse().expect(
                "failed to convert DB port number from ISUCON_DB_PORT environment variable into u16",
            )
        })
        .unwrap_or(3306);
    let user = std::env::var("ISUCON_DB_USER").unwrap_or_else(|_| "isucon".to_owned());
    let password = std::env::var("ISUCON_DB_PASSWORD").unwrap_or_else(|_| "isucon".to_owned());
    let dbname = std::env::var("ISUCON_DB_NAME").unwrap_or_else(|_| "isuride".to_owned());

    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(128)
        .min_connections(128)
        .connect_with(
            sqlx::mysql::MySqlConnectOptions::default()
                .host(&host)
                .port(port)
                .username(&user)
                .password(&password)
                .database(&dbname),
        )
        .await?;

    let repo = Arc::new(Repository::new(&pool).await);
    #[cfg(feature = "speed")]
    let speed = SpeedStatistics::new();
    let client = reqwest::Client::builder()
        .tcp_keepalive(Duration::from_secs(10))
        .build()
        .unwrap();

    let app_state = Arc::new(AppStateInner {
        pool,
        repo,
        #[cfg(feature = "speed")]
        speed,
        client,
    });

    spawn_matching_thread(app_state.clone());

    let listener = TcpListener::bind(&SocketAddr::from(([0, 0, 0, 0], 8080))).await?;
    loop {
        let app_state = Arc::clone(&app_state);
        let (stream, _) = listener.accept().await?;
        tokio::task::spawn(async move {
            if let Err(_err) = http1::Builder::new()
                .timer(TokioTimer::new())
                .keep_alive(true)
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|x| service(Arc::clone(&app_state), x)),
                )
                .await
            {
                // eprintln!("Error serving connection: {}", err);
            }
        });
    }
}

async fn post_initialize(c: &mut Controller) -> Result<impl Serialize, Error> {
    #[derive(serde::Deserialize)]
    struct Req {
        payment_server: Symbol,
    }

    COMMIT_CHAN.0.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let req: Req = c.body().await?;
    let state = c.state();

    let status = tokio::process::Command::new("../sql/init.sh")
        .stdout(Stdio::inherit())
        .status()
        .await?;
    if !status.success() {
        return Err(Error::Initialize {
            stdout: String::new(),
            stderr: String::new(),
        });
    }

    state.repo.reinit().await;
    state.repo.pgw_set(req.payment_server).await?;

    tokio::spawn(async {
        tracing::info!("scheduling commit after 1m30s");
        tokio::time::sleep(Duration::from_secs(90)).await;
        COMMIT_CHAN.0.send(()).unwrap();
    });

    #[derive(serde::Serialize)]
    struct Res {
        language: &'static str,
    }
    Ok(Res { language: "rust" })
}

type HyperRes = Either<Full<Bytes>, SseBody>;

pub async fn service(state: AppState, req: Request<Incoming>) -> Result<Response<HyperRes>, Error> {
    match response(state, req).await {
        Ok(o) => Ok(o),
        Err(e) => {
            tracing::error!("tracing error: {e:?}");
            Err(e)
        }
    }
}

pub async fn response(
    state: AppState,
    req: Request<Incoming>,
) -> Result<Response<HyperRes>, Error> {
    #[inline(always)]
    fn json<B: serde::Serialize>(body: impl Into<Option<B>>) -> HyperRes {
        Either::Left(Full::new(body.into().map_or_else(Bytes::new, |x| {
            Bytes::from(sonic_rs::to_string(&x).unwrap())
        })))
    }
    let empty = || json::<()>(None);

    let method = req.method().clone();
    let uri = req.uri().to_owned();

    let mut c = Controller::new(req, state);

    let param;
    let (code, body) = {
        let path = uri.path();
        if let Some(app_path) = path.strip_prefix("/api/app") {
            use isuride::app_handlers::*;
            match (method, app_path) {
                (Method::POST, "/users") => {
                    let (code, res) = app_post_users(&mut c).await?;
                    (code, json(res))
                }
                (Method::POST, "/payment-methods") => {
                    let code = app_post_payment_methods(&mut c).await?;
                    (code, empty())
                }
                (Method::GET, "/rides") => {
                    let res = app_get_rides(&mut c)?;
                    (StatusCode::OK, json(res))
                }
                (Method::POST, "/rides") => {
                    let (code, res) = app_post_rides(&mut c).await?;
                    (code, json(res))
                }
                (Method::POST, "/rides/estimated-fare") => {
                    let res = app_post_rides_estimated_fare(&mut c).await?;
                    (StatusCode::OK, json(res))
                }
                // POST /api/app/rides/:ride_id/evaluation
                (Method::POST, s)
                    if {
                        param = s
                            .strip_prefix("/rides/")
                            .and_then(|x| x.strip_suffix("/evaluation"));
                        param.is_some()
                    } =>
                {
                    let param = param.unwrap();
                    let param = Id::new_from(Symbol::new_from_ref(param));
                    let res = app_post_ride_evaluation(&mut c, param).await?;
                    (StatusCode::OK, json(res))
                }
                (Method::GET, "/notification") => {
                    let stream = app_get_notification(&mut c)?;
                    (StatusCode::OK, Either::Right(SseBody::new(stream)))
                }
                (Method::GET, "/nearby-chairs") => {
                    let Some((dist, lat, long)) = ('query: {
                        let Some(q) = uri.query().map(querystring::querify) else {
                            break 'query None;
                        };
                        let f = |t: &str| q.iter().find(|&&(k, _)| k == t).map(|(_, v)| v.parse());
                        match (f("distance").transpose(), f("latitude"), f("longitude")) {
                            (Ok(dist), Some(Ok(lat)), Some(Ok(long))) => Some((dist, lat, long)),
                            _ => None,
                        }
                    }) else {
                        return Err(Error::BadRequest("bad querystring"));
                    };
                    let res = app_get_nearby_chairs(&mut c, dist, lat, long)?;
                    (StatusCode::OK, json(res))
                }
                _ => {
                    return Ok(Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(empty())
                        .unwrap())
                }
            }
        } else if let Some(chair_path) = path.strip_prefix("/api/chair") {
            use isuride::chair_handlers::*;
            let param;
            match (method, chair_path) {
                (Method::POST, "/chairs") => {
                    let (code, res) = chair_post_chairs(&mut c).await?;
                    (code, json(res))
                }
                (Method::POST, "/activity") => {
                    let code = chair_post_activity(&mut c).await?;
                    (code, empty())
                }
                (Method::POST, "/coordinate") => {
                    let res = chair_post_coordinate(&mut c).await?;
                    (StatusCode::OK, json(res))
                }
                (Method::GET, "/notification") => {
                    let stream = chair_get_notification(&mut c)?;
                    (StatusCode::OK, Either::Right(SseBody::new(stream)))
                }
                (Method::POST, p)
                    if {
                        param = p
                            .strip_prefix("/rides/")
                            .and_then(|x| x.strip_suffix("/status"));
                        param.is_some()
                    } =>
                {
                    let param = param.unwrap();
                    let param = Id::new_from(Symbol::new_from_ref(param));
                    let code = chair_post_ride_status(&mut c, param).await?;
                    (code, empty())
                }
                _ => {
                    return Ok(Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(empty())
                        .unwrap())
                }
            }
        } else if let Some(owner_path) = path.strip_prefix("/api/owner") {
            use isuride::owner_handlers::*;
            match (method, owner_path) {
                (Method::POST, "/owners") => {
                    let (code, res) = owner_post_owners(&mut c).await?;
                    (code, json(res))
                }
                (Method::GET, "/sales") => {
                    let Some((since, until)) = ('query: {
                        let Some(q) = uri.query().map(querystring::querify) else {
                            break 'query None;
                        };
                        let f = |t: &str| q.iter().find(|&&(k, _)| k == t).map(|(_, v)| v.parse());
                        match (f("since").transpose(), f("until").transpose()) {
                            (Ok(since), Ok(until)) => Some((since, until)),
                            _ => None,
                        }
                    }) else {
                        return Err(Error::BadRequest("bad querystring"));
                    };
                    let res = owner_get_sales(&mut c, since, until)?;
                    (StatusCode::OK, json(res))
                }
                (Method::GET, "/chairs") => {
                    let res = owner_get_chairs(&mut c)?;
                    (StatusCode::OK, json(res))
                }
                _ => {
                    return Ok(Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(empty())
                        .unwrap())
                }
            }
        } else if path == "/api/initialize" {
            let res = post_initialize(&mut c).await?;
            (StatusCode::OK, json(res))
        } else {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(empty())
                .unwrap());
        }
    };

    let is_stream = matches!(body, Either::Right(_));

    let mut res = Response::new(body);
    *res.status_mut() = code;
    c.cookie_encode(res.headers_mut());

    let headers = res.headers_mut();
    headers.append(header::CACHE_CONTROL, "no-cache".parse().unwrap());
    headers.append(
        header::CONTENT_TYPE,
        if is_stream {
            mime::TEXT_EVENT_STREAM.as_ref()
        } else {
            mime::APPLICATION_JSON.as_ref()
        }
        .parse()
        .unwrap(),
    );

    Ok(res)
}
