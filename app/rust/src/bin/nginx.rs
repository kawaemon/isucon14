use std::{convert::Infallible, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use futures::StreamExt;
use http_body_util::{BodyExt, Either, Full};
use hyper::{
    body::Incoming,
    header::{CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, COOKIE, EXPIRES, SET_COOKIE},
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use isuride::{
    fw::{Event, SseBody},
    HashMap,
};
use reqwest::Client;
use reqwest_eventsource::EventSource;
use tokio::net::TcpListener;

use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    server::ServerSessionMemoryCache,
    ServerConfig,
};
use tokio_rustls::TlsAcceptor;

fn load_certs(filename: &str) -> Vec<CertificateDer<'static>> {
    let certfile = std::fs::File::open(filename).unwrap();
    let mut reader = std::io::BufReader::new(certfile);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn load_private_key(filename: &str) -> PrivateKeyDer<'static> {
    let keyfile = std::fs::File::open(filename).unwrap();
    let mut reader = std::io::BufReader::new(keyfile);

    rustls_pemfile::private_key(&mut reader).unwrap().unwrap()
}

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info");
    }
    tracing_subscriber::fmt::init();

    let state = AppState::new(AppStateInner::new().await);

    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "7890".to_owned())
        .parse()
        .expect("failed to parse PORT");

    let certs = load_certs("./nginx/tls/_.xiv.isucon.net.crt");
    let key = load_private_key("./nginx/tls/_.xiv.isucon.net.key");

    let incoming = TcpListener::bind(&SocketAddr::from(([0, 0, 0, 0], port)))
        .await
        .unwrap();
    let mut server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    server_config.session_storage = ServerSessionMemoryCache::new(9999999);
    let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

    tracing::info!("upstream = {}", *UPSTREAM);
    tracing::info!("listening at port {port}");

    loop {
        let (stream, _) = incoming.accept().await.unwrap();
        let tls_acceptor = tls_acceptor.clone();
        let state = state.clone();
        tokio::spawn(async move {
            let Ok(stream) = tls_acceptor.accept(stream).await else {
                return;
            };
            if let Err(_e) = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(move |x| response(state.clone(), x)),
                )
                .await
            {
                // do nothing
            }
        });
    }
}

isuride::conf_env!(static UPSTREAM: String = {
    from: "UPSTREAM",
    default: "http://localhost:8080",
});

async fn walk(p: &str) -> HashMap<String, Bytes> {
    let mut res = HashMap::default();

    let mut dir = tokio::fs::read_dir(p).await.unwrap();
    while let Ok(Some(ent)) = dir.next_entry().await {
        let name = ent.file_name();
        let name = name.to_str().unwrap();
        let name = format!("{p}/{name}");

        let ty = ent.file_type().await.unwrap();
        if ty.is_dir() {
            for (k, v) in Box::pin(walk(&name)).await {
                res.insert(k, v);
            }
            continue;
        }
        assert!(ty.is_file());
        let content = Bytes::from(tokio::fs::read(&name).await.unwrap());
        res.insert(name, content);
    }

    res
}

type AppState = Arc<AppStateInner>;
struct AppStateInner {
    files: HashMap<String, Bytes>,
    client: Client,
}
impl AppStateInner {
    async fn new() -> Self {
        let files = walk("../public").await;
        let mut path_to_file = HashMap::default();
        for (k, v) in files {
            let k = k.strip_prefix("../public").unwrap();
            path_to_file.insert(k.to_owned(), v);
            tracing::info!("loaded {k}");
        }

        AppStateInner {
            files: path_to_file,
            client: Client::new(),
        }
    }
}

type HyperRes = Either<Full<Bytes>, SseBody>;

async fn response(
    state: AppState,
    req: Request<Incoming>,
) -> Result<Response<HyperRes>, Infallible> {
    let (req, body) = req.into_parts();
    let path = req.uri.path();

    if path.starts_with("/api") {
        let cookie = req.headers.get("Cookie");
        if path.contains("notification") {
            let sse = forward_sse(&state, path, cookie.unwrap().to_str().unwrap());
            let mut resp = Response::new(Either::Right(sse));
            let headers = resp.headers_mut();
            headers.append(CACHE_CONTROL, "no-cache".parse().unwrap());
            headers.insert(
                CONTENT_TYPE,
                mime::TEXT_EVENT_STREAM.as_ref().parse().unwrap(),
            );
            return Ok(resp);
        }

        let mut query = String::new();
        if let Some(q) = req.uri.query() {
            query.push('?');
            query.push_str(q);
        }

        let url = format!("{}{path}{query}", *UPSTREAM);
        let mut builder = match req.method {
            Method::GET => state.client.get(&url),
            Method::POST => state.client.post(&url),
            _ => todo!(),
        };
        if let Some(cookie) = cookie {
            builder = builder.header(COOKIE, cookie);
        }
        let body = body.collect().await.unwrap().to_bytes();
        // dont care about CONTENT_TYPE
        let upstream_res = builder.body(body).send().await.unwrap();
        let status = upstream_res.status();
        let cookie = upstream_res.headers().get(SET_COOKIE).cloned();

        let res = upstream_res.bytes().await.unwrap();
        let mut res = Response::new(Either::Left(Full::new(res)));
        *res.status_mut() = status;
        let headers = res.headers_mut();
        if let Some(cookie) = cookie {
            headers.insert(SET_COOKIE, cookie);
        }

        return Ok(res);
    }

    if let Some(content) = state.files.get(&format!("{path}.gz")) {
        let mut res = Response::new(Either::Left(Full::new(content.clone())));
        let headers = res.headers_mut();
        headers.reserve(3);
        headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        headers.insert(EXPIRES, "Mon, 21 Oct 2030 07:28:00 GMT".parse().unwrap());
        headers.insert(CACHE_CONTROL, "public, immutable".parse().unwrap());
        // Content-Type?
        return Ok(res);
    }

    Ok(Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Either::Left(Full::new(Bytes::new())))
        .unwrap())
}

fn forward_sse(state: &AppState, path: &str, cookie: &str) -> SseBody {
    let req = state
        .client
        .get(format!("{}{path}", *UPSTREAM))
        .header("Cookie", cookie);

    let stream = EventSource::new(req).unwrap().filter_map(|x| async move {
        match x.unwrap() {
            reqwest_eventsource::Event::Open => None,
            reqwest_eventsource::Event::Message(event) => Some(Ok(Event::new_raw(&event.data))),
        }
    });

    SseBody::new(Box::new(Box::pin(stream)))
}
