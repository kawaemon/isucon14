use std::pin::Pin;
use std::task::Poll;

use bytes::Bytes;
use cookie::Cookie;
use futures::Stream;
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::body::Frame;
use hyper::body::Incoming;
use hyper::HeaderMap;
use hyper::Request;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sync_wrapper::SyncWrapper;

use crate::models::{Owner, Symbol, User};
use crate::repo::chair::EffortlessChair;
use crate::{AppState, Error};

pub trait SerializeJson {
    fn size_hint(&self) -> usize;
    fn ser(&self, buf: &mut String);
}
impl SerializeJson for () {
    fn size_hint(&self) -> usize {
        0
    }
    fn ser(&self, _buf: &mut String) {}
}

pub struct Controller {
    /// can be read once
    body: Option<Incoming>,
    cookie: cookie::CookieJar,
    state: AppState,
}

fn cookies_from_request(headers: &HeaderMap) -> impl Iterator<Item = Cookie<'static>> + '_ {
    headers
        .get_all(hyper::header::COOKIE)
        .into_iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|cookie| Cookie::parse_encoded(cookie.to_owned()).ok())
}

impl Controller {
    pub fn new(r: Request<Incoming>, state: AppState) -> Self {
        let (parts, body) = r.into_parts();

        let mut jar = cookie::CookieJar::new();
        for cookie in cookies_from_request(&parts.headers) {
            jar.add_original(cookie);
        }

        Self {
            cookie: jar,
            body: Some(body),
            state,
        }
    }
    pub fn state(&self) -> &AppState {
        &self.state
    }
    pub async fn body<T: DeserializeOwned>(&mut self) -> Result<T, Error> {
        let b = self.body.take().expect("body was already taken");
        let b = b.collect().await?.to_bytes();
        let b = unsafe { sonic_rs::from_slice_unchecked(&b)? };
        Ok(b)
    }
    pub fn cookie_get(&self, k: &str) -> Option<&str> {
        self.cookie.get(k).map(|x| x.value())
    }
    pub fn cookie_add(&mut self, cookie: impl Into<Cookie<'static>>) {
        self.cookie.add(cookie);
    }
    pub fn cookie_encode(&self, headers: &mut HeaderMap) {
        for cookie in self.cookie.delta() {
            if let Ok(header_value) = cookie.encoded().to_string().parse() {
                headers.append(hyper::header::SET_COOKIE, header_value);
            }
        }
    }
    pub fn auth_app(&self) -> Result<User, Error> {
        let Some(c) = self.cookie_get("app_session") else {
            return Err(Error::Unauthorized("app_session cookie is required"));
        };
        let access_token = Symbol::new_from_ref(c);
        let Some(user): Option<User> = self.state.repo.user_get_by_access_token(access_token)?
        else {
            return Err(Error::Unauthorized("invalid access token"));
        };
        Ok(user)
    }
    pub fn auth_owner(&self) -> Result<Owner, Error> {
        let Some(c) = self.cookie_get("owner_session") else {
            return Err(Error::Unauthorized("owner_session cookie is required"));
        };
        let access_token = Symbol::new_from_ref(c);
        let Some(owner): Option<Owner> = self.state.repo.owner_get_by_access_token(access_token)?
        else {
            return Err(Error::Unauthorized("invalid access token"));
        };
        Ok(owner)
    }
    pub fn auth_chair(&self) -> Result<EffortlessChair, Error> {
        let Some(c) = self.cookie_get("chair_session") else {
            return Err(Error::Unauthorized("chair_session cookie is required"));
        };
        let access_token = Symbol::new_from_ref(c);
        let Some(chair): Option<EffortlessChair> =
            self.state.repo.chair_get_by_access_token(access_token)?
        else {
            return Err(Error::Unauthorized("invalid access token"));
        };
        Ok(chair)
    }
}

// almost copy-pasted from axum

pub type BoxStream = Box<dyn Send + Unpin + Stream<Item = Result<Event, Error>>>;

#[pin_project::pin_project]
pub struct SseBody {
    #[pin]
    event_stream: SyncWrapper<BoxStream>,
}

impl SseBody {
    pub fn new(s: BoxStream) -> Self {
        Self {
            event_stream: SyncWrapper::new(s),
        }
    }
}

impl Body for SseBody {
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        match this.event_stream.get_pin_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(event))) => Poll::Ready(Some(Ok(Frame::data(event.buf)))),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}

pub struct Event {
    buf: Bytes,
}

impl Event {
    pub fn new_raw(s: &str) -> Self {
        let mut buf = vec![];

        // json_data
        buf.extend_from_slice(b"data: ");
        buf.extend_from_slice(s.as_bytes());
        buf.push(b'\n');

        // finalize
        buf.push(b'\n');

        Self {
            buf: Bytes::from(buf),
        }
    }

    pub fn new(data: impl Serialize) -> Self {
        let mut buf = vec![];

        // json_data
        buf.extend_from_slice(b"data: ");
        sonic_rs::to_writer(&mut buf, &data).unwrap();
        debug_assert!(!buf.contains(&b'\n'));

        buf.extend_from_slice(b"\n\n");

        Self {
            buf: Bytes::from(buf),
        }
    }
}
