use std::pin::Pin;
use std::task::Poll;

use bytes::Bytes;
use cookie::Cookie;
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::body::Frame;
use hyper::body::Incoming;
use hyper::HeaderMap;
use hyper::Request;
use serde::de::DeserializeOwned;
use sync_wrapper::SyncWrapper;
use tokio_stream::Stream;

use crate::models::{Owner, Symbol, User};
use crate::repo::chair::EffortlessChair;
use crate::{AppState, Error};

pub trait SerializeJson {
    fn size_est(&self) -> usize;
    fn ser(&self, buf: &mut String);
}
impl SerializeJson for () {
    #[inline(always)]
    fn size_est(&self) -> usize {
        0
    }
    #[inline(always)]
    fn ser(&self, _buf: &mut String) {}
}
impl SerializeJson for String {
    #[inline(always)]
    fn size_est(&self) -> usize {
        self.len() + 2
    }
    #[inline(always)]
    fn ser(&self, buf: &mut String) {
        buf.push('"');
        buf.push_str(self);
        buf.push('"');
    }
}
impl<T: SerializeJson> SerializeJson for Vec<T> {
    #[inline(always)]
    fn size_est(&self) -> usize {
        if self.is_empty() {
            return 2;
        }
        2 + self.iter().map(|x| x.size_est()).sum::<usize>()
    }
    fn ser(&self, buf: &mut String) {
        if self.is_empty() {
            buf.push_str("[]");
            return;
        }

        buf.push('[');
        let stop = self.len() - 1;
        for i in 0..self.len() {
            self[i].ser(buf);
            if i < stop {
                buf.push(',');
            }
        }
        buf.push(']');
    }
}
impl SerializeJson for i32 {
    #[inline(always)]
    fn size_est(&self) -> usize {
        8
    }
    #[inline(always)]
    fn ser(&self, buf: &mut String) {
        let mut buffer = itoa::Buffer::new();
        let s = buffer.format(*self);
        buf.push_str(s);
    }
}
impl SerializeJson for i64 {
    #[inline(always)]
    fn size_est(&self) -> usize {
        16
    }
    #[inline(always)]
    fn ser(&self, buf: &mut String) {
        let mut buffer = itoa::Buffer::new();
        let s = buffer.format(*self);
        buf.push_str(s);
    }
}
impl SerializeJson for f64 {
    fn size_est(&self) -> usize {
        16
    }
    fn ser(&self, buf: &mut String) {
        if self.is_nan() || self.is_infinite() {
            tracing::warn!("Nan|Infinite({self}) cannot be serialized");
            buf.push_str("null");
        } else {
            let mut buffer = ryu::Buffer::new();
            let s = buffer.format_finite(*self);
            buf.push_str(s);
        }
    }
}
impl SerializeJson for bool {
    #[inline(always)]
    fn size_est(&self) -> usize {
        5 // 1 byte ぐらいいいだろ
    }
    #[inline(always)]
    fn ser(&self, buf: &mut String) {
        buf.push_str(if *self { "true" } else { "false" });
    }
}
impl<T: SerializeJson> SerializeJson for Option<T> {
    #[inline(always)]
    fn size_est(&self) -> usize {
        match self.as_ref() {
            Some(t) => t.size_est(),
            None => 4,
        }
    }
    #[inline(always)]
    fn ser(&self, buf: &mut String) {
        match self.as_ref() {
            Some(t) => t.ser(buf),
            None => buf.push_str("null"),
        }
    }
}

#[inline(never)] // for profiling
pub fn format_json<S: SerializeJson>(target: &S) -> String {
    let est = target.size_est();
    let mut buf = String::with_capacity(est);

    let before_cap = buf.capacity();
    target.ser(&mut buf);
    let after_cap = buf.capacity();

    #[cfg(debug_assertions)]
    if before_cap != after_cap {
        eprintln!("{buf}");
        assert_eq!(before_cap, after_cap, "reallocation happened");
    }
    buf
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
    pub fn new(data: impl SerializeJson) -> Self {
        let est = data.size_est() + "data: \n\n".len();
        let mut buf = String::with_capacity(est);
        let before_cap = buf.capacity();

        buf.push_str("data: ");

        data.ser(&mut buf);
        debug_assert!(!buf.contains('\n'));

        buf.push_str("\n\n");

        let after_cap = buf.capacity();

        #[cfg(debug_assertions)]
        if before_cap != after_cap {
            eprintln!("{buf}");
            assert_eq!(before_cap, after_cap, "reallocation happened");
        }

        Self {
            buf: Bytes::from(buf),
        }
    }
}
