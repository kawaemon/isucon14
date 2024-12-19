use crate::FxHashMap as HashMap;
use std::{future::Future, sync::Arc, time::Duration};

use derivative::Derivative;
use futures::{stream::SplitSink, Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, Mutex},
};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tungstenite::Message;

use crate::models::Id;

pub mod coordinate;
pub mod pgw;

pub type TungsteniteTx = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PacketType {
    Request,
    Response,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Packet<T> {
    pub id: Id<()>,
    pub ty: PacketType,
    pub param: T,
}

pub trait WsSystemHandler: 'static + Send + Clone {
    // こっちから送るほう
    type Request: std::fmt::Debug + Send + 'static + Serialize + DeserializeOwned;
    type Response: std::fmt::Debug + Send + 'static + Serialize + DeserializeOwned;

    // あっちから来るほう
    type Notification: std::fmt::Debug + Send + 'static + Serialize + DeserializeOwned;
    type NotificationResponse: std::fmt::Debug + Send + 'static + Serialize + DeserializeOwned;

    fn handle(
        &self,
        req: Self::Notification,
    ) -> impl Future<Output = Self::NotificationResponse> + Send;
}

type Jobs<R> = Arc<Mutex<HashMap<Id<()>, oneshot::Sender<R>>>>;

// ほんとうに trait alias がほしい
pub trait SendError: std::fmt::Debug + Send + 'static {}
impl<E: std::fmt::Debug + Send + 'static> SendError for E {}
pub trait SystemTx:
    'static + Send + Unpin + Sink<Message, Error = <Self as SystemTx>::Error> + SinkExt<Message>
{
    type Error: SendError;
}
impl<Tx, E: SendError> SystemTx for Tx
where
    Tx: 'static + Send + Unpin + Sink<Message, Error = E> + SinkExt<Message>,
    Tx::Error: std::fmt::Debug,
{
    type Error = E;
}
pub trait SystemRx:
    'static + Send + Unpin + Stream<Item = Result<Message, <Self as SystemRx>::Error>>
{
    type Error: SendError;
}
impl<Rx, E> SystemRx for Rx
where
    Rx: 'static + Send + Unpin + Stream<Item = Result<Message, E>>,
    E: SendError,
{
    type Error = E;
}

/// clonable
#[derive(Derivative)]
#[derivative(Debug, Clone(bound = ""))]
pub struct WsSystem<H: WsSystemHandler> {
    jobs: Jobs<H::Response>,
    tx: mpsc::UnboundedSender<Message>,
}

impl<H: WsSystemHandler> WsSystem<H> {
    pub fn new_wait_for_recv(
        handler: H,
        mut tx: impl SystemTx,
        rx: impl SystemRx,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let jobs = Arc::new(Mutex::new(HashMap::default()));
        let (ntx, mut nrx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Some(m) = nrx.recv().await {
                tx.send(m).await.unwrap();
            }
        });

        let rx_waiter = tokio::spawn({
            let jobs = Arc::clone(&jobs);
            let tx = ntx.clone();
            async move {
                Self::recv_routine(tx, rx, handler, jobs).await;
            }
        });

        (Self { jobs, tx: ntx }, rx_waiter)
    }
    pub fn new(handler: H, tx: impl SystemTx, rx: impl SystemRx) -> Self {
        let (s, _j) = Self::new_wait_for_recv(handler, tx, rx);
        s
    }

    async fn recv_routine(
        tx: mpsc::UnboundedSender<Message>,
        mut rx: impl SystemRx,
        handler: H,
        jobs: Jobs<H::Response>,
    ) {
        while let Some(Ok(msg)) = rx.next().await {
            let msg = match msg {
                Message::Text(msg) => msg,
                Message::Close(_) => break,
                Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
                    continue
                }
            };

            #[derive(Deserialize)]
            struct PacketTypeOnly {
                ty: PacketType,
            }

            let packet: PacketTypeOnly = serde_json::from_str(&msg).unwrap();

            match packet.ty {
                PacketType::Response => {
                    let packet: Packet<H::Response> = serde_json::from_str(&msg).unwrap();

                    // tracing::info!("got response({:?}): {:?}", packet.id, packet.param);

                    jobs.lock()
                        .await
                        .remove(&packet.id)
                        .unwrap()
                        .send(packet.param)
                        .unwrap();
                }

                PacketType::Request => {
                    let packet: Packet<H::Notification> = serde_json::from_str(&msg).unwrap();
                    let handler = handler.clone();
                    let tx = tx.clone();

                    // tracing::info!("incoming request({:?}): {:?}", packet.id, packet.param);

                    tokio::spawn(async move {
                        let res = handler.handle(packet.param).await;
                        let packet = Packet {
                            id: packet.id,
                            ty: PacketType::Response,
                            param: res,
                        };
                        // tracing::info!("replying response({:?}): {:?}", packet.id, packet.param);
                        let msg = serde_json::to_string(&packet).unwrap();
                        let msg = Message::text(msg);
                        tx.send(msg).unwrap();
                    });
                }
            };
        }
        tracing::error!("connection ended");
    }

    pub async fn enqueue(&self, req: H::Request) -> H::Response {
        let id = Id::<()>::new();
        let (tx, mut rx) = oneshot::channel();

        {
            self.jobs.lock().await.insert(id.clone(), tx);
        }

        let packet = Packet {
            id,
            ty: PacketType::Request,
            param: req,
        };
        // tracing::info!("sending request({:?}): {:?}", packet.id, packet.param);
        let msg = serde_json::to_string(&packet).unwrap();

        {
            self.tx.send(Message::text(&msg)).unwrap();
        }

        loop {
            let sleep = tokio::time::sleep(Duration::from_millis(500));
            tokio::select! {
                _ = sleep => {
                    tracing::warn!("no ws response for 500ms: {msg}");
                }
                msg = &mut rx => {
                    return msg.unwrap();
                }
            }
        }
    }
}

pub fn from_axum(
    tx: impl 'static + Send + SinkExt<axum::extract::ws::Message, Error = axum::Error>,
    rx: impl 'static + Send + TryStreamExt<Ok = axum::extract::ws::Message, Error = axum::Error>,
) -> (
    impl 'static + Send + SinkExt<tungstenite::Message, Error = axum::Error>,
    impl 'static + Send + Stream<Item = Result<tungstenite::Message, axum::Error>>,
) {
    use axum::extract::ws as ax;
    use tungstenite as ts;
    let tx = tx.with(|x| async move {
        match x {
            ts::Message::Text(text) => Ok(ax::Message::Text(text)),
            ts::Message::Binary(binary) => Ok(ax::Message::Binary(binary)),
            ts::Message::Ping(ping) => Ok(ax::Message::Ping(ping)),
            ts::Message::Pong(pong) => Ok(ax::Message::Pong(pong)),
            ts::Message::Close(close) => Ok(ax::Message::Close(close.map(|x| ax::CloseFrame {
                code: x.code.into(),
                reason: x.reason,
            }))),
            ts::Message::Frame(_) => Err(axum::Error::new("tried to send ts::Message::frame")),
        }
    });
    let rx = rx.and_then(|x| async move {
        let t = match x {
            ax::Message::Text(t) => ts::Message::Text(t),
            ax::Message::Binary(vec) => ts::Message::Binary(vec),
            ax::Message::Ping(vec) => ts::Message::Ping(vec),
            ax::Message::Pong(vec) => ts::Message::Pong(vec),
            ax::Message::Close(close_frame) => {
                ts::Message::Close(close_frame.map(|x| ts::protocol::CloseFrame {
                    code: x.code.into(),
                    reason: x.reason,
                }))
            }
        };
        Ok(t)
    });
    (tx, rx)
}
