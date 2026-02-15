pub mod fd;
pub mod handlers;
pub mod message;
pub mod ring;

use std::{
    collections::HashMap,
    env,
    hash::Hash,
    net::{Ipv4Addr, SocketAddrV4},
    str::FromStr,
    sync::{LazyLock, OnceLock, atomic::AtomicU64},
    time::Duration,
};

use axum::{
    Json, Router,
    http::StatusCode,
    routing::{get, post},
};
use dashmap::DashMap;
use libc::send;
use reqwest::Client;
use serde::Deserialize;
use tokio::{
    net::TcpListener,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot,
    },
};
use tracing::{error, info, warn};

use crate::{
    fd::set_fd_limit,
    handlers::{discover_handler, join_handler, start_handler},
    message::{Addr, CHUNK_SIZE, InternalMessage, Message, MessagePayload, RingId},
};

// pub static ROUTER: LazyLock<DashMap<RingId, mpsc::UnboundedSender<InternalMessage>>> =
//     LazyLock::new(DashMap::new);

#[derive(Clone)]
pub struct RingMeta {
    // addr: Addr,
    tx: UnboundedSender<InternalMessage>, // pending_ops:
    chunk_num: u64,
}

pub static ROUTER: LazyLock<DashMap<RingId, RingMeta>> = LazyLock::new(DashMap::new);
pub static CLIENT: LazyLock<Client> = LazyLock::new(Client::new);
pub static MEEEE: OnceLock<Addr> = OnceLock::new();

// let me: SocketAddrV4 = "127.0.0.1:8000".parse().unwrap();

// let (tx, rx) = mpsc::unbounded_channel();

// let listener = TcpListener::bind(me).await.unwrap();

// loop {
//     let (a, b) = listener.accept().await.unwrap();
// }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // initialize tracing
    tracing_subscriber::fmt::init();

    let me: SocketAddrV4 = env::var("CHUNKJUGGLER_ADDR")
        .unwrap_or("127.0.0.1:6767".to_string())
        .parse()
        .unwrap();

    MEEEE.set(me.into()).unwrap();

    info!("Chunk Juggler running on: {me:?}");

    set_fd_limit();

    // let ring_id = create_ring();

    // build our application with a route
    let app = Router::new()
        .route("/", post(chunk_catcher))
        .route("/read", post(read_handler))
        .route("/write", post(write_handler))
        .route("/discover", get(discover_handler))
        .route("/join", post(join_handler))
        .route("/start", post(start_handler));

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind(MEEEE.get().unwrap().into_std())
        .await
        .unwrap();

    axum::serve(listener, app).await?;
    Ok(())
}

pub enum PendingOp {
    Read(oneshot::Sender<Vec<u8>>),
    Write {
        data: Vec<u8>,
        tx: oneshot::Sender<()>,
    },
}

async fn chunk_thrower(
    ring_id: RingId,
    mut rx: UnboundedReceiver<InternalMessage>,
    target: Addr,
    chunk_num: u64,
) {
    info!("Chunk thrower started for ring: {ring_id:?}");
    let mut target = target;
    let mut pending: HashMap<u64, Vec<PendingOp>> = HashMap::new();

    while let Some(imsg) = rx.recv().await {
        match imsg {
            InternalMessage::Juggled(msg) => {
                let out_msg = match msg.payload {
                    MessagePayload::Chunk { id, mut data } => {
                        // process
                        if let Some(ops) = pending.remove(&id) {
                            for op in ops {
                                match op {
                                    PendingOp::Read(sender) => sender.send(data.clone()).unwrap(),
                                    PendingOp::Write { data: new_data, tx } => {
                                        data = new_data;
                                        tx.send(()).unwrap();
                                    }
                                }
                            }
                        }

                        Some(MessagePayload::Chunk { id, data })
                    }
                    MessagePayload::Switch { old, new } => {
                        if old == target {
                            target = new.clone();
                            None
                        } else {
                            Some(MessagePayload::Switch { old, new })
                        }
                    }
                };

                if let Some(payload) = out_msg {
                    // tokio::spawn(async move {
                    //     CLIENT
                    //         .post(target.into_url())
                    //         .json(&msg)
                    //         .send()
                    //         .await
                    //         .unwrap()
                    // });
                    CLIENT
                        .post(target.into_url())
                        .json(&Message {
                            ring_id: ring_id.clone(),
                            payload,
                        })
                        .send()
                        .await
                        .unwrap()
                        .error_for_status()
                        .unwrap();
                }
            }
            InternalMessage::Read { chunk_id, tx } => {
                if chunk_id >= chunk_num {
                    tx.send(vec![0; CHUNK_SIZE]).unwrap();
                } else {
                    pending
                        .entry(chunk_id)
                        .or_default()
                        .push(PendingOp::Read(tx));
                }
            }
            InternalMessage::Write { chunk_id, data, tx } => {
                pending
                    .entry(chunk_id)
                    .or_default()
                    .push(PendingOp::Write { data, tx });
            }
        }
    }

    info!("Chunk thower shutting down! BYE!!!");

    // let out_msg = match &message.payload {
    //     MessagePayload::Chunk { .. } => Some(message),
    //     MessagePayload::Switch { old, new } => {
    //         if old == &ring_meta.addr {
    //             let new_meta = RingMeta { addr: new.clone() };
    //             ROUTER.insert(message.ring_id, new_meta);
    //         }
    //         None
    //     }
    // };
}

#[derive(Deserialize)]
pub struct ReadMessage {
    ring_id: RingId,
    chunk_id: u64,
}

async fn read_handler(Json(message): Json<ReadMessage>) -> (StatusCode, Vec<u8>) {
    let (otx, orx) = oneshot::channel();

    let Some(ring_meta) = ROUTER.get(&message.ring_id).map(|v| v.clone()) else {
        error!("unknown ring id: {:?}", message.ring_id);
        return (StatusCode::NOT_FOUND, vec![]);
    };

    if message.chunk_id >= ring_meta.chunk_num {
        return (StatusCode::BAD_GATEWAY, vec![]);
    }

    ring_meta
        .tx
        .send(InternalMessage::Read {
            chunk_id: message.chunk_id,
            tx: otx,
        })
        .unwrap();

    if let Ok(data) = orx.await {
        (StatusCode::OK, data)
    } else {
        (StatusCode::IM_A_TEAPOT, vec![])
    }
}

#[derive(Deserialize)]
pub struct WriteMessage {
    ring_id: RingId,
    chunk_id: u64,
    data: Vec<u8>,
}

async fn write_handler(Json(message): Json<WriteMessage>) -> StatusCode {
    let (otx, orx) = oneshot::channel();
    if message.data.len() != CHUNK_SIZE {
        warn!("Got message of length: {}", message.data.len());
        return StatusCode::BAD_REQUEST;
    }

    let Some(ring_meta) = ROUTER.get(&message.ring_id).map(|v| v.clone()) else {
        error!("unknown ring id: {:?}", message.ring_id);
        return StatusCode::NOT_FOUND;
    };

    if message.chunk_id >= ring_meta.chunk_num {
        return StatusCode::BAD_REQUEST;
    }

    ring_meta
        .tx
        .send(InternalMessage::Write {
            chunk_id: message.chunk_id,
            data: message.data,
            tx: otx,
        })
        .unwrap();

    if let Ok(_) = orx.await {
        StatusCode::OK
    } else {
        StatusCode::IM_A_TEAPOT
    }
}

async fn chunk_catcher(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    Json(message): Json<Message>,
) -> StatusCode {
    // info!(?message, "recieved message");
    let Some(ring_meta) = ROUTER.get(&message.ring_id).map(|v| v.clone()) else {
        error!("unknown ring id: {:?}", message.ring_id);
        return StatusCode::NOT_FOUND;
    };

    ring_meta
        .tx
        .send(InternalMessage::Juggled(message))
        .unwrap();

    // let out_msg = match &message.payload {
    //     MessagePayload::Chunk { .. } => Some(message),
    //     MessagePayload::Switch { old, new } => {
    //         if old == &ring_meta.addr {
    //             let new_meta = RingMeta { addr: new.clone() };
    //             ROUTER.insert(message.ring_id, new_meta);
    //         }
    //         None
    //     }
    // };

    // if let Some(msg) = out_msg {
    //     // tokio::spawn(CLIENT.post(ring_meta.addr.into_url()).json(&msg).send());
    // }
    StatusCode::OK
}
