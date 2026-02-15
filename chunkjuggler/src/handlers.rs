use std::net::SocketAddrV4;

use axum::Json;
use reqwest::{StatusCode, Url};
use serde::Deserialize;
use tracing::warn;

use crate::{
    CLIENT, ROUTER,
    message::{Addr, CHUNK_SIZE, Message, MessagePayload, RingId},
    ring::join_ring,
};

pub async fn discover_handler() -> Json<Vec<(RingId, u64)>> {
    Json(
        ROUTER
            .iter()
            .map(|e| (e.key().clone(), e.value().chunk_num))
            .collect(),
    )
}

#[derive(Deserialize, Debug)]
pub struct StartMessage {
    ring_id: RingId,
    target: String,
    chunk_num: u64,
}

pub async fn start_handler(Json(message): Json<StartMessage>) -> StatusCode {
    let Ok(target) = message.target.parse::<SocketAddrV4>() else {
        warn!("Bad target: {:?}", message);
        return StatusCode::BAD_REQUEST;
    };

    let target_addr = Addr::from(target);

    join_ring(
        message.ring_id.clone(),
        target_addr.clone(),
        message.chunk_num,
    )
    .await;

    for i in 0..message.chunk_num {
        CLIENT
            .post(target_addr.into_url())
            .json(&Message {
                ring_id: message.ring_id.clone(),
                payload: MessagePayload::Chunk {
                    id: i,
                    data: vec![0; CHUNK_SIZE],
                },
            })
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    StatusCode::OK
}

#[derive(Deserialize, Debug)]
pub struct JoinMessage {
    ring_id: RingId,
    target: String,
    chunk_num: u64,
}

pub async fn join_handler(Json(message): Json<JoinMessage>) -> StatusCode {
    let Ok(target) = message.target.parse::<SocketAddrV4>() else {
        warn!("Bad target: {:?}", message);
        return StatusCode::BAD_REQUEST;
    };

    let target_addr = Addr::from(target);

    join_ring(message.ring_id, target_addr, message.chunk_num).await;

    StatusCode::OK
}

pub async fn shutdown_handler() -> StatusCode {
    for e in ROUTER.iter() {
        let meta = e.value();
        let _ = meta.tx.send(crate::message::InternalMessage::Leave);
    }
    StatusCode::OK
}
