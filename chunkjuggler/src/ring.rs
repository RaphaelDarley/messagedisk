use tokio::sync::mpsc;
use tracing::info;

use crate::{
    CLIENT, MEEEE, ROUTER, RingMeta, chunk_thrower,
    message::{Addr, InternalMessage, Message, MessagePayload, RingId},
};

pub async fn create_ring() -> RingId {
    let ring_id = RingId::new();

    join_ring(ring_id.clone(), MEEEE.get().unwrap().clone(), 2048).await;

    info!("Created Ring: {:?}", ring_id);

    ring_id
}

pub async fn join_ring(ring_id: RingId, target: Addr, chunk_num: u64) {
    let (tx, rx) = mpsc::unbounded_channel::<InternalMessage>();

    tokio::spawn(chunk_thrower(
        ring_id.clone(),
        rx,
        target.clone(),
        chunk_num,
    ));

    ROUTER.insert(
        ring_id.clone(),
        RingMeta {
            // addr: MEEEE.get().unwrap().clone(),
            tx,
            chunk_num,
        },
    );

    CLIENT
        .post(target.into_url())
        .json(&Message {
            ring_id: ring_id.clone(),
            payload: MessagePayload::Switch {
                old: target,
                new: MEEEE.get().unwrap().clone(),
            },
        })
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    info!("Joined Ring: {:?}", ring_id);
}
