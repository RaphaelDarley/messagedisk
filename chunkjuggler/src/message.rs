use rand::RngExt;
use reqwest::{IntoUrl, Url};
use serde::{Deserialize, Serialize};
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    ops::Add,
};
use tokio::sync::oneshot;

pub const CHUNK_SIZE: usize = 512;

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub struct RingId(u64);

impl RingId {
    pub fn new() -> RingId {
        let mut rng = rand::rng();
        // let id = rng.random();
        RingId(1)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Addr {
    ip: u32,
    port: u16,
}

impl From<SocketAddrV4> for Addr {
    fn from(value: SocketAddrV4) -> Self {
        Addr {
            ip: value.ip().to_bits(),
            port: value.port(),
        }
    }
}

impl Addr {
    pub fn new(ip: u32, port: u16) -> Self {
        Addr { ip, port }
    }
    pub fn into_std(&self) -> SocketAddr {
        SocketAddrV4::new(Ipv4Addr::from_bits(self.ip), self.port).into()
    }

    pub fn into_url(&self) -> Url {
        Url::parse(&format!("http://{}", self.into_std())).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    pub ring_id: RingId,
    pub payload: MessagePayload,
}

pub enum InternalMessage {
    Juggled(Message),
    Read {
        chunk_id: u64,
        tx: oneshot::Sender<Vec<u8>>,
    },
    Write {
        chunk_id: u64,
        data: Vec<u8>,
        tx: oneshot::Sender<()>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MessagePayload {
    Chunk { id: u64, data: Vec<u8> },
    Switch { old: Addr, new: Addr },
}
