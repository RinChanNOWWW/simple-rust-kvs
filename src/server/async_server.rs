use crate::Result;
use crate::{
    network::{Request, Response},
    KvsEngine,
};
use futures::prelude::*;
use log::error;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_serde::formats::SymmetricalJson;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

pub struct KvsServer<E: KvsEngine> {
    engine: E,
    state: Arc<AtomicBool>,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E) -> KvsServer<E> {
        KvsServer {
            engine,
            state: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.state.store(true, Ordering::SeqCst);

        loop {
            let (stream, _) = listener.accept().await?;
            if !self.state.load(Ordering::SeqCst) {
                break;
            }
            let engine = self.engine.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, engine).await {
                    error!("Handle Connection error: {}", e);
                }
            });
        }
        Ok(())
    }

    pub fn new_with_state(engine: E) -> (KvsServer<E>, Arc<AtomicBool>) {
        let state = Arc::new(AtomicBool::new(false));
        (
            KvsServer {
                engine,
                state: Arc::clone(&state),
            },
            state,
        )
    }
}

async fn handle_connection<E: KvsEngine>(mut stream: TcpStream, engine: E) -> Result<()> {
    let (read_half, write_half) = stream.split();
    let mut reader = tokio_serde::SymmetricallyFramed::new(
        FramedRead::new(read_half, LengthDelimitedCodec::new()),
        SymmetricalJson::<Request>::default(),
    );
    let mut writer = tokio_serde::SymmetricallyFramed::new(
        FramedWrite::new(write_half, LengthDelimitedCodec::new()),
        SymmetricalJson::<Response>::default(),
    );

    while let Some(req) = reader.try_next().await? {
        let resp = match req {
            Request::Get { key } => match engine.get(key) {
                Ok(value) => Response::Get(value),
                Err(e) => Response::Err(e.to_string()),
            },
            Request::Set { key, value } => match engine.set(key, value) {
                Ok(()) => Response::Set,
                Err(e) => Response::Err(e.to_string()),
            },
            Request::Remove { key } => match engine.remove(key) {
                Ok(()) => Response::Remove,
                Err(e) => Response::Err(e.to_string()),
            },
        };
        writer.send(resp).await?;
    }
    Ok(())
}

pub async fn stop_server<A: ToSocketAddrs>(state: Arc<AtomicBool>, addr: A) {
    state.store(false, Ordering::SeqCst);
    TcpStream::connect(addr).await.unwrap();
}
