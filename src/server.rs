use crate::{
    network::{send_data, Request, Response},
    KvsEngine,
};
use crate::{thread_pool::ThreadPool, Result};
use log::error;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::{
    io::{BufReader, BufWriter},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::atomic::AtomicBool,
};

pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
    state: Arc<AtomicBool>,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    pub fn new(engine: E, pool: P) -> KvsServer<E, P> {
        KvsServer {
            engine,
            pool,
            state: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn new_with_state(engine: E, pool: P) -> (KvsServer<E, P>, Arc<AtomicBool>) {
        let state = Arc::new(AtomicBool::new(false));
        (
            KvsServer {
                engine,
                pool,
                state: Arc::clone(&state),
            },
            state,
        )
    }

    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        self.state.store(true, Ordering::SeqCst);
        for stream in listener.incoming() {
            if !self.state.load(Ordering::SeqCst) {
                break;
            }
            let engine = self.engine.clone();
            self.pool.spawn(move || match stream {
                Ok(s) => {
                    if let Err(e) = handle_connection(s, engine) {
                        error!("Handle Connection error: {}", e);
                    }
                }
                Err(e) => {
                    error!("Network connection error: {}", e);
                }
            })
        }
        Ok(())
    }
}

fn handle_connection<E: KvsEngine>(stream: TcpStream, engine: E) -> Result<()> {
    let mut reader =
        serde_json::Deserializer::from_reader(BufReader::new(&stream)).into_iter::<Request>();
    while let Some(req) = reader.next() {
        let writer = BufWriter::new(&stream);
        match req? {
            Request::Get { key } => {
                let resp = match engine.get(key) {
                    Ok(value) => Response::Get(value),
                    Err(e) => Response::Err(e.to_string()),
                };
                send_data::<Response>(writer, resp)?;
            }
            Request::Set { key, value } => {
                let resp = match engine.set(key, value) {
                    Ok(()) => Response::Set,
                    Err(e) => Response::Err(e.to_string()),
                };
                send_data::<Response>(writer, resp)?;
            }
            Request::Remove { key } => {
                let resp = match engine.remove(key) {
                    Ok(()) => Response::Remove,
                    Err(e) => Response::Err(e.to_string()),
                };
                send_data::<Response>(writer, resp)?;
            }
        }
    }
    Ok(())
}

pub fn stop_server<A: ToSocketAddrs>(state: Arc<AtomicBool>, addr: A) {
    state.store(false, Ordering::SeqCst);
    TcpStream::connect(addr).unwrap();
}
