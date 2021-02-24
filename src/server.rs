use crate::{
    network::{send_data, GetResponse, RemoveResponse},
    thread_pool::ThreadPool,
    Result,
};
use crate::{
    network::{Request, SetResponse},
    KvsEngine,
};
use log::error;
use std::{
    io::{BufReader, BufWriter},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    pub fn new(engine: E, pool: P) -> KvsServer<E, P> {
        KvsServer { engine, pool }
    }

    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
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
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(e.to_string()),
                };
                send_data::<GetResponse>(writer, resp)?;
            }
            Request::Set { key, value } => {
                let resp = match engine.set(key, value) {
                    Ok(()) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(e.to_string()),
                };
                send_data::<SetResponse>(writer, resp)?;
            }
            Request::Remove { key } => {
                let resp = match engine.remove(key) {
                    Ok(()) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(e.to_string()),
                };
                send_data::<RemoveResponse>(writer, resp)?;
            }
        }
    }
    Ok(())
}
