use crate::{
    network::{GetResponse, RemoveResponse, Request, SetResponse},
    KvsError, Result,
};
use serde::Deserialize;
use serde_json::de::{Deserializer, IoRead};
use std::{
    io::{BufWriter, Write},
    net::{TcpStream, ToSocketAddrs},
};

pub struct KvsClient {
    reader: Deserializer<IoRead<TcpStream>>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    pub fn new<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;

        Ok(KvsClient {
            reader: serde_json::Deserializer::from_reader(stream.try_clone()?),
            writer: BufWriter::new(stream),
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &Request::Get { key })?;
        self.writer.flush()?;
        let resp: GetResponse = GetResponse::deserialize(&mut self.reader)?;
        match resp {
            GetResponse::Ok(res) => Ok(res),
            GetResponse::Err(e) => Err(KvsError::OtherError(e)),
        }
    }
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Set { key, value })?;
        self.writer.flush()?;
        let resp = SetResponse::deserialize(&mut self.reader)?;
        match resp {
            SetResponse::Ok(()) => Ok(()),
            SetResponse::Err(e) => Err(KvsError::OtherError(e)),
        }
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Remove { key })?;
        self.writer.flush()?;
        let resp = RemoveResponse::deserialize(&mut self.reader)?;
        match resp {
            RemoveResponse::Ok(()) => Ok(()),
            RemoveResponse::Err(e) => Err(KvsError::OtherError(e)),
        }
    }
}
