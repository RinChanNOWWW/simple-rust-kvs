use crate::{
    network::{Request, Response},
    KvsError, Result,
};
use serde::Deserialize;
use serde_json::de::{Deserializer, IoRead};
use std::{
    io::{BufWriter, Write},
    net::{TcpStream, ToSocketAddrs},
};

#[allow(unused)]
pub struct KvsClient {
    reader: Deserializer<IoRead<TcpStream>>,
    writer: BufWriter<TcpStream>,
}

#[allow(unused)]
impl KvsClient {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;

        Ok(KvsClient {
            reader: serde_json::Deserializer::from_reader(stream.try_clone()?),
            writer: BufWriter::new(stream),
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &Request::Get { key })?;
        self.writer.flush()?;
        let resp: Response = Response::deserialize(&mut self.reader)?;
        match resp {
            Response::Get(s) => Ok(s),
            Response::Err(e) => Err(KvsError::OtherError(e)),
            _ => Err(KvsError::WrongCommandError),
        }
    }
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Set { key, value })?;
        self.writer.flush()?;
        let resp = Response::deserialize(&mut self.reader)?;
        match resp {
            Response::Set => Ok(()),
            Response::Err(e) => Err(KvsError::OtherError(e)),
            _ => Err(KvsError::WrongCommandError),
        }
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Remove { key })?;
        self.writer.flush()?;
        let resp = Response::deserialize(&mut self.reader)?;
        match resp {
            Response::Remove => Ok(()),
            Response::Err(e) => Err(KvsError::OtherError(e)),
            _ => Err(KvsError::WrongCommandError),
        }
    }
}
