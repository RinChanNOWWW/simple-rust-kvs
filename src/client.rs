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

pub struct KvsClient {
    reader: Deserializer<IoRead<TcpStream>>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;

        Ok(KvsClient {
            reader: serde_json::Deserializer::from_reader(stream.try_clone()?),
            writer: BufWriter::new(stream),
        })
    }

    pub fn get(mut self, key: String) -> Result<Option<String>> {
        let resp = self.send_data(Request::Get { key })?;
        match resp {
            Response::Get(s) => Ok(s),
            Response::Err(e) => Err(KvsError::OtherError(e)),
            _ => Err(KvsError::WrongCommandError),
        }
    }
    pub fn set(mut self, key: String, value: String) -> Result<()> {
        let resp = self.send_data(Request::Set { key, value })?;
        match resp {
            Response::Set => Ok(()),
            Response::Err(e) => Err(KvsError::OtherError(e)),
            _ => Err(KvsError::WrongCommandError),
        }
    }
    pub fn remove(mut self, key: String) -> Result<()> {
        let resp = self.send_data(Request::Remove { key })?;
        match resp {
            Response::Remove => Ok(()),
            Response::Err(e) => Err(KvsError::OtherError(e)),
            _ => Err(KvsError::WrongCommandError),
        }
    }

    fn send_data(&mut self, req: Request) -> Result<Response> {
        serde_json::to_writer(&mut self.writer, &req)?;
        self.writer.flush()?;
        Ok(Response::deserialize(&mut self.reader)?)
    }
}
