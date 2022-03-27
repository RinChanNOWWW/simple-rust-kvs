use crate::Result;
use serde::{Deserialize, Serialize};
use std::{
    io::{BufWriter, Write},
    net::TcpStream,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Get(Option<String>),
    Set,
    Remove,
    Err(String),
}

pub fn send_data<S: Serialize>(mut writer: BufWriter<&TcpStream>, data: S) -> Result<()> {
    serde_json::to_writer(&mut writer, &data)?;
    writer.flush()?;
    Ok(())
}
