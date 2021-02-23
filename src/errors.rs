use failure::Fail;
use std::{io, result};

#[derive(Debug, Fail)]
pub enum KvsError {
    #[fail(display = "Open kv file error: {}", _0)]
    IoError(io::Error),
    #[fail(display = "Key not found")]
    KeyNotFound,
    #[fail(display = "(De)serialization error: {}", _0)]
    SerDeError(serde_json::Error),
}

impl From<io::Error> for KvsError {
    fn from(e: io::Error) -> Self {
        KvsError::IoError(e)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(e: serde_json::Error) -> Self {
        KvsError::SerDeError(e)
    }
}

pub type Result<T> = result::Result<T, KvsError>;
