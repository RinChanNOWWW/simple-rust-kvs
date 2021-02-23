use failure::Fail;
use std::{io, result, string};

#[derive(Debug, Fail)]
pub enum KvsError {
    #[fail(display = "I/O Error: {}", _0)]
    IoError(io::Error),
    #[fail(display = "Key not found")]
    KeyNotFound,
    #[fail(display = "(De)serialization error: {}", _0)]
    SerDeError(serde_json::Error),
    #[fail(display = "Sled error: {}", _0)]
    SledError(sled::Error),
    #[fail(display = "From utf8 error: {}", _0)]
    FromUtf8Error(string::FromUtf8Error),
    #[fail(display = "Other error: {}", _0)]
    OtherError(String),
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

impl From<sled::Error> for KvsError {
    fn from(e: sled::Error) -> Self {
        KvsError::SledError(e)
    }
}

impl From<string::FromUtf8Error> for KvsError {
    fn from(e: string::FromUtf8Error) -> Self {
        KvsError::FromUtf8Error(e)
    }
}

pub type Result<T> = result::Result<T, KvsError>;
