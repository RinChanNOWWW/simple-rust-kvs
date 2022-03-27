use serde::{Deserialize, Serialize};

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
