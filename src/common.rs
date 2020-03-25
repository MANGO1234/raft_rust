use serde::Deserialize;
use serde::Serialize;
use std::net::SocketAddr;

#[derive(Serialize, Deserialize, Debug)]
pub enum SvrMsgCmd {
    ValGet(String),
    ValSet(String, String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum SvrMsgResp<'a> {
    Val(&'a str),
    Empty,
    Ok,
    Redirect(SocketAddr),
    Unavailable,
    Err(&'a str),
}
