use serde::Deserialize;
use serde::Serialize;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};

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

pub fn send_string(stream: &mut TcpStream, msg: &String) -> std::io::Result<()> {
    let len = msg.as_bytes().len() as u32;
    let t = [
        len as u8,
        (len >> 8) as u8,
        (len >> 16) as u8,
        (len >> 24) as u8,
    ];
    stream.write(&t)?;
    stream.write(msg.as_bytes())?;
    stream.flush()?;
    Ok(())
}
