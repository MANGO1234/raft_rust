use serde::Deserialize;
use serde::Serialize;

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
    Err(&'a str),
}
