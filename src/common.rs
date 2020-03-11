use serde::Serialize;
use serde::Deserialize;

#[derive(Serialize, Deserialize, Debug)]
pub enum SvrMsgCmd<'a> {
    ValGet(&'a str),
    ValSet(&'a str, &'a str),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum SvrMsgResp<'a> {
    Val(&'a str),
    Empty,
    Ok,
    Err(&'a str),
}