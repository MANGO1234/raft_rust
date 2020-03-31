use raft_rust::common::send_string;
use serde::{Deserialize, Serialize};
use std::env;
use std::net::TcpStream;

#[derive(Serialize, Deserialize, Debug)]
pub enum DebugMsg {
    SvrState,
    LogState,
}

fn print_help() {
    println!("[1|2|3] <cmd>")
}

#[allow(dead_code)]
fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        print_help();
        return Ok(());
    }

    let node_addr;
    if args[1].as_str() == "1" {
        node_addr = "127.0.0.1:10001";
    } else if args[1].as_str() == "2" {
        node_addr = "127.0.0.1:10002";
    } else if args[1].as_str() == "3" {
        node_addr = "127.0.0.1:10003";
    } else {
        print_help();
        return Ok(());
    }

    let mut msg = DebugMsg::SvrState;
    if args[2].as_str() == "2" {
        msg = DebugMsg::LogState;
    }

    let mut stream = TcpStream::connect(node_addr)?;
    let cmd = serde_json::to_string(&msg).unwrap();
    send_string(&mut stream, &cmd)?;
    return Ok(());
}
