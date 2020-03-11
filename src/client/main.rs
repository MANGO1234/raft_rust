use std::net::{TcpStream, Shutdown};
use std::io::{Write, Read};

use raft_rust::common::SvrMsgCmd;

fn run_cmd(cmd: SvrMsgCmd) -> std::io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:12345")?;
    let mut buf = String::new();
    let cmd = serde_json::to_string(&cmd).unwrap();
    let len = cmd.as_bytes().len() as u32;
    let t = [len as u8, (len >> 8) as u8, (len >> 16) as u8, (len >> 24) as u8];
    stream.write(&t)?;
    stream.write(cmd.as_bytes())?;
    stream.flush()?;
    stream.read_to_string(&mut buf)?;
    println!("{}={}", cmd, buf);
    Ok(())
}

fn main() -> std::io::Result<()> {
    run_cmd(SvrMsgCmd::ValSet("a", "b"))?;
    run_cmd(SvrMsgCmd::ValGet("a"))?;
    run_cmd(SvrMsgCmd::ValGet("c"))?;
    Ok(())
}
