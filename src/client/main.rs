use std::io::Read;
use std::net::{SocketAddr, TcpStream};

use raft_rust::common::{send_string, SvrMsgCmd, SvrMsgResp};
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;

fn run_cmd(cmd: SvrMsgCmd) -> std::io::Result<()> {
    let mut addr = SocketAddr::from_str("127.0.0.1:12001").unwrap();
    loop {
        let mut stream = TcpStream::connect(addr)?;
        let cmd = serde_json::to_string(&cmd).unwrap();
        send_string(&mut stream, &cmd)?;
        let mut buf = String::new();
        stream.read_to_string(&mut buf)?;
        println!("{}={}", cmd, buf);
        let resp: SvrMsgResp = serde_json::from_str(buf.as_str()).unwrap();
        match resp {
            SvrMsgResp::Redirect(new_addr) => {
                addr = new_addr;
            }
            SvrMsgResp::Unavailable => {
                sleep(Duration::from_millis(2000));
            }
            _ => {
                break;
            }
        }
    }
    Ok(())
}

fn main() -> std::io::Result<()> {
    run_cmd(SvrMsgCmd::ValSet("a".to_string(), "b".to_string()))?;
    run_cmd(SvrMsgCmd::ValGet("a".to_string()))?;
    run_cmd(SvrMsgCmd::ValGet("c".to_string()))?;
    Ok(())
}
