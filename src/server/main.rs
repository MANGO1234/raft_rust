mod error;

use std::io::{Read, Write};
use std::net::{TcpListener};
use std::net::TcpStream;
use std::thread;

use raft_rust::common::{SvrMsgCmd, SvrMsgResp};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::mpsc::{sync_channel, SyncSender, Receiver, channel, Sender};
use std::time::Duration;
use error::{Result};

struct Record {
    data: Vec<u8>,
}

struct Log {
    records: Vec<(Rc<String>, Rc<String>)>,
    map: HashMap<Rc<String>, Rc<String>>,
}

struct Ctx {
    log: Log,
}

fn handle_cmd(ctx: &mut Ctx, cmd_buf: Vec<u8>, resp_sender: Sender<String>) -> Result<()> {
    let cmd_str = String::from_utf8(cmd_buf)?;
    let cmd: SvrMsgCmd = serde_json::from_str(cmd_str.as_str())?;
    println!("handling cmd {:?}", cmd);

    match cmd {
        SvrMsgCmd::ValGet(key) => {
            let val = ctx.log.map.get(&key.to_string());
            match val {
                Some(val) => resp_sender.send(serde_json::to_string(&SvrMsgResp::Val(val.as_str())).unwrap()).unwrap(),
                None => resp_sender.send(serde_json::to_string(&SvrMsgResp::Empty).unwrap()).unwrap()
            };
        }
        SvrMsgCmd::ValSet(key, val) => {
            let key = Rc::new(key.to_string());
            let val = Rc::new(val.to_string());
            ctx.log.records.push((key.clone(), val.clone()));
            ctx.log.map.insert(key, val);
            resp_sender.send(serde_json::to_string(&SvrMsgResp::Ok).unwrap()).unwrap();
        }
    }
    Ok(())
}

fn exec(receiver: Receiver<(Vec<u8>, Sender<String>)>) {
    let mut ctx = Ctx {
        log: Log {
            records: Vec::new(),
            map: HashMap::new(),
        },
    };

    loop {
        match receiver.recv_timeout(Duration::from_secs(1)) {
            Ok((cmd_buf, resp_sender)) => {
                match handle_cmd(&mut ctx, cmd_buf, resp_sender) {
                    Ok(_) => continue,
                    Err(e) => println!("Error occur handling{:?}", e)
                }
            },
            Err(_) => continue,
        }
    }
}

fn handle_client(mut stream: TcpStream, cmd_sender: SyncSender<(Vec<u8>, Sender<String>)>) -> Result<()> {
    println!("handling cmd from {}", stream.peer_addr().unwrap());
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;

    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf)?;
    let size = buf[0] as u32 + ((buf[1] as u32) << 8) + ((buf[2] as u32) << 16) + ((buf[3] as u32) << 24);
    let mut cmd_buf = vec![0u8; size as usize];
    stream.read_exact(cmd_buf.as_mut_slice())?;

    let (resp_sender, resp_receiver) = channel();
    cmd_sender.send((cmd_buf, resp_sender)).unwrap();
    let cmd_msg = resp_receiver.recv().unwrap();
    stream.write(cmd_msg.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn main() -> std::io::Result<()> {
    let (cmd_sender, cmd_receiver) = sync_channel(1000);
    thread::spawn(move || {
        exec(cmd_receiver);
    });

    let listener = TcpListener::bind("127.0.0.1:12345")?;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let cmd_sender = cmd_sender.clone();
                thread::spawn(move || {
                    match handle_client(stream, cmd_sender) {
                        Err(e) => println!("Error occur handling {:?}", e),
                        _ => (),
                    }
                });
            },
            Err(e) => println!("Error occur accepting {:?}", e),
        }
    }
    Ok(())
}
