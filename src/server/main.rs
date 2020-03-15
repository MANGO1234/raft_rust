mod error;

use error::{Result, SvrErr};
use raft_rust::common::{SvrMsgCmd, SvrMsgResp};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::{env, thread};

static mut svr_addr: Option<&str> = None;

fn get_self_addr() -> &'static str {
    let a = unsafe { svr_addr.unwrap() };
    return a;
}

fn set_self_addr(s: &'static str) {
    unsafe {
        svr_addr = Some(s);
    }
}

enum SvrState {
    Leader,
    Follower,
    Candidate,
}

#[derive(Serialize, Deserialize)]
enum PeerMsg {
    AppendEntries,
    RequestVote,
}

struct SvrCtx {
    state: SvrState,
    term: u64,
    leader: Option<Peer>,
    leader_last_contact: Instant,
    peers: HashMap<SocketAddr, Peer>,
}

struct Peer {
    addr: SocketAddr,
    last_contact: Instant,
    sender: SyncSender<PeerMsg>,
}

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
                Some(val) => resp_sender
                    .send(serde_json::to_string(&SvrMsgResp::Val(val.as_str())).unwrap())
                    .unwrap(),
                None => resp_sender
                    .send(serde_json::to_string(&SvrMsgResp::Empty).unwrap())
                    .unwrap(),
            };
        }
        SvrMsgCmd::ValSet(key, val) => {
            let key = Rc::new(key.to_string());
            let val = Rc::new(val.to_string());
            ctx.log.records.push((key.clone(), val.clone()));
            ctx.log.map.insert(key, val);
            resp_sender
                .send(serde_json::to_string(&SvrMsgResp::Ok).unwrap())
                .unwrap();
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
            Ok((cmd_buf, resp_sender)) => match handle_cmd(&mut ctx, cmd_buf, resp_sender) {
                Ok(_) => continue,
                Err(e) => println!("Error occur handling{:?}", e),
            },
            Err(_) => continue,
        }
    }
}

fn handle_client(
    mut stream: TcpStream,
    cmd_sender: SyncSender<(Vec<u8>, Sender<String>)>,
) -> Result<()> {
    println!("handling cmd from {}", stream.peer_addr().unwrap());
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;

    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf)?;
    let size =
        buf[0] as u32 + ((buf[1] as u32) << 8) + ((buf[2] as u32) << 16) + ((buf[3] as u32) << 24);
    let mut cmd_buf = vec![0u8; size as usize];
    stream.read_exact(cmd_buf.as_mut_slice())?;

    let (resp_sender, resp_receiver) = channel();
    cmd_sender.send((cmd_buf, resp_sender)).unwrap();
    let cmd_msg = resp_receiver.recv().unwrap();
    stream.write(cmd_msg.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn send_string(stream: &mut TcpStream, msg: String) -> std::io::Result<()> {
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

fn raft_peer_sender(peer_addr: SocketAddr, receiver: Receiver<PeerMsg>) {
    let mut stream = None;
    loop {
        if let None = stream {
            match TcpStream::connect_timeout(&peer_addr, Duration::from_secs(1)) {
                Ok(s) => {
                    if let Ok(_) = s.set_write_timeout(Some(Duration::from_secs(2))) {
                        stream = Some(s)
                    }
                }
                Err(e) => println!("Error connecting to {} {}", peer_addr, e),
            }
        }
        if let Some(ref mut st) = stream {
            if let Ok(msg) = receiver.recv_timeout(Duration::from_secs(1)) {
                let msg = serde_json::to_string(&msg).unwrap();
                if let Err(e) = send_string(st, msg) {
                    println!("{}: Error sending to {} {}", get_self_addr(), peer_addr, e);
                    stream = None;
                }
            }
        } else {
            sleep(Duration::from_secs(1));
        }
    }
}

fn handle_peer_msg(mut stream: TcpStream, sender: SyncSender<(SocketAddr, PeerMsg)>) -> Result<()> {
    loop {
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf)?;
        let size = buf[0] as u32
            + ((buf[1] as u32) << 8)
            + ((buf[2] as u32) << 16)
            + ((buf[3] as u32) << 24);
        let mut msg = vec![0u8; size as usize];
        stream.read_exact(msg.as_mut_slice())?;
        let str = String::from_utf8(msg)?;
        let msg: PeerMsg = serde_json::from_str(str.as_str())?;
        sender.send((stream.peer_addr().unwrap(), msg));
        println!(
            "{}: received from {} {}",
            get_self_addr(),
            stream.peer_addr().unwrap(),
            str
        );
    }
    Ok(())
}

impl SvrCtx {
    fn addPeer(&mut self, addr: SocketAddr) {
        let (sender, receiver) = sync_channel(10000);
        let peer = Peer {
            addr: addr,
            last_contact: Instant::now(),
            sender: sender,
        };
        let a = peer.addr.clone();
        self.peers.insert(a.clone(), peer);
        thread::spawn(move || {
            raft_peer_sender(a, receiver);
        });
    }
}

fn raft_main(a_addr: String, b_addr: String, msg_receiver: Receiver<(SocketAddr, PeerMsg)>) {
    let mut ctx = SvrCtx {
        state: SvrState::Follower,
        term: 0,
        leader: None,
        leader_last_contact: Instant::now(),
        peers: HashMap::new(),
    };

    ctx.addPeer(SocketAddr::from_str(a_addr.as_str()).unwrap());
    ctx.addPeer(SocketAddr::from_str(b_addr.as_str()).unwrap());

    let mut last_heart_beat = Instant::now();
    loop {
        match msg_receiver.recv_timeout(Duration::from_millis(500)) {
            Ok((Addr, PeerMsg)) => (),
            Err(_) => (),
        }

        if last_heart_beat.elapsed().as_millis() >= 3000 {
            for (_, peer) in &ctx.peers {
                peer.sender.send(PeerMsg::AppendEntries);
            }
            last_heart_beat = Instant::now();
        }
    }
}

// I was going to try async io, then realize the design would be similar to threaded io anyway
// so just going to do that instead
fn raft_listen_main(self_addr: &str, a_addr: String, b_addr: String) {
    let (sender, receiver) = sync_channel(10000);
    thread::spawn(move || {
        raft_main(a_addr, b_addr, receiver);
    });

    let mut listener;
    loop {
        let l = TcpListener::bind(self_addr);
        if let Err(e) = l {
            println!("Error while binding to {}", self_addr);
        } else {
            listener = l.unwrap();
            break;
        }
        sleep(Duration::from_secs(1));
    }
    loop {
        match listener.accept() {
            Ok((mut stream, addr)) => {
                println!("{}: Accepted {}!", self_addr, addr);
                let sender = sender.clone();
                thread::spawn(move || match handle_peer_msg(stream, sender) {
                    Err(e) => println!("Error occur handling {:?}", e),
                    _ => (),
                });
            }
            Err(e) => println!("{}: Error occur accepting {:?}", self_addr, e),
        }
    }
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() == 1 {
        println!("[1|2|3]");
        return Ok(());
    }

    let cmd_addr;
    let self_addr;
    let a_addr;
    let b_addr;
    if args[1].as_str() == "1" {
        self_addr = "127.0.0.1:10001";
        a_addr = "127.0.0.1:10002";
        b_addr = "127.0.0.1:10003";
        cmd_addr = "127.0.0.1:12001";
    } else if args[1].as_str() == "2" {
        self_addr = "127.0.0.1:10002";
        a_addr = "127.0.0.1:10001";
        b_addr = "127.0.0.1:10003";
        cmd_addr = "127.0.0.1:12002";
    } else if args[1].as_str() == "3" {
        self_addr = "127.0.0.1:10003";
        a_addr = "127.0.0.1:10001";
        b_addr = "127.0.0.1:10002";
        cmd_addr = "127.0.0.1:12003";
    } else {
        println!("[1|2|3]");
        return Ok(());
    }
    set_self_addr(self_addr);

    thread::spawn(move || {
        raft_listen_main(self_addr, a_addr.to_string(), b_addr.to_string());
    });

    let (cmd_sender, cmd_receiver) = sync_channel(1000);
    thread::spawn(move || {
        exec(cmd_receiver);
    });

    let listener = TcpListener::bind(cmd_addr)?;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let cmd_sender = cmd_sender.clone();
                thread::spawn(move || match handle_client(stream, cmd_sender) {
                    Err(e) => println!("Error occur handling {:?}", e),
                    _ => (),
                });
            }
            Err(e) => println!("Error occur accepting {:?}", e),
        }
    }
    Ok(())
}
