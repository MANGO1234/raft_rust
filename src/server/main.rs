mod error;

use error::{Result, SvrErr};
use raft_rust::common::{SvrMsgCmd, SvrMsgResp};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::{env, thread};

static mut SVR_ADDR: Option<&str> = None;

fn get_self_addr() -> &'static str {
    let a = unsafe { SVR_ADDR.unwrap() };
    return a;
}

fn set_self_addr(s: &'static str) {
    unsafe {
        SVR_ADDR = Some(s);
    }
}

type NodeId = i32;

#[derive(Debug)]
enum SvrState {
    Leader,
    Follower {
        leader: Option<NodeId>,
        leader_last_contact: Instant,
    },
    Candidate {
        voted: HashSet<NodeId>,
        election_timeout: Duration,
        first_req_vote: Instant,
        last_req_vote: Instant,
    },
}

#[derive(Serialize, Deserialize, Debug)]
enum PeerMsg {
    HeartBeat {
        node_id: NodeId,
        term: u64,
    },
    HeartBeatResp {
        node_id: NodeId,
        term: u64,
        success: bool,
    },
    RequestVote {
        node_id: NodeId,
        term: u64,
        last_log_idx: u64,
    },
    RequestVoteResp {
        node_id: NodeId,
        term: u64,
        vote_granted: bool,
    },
}

struct Peer {
    node_id: NodeId,
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
        let _ = sender.send((stream.peer_addr().unwrap(), msg));
        println!(
            "{}: received from {} {}",
            get_self_addr(),
            stream.peer_addr().unwrap(),
            str
        );
    }
}

impl RaftCtx {
    fn add_peer(&mut self, node_id: NodeId, addr: SocketAddr) {
        let (sender, receiver) = sync_channel(10000);
        let peer = Peer {
            node_id: node_id,
            addr: addr,
            last_contact: Instant::now(),
            sender: sender,
        };
        let a = peer.addr.clone();
        self.peers.insert(node_id, peer);
        thread::spawn(move || {
            raft_peer_sender(a, receiver);
        });
    }

    fn find_peer(&self, node_id: NodeId) -> Option<&Peer> {
        self.peers.get(&node_id)
    }

    fn find_peer_mut(&mut self, node_id: NodeId) -> Option<&mut Peer> {
        self.peers.get_mut(&node_id)
    }

    fn to_candidate(&mut self) {
        self.term += 1;
        let mut rng = thread_rng();
        self.state = SvrState::Candidate {
            voted: HashSet::new(),
            election_timeout: Duration::from_millis(rng.gen_range(4000, 7000)),
            first_req_vote: Instant::now(),
            last_req_vote: Instant::now(),
        };
    }

    fn to_follower(&mut self, term: u64, node_id: NodeId) {
        self.state = SvrState::Follower {
            leader: Some(node_id),
            leader_last_contact: Instant::now(),
        };
        self.term = term;
    }

    fn to_leader(&mut self) {
        self.state = SvrState::Leader;
    }
}

struct PeerInfo {
    addr: SocketAddr,
    node_id: NodeId,
}

fn broadcast_request_vote_msg(term: u64, node_id: NodeId, peers: &mut HashMap<NodeId, Peer>) {
    for (_, peer) in peers {
        let _ = peer.sender.send(PeerMsg::RequestVote {
            term,
            node_id,
            last_log_idx: 0,
        });
    }
}

fn send_request_vote_resp_msg(ctx: &mut RaftCtx, peer_node_id: NodeId, vote_granted: bool) {
    let term = ctx.term;
    let node_id = ctx.node_id;
    let peer = ctx.find_peer_mut(peer_node_id);
    if let Some(peer) = peer {
        let _ = peer.sender.send(PeerMsg::RequestVoteResp {
            node_id,
            term,
            vote_granted,
        });
    }
}

fn send_heartbeat_msg(peer: &mut Peer, term: u64) {
    let _ = peer.sender.send(PeerMsg::HeartBeat {
        node_id: peer.node_id,
        term,
    });
}

struct RaftCtx {
    node_id: NodeId,
    state: SvrState,
    term: u64,
    peers: HashMap<NodeId, Peer>,
}

fn raft_main(
    a_addr: PeerInfo,
    b_addr: PeerInfo,
    msg_receiver: Receiver<(SocketAddr, PeerMsg)>,
    node_id: i32,
) {
    let mut ctx = RaftCtx {
        node_id,
        state: SvrState::Follower {
            leader: None,
            leader_last_contact: Instant::now(),
        },
        term: 0,
        peers: HashMap::new(),
    };

    ctx.add_peer(a_addr.node_id, a_addr.addr);
    ctx.add_peer(b_addr.node_id, b_addr.addr);

    let mut last_heart_beat = Instant::now();
    let mut last_print_state = Instant::now();
    loop {
        if last_print_state.elapsed().as_millis() >= 1000 {
            println!("{}: {:?}", get_self_addr(), ctx.state);
            last_print_state = Instant::now();
        }
        match msg_receiver.recv_timeout(Duration::from_millis(400)) {
            Ok((addr, msg)) => match msg {
                PeerMsg::HeartBeat { node_id, term } => {
                    if ctx.term < term {
                        ctx.to_follower(term, node_id);
                    } else if ctx.term == term {
                        match ctx.state {
                            SvrState::Follower {
                                leader,
                                ref mut leader_last_contact,
                            } => {
                                let mut updated = false;
                                if let Some(leader) = leader {
                                    if leader == node_id {
                                        *leader_last_contact = Instant::now();
                                        updated = true;
                                    }
                                }
                                if !updated {
                                    ctx.to_follower(term, node_id);
                                }
                            }
                            SvrState::Candidate { .. } => {
                                ctx.to_follower(term, node_id);
                            }
                            SvrState::Leader => {
                                // todo
                            }
                        }
                    } else {
                        // todo
                    }
                }
                PeerMsg::RequestVote {
                    node_id,
                    last_log_idx,
                    term,
                } => {
                    if ctx.term < term {
                        ctx.to_follower(term, node_id);
                        send_request_vote_resp_msg(&mut ctx, node_id, true);
                    } else if term == ctx.term {
                        send_request_vote_resp_msg(&mut ctx, node_id, false);
                    } else {
                        // ignore
                    }
                }
                PeerMsg::RequestVoteResp {
                    node_id,
                    term,
                    vote_granted,
                } => {
                    if let SvrState::Candidate { voted, .. } = &mut ctx.state {
                        if ctx.term < term {
                            ctx.term = term; // increased to newest term for next election
                        } else if ctx.term == term {
                            if vote_granted {
                                voted.insert(node_id);
                                if voted.len() >= ctx.peers.len() / 2 {
                                    ctx.to_leader();
                                }
                            }
                        } else {
                            // ignore
                        }
                    }
                }
                PeerMsg::HeartBeatResp {
                    node_id,
                    term,
                    success,
                } => {
                    // todo
                }
            },
            Err(_) => (),
        }

        match ctx.state {
            SvrState::Leader => {
                if last_heart_beat.elapsed().as_millis() >= 500 {
                    for (_, peer) in &mut ctx.peers {
                        send_heartbeat_msg(peer, ctx.term);
                    }
                    last_heart_beat = Instant::now();
                }
            }
            SvrState::Follower {
                leader_last_contact,
                ..
            } => {
                if leader_last_contact.elapsed().as_millis() >= 5000 {
                    ctx.to_candidate();
                    broadcast_request_vote_msg(ctx.term, ctx.node_id, &mut ctx.peers);
                }
            }
            SvrState::Candidate {
                first_req_vote,
                ref mut last_req_vote,
                election_timeout,
                ..
            } => {
                if first_req_vote.elapsed() >= election_timeout {
                    ctx.to_candidate();
                    broadcast_request_vote_msg(ctx.term, ctx.node_id, &mut ctx.peers);
                } else if last_req_vote.elapsed().as_millis() >= 500 {
                    broadcast_request_vote_msg(ctx.term, ctx.node_id, &mut ctx.peers);
                    *last_req_vote = Instant::now();
                }
            }
        }
    }
}

// I was going to try async io, then realize the design would be similar to threaded io anyway
// so just going to do that instead
fn raft_listen_main(self_addr: &str, a_addr: PeerInfo, b_addr: PeerInfo, node_id: i32) {
    let (sender, receiver) = sync_channel(10000);
    thread::spawn(move || {
        raft_main(a_addr, b_addr, receiver, node_id);
    });

    let listener;
    loop {
        let l = TcpListener::bind(self_addr);
        if let Err(e) = l {
            println!("Error while binding to {}: {}", self_addr, e);
        } else {
            listener = l.unwrap();
            break;
        }
        sleep(Duration::from_secs(1));
    }
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
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
    let node_id;
    if args[1].as_str() == "1" {
        self_addr = "127.0.0.1:10001";
        a_addr = PeerInfo {
            addr: SocketAddr::from_str("127.0.0.1:10002").unwrap(),
            node_id: 2,
        };
        b_addr = PeerInfo {
            addr: SocketAddr::from_str("127.0.0.1:10003").unwrap(),
            node_id: 3,
        };
        cmd_addr = "127.0.0.1:12001";
        node_id = 1;
    } else if args[1].as_str() == "2" {
        self_addr = "127.0.0.1:10002";
        a_addr = PeerInfo {
            addr: SocketAddr::from_str("127.0.0.1:10001").unwrap(),
            node_id: 1,
        };
        b_addr = PeerInfo {
            addr: SocketAddr::from_str("127.0.0.1:10003").unwrap(),
            node_id: 3,
        };
        cmd_addr = "127.0.0.1:12002";
        node_id = 2;
    } else if args[1].as_str() == "3" {
        self_addr = "127.0.0.1:10003";
        a_addr = PeerInfo {
            addr: SocketAddr::from_str("127.0.0.1:10001").unwrap(),
            node_id: 1,
        };
        b_addr = PeerInfo {
            addr: SocketAddr::from_str("127.0.0.1:10002").unwrap(),
            node_id: 2,
        };
        cmd_addr = "127.0.0.1:12003";
        node_id = 3;
    } else {
        println!("[1|2|3]");
        return Ok(());
    }
    set_self_addr(self_addr);

    thread::spawn(move || {
        raft_listen_main(self_addr, a_addr, b_addr, node_id);
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
