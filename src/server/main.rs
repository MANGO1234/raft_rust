mod debug;
mod error;
mod log;

use crossbeam::channel::{Receiver, Sender};
use debug::DebugMsg;
use error::Result;
use log::{CltMsgResp, Log, Record};
use raft_rust::common::{send_string, SvrMsgCmd, SvrMsgResp};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::Sub;
use std::str::FromStr;
use std::sync::Arc;
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
    Leader {
        last_heart_beat: Instant,
    },
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
struct MsgRecord {
    index: u64,
    term: u64,
    key: Arc<String>,
    val: Arc<String>,
}

impl From<&Record> for MsgRecord {
    fn from(item: &Record) -> Self {
        MsgRecord {
            index: item.index,
            term: item.term,
            key: item.key.clone(),
            val: item.val.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum PeerMsg {
    AppendEntries {
        node_id: NodeId,
        term: u64,
        prev_log_term: u64,
        prev_log_idx: u64,
        leader_commit_idx: u64,
        entries: Vec<MsgRecord>,
    },
    AppendEntriesResp {
        node_id: NodeId,
        term: u64,
        success: bool,
        match_idx: u64,
        next_idx: u64,
    },
    RequestVote {
        node_id: NodeId,
        term: u64,
        last_log_idx: u64,
        last_log_term: u64,
    },
    RequestVoteResp {
        node_id: NodeId,
        term: u64,
        vote_granted: bool,
    },
}

#[derive(Debug)]
enum InternalMsg {
    Peer(PeerMsg),
    Debug(DebugMsg),
    Clt(SvrMsgCmd, Sender<CltMsgResp>),
}

struct RaftCtx {
    node_id: NodeId,
    state: SvrState,
    term: u64,
    peers: HashMap<NodeId, Peer>,
    log: Log,
}

struct Peer {
    pub_addr: SocketAddr,
    addr: SocketAddr,
    sender: Sender<PeerMsg>,
    match_idx: u64,
    next_idx: u64,
}

impl RaftCtx {
    fn add_peer(&mut self, node_id: NodeId, addr: SocketAddr, pub_addr: SocketAddr) {
        let (sender, receiver) = crossbeam::bounded(10000);
        let peer = Peer {
            addr: addr,
            pub_addr: pub_addr,
            sender: sender,
            match_idx: 0,
            next_idx: 1,
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
        self.state = SvrState::Leader {
            // send heartbeat immediately
            last_heart_beat: Instant::now().sub(Duration::from_secs(10)),
        };
    }
}

struct PeerInfo {
    addr: SocketAddr,
    pub_addr: SocketAddr,
    node_id: NodeId,
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
            pub_addr: SocketAddr::from_str("127.0.0.1:12002").unwrap(),
            node_id: 2,
        };
        b_addr = PeerInfo {
            addr: SocketAddr::from_str("127.0.0.1:10003").unwrap(),
            pub_addr: SocketAddr::from_str("127.0.0.1:12003").unwrap(),
            node_id: 3,
        };
        cmd_addr = "127.0.0.1:12001";
        node_id = 1;
    } else if args[1].as_str() == "2" {
        self_addr = "127.0.0.1:10002";
        a_addr = PeerInfo {
            addr: SocketAddr::from_str("127.0.0.1:10001").unwrap(),
            pub_addr: SocketAddr::from_str("127.0.0.1:12001").unwrap(),
            node_id: 1,
        };
        b_addr = PeerInfo {
            addr: SocketAddr::from_str("127.0.0.1:10003").unwrap(),
            pub_addr: SocketAddr::from_str("127.0.0.1:12003").unwrap(),
            node_id: 3,
        };
        cmd_addr = "127.0.0.1:12002";
        node_id = 2;
    } else if args[1].as_str() == "3" {
        self_addr = "127.0.0.1:10003";
        a_addr = PeerInfo {
            addr: SocketAddr::from_str("127.0.0.1:10001").unwrap(),
            pub_addr: SocketAddr::from_str("127.0.0.1:12001").unwrap(),
            node_id: 1,
        };
        b_addr = PeerInfo {
            addr: SocketAddr::from_str("127.0.0.1:10002").unwrap(),
            pub_addr: SocketAddr::from_str("127.0.0.1:12002").unwrap(),
            node_id: 2,
        };
        cmd_addr = "127.0.0.1:12003";
        node_id = 3;
    } else {
        println!("[1|2|3]");
        return Ok(());
    }
    set_self_addr(self_addr);

    let (sender, receiver) = crossbeam::bounded(1000000);
    let peer_sender = sender.clone();
    thread::spawn(move || {
        raft_listen_main(self_addr, a_addr, b_addr, node_id, peer_sender, receiver);
    });

    let listener = TcpListener::bind(cmd_addr)?;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let cmd_sender = sender.clone();
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

// I was going to try async io, then realize the design would be similar to threaded io anyway
// so just going to do that instead
fn raft_listen_main(
    self_addr: &str,
    a_addr: PeerInfo,
    b_addr: PeerInfo,
    node_id: i32,
    sender: Sender<InternalMsg>,
    receiver: Receiver<InternalMsg>,
) {
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
                thread::spawn(move || match handle_peer_conn(stream, sender, node_id) {
                    Err(e) => println!("Error occur handling {:?}", e),
                    _ => (),
                });
            }
            Err(e) => println!("{}: Error occur accepting {:?}", self_addr, e),
        }
    }
}

fn raft_main(
    a_addr: PeerInfo,
    b_addr: PeerInfo,
    msg_receiver: Receiver<InternalMsg>,
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
        log: Log::new(),
    };

    ctx.add_peer(a_addr.node_id, a_addr.addr, a_addr.pub_addr);
    ctx.add_peer(b_addr.node_id, b_addr.addr, b_addr.pub_addr);

    let mut last_print_state = Instant::now();
    loop {
        if last_print_state.elapsed().as_millis() >= 4000 {
            println!(
                "{}: {:?} {} {}",
                get_self_addr(),
                ctx.state,
                ctx.log.latest_commit,
                ctx.log.latest_index
            );
            last_print_state = Instant::now();
        }
        match msg_receiver.recv_timeout(Duration::from_millis(400)) {
            Ok(msg) => match msg {
                InternalMsg::Peer(peer_msg) => {
                    handle_peer_msg(&mut ctx, peer_msg);
                }
                InternalMsg::Debug(debug_msg) => {
                    handle_debug_msg(&ctx, debug_msg);
                }
                InternalMsg::Clt(cmd, sender) => {
                    handle_clt_msg(&mut ctx, cmd, sender);
                }
            },
            Err(_) => (),
        }

        match ctx.state {
            SvrState::Leader { last_heart_beat } => {
                if last_heart_beat.elapsed().as_millis() >= 500 {
                    for (_, peer) in &ctx.peers {
                        send_heartbeat_msg(&ctx, peer);
                    }
                    ctx.state = SvrState::Leader {
                        last_heart_beat: Instant::now(),
                    };
                }
            }
            SvrState::Follower {
                leader_last_contact,
                ..
            } => {
                if leader_last_contact.elapsed().as_millis() >= 5000 {
                    ctx.to_candidate();
                    broadcast_request_vote_msg(&ctx.peers, &ctx.log, ctx.term, ctx.node_id);
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
                    broadcast_request_vote_msg(&ctx.peers, &ctx.log, ctx.term, ctx.node_id);
                } else if last_req_vote.elapsed().as_millis() >= 500 {
                    broadcast_request_vote_msg(&ctx.peers, &ctx.log, ctx.term, ctx.node_id);
                    *last_req_vote = Instant::now();
                }
            }
        }
    }
}

fn handle_peer_msg(ctx: &mut RaftCtx, msg: PeerMsg) {
    match msg {
        PeerMsg::AppendEntries {
            node_id,
            term,
            prev_log_idx,
            prev_log_term,
            leader_commit_idx,
            entries,
        } => {
            if ctx.term < term {
                ctx.to_follower(term, node_id);
            } else if ctx.term == term {
                if let SvrState::Candidate { .. } = ctx.state {
                    ctx.to_follower(term, node_id);
                }
                if let SvrState::Follower {
                    ref mut leader,
                    ref mut leader_last_contact,
                } = ctx.state
                {
                    *leader_last_contact = Instant::now();
                    if let Some(leader) = leader {
                        if *leader != node_id {
                            // something went wrong with server logic
                            panic!();
                        }
                    } else {
                        *leader = Some(node_id)
                    }

                    if prev_log_term != 0 {
                        let mut conflict = false;
                        match ctx.log.get_idx(prev_log_idx) {
                            Some(record) => {
                                if record.term != prev_log_term {
                                    conflict = true;
                                }
                            }
                            None => {
                                conflict = true;
                            }
                        }
                        if conflict {
                            send_append_entries_resp_msg(ctx, node_id, false, prev_log_idx);
                            return;
                        }
                    }

                    for e in entries {
                        ctx.log.insert_overwrite(e.key, e.val, e.term, e.index);
                    }
                    ctx.log.commit_up_to(leader_commit_idx);
                    send_append_entries_resp_msg(ctx, node_id, true, ctx.log.latest_index + 1);
                } else {
                    // something went wrong with server logic
                    panic!();
                }
            } else {
                // ignore, older leader's heartbeat
            }
        }
        PeerMsg::RequestVote {
            node_id,
            last_log_idx,
            last_log_term,
            term,
        } => {
            if ctx.term < term {
                let (self_last_log_idx, self_last_log_term) = ctx.log.last_info();
                let vote = self_last_log_term < last_log_term
                    || (self_last_log_term == last_log_term && self_last_log_idx <= last_log_idx);
                ctx.to_follower(term, node_id);
                send_request_vote_resp_msg(ctx, node_id, vote);
            } else {
                send_request_vote_resp_msg(ctx, node_id, false);
            }
        }
        PeerMsg::RequestVoteResp {
            node_id,
            term,
            vote_granted,
        } => {
            if let SvrState::Candidate { voted, .. } = &mut ctx.state {
                if ctx.term < term {
                    ctx.to_follower(term, node_id);
                } else if ctx.term == term {
                    if vote_granted {
                        voted.insert(node_id);
                        if voted.len() >= (ctx.peers.len() + 1) / 2 {
                            ctx.to_leader();
                        }
                    }
                } else {
                    // ignore, we don't need to tally votes if not candidate
                }
            }
        }
        PeerMsg::AppendEntriesResp {
            node_id,
            term,
            success,
            next_idx,
            match_idx,
        } => {
            if ctx.term < term {
                ctx.to_follower(term, node_id);
            } else if ctx.term == term {
                if let Some(peer) = ctx.find_peer_mut(node_id) {
                    peer.match_idx = match_idx;
                    peer.next_idx = next_idx;
                }
                let mut matches: Vec<_> =
                    ctx.peers.iter().map(|(_, peer)| peer.match_idx).collect();
                matches.sort();
                let latest_commit = max(matches[(matches.len() + 1) / 2], ctx.log.latest_commit);
                ctx.log.commit_up_to(latest_commit);

                if let Some(peer) = ctx.find_peer(node_id) {
                    if peer.next_idx <= ctx.log.latest_index {
                        send_append_entries_msg(ctx, peer, success);
                    }
                }
            } else {
                // ignore, older leader's response
            }
        }
    }
}

fn handle_debug_msg(ctx: &RaftCtx, msg: DebugMsg) {
    match msg {
        DebugMsg::SvrState => println!(
            "{}: state={:?} term={} index={} commit={}",
            ctx.node_id, ctx.state, ctx.term, ctx.log.latest_index, ctx.log.latest_commit
        ),
        DebugMsg::LogState => println!("{}: log={:?}", ctx.node_id, ctx.log.records),
    }
}

fn handle_clt_msg(ctx: &mut RaftCtx, cmd: SvrMsgCmd, resp_sender: Sender<CltMsgResp>) {
    if let SvrState::Leader { .. } = ctx.state {
        match cmd {
            SvrMsgCmd::ValGet(key) => {
                match ctx.log.map.get(&key.to_string()) {
                    Some(val) => {
                        let _ = resp_sender.try_send(CltMsgResp::Val(val.clone()));
                    }
                    None => {
                        let _ = resp_sender.try_send(CltMsgResp::Empty);
                    }
                };
            }
            SvrMsgCmd::ValSet(key, val) => {
                ctx.log
                    .insert_new(Arc::new(key), Arc::new(val), ctx.term, Some(resp_sender));
                for (_, peer) in &ctx.peers {
                    send_append_entries_msg(ctx, peer, false);
                }
            }
        }
    } else {
        let mut found = false;
        if let SvrState::Follower { leader, .. } = ctx.state {
            if let Some(leader) = leader {
                if let Some(peer) = ctx.find_peer(leader) {
                    let _ = resp_sender.try_send(CltMsgResp::Redirect(peer.pub_addr));
                    found = true;
                }
            }
        }
        if !found {
            let _ = resp_sender.try_send(CltMsgResp::Unavailable);
        }
    }
}

fn broadcast_request_vote_msg(
    peers: &HashMap<NodeId, Peer>,
    log: &Log,
    term: u64,
    node_id: NodeId,
) {
    for (_, peer) in peers {
        let (last_log_idx, last_log_term) = log.last_info();
        let _ = peer.sender.try_send(PeerMsg::RequestVote {
            term,
            node_id,
            last_log_idx,
            last_log_term,
        });
    }
}

fn send_request_vote_resp_msg(ctx: &RaftCtx, peer_node_id: NodeId, vote_granted: bool) {
    if let Some(peer) = ctx.find_peer(peer_node_id) {
        let _ = peer.sender.try_send(PeerMsg::RequestVoteResp {
            node_id: ctx.node_id,
            term: ctx.term,
            vote_granted,
        });
    }
}

fn send_heartbeat_msg(ctx: &RaftCtx, peer: &Peer) {
    let (prev_log_idx, prev_log_term) = ctx.log.last_info();
    let _ = peer.sender.try_send(PeerMsg::AppendEntries {
        node_id: ctx.node_id,
        term: ctx.term,
        prev_log_idx,
        prev_log_term,
        entries: Vec::with_capacity(0),
        leader_commit_idx: ctx.log.latest_commit,
    });
}

fn send_append_entries_msg(ctx: &RaftCtx, peer: &Peer, all: bool) {
    let mut prev_log_idx = 0;
    let mut prev_log_term = 0;
    let mut start_idx = 1;
    if peer.next_idx > 1 {
        if let Some(record) = ctx.log.get_idx(peer.next_idx - 1) {
            prev_log_idx = record.index;
            prev_log_term = record.term;
        } else {
            // something went wrong here
            panic!();
        }
        start_idx = peer.next_idx;
    }

    let _ = peer.sender.try_send(PeerMsg::AppendEntries {
        node_id: ctx.node_id,
        term: ctx.term,
        prev_log_idx,
        prev_log_term,
        leader_commit_idx: ctx.log.latest_commit,
        entries: if all {
            let mut entries = Vec::with_capacity((ctx.log.latest_index - start_idx + 1) as usize);
            for index in start_idx..=ctx.log.latest_index {
                // todo: use an iterator
                entries.push(ctx.log.get_idx(index).unwrap().into());
            }
            entries
        } else {
            let record = ctx.log.get_idx(start_idx).unwrap();
            vec![record.into()]
        },
    });
}

fn send_append_entries_resp_msg(ctx: &RaftCtx, peer_node_id: NodeId, success: bool, next_idx: u64) {
    if let Some(peer) = ctx.find_peer(peer_node_id) {
        let _ = peer.sender.try_send(PeerMsg::AppendEntriesResp {
            node_id: ctx.node_id,
            term: ctx.term,
            success,
            match_idx: ctx.log.latest_index,
            next_idx,
        });
    }
}

fn handle_client(mut stream: TcpStream, cmd_sender: Sender<InternalMsg>) -> Result<()> {
    println!("handling cmd from {}", stream.peer_addr().unwrap());
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;

    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf)?;
    let size =
        buf[0] as u32 + ((buf[1] as u32) << 8) + ((buf[2] as u32) << 16) + ((buf[3] as u32) << 24);
    let mut cmd_buf = vec![0u8; size as usize];
    stream.read_exact(cmd_buf.as_mut_slice())?;

    let cmd_str = String::from_utf8(cmd_buf)?;
    let cmd: SvrMsgCmd = serde_json::from_str(cmd_str.as_str())?;
    println!("handling cmd {:?}", cmd);
    let (sender, receiver) = crossbeam::bounded(0);
    let mut clt_resp = None;
    if let Ok(_) = cmd_sender.send(InternalMsg::Clt(cmd, sender)) {
        if let Ok(resp) = receiver.recv() {
            clt_resp = Some(resp);
        }
    };
    let resp = if let Some(clt_resp) = &clt_resp {
        match clt_resp {
            CltMsgResp::Ok => SvrMsgResp::Ok,
            CltMsgResp::Empty => SvrMsgResp::Empty,
            CltMsgResp::Redirect(addr) => SvrMsgResp::Redirect(addr.clone()),
            CltMsgResp::Unavailable => SvrMsgResp::Unavailable,
            CltMsgResp::Val(msg) => SvrMsgResp::Err(msg.as_str()),
        }
    } else {
        SvrMsgResp::Err("Internal Error")
    };
    stream.write(serde_json::to_string(&resp).unwrap().as_bytes())?;
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
                if let Err(e) = send_string(st, &msg) {
                    println!("{}: Error sending to {} {}", get_self_addr(), peer_addr, e);
                    stream = None;
                }
            }
        } else {
            sleep(Duration::from_millis(300));
        }
    }
}

fn handle_peer_conn(
    mut stream: TcpStream,
    sender: Sender<InternalMsg>,
    node_id: NodeId,
) -> Result<()> {
    let mut log_file = File::create(format!("F:/raft_{}.log", node_id))?;

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
        let msg = serde_json::from_str::<PeerMsg>(str.as_str());
        match msg {
            Ok(msg) => {
                let _ = sender.send(InternalMsg::Peer(msg));
                let _ = log_file.write_all(format!("{}: received {}\n", node_id, str).as_bytes());
                let _ = log_file.flush();
            }
            Err(_) => {
                let msg = serde_json::from_str::<DebugMsg>(str.as_str());
                if let Ok(msg) = msg {
                    let _ = sender.send(InternalMsg::Debug(msg));
                }
            }
        }
    }
}
