use crossbeam::channel::Sender;
use std::cmp::min;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

// don't really like the clt resp here, but just a hobby project so keep it simple
pub enum CltMsgResp {
    Val(Arc<String>),
    Empty,
    Ok,
    Redirect(SocketAddr),
    Unavailable,
}

#[derive(Debug)]
pub struct Record {
    pub index: u64,
    pub term: u64,
    pub key: Arc<String>,
    pub val: Arc<String>,
    pub resp_sender: Option<Sender<CltMsgResp>>,
}

pub struct Log {
    pub records: Vec<Record>,
    pub map: HashMap<Arc<String>, Arc<String>>,
    pub latest_index: u64,
    pub latest_commit: u64,
}

impl Log {
    pub fn new() -> Log {
        return Log {
            records: Vec::new(),
            map: HashMap::new(),
            latest_index: 0,
            latest_commit: 0,
        };
    }

    pub fn get_idx(&self, idx: u64) -> Option<&Record> {
        if idx == 0 {
            None
        } else {
            self.records.get((idx - 1) as usize)
        }
    }

    pub fn last_info(&self) -> (u64, u64) {
        if self.records.len() == 0 {
            (0, 0)
        } else {
            let record = self.records.last().unwrap();
            (record.index, record.term)
        }
    }

    pub fn del_greater_or_equal_idx(&mut self, idx: u64) {
        while self.records.len() >= idx as usize {
            if let None = self.records.pop() {
                break;
            }
        }
        self.latest_index = min(self.latest_index, idx);
    }

    pub fn insert_new(
        &mut self,
        key: Arc<String>,
        val: Arc<String>,
        term: u64,
        resp_sender: Option<Sender<CltMsgResp>>,
    ) {
        self.latest_index += 1;
        let record = Record {
            index: self.latest_index,
            term,
            key: key,
            val: val,
            resp_sender,
        };
        self.records.push(record);
    }

    pub fn insert_overwrite(&mut self, key: Arc<String>, val: Arc<String>, term: u64, idx: u64) {
        if let Some(entry) = self.get_idx(idx) {
            if entry.term != term {
                self.del_greater_or_equal_idx(idx);
            }
        }
        self.insert_new(key, val, term, None);
    }

    pub fn commit_up_to(&mut self, idx: u64) {
        if idx > self.latest_commit {
            for i in self.latest_index..=idx {
                let record = &mut self.records[(i - 1) as usize];
                self.map.insert(record.key.clone(), record.val.clone());
                if let Some(resp_sender) = &record.resp_sender {
                    let _ = resp_sender.try_send(CltMsgResp::Ok);
                    record.resp_sender = None;
                }
            }
            self.latest_commit = idx;
        }
    }
}
