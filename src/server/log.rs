use crate::error::{OtherError, Result, SvrErr};
use crossbeam::channel::Sender;
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::File;
use std::io::{BufReader, ErrorKind, Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::sync::Arc;

pub type NodeId = u32;

// don't really like the clt resp here, but just a hobby project so keep it simple
pub enum CltMsgResp {
    Val(Arc<String>),
    Empty,
    Ok,
    Redirect(SocketAddr),
    Unavailable,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Record {
    pub index: u64,
    pub term: u64,
    pub key: Arc<String>,
    pub val: Arc<String>,

    #[serde(skip)]
    resp_sender: Option<Sender<CltMsgResp>>,
}

pub struct Log {
    pub records: Vec<Record>,
    pub map: HashMap<Arc<String>, Arc<String>>,
    pub latest_index: u64,
    pub latest_commit: u64,
    term: u64,
    voted_for: NodeId,
    need_write_mdata: bool,
    latest_known_commit: u64,
    log_file: File,
}

const MAGIC_NUMBER: [u8; 4] = [38, 177, 149, 224];

impl Log {
    pub fn new(path: &str) -> Result<Log> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let metadata = file.metadata()?;
        let mut log = Log {
            records: Vec::new(),
            map: HashMap::new(),
            latest_index: 0,
            latest_commit: 0,
            latest_known_commit: 0,
            term: 0,
            voted_for: 0,
            need_write_mdata: false,
            log_file: file,
        };
        if metadata.len() == 0 {
            log.new_file()?;
        } else {
            log.load_file()?;
        }
        Ok(log)
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

    pub fn commit_up_to(&mut self, idx: u64) -> Result<()> {
        if idx > self.latest_commit {
            self.latest_known_commit = max(idx, self.latest_known_commit);
            self.log_file.seek(SeekFrom::End(0))?;
            for i in self.latest_commit + 1..=idx {
                {
                    let record = &mut self.records[(i - 1) as usize];
                    self.map.insert(record.key.clone(), record.val.clone());
                    if let Some(resp_sender) = &record.resp_sender {
                        let _ = resp_sender.try_send(CltMsgResp::Ok);
                        record.resp_sender = None;
                    }
                }
                self.file_append_record(i)?;
                self.latest_commit = i;
            }
            while let Err(_) = self.log_file.sync_data() {}
        }
        Ok(())
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn set_term(&mut self, term: u64, node_id: NodeId) {
        if self.term < term {
            self.term = term;
            self.voted_for = node_id;
            self.need_write_mdata = true;
        }
    }

    pub fn increment_term(&mut self) {
        self.term += 1;
        self.voted_for = 0;
        self.need_write_mdata = true;
    }

    // a very simple append only log file, no versioning, no checksum etc.
    fn new_file(&mut self) -> Result<()> {
        let t = [0; 12];
        self.log_file.write(&t)?;
        self.log_file.flush()?;
        self.log_file.sync_data()?;
        Ok(())
    }

    fn load_file(&mut self) -> Result<()> {
        let mut hdr = [0; 12];
        self.log_file.read_exact(&mut hdr)?;
        self.term = u64::from_be_bytes(hdr[0..8].try_into().unwrap());
        self.voted_for = u32::from_be_bytes(hdr[8..12].try_into().unwrap());

        let mut reader = BufReader::new(&self.log_file);
        let mut buf = Vec::with_capacity(4096);
        self.latest_index = 0;
        loop {
            let mut hdr = [0; 8];
            let mut eof = false;

            let r = reader.read_exact(&mut hdr);
            if let Err(e) = r {
                if e.kind() == ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(SvrErr::Io(e));
                }
            }
            loop {
                if &hdr[0..4] == &MAGIC_NUMBER {
                    break;
                }
                hdr.copy_within(1..8, 0);
                let r = reader.read_exact(&mut hdr[7..8]);
                if let Err(e) = r {
                    if e.kind() == ErrorKind::UnexpectedEof {
                        eof = true;
                        break;
                    } else {
                        return Err(SvrErr::Io(e));
                    }
                }
            }
            if eof {
                break;
            }

            let len = u32::from_be_bytes(hdr[4..8].try_into().unwrap()) as usize;
            if buf.len() < len {
                buf.resize(len, 0);
            }
            reader.read_exact(&mut buf[0..len])?;

            let str = String::from_utf8(buf.clone())?;
            let record: Record = serde_json::from_str(str.as_str())?;
            if record.index == self.latest_index {
                // dup write -> skip
                continue;
            } else if record.index != self.latest_index + 1 {
                return Err(SvrErr::Other(OtherError::new("parsing error")));
            }
            self.latest_index += 1;
            self.map.insert(record.key.clone(), record.val.clone());
            self.records.push(record);
        }
        drop(reader);

        self.latest_commit = self.latest_index;
        self.latest_known_commit = self.latest_index;
        let len = self.log_file.seek(SeekFrom::Current(0))?;
        self.log_file.set_len(len)?;
        Ok(())
    }

    fn file_append_record(&mut self, i: u64) -> Result<()> {
        let record = &mut self.records[(i - 1) as usize];
        let b = serde_json::to_vec(record)?;
        let mut hdr = [0; 8];
        &mut hdr[0..4].copy_from_slice(&MAGIC_NUMBER);
        &mut hdr[4..8].copy_from_slice(&(b.len() as u32).to_be_bytes());
        self.log_file.write_all(&hdr)?;
        self.log_file.write_all(b.as_slice())?;
        Ok(())
    }

    pub fn file_flush_metadata_if_changed(&mut self) -> Result<()> {
        if !self.need_write_mdata {
            return Ok(());
        }
        self.log_file.seek(SeekFrom::Start(0))?;
        let mut hdr = [0; 12];
        &mut hdr[0..8].copy_from_slice(&self.term.to_be_bytes());
        &mut hdr[8..12].copy_from_slice(&self.voted_for.to_be_bytes());
        self.log_file.write_all(&hdr)?;
        while let Err(_) = self.log_file.flush() {}
        while let Err(_) = self.log_file.sync_data() {}
        self.need_write_mdata = false;
        Ok(())
    }

    pub fn file_flush_records_if_err(&mut self) -> Result<()> {
        self.commit_up_to(self.latest_known_commit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const TEST_LOG_FILE: &str = "F:/test_raft_log.txt";

    #[test]
    fn test_log_new() -> Result<()> {
        std::fs::remove_file(TEST_LOG_FILE)?;
        let log = Log::new(TEST_LOG_FILE)?;
        assert_eq!(log.latest_index, 0);
        assert_eq!(log.latest_commit, 0);
        Ok(())
    }

    fn _append_log(log: &mut Log, key: &str, val: &str, term: u64) {
        log.insert_new(
            Arc::new(key.to_string()),
            Arc::new(val.to_string()),
            term,
            None,
        );
    }

    #[test]
    fn test_log_write_and_read() -> Result<()> {
        std::fs::remove_file(TEST_LOG_FILE)?;
        {
            let mut log = Log::new(TEST_LOG_FILE)?;
            log.set_term(1, 123);
            log.file_update_metadata();
            _append_log(&mut log, "key1", "val1", 1);
            _append_log(&mut log, "key2", "val2", 2);
            _append_log(&mut log, "key3", "val3", 3);
            log.commit_up_to(3);
        }

        let mut log = Log::new(TEST_LOG_FILE)?;
        assert_eq!(log.term, 1);
        assert_eq!(log.voted_for, 123);
        assert_eq!(log.latest_index, 3);
        assert_eq!(log.latest_commit, 3);
        let record = log.get_idx(1).unwrap();
        assert_eq!(record.key.as_str(), "key1");
        assert_eq!(record.val.as_str(), "val1");
        assert_eq!(record.term, 1);
        let record = log.get_idx(2).unwrap();
        assert_eq!(record.key.as_str(), "key2");
        assert_eq!(record.val.as_str(), "val2");
        assert_eq!(record.term, 2);
        let record = log.get_idx(3).unwrap();
        assert_eq!(record.key.as_str(), "key3");
        assert_eq!(record.val.as_str(), "val3");
        assert_eq!(record.term, 3);
        Ok(())
    }
}
