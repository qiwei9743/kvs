#![deny(missing_docs)]

//! This project is only for pingcap project.
//! Have fun.

use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::convert::AsRef;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json;

mod error;
use error::Result;

mod wal;

#[derive(Serialize, Deserialize, Debug)]
enum Value {
    Location(u64),
    Content(String),
    Deleted,
}

#[derive(Serialize, Deserialize, Debug)]
struct Command {
    sequence: u64,
    key: String,
    value: Value,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum OnDiskValue {
    Deleted,
    Content(String),
}

#[derive(Serialize, Deserialize, Debug)]
enum OnDiskIndex {
    ValueIndex { key: String, fid: u8, offset: u32},
    FreeIndex { fid: u8, start: u32, length: u32},
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct OnDiskCommand {
    sequence: u64,
    key: String,
    value: OnDiskValue,
}

#[derive(Serialize, Deserialize, Debug)]
struct HintBlock {
    key: String,
}

/// core data structure for kvs store
pub struct KvStore {
    wal_log: wal::WalLog<OnDiskCommand, File>,
    latest_seq: u64,
    location_finder: HashMap<String, Value>
}

impl KvStore {
    /// create a new object for KvStore within a given directory.
    pub fn new_from<P: AsRef<Path>>(p: P) -> Result<Self> {
        Ok(
            Self {
                wal_log: wal::WalLog::<OnDiskCommand, File>::new(
                    OpenOptions::new().read(true).write(true).create(true)
                        .open(p.as_ref().join("wal.log"))?, 0),
                latest_seq: 0,
                location_finder: HashMap::new(),
            }
        )
    }

    /// create a new object for KvStore with wal.log
    pub fn new() -> Result<Self> {
        Ok(
            Self {
                wal_log: wal::WalLog::<OnDiskCommand, File>::new(
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open("data.wal")?, 0),
                latest_seq: 0,
                location_finder: HashMap::new(),
            }
        )
    }

    /// recover from a wal log.
    pub fn from_wal<P: AsRef<Path>>(p: P) -> Result<Self> {
        let wal_path = p.as_ref().join("wal.log");
        println!("from_wal {:?}", wal_path);
        let wal_fd = OpenOptions::new().read(true).write(true).open(&wal_path).unwrap();
        let mut wal_log = wal::WalLog::<OnDiskCommand, File>::new(wal_fd, 0);

        // let mut reader = BufReader::new(&wal);
        let mut location_finder = HashMap::new();
        //let meta = std::fs::metadata(&wal_path)?;
        //let wal_size = meta.len();
        let mut latest_seq = 0;

        for (offset, OnDiskCommand{sequence, key, value}) in wal_log.iter() {
            match value {
                OnDiskValue::Content(_) => {
                    location_finder.entry(key)
                        .and_modify(|e: &mut (u64, Value)| {
                            if e.0 < sequence {
                                *e = (sequence, Value::Location(offset));
                            }
                        })
                        .or_insert((sequence, Value::Location(offset)));

                    // location_finder.insert(
                    //     cmd.key, (cmd.sequence, Value::Location(offset)));
                },
                OnDiskValue::Deleted => {
                    location_finder.entry(key)
                        .and_modify(|e| {
                            if e.0 < sequence {
                                *e = (sequence, Value::Deleted)
                            }
                        })
                        .or_insert((sequence, Value::Deleted));
                }
            };
        }
        let latest_seq = location_finder.iter().map(|(k, v)| v.0 ).max().unwrap_or(0);
        let location_finder = location_finder.into_iter().map(|(k, v)|{
            (k, v.1)
        }).collect();

        // loop {
        //     let offset = reader.seek(SeekFrom::Current(0))?;
        //     if offset == wal_size {
        //         break
        //     }

        //     let mut bs = [0u8; 4];
        //     reader.read_exact(&mut bs)?;
        //     let bytes_count = u32::from_be_bytes(bs);
        //     let mut buff = vec![0u8; bytes_count as usize];
        //     reader.read_exact(&mut buff)?;

        //     let cmd: OnDiskCommand = serde_json::from_slice(&buff)?;
        //     match cmd.value {
        //         OnDiskValue::Content(_) => {
        //             location_finder.insert(cmd.key, Value::Location(offset));
        //         },
        //         OnDiskValue::Deleted => {
        //             location_finder.remove(&cmd.key);
        //         }
        //     };
        //     latest_seq = cmd.sequence;
        // }

        // // start
        // let max_seq_index = meta_wal.max_seq();
        // let max_seq_data = meta_wal.max_seq();
        // if max_seq_index > max_seq_data {
        //     // recover from index until max_seq_data
        //     // and truncate *index* after @max_seq_data.
        //     // This is is not expected to occur which may result in consistent
        //     // in client side. It's better to sync data wal before sync index.
        //     // assert False
        // } else if max_seq_index < max_seq_data {
        //     // recover from index until max_seq_index.
        //     // then rebuild index log with data between max_seq_index and max_seq_data.
        // }
        // let location_finder = meta_wal.iter().collect<HashMap<_>>();

        Ok(
            Self {
                wal_log,
                latest_seq,
                location_finder,
            }
        )
    }

    /// get a value with a given key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(value) = self.location_finder.get(&key) {
            match value {
                Value::Location(offset) => {
                    let od_cmd = self.read_wal(*offset)?;
                    match od_cmd.value {
                        OnDiskValue::Content(content) => {
                            Ok(Some(content))
                        },
                        OnDiskValue::Deleted => {
                            Ok(None)
                        }
                    }
                },
                Value::Content(content) => {
                    Ok(Some(content.clone()))
                },
                Value::Deleted => {
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    /// set a key/value pairs
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.latest_seq += 1;
        let cmd = OnDiskCommand{
            sequence: self.latest_seq,
            key,
            value: OnDiskValue::Content(value)};

        let offset = self.append_wal(&cmd)?;
        // let offset = self.data_wal.append(&cmd)?;
        // self.meta_wal.append(key_meta)?; // need sync point
        self.location_finder.insert(cmd.key, Value::Location(offset));
        Ok(())
    }

    /// remove a key/value pairs by a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.location_finder.get(&key).is_some() {
            self.latest_seq += 1;

            let cmd = OnDiskCommand{
                sequence: self.latest_seq,
                key,
                value: OnDiskValue::Deleted,
            };
            self.append_wal(&cmd)?;
            self.location_finder.remove(&cmd.key);
            Ok(())
        } else {
            Err(error::KvsError::NotFound)
        }
    }

    fn append_wal(&mut self, cmd: &OnDiskCommand) -> Result<u64> {
        Ok(self.wal_log.append(cmd)?)
    }

    // FIXME: get rid of &mut since it's a read operation.
    fn read_wal(&mut self, offset: u64) -> Result<OnDiskCommand> {
        Ok(self.wal_log.read(offset)?)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_set_two_key() {
        //let mut kvs = KvStore::new_from(tempfile::tempdir().unwrap()).unwrap();
        let mut kvs = KvStore::new().unwrap();
        kvs.set("key1".into(), "value2".into()).unwrap();
        kvs.set("key2".into(), "value2".into()).unwrap();
    }

    #[test]
    fn test_set_and_recover() {
        let tmpdir = tempfile::tempdir().unwrap();
        let mut kvs = KvStore::new_from(&tmpdir).unwrap();
        kvs.set("key1".into(), "value1".into()).unwrap();
        kvs.set("key2".into(), "value2".into()).unwrap();
        std::mem::drop(kvs);
        kvs = KvStore::from_wal(&tmpdir).unwrap();
        assert_eq!(kvs.get("key1".into()).unwrap(), Some(String::from("value1")));
        assert_eq!(kvs.get("key2".into()).unwrap(), Some(String::from("value2")));
    }

    #[test]
    fn test_remove_and_recover() {
        let tmpdir = tempfile::tempdir().unwrap();
        let mut kvs = KvStore::new_from(&tmpdir).unwrap();
        kvs.set("key1".into(), "value1".into()).unwrap();
        kvs.set(String::from("key2"), String::from("value2")).unwrap();
        kvs.remove(String::from("key1")).unwrap();
        std::mem::drop(kvs);
        kvs = KvStore::from_wal(&tmpdir).unwrap();
        assert_eq!(kvs.get(String::from("key1")).unwrap(), None);
    }
}
