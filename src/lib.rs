#![deny(missing_docs)]

//! This project is only for pingcap project.
//! Have fun.

use std::fs::OpenOptions;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::convert::AsRef;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
enum OnDiskMeta {
    LogPointer {key: String, value: OnDiskValue}
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum OnDiskValue {
    Deleted,
    Pointer{fid: u16, offset: u64},
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
    wal_cmd: wal::WalLog<OnDiskCommand>,
    wal_meta: wal::WalLog<OnDiskMeta>,

    latest_seq: u64,
    location_finder: HashMap<String, Value>
}

impl<'a> KvStore {
    /// create a new object for KvStore within a given directory.
    pub fn new_from<P: AsRef<Path>>(p: P) -> Result<Self> {
        let meta_fd = OpenOptions::new().read(true).write(true).create(true)
            .open(p.as_ref().join("meta.wal"))?;
        //let wal_meta = BufWriter::new(meta_fd);
        Ok(
            Self {
                wal_cmd: wal::WalLog::<OnDiskCommand>::new(
                    OpenOptions::new().read(true).write(true).create(true)
                        .open(p.as_ref().join("cmd.wal"))?),
                wal_meta: wal::WalLog::<OnDiskMeta>::new(meta_fd),
                latest_seq: 0,
                location_finder: HashMap::new(),
            }
        )
    }

    /// create a new object for KvStore with wal.log
    pub fn new() -> Result<Self> {
        let meta_fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("meta.wal")?;
        //let bufwriter_meta = BufWriter::new(&meta_fd);
        Ok(
            Self {
                wal_cmd: wal::WalLog::<OnDiskCommand>::new(
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open("cmd.wal")?),
                wal_meta: wal::WalLog::<OnDiskMeta>::new(meta_fd),
                latest_seq: 0,
                location_finder: HashMap::new(),
            }
        )
    }

    /// recover from a wal log.
    pub fn from_wal<P: AsRef<Path>>(p: P) -> Result<Self> {
        let wal_cmd_path = p.as_ref().join("cmd.wal");
        let wal_meta_path = p.as_ref().join("meta.wal");

        let wal_cmd_fd = OpenOptions::new().read(true).write(true).open(&wal_cmd_path)?;
        let wal_meta_fd = OpenOptions::new().read(true).write(true).open(&wal_meta_path)?;

        let mut wal_cmd_reader = BufReader::new(&wal_cmd_fd);
        //let mut wal_meta_reader = BufReader::new(&wal_meta_fd);

        let mut location_finder = HashMap::new();

        //Self::recover_index();

        for (offset, OnDiskCommand{sequence, key, value}) in
            wal::WalLog::<OnDiskCommand>::iter(&mut wal_cmd_reader) {

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
                },
                OnDiskValue::Pointer{fid:_, offset:_} => {
                    return Err(error::KvsError::FoundPointerFromDataWal);
                }
            };
        }
        let latest_seq = location_finder.iter().map(|(_k, v)| v.0 ).max().unwrap_or(0);
        let location_finder = location_finder.into_iter().filter(|(_k, v)| {
            match v.1 {
                Value::Location(_) => true ,
                _ => false
            }
        }).map(|(k, v)|{
            (k, v.1)
        }).collect();

        //let bufwriter_meta = BufWriter::new(&wal_meta_fd);
        let wal_cmd = wal::WalLog::<OnDiskCommand>::new(wal_cmd_fd);
        let wal_meta = wal::WalLog::<OnDiskMeta>::new(wal_meta_fd);
        Ok(
            Self {
                wal_cmd,
                wal_meta,
                latest_seq,
                location_finder,
            }
        )
    }

    // fn recover_index(meta_reader: impl Reader+Seek) -> Result<()> {

    //     unimplemented!()
    // }

    /// get a value with a given key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(value) = self.location_finder.get(&key) {
            match value {
                Value::Location(offset) => {
                    let od_cmd = self.read_cmd_wal(*offset)?;
                    match od_cmd.value {
                        OnDiskValue::Content(content) => {
                            Ok(Some(content))
                        },
                        OnDiskValue::Deleted => {
                            Ok(None)
                        }
                        OnDiskValue::Pointer{fid: _, offset: _} => {
                            // should NOT find a pointer from cmd.wal file
                            Err(error::KvsError::FoundPointerFromDataWal)
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
        let cmd = OnDiskCommand {
            sequence: self.latest_seq,
            key: key.clone(),
            value: OnDiskValue::Content(value)
        };

        let offset = self.append_cmd_wal(&cmd)?;

        let pl = OnDiskMeta::LogPointer{
            key,
            value: OnDiskValue::Pointer{fid: 0, offset},
        };

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
            self.append_cmd_wal(&cmd)?;
            self.location_finder.remove(&cmd.key);
            Ok(())
        } else {
            Err(error::KvsError::NotFound)
        }
    }

    fn append_cmd_wal(&mut self, cmd: &OnDiskCommand) -> Result<u64> {
        let mut writer = BufWriter::new(&self.wal_cmd.fd);
        let offset = self.wal_cmd.append(&mut writer, cmd)?;
        writer.flush()?;
        Ok(offset)
    }

    // FIXME: get rid of &mut since it's a read operation.
    fn read_cmd_wal(&self, offset: u64) -> Result<OnDiskCommand> {
        let mut reader = BufReader::new(&self.wal_cmd.fd);
        Ok(self.wal_cmd.read(&mut reader, offset)?)
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
