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

#[derive(Serialize, Deserialize, Debug)]
enum Value {
    Location(u64),
    Content(String),
}

#[derive(Serialize, Deserialize, Debug)]
struct Command {
    sequence: u64,
    key: String,
    value: Value,
}

#[derive(Serialize, Deserialize, Debug)]
enum OnDiskValue {
    Deleted,
    Content(String),
}

#[derive(Serialize, Deserialize, Debug)]
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
    wal: File,
    latest_seq: u64,
    location_finder: HashMap<String, Value>
}

type Result<T> = std::result::Result<T, error::KvsError>;

impl KvStore {
    /// create a new object for KvStore within a given directory.
    pub fn new_from<P: AsRef<Path>>(mut p: P) -> Result<Self> {
        Ok(
            Self {
                wal: OpenOptions::new().read(true).write(true).create(true)
                    .open(p.as_ref().join("wal.log")).unwrap(),
                latest_seq: 0,
                location_finder: HashMap::new(),
            }
        )
    }

    /// create a new object for KvStore with wal.log
    pub fn new() -> Self {
        Self {
            //wal: File::create("wal.log").unwrap(),
            wal: OpenOptions::new().read(true).write(true).create(true)
                .open("wal.log").unwrap(),
            latest_seq: 0,
            location_finder: HashMap::new(),
        }
    }

    /// recover from a wal log.
    pub fn from_wal<P: AsRef<Path>>(p: P) -> Result<Self> {
        let wal_path = p.as_ref().join("wal.log");
        println!("from_wal {:?}", wal_path);
        let mut wal = OpenOptions::new().read(true).write(true).open(&wal_path).unwrap();
        let mut location_finder = HashMap::new();
        let meta = std::fs::metadata(&wal_path)?;
        let wal_size = meta.len();
        let mut latest_seq = 0;

        loop {
            let offset = wal.seek(SeekFrom::Current(0))?;
            if offset == wal_size {
                break
            }

            let mut bs = [0u8; 4];
            wal.read(&mut bs)?;
            let bytes_count = u32::from_be_bytes(bs);
            let mut buff = vec![0u8; bytes_count as usize];
            wal.read_exact(&mut buff)?;

            let cmd: OnDiskCommand = serde_json::from_slice(&buff)?;
            match cmd.value {
                OnDiskValue::Content(_) => {
                    location_finder.insert(cmd.key, Value::Location(offset));
                },
                OnDiskValue::Deleted => {
                    location_finder.remove(&cmd.key);
                }
            };
            latest_seq = cmd.sequence;
        }

        Ok(
            Self {
                wal,
                latest_seq,
                location_finder,
            }
        )
    }

    /// get a value with a given key.
    pub fn get(&self, key: String) -> Result<Option<String>> {
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
            key: key,
            value: OnDiskValue::Content(value)};

        let offset = self.append_wal(&cmd)?;
        self.location_finder.insert(cmd.key, Value::Location(offset));
        Ok(())
    }

    /// remove a key/value pairs by a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(_) = self.location_finder.get(&key) {
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
        let mut writer = BufWriter::new(&mut self.wal);
        let offset = writer.seek(SeekFrom::End(0))?;
        let data = serde_json::to_vec(cmd)?;
        let data_len = data.len() as u32;
        let data_len_bytes = data_len.to_be_bytes();
        writer.write(&data_len_bytes)?;
        writer.write(&data)?;
        writer.flush()?;
        Ok(offset)
    }

    fn read_wal(&self, offset: u64) -> Result<OnDiskCommand> {
        let mut reader = BufReader::new(&self.wal);
        reader.seek(SeekFrom::Start(offset))?;
        let mut count_bytes = [0u8; 4];
        reader.read(&mut count_bytes)?;
        let data_bytes_count = u32::from_be_bytes(count_bytes);
        let mut buf = vec![0u8; data_bytes_count as usize];
        reader.read_exact(&mut buf)?;
        let cmd = serde_json::from_slice(&buf)?;
        Ok(cmd)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_set_two_key() {
        let mut kvs = KvStore::new_from(tempfile::tempdir().unwrap()).unwrap();
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
