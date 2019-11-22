#![deny(missing_docs)]

//! This project is only for pingcap project.
//! Have fun.

use std::fs::OpenOptions;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Seek, SeekFrom};
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
struct OnDiskPointer {
    fid: u32,
    offset: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum OnDiskValue {
    DeletedKey(u64),
    Pointer(u64, OnDiskPointer),
    Content(u64, String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum OnDiskCompaction {
    Start,
    Commit,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum OnDiskMeta {
    CmdIndex(OnDiskCommand),
    Compaction(OnDiskCompaction)
}

#[derive(Serialize, Deserialize, Debug)]
enum OnDiskIndex {
    ValueIndex { key: String, fid: u8, offset: u32},
    FreeIndex { fid: u8, start: u32, length: u32},
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct OnDiskCommand {
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
    wal_meta_writer: BufWriter<File>,

    latest_seq: u64,
    location_finder: HashMap<String, Value>
}

impl KvStore {
    /// create a new object for KvStore within a given directory.
    pub fn new_from<P: AsRef<Path>>(p: P) -> Result<Self> {
        let meta_fd = OpenOptions::new().read(true).write(true).create(true)
            .open(p.as_ref().join("meta.wal"))?;

        let wal_meta = wal::WalLog::<OnDiskMeta>::new(meta_fd.try_clone()?);
        let wal_meta_writer = BufWriter::new(meta_fd);
        Ok(
            Self {
                wal_cmd: wal::WalLog::<OnDiskCommand>::new(
                    OpenOptions::new().read(true).write(true).create(true)
                        .open(p.as_ref().join("cmd.wal"))?),
                wal_meta,
                wal_meta_writer,
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
        let wal_meta = wal::WalLog::<OnDiskMeta>::new(meta_fd.try_clone()?);
        let wal_meta_writer = BufWriter::new(meta_fd);
        let s = Self {
            wal_cmd: wal::WalLog::<OnDiskCommand>::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open("cmd.wal")?),
            wal_meta,
            wal_meta_writer,
            latest_seq: 0,
            location_finder: HashMap::new(),
        };
        Ok(s)
    }

    /// recover from a wal log.
    pub fn from_wal<P: AsRef<Path>>(p: P) -> Result<Self> {
        let wal_cmd_path = p.as_ref().join("cmd.wal");
        let wal_meta_path = p.as_ref().join("meta.wal");

        let wal_cmd_fd = OpenOptions::new().read(true).write(true).open(&wal_cmd_path)?;
        let wal_meta_fd = OpenOptions::new().read(true).write(true).open(&wal_meta_path)?;

        let mut wal_cmd_reader = BufReader::new(wal_cmd_fd.try_clone()?);
        let mut wal_meta_reader = BufReader::new(wal_meta_fd.try_clone()?);

        let wal_cmd = wal::WalLog::<OnDiskCommand>::new(wal_cmd_fd);
        let wal_meta = wal::WalLog::<OnDiskMeta>::new(wal_meta_fd.try_clone()?);
        let mut wal_meta_writer = BufWriter::new(wal_meta_fd);

        let mut location_finder = HashMap::new();

        let mut latest_cmd_pos = 0u64;
        let mut latest_seq = 0u64;
        for (_, meta) in wal::WalLog::<OnDiskMeta>::iter(&mut wal_meta_reader) {
            if let OnDiskMeta::CmdIndex(OnDiskCommand{key, value}) = meta {
                    let (seq, pos) = Self::fill_from_meta(&mut location_finder, key, value);
                latest_seq = std::cmp::max(latest_seq, seq);
                latest_cmd_pos = std::cmp::max(latest_cmd_pos, pos.unwrap_or(0));
            }
        }

        wal_cmd_reader.seek(SeekFrom::Start(latest_cmd_pos))?;
        wal_meta_writer.seek(SeekFrom::End(0))?;

        let mut kvs = Self {
            wal_cmd,
            wal_meta,
            wal_meta_writer,
            latest_seq: 0,
            location_finder: HashMap::new(),
        };

        for (offset, OnDiskCommand{key, value}) in
            wal::WalLog::<OnDiskCommand>::iter(&mut wal_cmd_reader).skip(1) {
                kvs.fill_from_cmd(&mut location_finder, key, value, offset)?;

        }
        latest_seq = std::cmp::max(
            location_finder.iter().map(|(_k, v)| v.0 ).max().unwrap_or(0),
            latest_seq);

        let location_finder = location_finder.into_iter().filter(|(_k, v)| {
            match v.1 {
                Value::Location(_) => true ,
                _ => false
            }
        }).map(|(k, v)|{
            (k, v.1)
        }).collect();

        kvs.latest_seq = latest_seq;
        kvs.wal_meta_writer.flush()?;
        kvs.location_finder = location_finder;
        Ok (kvs)
    }

    fn fill_from_meta(map: &mut std::collections::HashMap<String, (u64, Value)>,
            key: String, value: OnDiskValue) -> (u64, Option<u64>) {

        match value {
            OnDiskValue::DeletedKey(sequence) => {
                map.entry(key)
                    .and_modify(| e: &mut(u64, Value) |{
                        if e.0 < sequence {
                            *e = (sequence, Value::Deleted);
                        }
                    })
                    .or_insert((sequence, Value::Deleted));
                (sequence, None)
            },
            OnDiskValue::Pointer(sequence, OnDiskPointer{offset, ..}) => {
                map.entry(key)
                    .and_modify(| e: &mut(u64, Value) |{
                        if e.0 < sequence {
                            *e = (sequence, Value::Location(offset));
                        }
                    })
                    .or_insert((sequence, Value::Location(offset)));
                (sequence, Some(offset))
            },
            OnDiskValue::Content(sequence, value) => {
                // key may very short. So keep it in memory
                map.entry(key)
                    .and_modify(| e: &mut(u64, Value)|{
                        if e.0 < sequence {
                            *e = (sequence, Value::Content(value.clone()));
                        }
                    })
                    .or_insert((sequence, Value::Content(value)));
                (sequence, None)
            }
        }
    }

    fn fill_from_cmd(&mut self, map: &mut std::collections::HashMap<String, (u64, Value)>,
                     key: String, value: OnDiskValue, offset: u64) -> Result<()> {

        match value {
            OnDiskValue::DeletedKey(sequence) => {
                map.entry(key.clone())
                    .and_modify(| e: &mut(u64, Value) |{
                        if e.0 < sequence {
                            *e = (sequence, Value::Deleted);
                        }
                    })
                    .or_insert((sequence, Value::Deleted));

                let pl = OnDiskMeta::CmdIndex (OnDiskCommand {
                    key,
                    value: OnDiskValue::DeletedKey(self.latest_seq),
                });
                self.append_meta_wal(&pl)?;
            },
            OnDiskValue::Pointer(sequence, OnDiskPointer{offset: loffset, ..}) => {
                map.entry(key.clone())
                    .and_modify(| e: &mut(u64, Value) |{
                        if e.0 < sequence {
                            *e = (sequence, Value::Location(loffset));
                        }
                    })
                    .or_insert((sequence, Value::Location(loffset)));

                let pl = OnDiskMeta::CmdIndex(
                    OnDiskCommand {
                        key,
                        value: OnDiskValue::Pointer(
                            sequence,
                            OnDiskPointer {
                                fid: 0, offset: loffset
                            }
                        )
                    }
                );
                self.append_meta_wal(&pl)?;
            },
            OnDiskValue::Content(sequence, ..) => {
                // key in cmd.wal may be long. To save memory,
                // only keep lcoation
                map.entry(key.clone())
                    .and_modify(| e: &mut(u64, Value)|{
                        if e.0 < sequence {
                            *e = (sequence, Value::Location(offset));
                        }
                    })
                    .or_insert((sequence, Value::Location(offset)));
                let pl = OnDiskMeta::CmdIndex(
                    OnDiskCommand {
                        key,
                        value: OnDiskValue::Pointer(
                            sequence,
                            OnDiskPointer {
                                fid: 0, offset
                            }
                        )
                    }
                );
                self.append_meta_wal(&pl)?;
            }
        }
        Ok(())
    }

    /// get a value with a given key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(value) = self.location_finder.get(&key) {
            match value {
                Value::Location(offset) => {
                    let od_cmd = self.read_cmd_wal(*offset)?;
                    match od_cmd.value {
                        OnDiskValue::Content(_sequence, content) => {
                            Ok(Some(content))
                        },
                        OnDiskValue::DeletedKey(_sequence) => {
                            Ok(None)
                        }
                        OnDiskValue::Pointer(_sequence, OnDiskPointer{..}) => {
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
            key,
            value: OnDiskValue::Content(self.latest_seq, value)
        };

        let offset = self.append_cmd_wal(&cmd)?;

        let pl = OnDiskMeta::CmdIndex(
            OnDiskCommand {
                key: cmd.key,
                value: OnDiskValue::Pointer(
                    self.latest_seq,
                    OnDiskPointer{
                        fid: 0, offset
                    }
                ),
            },
        );

        self.append_meta_wal(&pl)?;

        let key = match pl {
            OnDiskMeta::CmdIndex ( OnDiskCommand{key, ..}) => key,
            _ => panic!("unable to be here"),
        };
        self.location_finder.insert(key, Value::Location(offset));
        Ok(())
    }

    /// remove a key/value pairs by a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.location_finder.get(&key).is_some() {
            self.latest_seq += 1;

            let cmd = OnDiskCommand {
                key,
                value: OnDiskValue::DeletedKey(self.latest_seq),
            };
            self.append_cmd_wal(&cmd)?;

            let pl = OnDiskMeta::CmdIndex (OnDiskCommand{
                key: cmd.key,
                value: OnDiskValue::DeletedKey(self.latest_seq),
            });
            self.append_meta_wal(&pl)?;

            let key = match pl {
                OnDiskMeta::CmdIndex (OnDiskCommand{
                    key, ..}) => key,
                _ => panic!("unable to here"),
            };

            self.location_finder.remove(&key);
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

    fn append_meta_wal(&mut self, meta: &OnDiskMeta) -> Result<u64> {
        let offset = self.wal_meta_writer.seek(SeekFrom::End(0))?;
        self.wal_meta.append(&mut self.wal_meta_writer, meta)?;
        Ok(offset)
    }

    /// compaction reduntant data
    pub fn compact(&mut self) -> Result<u64> {
        unimplemented!()
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
