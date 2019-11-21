use std::io::{BufReader, BufWriter, SeekFrom, Seek, Read, Write};
use std::fs::File;
use std::marker::PhantomData;
use std::ops::Range;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use crate::error::Result;

/// flush in background when buffer nearly full
/// and continue writing in a another buffer.
pub struct WalLog<T, S>
where T: Serialize+DeserializeOwned, S: Seek+Read+Write {
    stor: S,
    latest_seq: u64,
    _t: PhantomData<T>,
}

impl<T,S> WalLog<T,S>
where T: Serialize+DeserializeOwned, S: Seek+Read+Write {
    pub fn new(s: S, latest_seq: u64) -> Self {
        Self {
            stor: s,
            latest_seq: 0,
            _t: PhantomData{},
        }
    }

    pub fn append(&mut self, cmd: &T) -> Result<u64> {
        let offset = self.stor.seek(SeekFrom::End(0))?;
        write_wal_entry(&mut self.stor, cmd)?;
        Ok(offset)
    }

    pub fn read(&mut self, offset: u64) -> Result<T> {
        self.stor.seek(SeekFrom::Start(offset))?;
        Ok(read_wal_entry(&mut self.stor)?)
    }

    pub fn iter(&mut self) -> WalIterator<T, S> {
        WalIterator::new(&mut self.stor).unwrap()
    }

    pub fn truncate<Idx>(&mut self, rang: Range<Idx>) -> Result<()> {
        unimplemented!()
    }
}


#[derive(Deserialize, Serialize)]
struct Location {
    fid: u16,
    offset: u32,
}

#[derive(Deserialize, Serialize)]
struct OnDiskIndex {
    key: String,
    loc: Location,
}

pub struct WalIterator<'a, T, S> where T: DeserializeOwned, S: Read+Seek {
    reader: &'a mut S,
    _t: PhantomData<T>,
}

impl<'a, T, S> Iterator for WalIterator<'a, T, S>
where T: DeserializeOwned, S: Read+Seek {
    type Item = (u64, T);

    fn next(&mut self) -> Option<Self::Item> {
        self._next().ok()
    }
}

impl<'a, T, S> WalIterator<'a, T, S>
where T: DeserializeOwned, S:Seek+Read {
    fn new(reader: &'a mut S) -> Result<Self> {
        // let mut reader = BufReader::new(s);
        reader.seek(SeekFrom::Start(0))?;
        Ok(
            Self { reader, _t: PhantomData{} }
        )
    }
    fn _next(&mut self) -> Result<(<WalIterator<'a, T, S> as Iterator>::Item)> {
        Ok(
            (self.reader.seek(SeekFrom::Current(0))?,
             read_wal_entry(self.reader)?)
        )
    }
}

fn write_wal_entry<T>(mut writer: impl Write, data: T) -> Result<()>
where T: Serialize {
    let data = serde_json::to_vec(&data)?;
    let data_len = data.len() as u32;
    let data_len_bytes = data_len.to_be_bytes();
    writer.write_all(&data_len_bytes)?;
    writer.write_all(&data)?;
    //writer.flush()?;
    Ok(())
}

fn read_wal_entry<T>(mut reader: &mut impl Read) -> Result<T>
where T: DeserializeOwned {
    //let offset = self.reader.seek(SeekFrom::Current(0))?;
    let mut count_bytes = [0u8; 4];
    reader.read_exact(&mut count_bytes)?;
    let data_bytes_count = u32::from_be_bytes(count_bytes);
    let mut buf = vec![0u8; data_bytes_count as usize];
    reader.read_exact(&mut buf)?;
    Ok(serde_json::from_slice(&buf)?)
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::OnDiskCommand;

    #[test]
    fn test_wal_iter() {
        let tmpdir = tempfile::tempdir().unwrap();
        let mut kvs = crate::KvStore::new_from(&tmpdir).unwrap();
        for i in 0..1000 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            kvs.set(key, value);
        }
        std::mem::drop(kvs);

        let mut fd = File::open(tmpdir.into_path().join("wal.log")).unwrap();
        let mut wi: WalIterator<OnDiskCommand, File> = WalIterator::new(&mut fd).unwrap();
        let kvvec:Vec<_> = wi.collect();
        for (idx, (_, cmd)) in kvvec.iter().enumerate() {
            let key = format!("key{}", idx);
            let value = format!("value{}", idx);
            assert_eq!(key, cmd.key);
            if let crate::OnDiskValue::Content(content) = &cmd.value {
                assert_eq!(value, *content);
            } else {
                panic!("assert fail");
            }
        }
    }
}
