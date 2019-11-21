use std::io::{BufReader, BufWriter, SeekFrom, Seek, Read, Write};
use std::fs::File;
use std::marker::PhantomData;
use std::ops::Range;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use crate::error::Result;

/// flush in background when buffer nearly full
/// and continue writing in a another buffer.
struct WalLog<T, S>
where T: Serialize+DeserializeOwned, S: Seek+Read+Write {

    stor: S,
    latest_seq: u64,
    _t: PhantomData<T>,
}

impl<T,S> WalLog<T,S>
where T: Serialize+DeserializeOwned, S: Seek+Read+Write {

    pub fn new() -> Self {
        unimplemented!()
    }

    pub fn append(&self, cmd: &T) -> Result<u64> {
        unimplemented!()
    }

    pub fn read(&self, offset: u64) -> Result<T> {
        unimplemented!()
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

struct WalIterator<'a, T> where T: DeserializeOwned {
    reader: BufReader<&'a File>,
    _t: PhantomData<T>,
}

impl<'a, T> Iterator for WalIterator<'a, T> where T: DeserializeOwned {
    type Item = (u64, T);

    fn next(&mut self) -> Option<Self::Item> {
        self._next().ok()
    }
}

impl<'a, T> WalIterator<'a, T> where T: DeserializeOwned {
    fn new(fd: &'a File) -> Result<Self> {
        let mut reader = BufReader::new(fd);
        reader.seek(SeekFrom::Start(0))?;
        Ok(
            Self { reader, _t: PhantomData{} }
        )
    }
    fn _next(&mut self) -> Result<(<WalIterator<'a, T> as Iterator>::Item)> {
        Ok(
            (self.reader.seek(SeekFrom::Current(0))?,
             read_wal_entry(&mut self.reader)?)
        )
    }
}

fn read_wal_entry<T>(mut reader: impl Read) -> Result<T>
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
        let mut wi: WalIterator<OnDiskCommand> = WalIterator::new(&fd).unwrap();
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
