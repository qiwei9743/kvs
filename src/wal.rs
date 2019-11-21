use std::io::{SeekFrom, Seek, Read, Write};
use std::fs::File;
use std::marker::PhantomData;
//use std::ops::Range;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use crate::error::Result;

/// flush in background when buffer nearly full
/// and continue writing in a another buffer.
pub struct WalLog<T>
where T: Serialize+DeserializeOwned {
    pub fd: File,
    _t: PhantomData<T>,
}

impl<T> WalLog<T>
where T: Serialize+DeserializeOwned {
    pub fn new(fd: File) -> Self {
        Self {
            fd,
            _t: PhantomData{},
        }
    }

    pub fn append(&self, mut writer: impl Seek+Write, cmd: &T) -> Result<u64> {
        let offset = writer.seek(SeekFrom::End(0))?;
        write_wal_entry(writer, cmd)?;
        Ok(offset)
    }

    pub fn read(&self, mut reader: impl Read+Seek,offset: u64) -> Result<T> {
        reader.seek(SeekFrom::Start(offset))?;
        Ok(read_wal_entry(reader)?)
    }

    pub fn iter<S: Read+Seek>(reader: &mut S) -> WalIterator<T, S> {
        WalIterator::new(reader).unwrap()
    }

    // pub fn truncate<Idx>(&mut self, rang: Range<Idx>) -> Result<()> {
    //     unimplemented!()
    // }
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
    fn _next(&mut self) -> Result<(<WalIterator<T, S> as Iterator>::Item)> {
        Ok(
            (self.reader.seek(SeekFrom::Current(0))?,
             read_wal_entry(&mut self.reader)?)
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

fn read_wal_entry<T>(mut reader: impl Read) -> Result<T>
where T: DeserializeOwned {
    let mut count_bytes = [0u8; 4];
    reader.read_exact(&mut count_bytes)?;
    let data_bytes_count = u32::from_be_bytes(count_bytes);
    let mut buf = vec![0u8; data_bytes_count as usize];
    reader.read_exact(&mut buf)?;
    Ok(serde_json::from_slice(&buf)?)
}


#[cfg(test)]
mod tests {
    use std::io::BufReader;
    use super::*;
    use crate::OnDiskCommand;

    #[test]
    fn test_wal_iter() {
        let tmpdir = tempfile::tempdir().unwrap();
        let mut kvs = crate::KvStore::new_from(&tmpdir).unwrap();
        for i in 0..1000 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            kvs.set(key, value).unwrap();
        }
        std::mem::drop(kvs);

        let fd = File::open(tmpdir.into_path().join("wal.log")).unwrap();
        let mut reader = BufReader::new(&fd);
        let wi: WalIterator<OnDiskCommand, BufReader<&File>> = WalIterator::new(
            &mut reader).unwrap();
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
