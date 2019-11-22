use std::convert::From;

#[derive(Debug)]
pub enum KvsError {
    NotFound,
    PartialWritten(usize, usize),
    IoError(std::io::Error),
    SerdeError(serde_json::error::Error),
    FromUtf8Error(std::string::FromUtf8Error),
    FoundPointerFromDataWal,
}

impl From<std::io::Error> for KvsError {
    fn from(err: std::io::Error) -> Self {
        KvsError::IoError(err)
    }
}

impl From<serde_json::error::Error> for KvsError {
    fn from(err: serde_json::error::Error) -> Self {
        KvsError::SerdeError(err)
    }
}

impl From<std::string::FromUtf8Error> for KvsError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        KvsError::FromUtf8Error(err)
    }
}


pub type Result<T> = std::result::Result<T, KvsError>;
