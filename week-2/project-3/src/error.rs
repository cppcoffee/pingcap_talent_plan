use failure::Fail;
use std::io;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),

    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),

    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;
