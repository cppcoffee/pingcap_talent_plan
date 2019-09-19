use failure::Fail;
use std::io;
use std::string::FromUtf8Error;

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

    #[fail(display = "Unknown kv engine type")]
    UnknownEngineType,

    #[fail(display = "{}", _0)]
    StringError(String),

    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8(#[cause] FromUtf8Error),

    #[fail(display = "sled error: {}", _0)]
    Sled(#[cause] sled::Error),

    #[fail(display = "")]
    RayonThreadPool(#[cause] rayon::ThreadPoolBuildError),
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

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> KvsError {
        KvsError::Utf8(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> KvsError {
        KvsError::Sled(err)
    }
}

impl From<rayon::ThreadPoolBuildError> for KvsError {
    fn from(err: rayon::ThreadPoolBuildError) -> KvsError {
        KvsError::RayonThreadPool(err)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;
