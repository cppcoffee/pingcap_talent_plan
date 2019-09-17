// kv storey engines module.
use crate::Result;

pub trait KvsEngine: Clone + Send + 'static {
    // Set the value of a string key to a string.
    // Return an error if the value is not written successfully.
    fn set(&self, key: String, value: String) -> Result<()>;

    // Get the string value of a string key. If the key does not exist, return None.
    // Return an error if the value is not read successfully.
    fn get(&self, key: String) -> Result<Option<String>>;

    // Remove a given string key.
    // Return an error if the key does not exit or value is not read successfully.
    fn remove(&self, key: String) -> Result<()>;
}

mod kvs;
pub use self::kvs::KvStore;
mod sled;
pub use self::sled::SledKvsEngine;
