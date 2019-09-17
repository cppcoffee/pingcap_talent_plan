use std::path::Path;

use sled::Db;

use super::KvsEngine;
use crate::error::{KvsError, Result};

#[derive(Clone)]
pub struct SledKvsEngine {
    db: Db,
}

impl SledKvsEngine {
    pub fn open(path: &Path) -> Result<Self> {
        let t = Db::open(&path)?;
        Ok(SledKvsEngine { db: t })
    }
}

impl KvsEngine for SledKvsEngine {
    // Set the value of a string key to a string.
    // Return an error if the value is not written successfully.
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.into_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    // Get the string value of a string key. If the key does not exist, return None.
    // Return an error if the value is not read successfully.
    fn get(&self, key: String) -> Result<Option<String>> {
        match self.db.get(key)? {
            Some(value) => {
                let ans = String::from_utf8(value.as_ref().to_vec())?;
                Ok(Some(ans))
            }
            None => Ok(None),
        }
    }

    // Remove a given string key.
    // Return an error if the key does not exit or value is not read successfully.
    fn remove(&self, key: String) -> Result<()> {
        match self.db.remove(key)? {
            Some(_) => {
                self.db.flush()?;
                Ok(())
            }
            None => Err(KvsError::KeyNotFound),
        }
    }
}
