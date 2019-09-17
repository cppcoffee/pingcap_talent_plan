use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicU64, atomic::Ordering, Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use super::KvsEngine;
use crate::error::{KvsError, Result};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;
const KV_LOG_NAME: &'static str = "kv.log";
const KV_SWAP_NAME: &'static str = "kv.tmp";

// KvStore database.
#[derive(Clone)]
pub struct KvStore {
    path: Arc<PathBuf>,
    index: Arc<Mutex<HashMap<String, Value>>>,
    file: Arc<Mutex<File>>,
    uncompacted: Arc<AtomicU64>,
}

impl KvStore {
    pub fn open(path: &Path) -> Result<Self> {
        fs::create_dir_all(&path)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path.clone().join(KV_LOG_NAME))?;

        let mut index = HashMap::new();
        read_commands(&file, &mut index)?;

        Ok(KvStore {
            path: Arc::new(path.into()),
            index: Arc::new(Mutex::new(index)),
            file: Arc::new(Mutex::new(file)),
            uncompacted: Arc::new(AtomicU64::new(0)),
        })
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut fp = self.file.lock().unwrap();
        let len = write_command(
            &mut *fp,
            Command::Set {
                key: key.clone(),
                value: value.clone(),
            },
        )?;

        let mut index = self.index.lock().unwrap();

        if index.insert(key, Value::Inner(value)).is_some() {
            self.uncompacted.fetch_add(len, Ordering::SeqCst);
        }

        if self.uncompacted.load(Ordering::SeqCst) > COMPACTION_THRESHOLD {
            rebuild_log(&mut fp, &mut index, &self.path)?;
            return Ok(());
        }

        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let mut index = self.index.lock().unwrap();

        if let Some(value) = index.get(&key) {
            match value {
                Value::Position { start, length } => {
                    let v = read_value(&mut self.file.lock().unwrap(), *start, *length)?;
                    index.insert(key, Value::Inner(v.clone()));
                    Ok(Some(v))
                }
                Value::Inner(v) => Ok(Some(v.clone())),
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        let mut index = self.index.lock().unwrap();

        if index.contains_key(&key) {
            index.remove(&key);

            write_command(&mut *self.file.lock().unwrap(), Command::Remove { key })?;

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

fn rebuild_log(fp: &mut File, index: &mut HashMap<String, Value>, path: &Path) -> Result<()> {
    let swap_path = path.clone().join(KV_SWAP_NAME);

    let mut writer = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&swap_path)?;

    for (key, val) in index.iter_mut() {
        match val {
            Value::Position { start, length } => {
                let mut buf = vec![0; *length as usize];
                fp.read(&mut buf)?;

                let pos = writer.seek(SeekFrom::Current(0))?;
                writer.write(&buf)?;

                // update in-memory value.
                *start = pos;
            }
            Value::Inner(v) => {
                write_command(
                    &mut writer,
                    Command::Set {
                        key: key.clone(),
                        value: v.clone(),
                    },
                )?;
            }
        }
    }

    mem::replace(&mut *fp, writer);
    fs::rename(&swap_path, &path.clone().join(KV_LOG_NAME))?;

    Ok(())
}

fn read_value(file: &mut File, start: u64, length: u64) -> Result<String> {
    let mut buffer = vec![0; length as usize];

    let pos = file.seek(SeekFrom::Current(0))?;
    file.seek(SeekFrom::Start(start))?;
    file.read(&mut buffer)?;
    file.seek(SeekFrom::Start(pos))?;

    if let Command::Set { value, .. } = serde_json::from_slice(&buffer)? {
        Ok(value)
    } else {
        Err(KvsError::UnexpectedCommandType)
    }
}

fn read_commands<R: Read>(r: R, res: &mut HashMap<String, Value>) -> Result<u32> {
    let mut count = 0_u32;
    let mut start = 0_u64;
    let mut stream = Deserializer::from_reader(r).into_iter::<Command>();

    while let Some(cmd) = stream.next() {
        let offset = stream.byte_offset() as u64;

        match cmd? {
            Command::Remove { key } => {
                res.remove(&key);
            }
            Command::Set { key, .. } => {
                res.insert(
                    key,
                    Value::Position {
                        start,
                        length: offset - start,
                    },
                );
                count += 1;
            }
        }
        start = offset;
    }

    Ok(count)
}

fn write_command<W: Write>(w: &mut W, cmd: Command) -> Result<u64> {
    let s = serde_json::to_string(&cmd)?;
    w.write(s.as_bytes())?;

    Ok(s.len() as u64)
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Debug)]
enum Value {
    // record position.
    Position { start: u64, length: u64 },

    // native value
    Inner(String),
}
