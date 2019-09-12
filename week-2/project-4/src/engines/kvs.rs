use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use super::KvsEngine;
use crate::error::{KvsError, Result};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;
const KV_LOG_NAME: &'static str = "kv.log";
const KV_SWAP_NAME: &'static str = "kv.tmp";

// KvStore database.
pub struct KvStore {
    path: PathBuf,
    index: Box<HashMap<String, Value>>,
    file: File,
    uncompacted: u64,
}

impl KvStore {
    pub fn open(path: &Path) -> Result<Self> {
        fs::create_dir_all(&path)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path.clone().join(KV_LOG_NAME))?;

        let index = read_commands(&file)?;

        Ok(KvStore {
            path: path.into(),
            index: index,
            file: file,
            uncompacted: 0,
        })
    }

    fn rebuild_log(&mut self) -> Result<()> {
        let swap_path = self.path.clone().join(KV_SWAP_NAME);

        let mut reader = self.file.try_clone()?;
        let mut writer = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&swap_path)?;

        for (key, val) in self.index.iter_mut() {
            match val {
                Value::Position { start, length } => {
                    let mut buf = vec![0; *length as usize];
                    reader.read(&mut buf)?;

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

        self.file = writer;
        fs::rename(&swap_path, &self.path.clone().join(KV_LOG_NAME))?;

        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let len = write_command(
            &mut self.file,
            Command::Set {
                key: key.clone(),
                value: value.clone(),
            },
        )?;

        if self.index.insert(key, Value::Inner(value)).is_some() {
            self.uncompacted += len as u64;
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.rebuild_log()?;
            return Ok(());
        }

        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(value) = self.index.get(&key) {
            match value {
                Value::Position { start, length } => {
                    let v = read_value(&mut self.file, *start, *length)?;
                    self.index.insert(key, Value::Inner(v.clone()));
                    Ok(Some(v))
                }
                Value::Inner(v) => Ok(Some(v.clone())),
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            self.index.remove(&key);

            write_command(&mut self.file, Command::Remove { key })?;

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
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

fn read_commands<R: Read>(r: R) -> Result<Box<HashMap<String, Value>>> {
    let mut ans = Box::new(HashMap::new());

    let mut start = 0_u64;
    let mut stream = Deserializer::from_reader(r).into_iter::<Command>();

    while let Some(cmd) = stream.next() {
        let offset = stream.byte_offset() as u64;

        match cmd? {
            Command::Remove { key } => {
                ans.remove(&key);
            }
            Command::Set { key, .. } => {
                ans.insert(
                    key,
                    Value::Position {
                        start,
                        length: offset - start,
                    },
                );
            }
        }
        start = offset;
    }

    Ok(ans)
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
