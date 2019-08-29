use std::env::current_dir;
use std::process::exit;
use structopt::StructOpt;

use kvs::{KvStore, KvsError, Result};

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs", about = "kvs project")]
enum Opt {
    #[structopt(name = "set", about = "Set the value of a string key to a string")]
    Set { key: String, value: String },
    #[structopt(name = "get", about = "Get the string value of a given string key")]
    Get { key: String },
    #[structopt(name = "rm", about = "Remove a given key")]
    Rm { key: String },
}

fn main() -> Result<()> {
    let opt = Opt::from_args();

    match opt {
        Opt::Set { key: k, value: v } => {
            let mut store = KvStore::open(&current_dir()?)?;
            store.set(k, v)?;
        }
        Opt::Get { key: k } => {
            let mut store = KvStore::open(&current_dir()?)?;
            if let Ok(Some(value)) = store.get(k) {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Opt::Rm { key: k } => {
            let mut store = KvStore::open(&current_dir()?)?;
            match store.remove(k) {
                Ok(()) => {}
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1)
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok(())
}
