use std::io::{BufRead, Write};
use std::io::{BufReader, BufWriter};
use std::net::{SocketAddr, TcpListener, TcpStream};

use log::{error, info};

use crate::{GetResponse, RemoveResponse, Request, SetResponse};
use crate::{KvsEngine, Result, ThreadPool};

pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    pub fn new(engine: E, pool: P) -> Self {
        Self { engine, pool }
    }

    pub fn run(&mut self, addr: &SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            let engine = self.engine.clone();
            self.pool.spawn(move || match stream {
                Ok(stream) => {
                    if let Err(e) = handle_client(stream, engine) {
                        error!("handle_client error, {}", e);
                    }
                }
                Err(e) => {
                    error!("Connection failed: {}", e);
                }
            });
        }

        Ok(())
    }
}

fn handle_client<E: KvsEngine>(sock: TcpStream, engine: E) -> Result<()> {
    let reader = BufReader::new(&sock);
    let mut writer = BufWriter::new(&sock);

    for line in reader.lines() {
        let line = line?;

        info!("{}: '{}'", sock.peer_addr()?.to_string(), line);

        let req: Request = serde_json::from_str(&line)?;
        let mut text = match req {
            Request::Get { key } => handle_get_command(&engine, key)?,
            Request::Set { key, value } => handle_set_command(&engine, key, value)?,
            Request::Remove { key } => handle_remove_command(&engine, key)?,
        };

        text.push_str("\n");
        writer.write_all(text.as_bytes())?;
        writer.flush()?;
    }

    Ok(())
}

fn handle_get_command<E: KvsEngine>(engine: &E, key: String) -> Result<String> {
    let ans: String;
    match engine.get(key) {
        Ok(val) => {
            ans = serde_json::to_string(&GetResponse::Ok(val))?;
        }
        Err(e) => {
            ans = serde_json::to_string(&GetResponse::Err(e.to_string()))?;
        }
    }
    Ok(ans)
}

fn handle_set_command<E: KvsEngine>(engine: &E, key: String, value: String) -> Result<String> {
    let ans: String;
    match engine.set(key, value) {
        Ok(_) => {
            ans = serde_json::to_string(&SetResponse::Ok(()))?;
        }
        Err(e) => {
            ans = serde_json::to_string(&SetResponse::Err(e.to_string()))?;
        }
    }
    Ok(ans)
}

fn handle_remove_command<E: KvsEngine>(engine: &E, key: String) -> Result<String> {
    let ans: String;
    match engine.remove(key) {
        Ok(_) => {
            ans = serde_json::to_string(&RemoveResponse::Ok(()))?;
        }
        Err(e) => {
            ans = serde_json::to_string(&RemoveResponse::Err(e.to_string()))?;
        }
    }
    Ok(ans)
}
