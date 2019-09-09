use std::io::{BufRead, Write};
use std::io::{BufReader, BufWriter};
use std::net::{SocketAddr, TcpListener, TcpStream};

use log::info;

use crate::{GetResponse, RemoveResponse, Request, SetResponse};
use crate::{KvsEngine, Result};

pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E) -> Self {
        Self { engine: engine }
    }

    pub fn run(&mut self, addr: &SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            self.handle_client(stream?)?;
        }

        Ok(())
    }

    fn handle_client(&mut self, sock: TcpStream) -> Result<()> {
        let reader = BufReader::new(&sock);
        let mut writer = BufWriter::new(&sock);

        for line in reader.lines() {
            let line = line?;

            info!("{}: '{}'", sock.peer_addr()?.to_string(), line);

            let req: Request = serde_json::from_str(&line)?;
            let mut text = match req {
                Request::Get { key } => self.handle_get_command(key)?,

                Request::Set { key, value } => self.handle_set_command(key, value)?,

                Request::Remove { key } => self.handle_remove_command(key)?,
            };

            text.push_str("\n");
            writer.write_all(text.as_bytes())?;
            writer.flush()?;
        }

        Ok(())
    }

    fn handle_get_command(&mut self, key: String) -> Result<String> {
        let ans: String;
        match self.engine.get(key) {
            Ok(val) => {
                ans = serde_json::to_string(&GetResponse::Ok(val))?;
            }
            Err(e) => {
                ans = serde_json::to_string(&GetResponse::Err(e.to_string()))?;
            }
        }
        Ok(ans)
    }

    fn handle_set_command(&mut self, key: String, value: String) -> Result<String> {
        let ans: String;
        match self.engine.set(key, value) {
            Ok(_) => {
                ans = serde_json::to_string(&SetResponse::Ok(()))?;
            }
            Err(e) => {
                ans = serde_json::to_string(&SetResponse::Err(e.to_string()))?;
            }
        }
        Ok(ans)
    }

    fn handle_remove_command(&mut self, key: String) -> Result<String> {
        let ans: String;
        match self.engine.remove(key) {
            Ok(_) => {
                ans = serde_json::to_string(&RemoveResponse::Ok(()))?;
            }
            Err(e) => {
                ans = serde_json::to_string(&RemoveResponse::Err(e.to_string()))?;
            }
        }
        Ok(ans)
    }
}
