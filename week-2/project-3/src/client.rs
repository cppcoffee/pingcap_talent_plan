use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpStream};

use crate::{GetResponse, KvsError, RemoveResponse, Request, Result, SetResponse};

pub struct KvsClient {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    pub fn connect(addr: &SocketAddr) -> Result<Self> {
        let tcp_reader = TcpStream::connect(addr)?;
        let tcp_writer = tcp_reader.try_clone()?;
        Ok(KvsClient {
            reader: BufReader::new(tcp_reader),
            writer: BufWriter::new(tcp_writer),
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.send_command(&Request::Get { key })?;

        let mut data = String::new();
        self.reader.read_line(&mut data)?;

        let resp: GetResponse = serde_json::from_str(&data)?;
        match resp {
            GetResponse::Ok(v) => Ok(v),
            GetResponse::Err(msg) => Err(KvsError::StringError(msg)),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.send_command(&Request::Set { key, value })?;

        let mut data = String::new();
        self.reader.read_line(&mut data)?;

        let resp: SetResponse = serde_json::from_str(&data)?;
        match resp {
            SetResponse::Ok(_) => Ok(()),
            SetResponse::Err(msg) => Err(KvsError::StringError(msg)),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.send_command(&Request::Remove { key })?;

        let mut data = String::new();
        self.reader.read_line(&mut data)?;

        let resp: RemoveResponse = serde_json::from_str(&data)?;
        match resp {
            RemoveResponse::Ok(_) => Ok(()),
            RemoveResponse::Err(msg) => Err(KvsError::StringError(msg)),
        }
    }

    fn send_command(&mut self, r: &Request) -> Result<()> {
        let mut s = serde_json::to_string(r)?;
        s.push_str("\n");

        self.writer.write_all(s.as_bytes())?;
        self.writer.flush()?;

        Ok(())
    }
}
