use std::net::SocketAddr;

use crate::KvsEngine;

pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E) -> Self {
        Self { engine: engine }
    }

    pub fn run(&self, addr: &SocketAddr) -> failure::Fallible<()> {
        println!("accept client ip: {:?}", addr);
        Ok(())
    }
}
