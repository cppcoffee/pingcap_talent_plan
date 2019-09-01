#[macro_use]
extern crate failure;

use std::env;
use std::net::SocketAddr;
use std::str::FromStr;

use env_logger::WriteStyle;
use log::{info, LevelFilter};
use structopt::StructOpt;

use kvs::*;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::Kvs;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-server", about = "An key-value store server.")]
struct Opt {
    #[structopt(
        long = "addr",
        help = "Sets the listening address",
        value_name = "IP:PORT",
        raw(default_value = "DEFAULT_LISTENING_ADDRESS"),
        parse(try_from_str)
    )]
    addr: SocketAddr,

    #[structopt(
        long = "engine",
        help = "Sets the kv storage engine, support kvs|sled",
        value_name = "ENGINE-NAME"
    )]
    engine: Option<Engine>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Engine {
    Kvs,
    Sled,
}

impl FromStr for Engine {
    type Err = failure::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "Kvs" => Ok(Engine::Kvs),
            "Sled" => Ok(Engine::Sled),
            _ => bail!("unknown kv engine."),
        }
    }
}

fn main() -> failure::Fallible<()> {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .write_style(WriteStyle::Never)
        .init();

    let opt = Opt::from_args();
    let engine = opt.engine.unwrap_or(DEFAULT_ENGINE);

    info!("kvs-server version: {}", env!("CARGO_PKG_VERSION"));
    info!("listen address: {}", opt.addr.to_string());
    info!("use engine: {:?}", engine);

    let path = env::current_dir()?;

    let server = match engine {
        Engine::Kvs => KvsServer::new(KvStore::open(&path)?),
        Engine::Sled => unimplemented!(),
    };

    server.run(&opt.addr)
}
