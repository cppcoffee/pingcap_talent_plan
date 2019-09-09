use std::env;
use std::fs;
use std::net::SocketAddr;
use std::process::exit;
use std::str::FromStr;

use env_logger::WriteStyle;
use log::{error, info, LevelFilter};
use structopt::StructOpt;

use kvs::*;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::kvs;

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

#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Engine {
    kvs,
    sled,
}

impl FromStr for Engine {
    type Err = KvsError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "kvs" => Ok(Engine::kvs),
            "sled" => Ok(Engine::sled),
            _ => Err(KvsError::UnknownEngineType),
        }
    }
}

fn main() {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .write_style(WriteStyle::Never)
        .init();

    let opt = Opt::from_args();
    let engine = opt.engine.unwrap_or(DEFAULT_ENGINE);

    info!("kvs-server version: {}", env!("CARGO_PKG_VERSION"));
    info!("listen address: {}", opt.addr.to_string());
    info!("use engine: {:?}", engine);

    let res = current_engine().and_then(|curr_engine| {
        if curr_engine.is_some() && engine != curr_engine.unwrap() {
            error!("The content of engine file is invalid:: {:?}", engine);
            exit(1);
        }

        let curr_path = env::current_dir()?;

        fs::write(env::current_dir()?.join("engine"), format!("{:?}", engine))?;

        match engine {
            Engine::kvs => run_with_engine(KvStore::open(&curr_path)?, opt.addr),
            Engine::sled => run_with_engine(SledKvsEngine::open(&curr_path)?, opt.addr),
        }
    });

    if let Err(e) = res {
        error!("{}", e);
        exit(1);
    }
}

fn run_with_engine<E: KvsEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let mut server = KvsServer::new(engine);
    server.run(&addr)
}

fn current_engine() -> Result<Option<Engine>> {
    let path = env::current_dir()?.join("engine");
    if !path.exists() {
        return Ok(None);
    }

    let text = String::from_utf8(fs::read(&path)?)?;

    Ok(Some(Engine::from_str(&text)?))
}
