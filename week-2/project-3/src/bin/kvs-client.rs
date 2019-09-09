use std::net::SocketAddr;

use structopt::StructOpt;

use kvs::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-client", about = "kvs client command project")]
enum Opt {
    #[structopt(name = "set", about = "Set the value of a string key to a string")]
    Set {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
        #[structopt(name = "VALUE", help = "A string value")]
        value: String,
        #[structopt(
            long,
            help = "Sets the server address",
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },
    #[structopt(name = "get", about = "Get the string value of a given string key")]
    Get {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
        #[structopt(
            long,
            help = "Sets the server address",
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },
    #[structopt(name = "rm", about = "Remove a given key")]
    Rm {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
        #[structopt(
            long,
            help = "Sets the server address",
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },
}

fn main() -> Result<()> {
    let opt = Opt::from_args();

    match opt {
        Opt::Get { key, addr } => {
            let mut cli = KvsClient::connect(&addr)?;
            match cli.get(key)? {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            }
        }
        Opt::Set { key, value, addr } => {
            let mut cli = KvsClient::connect(&addr)?;
            cli.set(key, value)?;
        }
        Opt::Rm { key, addr } => {
            let mut cli = KvsClient::connect(&addr)?;
            cli.remove(key)?;
        }
    }

    Ok(())
}
