use std::process::exit;
use structopt::StructOpt;

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

fn main() {
    let opt = Opt::from_args();

    match opt {
        Opt::Set { key: _k, value: _v } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Opt::Get { key: _k } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Opt::Rm { key: _k } => {
            eprintln!("unimplemented");
            exit(1);
        }
    }
}
