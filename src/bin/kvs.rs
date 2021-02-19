use clap::{AppSettings, Clap};
use kvs::KvStore;
use std::{env, process::exit};

#[derive(Clap)]
#[clap(version = env!("CARGO_PKG_VERSION"), setting = AppSettings::DisableHelpSubcommand)]
struct Opt {
    #[clap(subcommand)]
    cmd: Command,
}
#[derive(Clap)]
enum Command {
    #[clap(name = "set", about = "Set the value of a string key to a string")]
    Set {
        #[clap(name = "KEY", required = true, about = "The key")]
        key: String,
        #[clap(name = "VALUE", required = true, about = "The value")]
        value: String,
    },
    #[clap(name = "get", about = "Get the string value of a given string key")]
    Get {
        #[clap(name = "KEY", required = true, about = "The key")]
        key: String,
    },
    #[clap(name = "rm", about = "Remove a given key")]
    Remove {
        #[clap(name = "KEY", required = true, about = "The key")]
        key: String,
    },
}

fn parse_args() -> Opt {
    let opt: Opt = Opt::parse();
    return opt;
}

fn dispatch(mut kv: KvStore, opt: Opt) {
    match opt.cmd {
        Command::Set { key, value } => match kv.set(key, value) {
            Err(e) => {
                println!("{}", e);
                exit(1);
            }
            _ => {}
        },
        Command::Get { key } => match kv.get(key) {
            Err(e) => {
                println!("{}", e);
                exit(1)
            }
            Ok(Some(value)) => {
                println!("{}", value);
            }
            _ => {
                println!("Key not found");
            }
        },
        Command::Remove { key } => match kv.remove(key) {
            Err(e) => {
                println!("{}", e);
                exit(1)
            }
            _ => {}
        },
    }
}

fn main() {
    let kv = KvStore::open(env::current_dir().unwrap()).unwrap();
    dispatch(kv, parse_args());
}
