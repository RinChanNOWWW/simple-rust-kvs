use clap::{AppSettings, Clap};
use kvs::Result;
use std::{env, net::SocketAddr, process::exit};

#[derive(Clap)]
#[clap(name= "kvs-client", version = env!("CARGO_PKG_VERSION"), setting = AppSettings::DisableHelpSubcommand)]
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
        #[clap(
            long,
            value_name = "IP:PROT",
            default_value = "127.0.0.1:4000",
            about = "Specify the server address"
        )]
        addr: SocketAddr,
    },
    #[clap(name = "get", about = "Get the string value of a given string key")]
    Get {
        #[clap(name = "KEY", required = true, about = "The key")]
        key: String,
        #[clap(
            long,
            value_name = "IP:PROT",
            default_value = "127.0.0.1:4000",
            about = "Specify the server address"
        )]
        addr: SocketAddr,
    },
    #[clap(name = "rm", about = "Remove a given key")]
    Remove {
        #[clap(name = "KEY", required = true, about = "The key")]
        key: String,
        #[clap(
            long,
            value_name = "IP:PROT",
            default_value = "127.0.0.1:4000",
            about = "Specify the server address"
        )]
        addr: SocketAddr,
    },
}

fn parse_args() -> Opt {
    let opt: Opt = Opt::parse();
    return opt;
}

fn dispatch(opt: Opt) -> Result<()> {
    match opt.cmd {
        Command::Set { key, value, addr } => {}
        Command::Get { key, addr } => {}
        Command::Remove { key, addr } => {}
    };
    Ok(())
}

fn main() {
    if let Err(e) = dispatch(parse_args()) {
        eprintln!("{}", e);
        exit(1);
    }
}
