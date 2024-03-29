use clap::{AppSettings, Clap};
use kvs::{async_client, Result};
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

async fn dispatch(opt: Opt) -> Result<()> {
    match opt.cmd {
        Command::Set { key, value, addr } => {
            let client = async_client::KvsClient::connect(addr).await?;
            client.set(key, value).await?;
        }
        Command::Get { key, addr } => {
            let client = async_client::KvsClient::connect(addr).await?;
            if let Some(value) = client.get(key).await? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Command::Remove { key, addr } => {
            let client = async_client::KvsClient::connect(addr).await?;
            client.remove(key).await?;
        }
    };
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(e) = dispatch(parse_args()).await {
        eprintln!("{}", e);
        exit(1);
    }
}
