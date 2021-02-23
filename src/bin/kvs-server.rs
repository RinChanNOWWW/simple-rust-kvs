use std::{net::SocketAddr, str::FromStr};

use clap::Clap;

enum SupportEngines {
    Kvs,
    Sled,
}

impl FromStr for SupportEngines {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "kvs" => Ok(SupportEngines::Kvs),
            "sled" => Ok(SupportEngines::Sled),
            _ => Err("invalid engine"),
        }
    }
}

#[derive(Clap)]
#[clap(name = "kvs-server", version = env!("CARGO_PKG_VERSION"))]
struct Opt {
    #[clap(
        long,
        value_name = "IP-PORT",
        default_value = "127.0.0.1:4000",
        about = "Specify the address listening to"
    )]
    addr: SocketAddr,
    #[clap(
        long,
        value_name = "ENGINE-NAME",
        default_value = "kvs",
        about = "Specify the storage engine",
        possible_values = &["kvs", "sled"]
    )]
    engine: String,
}

fn main() {
    let opt: Opt = Opt::parse();
}
