use clap::Clap;
use core::fmt;
use kvs::{async_server, KvStore, KvsEngine, Result, SledKvsEngine};
use log::{error, info, warn};
use std::{
    env,
    fmt::{Display, Formatter},
    fs,
    net::SocketAddr,
    process::exit,
    str::FromStr,
    write,
};

macro_rules! enum_to_str {
    (enum $name:ident {
        $($variant:ident),*,
    }) => {
        #[allow(non_camel_case_types)]
        #[derive(PartialEq)]
        enum $name {
            $($variant),*
        }

        impl $name {
            fn name(&self) -> &'static str {
                match self {
                    $($name::$variant => stringify!($variant)),*
                }
            }
        }
    };
}

enum_to_str! {
    enum SupportEngines {
        kvs,
        sled,
    }
}

impl FromStr for SupportEngines {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "kvs" => Ok(SupportEngines::kvs),
            "sled" => Ok(SupportEngines::sled),
            _ => Err("invalid engine"),
        }
    }
}

impl Display for SupportEngines {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
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
    engine: SupportEngines,
}

#[tokio::main]
async fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    let opt: Opt = Opt::parse();
    match check_current_engine() {
        Err(e) => {
            error!("{}", e);
            exit(1);
        }
        Ok(Some(engine)) => {
            if engine != opt.engine {
                error!("Wrong engine");
                exit(1);
            }
        }
        Ok(None) => {}
    }
    if let Err(e) = run(opt).await {
        error!("{}", e);
        exit(1);
    }
}

async fn run(opt: Opt) -> Result<()> {
    info!("Server[{}] start.", env!("CARGO_PKG_VERSION"));
    info!("Listening to {}.", opt.addr);
    info!("Choosen storage engine: {}.", opt.engine);
    fs::write(env::current_dir()?.join("engine"), opt.engine.to_string())?;
    match opt.engine {
        SupportEngines::kvs => start_engine(KvStore::open(env::current_dir()?)?, opt.addr).await,
        SupportEngines::sled => {
            start_engine(SledKvsEngine::open(env::current_dir()?)?, opt.addr).await
        }
    }
}

async fn start_engine<E: KvsEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let mut server = async_server::KvsServer::new(engine);
    server.run(addr).await
}

fn check_current_engine() -> Result<Option<SupportEngines>> {
    let engine_file_path = env::current_dir()?.join("engine");
    if !engine_file_path.exists() {
        return Ok(None);
    }
    let parse_engine: std::result::Result<SupportEngines, &str> =
        fs::read_to_string(engine_file_path)?.parse();
    match parse_engine {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            warn!("Invalid engine file: {}", e);
            Ok(None)
        }
    }
}
