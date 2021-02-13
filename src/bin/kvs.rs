#![allow(unused)]
use std::{process::exit};
use clap::{Clap, AppSettings};

#[derive(Clap)]
#[clap(version = env!("CARGO_PKG_VERSION"), setting = AppSettings::DisableHelpSubcommand)]
struct Opt {
    #[clap(subcommand)]
    cmd: Command

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
        key: String
    },
    #[clap(name = "rm", about = "Remove a given key")]
    Remove {
        #[clap(name = "KEY", required = true, about = "The key")]
        key: String
    }
}

fn parse_args() -> Opt {
    let opt: Opt = Opt::parse();
    return opt;
}

fn dispatch(opt: Opt) {
    match opt.cmd {
        Command::Set{key, value}=> {
            eprintln!("unimplemented");
            exit(1);
        },
        Command::Get{key} => {
            eprintln!("unimplemented");
            exit(1);
        },
        Command::Remove{key} => {
            eprintln!("unimplemented");
            exit(1);
        }
    }
}

fn main() {
    dispatch(parse_args())
}