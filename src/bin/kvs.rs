use std::{process::exit, unreachable};

use clap::{App, AppSettings, Arg, ArgMatches};

fn parse_args() -> ArgMatches {
    App::new(env!("CARGO_PKG_NAME"))
        .setting(AppSettings::VersionlessSubcommands)
        .setting(AppSettings::DisableHelpSubcommand)
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(App::new("set")
            .about("Set the value of a string key to a string")
            .arg(Arg::new("KEY")
                .required(true))
            .arg(Arg::new("VALUE")
                .required(true)))
        .subcommand(App::new("get")
            .about("Get the string value of a given string key")
            .arg(Arg::new("KEY")
                .required(true)))
        .subcommand(App::new("rm") 
            .about("Remove a given key")
            .arg(Arg::new("KEY")
                .required(true)))
    .get_matches()
}

fn dispatch(m: ArgMatches) {
    match m.subcommand_name() {
        Some("set") => {
            eprintln!("unimplemented");
            exit(1);
        },
        Some("get") => {
            eprintln!("unimplemented");
            exit(1);
        },
        Some("rm") => {
            eprintln!("unimplemented");
            exit(1);
        }
        _ => {
            unreachable!()
        }
    }
}

fn main() {
    dispatch(parse_args())
}