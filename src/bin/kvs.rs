#[macro_use]
extern crate clap;
use clap::App;

fn main() -> std::result::Result<(), &'static str> {
    let yaml = load_yaml!("cli.yml");
    let _matches = App::from_yaml(yaml).get_matches();
    // match matches.subcommand_name() {
    //     Some("get") => eprintln!("unimplemented"),
    //     Some("set") => eprintln!("unimplemented"),
    //     Some("rm") => eprintln!("unimplemented"),
    //     _ => eprintln!("unknown command"),
    // }
    Err("unimplemented")
}
