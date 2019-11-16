#[macro_use]
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(about = "kvscli 0.1.0")]
enum KvsCliOpt {
    Get {
        #[structopt()]
        key: Option<String>,
    },
    Set {
        #[structopt()]
        key: Option<String>,
        #[structopt()]
        value: Option<String>,
    },
    Rm {
        #[structopt()]
        key: Option<String>
    }
}



fn main() -> std::result::Result<(), &'static str> {
    let arg = KvsCliOpt::from_args();
    println!("{:?}", arg);
    Err("unimplemented")
}
