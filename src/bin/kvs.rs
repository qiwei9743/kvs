use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(about = "about kvscli")]
/// The help message
enum KvsCliOpt {
    /// Get the value of a given key.
    Get {
        #[structopt()]
        /// The key to get value
        key: String,
    },
    /// Set key/value pairs
    Set {
        #[structopt()]
        /// The key in k/v pairs
        key: String,

        #[structopt()]
        /// The value in k/v pairs
        value: String,
    },
    /// Remove a key from kv Store
    Rm {
        #[structopt()]
        /// The key to remove from kv Store
        key: String
    }
}



fn main() -> std::result::Result<(), &'static str> {
    let arg = KvsCliOpt::from_args();
    println!("{:?}", arg);
    Err("unimplemented")
}
