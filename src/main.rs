/// Main entry for the PostgreSQL based RPC Server
use {
    clap::{
        crate_description, crate_name, value_t, App,
        Arg,
    }
};

#[allow(unused_variables)]
pub fn main() {
    let matches = App::new(crate_name!())
        .about(crate_description!())
        .arg(
            Arg::with_name("rpc_port")
                .long("rpc-port")
                .value_name("PORT")
                .takes_value(true)
                .help("The JSON RPC on this port"),
        )
        .arg(
            Arg::with_name("db_config")
                .long("db-config")
                .value_name("FILE")
                .takes_value(true)
                .help("Specify the configuration file for the PostgreSQL database."),
        )
        .get_matches();

    let rpc_addr = value_t!(matches, "rpc_port", u16).expect("rpc-port is required");
    let db_config = value_t!(matches, "db_config", String).expect("db-config is required");
}