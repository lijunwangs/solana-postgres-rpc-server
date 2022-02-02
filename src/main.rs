/// Main entry for the PostgreSQL based RPC Server
use {
    clap::{crate_description, crate_name, value_t, value_t_or_exit, App, Arg},
    solana_clap_utils::input_validators::{is_niceness_adjustment_valid, is_parsable},
    solana_postgres_rpc_server::rpc_service::{JsonRpcConfig, JsonRpcService},
    std::net::SocketAddr,
};

pub const MAX_MULTIPLE_ACCOUNTS: usize = 100;

#[allow(unused_variables)]
pub fn main() {
    let default_rpc_max_multiple_accounts = &MAX_MULTIPLE_ACCOUNTS.to_string();
    let default_rpc_threads = num_cpus::get().to_string();

    let matches = App::new(crate_name!())
        .about(crate_description!())
        .arg(
            Arg::with_name("rpc_bind_address")
                .long("rpc-bind-address")
                .value_name("HOST")
                .takes_value(true)
                .validator(solana_net_utils::is_host)
                .help("IP address to bind the RPC port [default: 127.0.0.1]"),
        )
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
        .arg(
            Arg::with_name("rpc_max_multiple_accounts")
                .long("rpc-max-multiple-accounts")
                .value_name("MAX ACCOUNTS")
                .takes_value(true)
                .default_value(default_rpc_max_multiple_accounts)
                .help(
                    "Override the default maximum accounts accepted by \
                       the getMultipleAccounts JSON RPC method",
                ),
        )
        .arg(
            Arg::with_name("rpc_threads")
                .long("rpc-threads")
                .value_name("NUMBER")
                .validator(is_parsable::<usize>)
                .takes_value(true)
                .default_value(&default_rpc_threads)
                .help("Number of threads to use for servicing RPC requests"),
        )
        .arg(
            Arg::with_name("rpc_niceness_adj")
                .long("rpc-niceness-adjustment")
                .value_name("ADJUSTMENT")
                .takes_value(true)
                .validator(is_niceness_adjustment_valid)
                .default_value("0")
                .help(
                    "Add this value to niceness of RPC threads. Negative value \
                      increases priority, positive value decreases priority.",
                ),
        )
        .get_matches();

    let rpc_bind_address = if matches.is_present("rpc_bind_address") {
        solana_net_utils::parse_host(matches.value_of("rpc_bind_address").unwrap())
            .expect("invalid rpc_bind_address")
    } else {
        solana_net_utils::parse_host("127.0.0.1").unwrap()
    };

    let rpc_port = value_t!(matches, "rpc_port", u16).expect("rpc-port is required");
    let rpc_addr = SocketAddr::new(rpc_bind_address, rpc_port);
    let max_multiple_accounts = Some(value_t_or_exit!(
        matches,
        "rpc_max_multiple_accounts",
        usize
    ));
    let rpc_threads = value_t_or_exit!(matches, "rpc_threads", usize);
    let rpc_niceness_adj = value_t_or_exit!(matches, "rpc_niceness_adj", i8);

    let db_config = value_t!(matches, "db_config", String).expect("db-config is required");
    let config = JsonRpcConfig {
        max_multiple_accounts,
        rpc_threads,
        rpc_niceness_adj,
    };
    let json_rpc_service = JsonRpcService::new(rpc_addr, config);
    json_rpc_service.join().unwrap();
}
