/// Main entry for the PostgreSQL based RPC Server
use {
    clap::{crate_description, crate_name, value_t, value_t_or_exit, App, Arg},
    solana_clap_utils::input_validators::{is_niceness_adjustment_valid, is_parsable},
    solana_postgres_rpc_server::{
        postgres_client::SimplePostgresClient,
        postgres_rpc_server_config::PostgresRpcServerConfig,
        rpc_service::{JsonRpcConfig, JsonRpcService},
    },
    std::{env, fs::OpenOptions, net::SocketAddr, process::exit, thread::JoinHandle},
};

pub const MAX_MULTIPLE_ACCOUNTS: usize = 100;

#[cfg(unix)]
fn redirect_stderr(filename: &str) {
    use std::os::unix::io::AsRawFd;
    match OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(filename)
    {
        Ok(file) => unsafe {
            libc::dup2(file.as_raw_fd(), libc::STDERR_FILENO);
        },
        Err(err) => eprintln!("Unable to open {}: {}", filename, err),
    }
}

// Redirect stderr to a file with support for logrotate by sending a SIGUSR1 to the process.
//
// Upon success, future `log` macros and `eprintln!()` can be found in the specified log file.
pub fn redirect_stderr_to_file(logfile: Option<String>) -> Option<JoinHandle<()>> {
    // Default to RUST_BACKTRACE=1 for more informative validator logs
    if env::var_os("RUST_BACKTRACE").is_none() {
        env::set_var("RUST_BACKTRACE", "1")
    }

    let filter = "solana=info";
    match logfile {
        None => {
            solana_logger::setup_with_default(filter);
            None
        }
        Some(logfile) => {
            #[cfg(unix)]
            {
                use log::info;
                let mut signals =
                    signal_hook::iterator::Signals::new(&[signal_hook::consts::SIGUSR1])
                        .unwrap_or_else(|err| {
                            eprintln!("Unable to register SIGUSR1 handler: {:?}", err);
                            exit(1);
                        });

                solana_logger::setup_with_default(filter);
                redirect_stderr(&logfile);
                Some(std::thread::spawn(move || {
                    for signal in signals.forever() {
                        info!(
                            "received SIGUSR1 ({}), reopening log file: {:?}",
                            signal, logfile
                        );
                        redirect_stderr(&logfile);
                    }
                }))
            }
            #[cfg(not(unix))]
            {
                println!("logrotate is not supported on this platform");
                solana_logger::setup_file_with_default(&logfile, filter);
                None
            }
        }
    }
}

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
        .arg(
            Arg::with_name("logfile")
                .short("o")
                .long("log")
                .value_name("FILE")
                .takes_value(true)
                .help(
                    "Redirect logging to the specified file, '-' for standard error. \
                       Sending the SIGUSR1 signal to the process will cause it \
                       to re-open the log file",
                ),
        )
        .get_matches();

    let logfile = {
        let logfile = matches
            .value_of("logfile")
            .map(|s| s.into())
            .unwrap_or_else(|| "solana-postgres-rpc-server.log".to_string());

        if logfile == "-" {
            None
        } else {
            println!("log file: {}", logfile);
            Some(logfile)
        }
    };
    let _logger_thread = redirect_stderr_to_file(logfile);

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
    let db_config =
        PostgresRpcServerConfig::load_config_from_file(&db_config).unwrap_or_else(|err| {
            println!(
                "Could not load the configuration file from {:?}. Please make \
                sure it exists and is in valid JSON format. Error details: ({})",
                db_config, err
            );
            std::process::exit(1);
        });

    let db_client = SimplePostgresClient::new(&db_config).unwrap_or_else(
        |err| {
            println!("Could not connect to the database server. Please review the \
            configuration information. Error details: ({})", err);
            std::process::exit(1);
        }
    );

    let config = JsonRpcConfig {
        max_multiple_accounts,
        rpc_threads,
        rpc_niceness_adj,
    };
    let json_rpc_service = JsonRpcService::new(rpc_addr, config, db_client);
    json_rpc_service.join().unwrap();
}
