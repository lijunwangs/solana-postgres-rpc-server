use {
    crate::{
        postgres_rpc_server_config::PostgresRpcServerConfig,
        postgres_rpc_server_error::PostgresRpcServerError
    },
    log::*,
    openssl::ssl::{SslConnector, SslFiletype, SslMethod},
    postgres::{Client, NoTls, Statement},
    postgres_openssl::MakeTlsConnector,
    std::{
        sync::Mutex,
    },
};

const DEFAULT_POSTGRES_PORT: u16 = 5432;

struct PostgresSqlClientWrapper {
    client: Client,
    get_account_stmt: Statement,
}

pub struct SimplePostgresClient {
    client: Mutex<PostgresSqlClientWrapper>,
}

impl SimplePostgresClient {
    pub fn connect_to_db(
        config: &PostgresRpcServerConfig,
    ) -> Result<Client, PostgresRpcServerError> {
        let port = config.port.unwrap_or(DEFAULT_POSTGRES_PORT);

        let connection_str = if let Some(connection_str) = &config.connection_str {
            connection_str.clone()
        } else {
            if config.host.is_none() || config.user.is_none() {
                let msg = format!(
                    "\"connection_str\": {:?}, or \"host\": {:?} \"user\": {:?} must be specified",
                    config.connection_str, config.host, config.user
                );
                return Err(
                    PostgresRpcServerError::ConfigurationError { msg },
                );
            }
            format!(
                "host={} user={} port={}",
                config.host.as_ref().unwrap(),
                config.user.as_ref().unwrap(),
                port
            )
        };

        let result = if let Some(true) = config.use_ssl {
            if config.server_ca.is_none() {
                let msg = "\"server_ca\" must be specified when \"use_ssl\" is set".to_string();
                return Err(
                    PostgresRpcServerError::ConfigurationError { msg },
                );
            }
            if config.client_cert.is_none() {
                let msg = "\"client_cert\" must be specified when \"use_ssl\" is set".to_string();
                return Err(
                    PostgresRpcServerError::ConfigurationError { msg },
                );
            }
            if config.client_key.is_none() {
                let msg = "\"client_key\" must be specified when \"use_ssl\" is set".to_string();
                return Err(
                    PostgresRpcServerError::ConfigurationError { msg },
                );
            }
            let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
            if let Err(err) = builder.set_ca_file(config.server_ca.as_ref().unwrap()) {
                let msg = format!(
                    "Failed to set the server certificate specified by \"server_ca\": {}. Error: ({})",
                    config.server_ca.as_ref().unwrap(), err);
                return Err(
                    PostgresRpcServerError::ConfigurationError { msg },
                );
            }
            if let Err(err) =
                builder.set_certificate_file(config.client_cert.as_ref().unwrap(), SslFiletype::PEM)
            {
                let msg = format!(
                    "Failed to set the client certificate specified by \"client_cert\": {}. Error: ({})",
                    config.client_cert.as_ref().unwrap(), err);
                return Err(
                    PostgresRpcServerError::ConfigurationError { msg },
                );
            }
            if let Err(err) =
                builder.set_private_key_file(config.client_key.as_ref().unwrap(), SslFiletype::PEM)
            {
                let msg = format!(
                    "Failed to set the client key specified by \"client_key\": {}. Error: ({})",
                    config.client_key.as_ref().unwrap(),
                    err
                );
                return Err(
                    PostgresRpcServerError::ConfigurationError { msg },
                );
            }

            let mut connector = MakeTlsConnector::new(builder.build());
            connector.set_callback(|connect_config, _domain| {
                connect_config.set_verify_hostname(false);
                Ok(())
            });
            Client::connect(&connection_str, connector)
        } else {
            Client::connect(&connection_str, NoTls)
        };

        match result {
            Err(err) => {
                let msg = format!(
                    "Error in connecting to the PostgreSQL database: {:?} connection_str: {:?}",
                    err, connection_str
                );
                error!("{}", msg);
                Err(
                    PostgresRpcServerError::DataStoreConnectionError { msg }
                )
            }
            Ok(client) => Ok(client),
        }
    }

    fn build_get_account_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> Result<Statement, PostgresRpcServerError> {
        let stmt = "SELECT pubkey, slot, owner, lamports, executable, rent_epoch, data, write_version, updated_on FROM account AS acct \
            WHERE pubkey = $1";

        let stmt = client.prepare(stmt);

        match stmt {
            Err(err) => {
                return Err(PostgresRpcServerError::DataSchemaError {
                    msg: format!(
                        "Error in preparing for the accounts update PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                        err, config.host, config.user, config
                    ),
                });
            }
            Ok(update_account_stmt) => Ok(update_account_stmt),
        }
    }

    pub fn new(config: &PostgresRpcServerConfig) -> Result<Self, PostgresRpcServerError> {
        info!("Creating SimplePostgresClient...");
        let mut client = Self::connect_to_db(config)?;
        let get_account_stmt = Self::build_get_account_stmt(&mut client, config)?;

        info!("Created SimplePostgresClient.");
        Ok(Self {
            client: Mutex::new(PostgresSqlClientWrapper {
                client,
                get_account_stmt,
            }),
        })
    }    
}
