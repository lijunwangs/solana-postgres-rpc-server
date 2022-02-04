use {
    log::*,
    openssl::ssl::{SslConnector, SslFiletype, SslMethod},
    postgres::{Client, NoTls, Statement},
    postgres_openssl::MakeTlsConnector,
    std::{
        io,
        sync::Mutex,
    },
    thiserror::Error,
};

const DEFAULT_POSTGRES_PORT: u16 = 5432;

/// Errors returned by plugin calls
#[derive(Error, Debug)]
pub enum PostgresRpcServerError {
    /// Error opening the configuration file; for example, when the file
    /// is not found or when the validator process has no permission to read it.
    #[error("Error opening config file. Error detail: ({0}).")]
    ConfigFileOpenError(#[from] io::Error),

    /// Error in reading the content of the config file or the content
    /// is not in the expected format.
    #[error("Error reading config file. Error message: ({msg})")]
    ConfigFileReadError { msg: String },

     #[error("Error connecting to the backend data store. Error message: ({msg})")]
    DataStoreConnectionError { msg: String },

    #[error("Error preparing data store schema. Error message: ({msg})")]
    DataSchemaError { msg: String },

    #[error("Error preparing data store schema. Error message: ({msg})")]
    ConfigurationError { msg: String },
}

pub struct AccountsDbPluginPostgresConfig {
    pub host: Option<String>,
    pub user: Option<String>,
    pub port: Option<u16>,
    pub connection_str: Option<String>,
    pub use_ssl: Option<bool>,
    pub server_ca: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
}

struct PostgresSqlClientWrapper {
    client: Client,
    get_account_stmt: Statement,
}

pub struct SimplePostgresClient {
    client: Mutex<PostgresSqlClientWrapper>,
}

impl SimplePostgresClient {
    pub fn connect_to_db(
        config: &AccountsDbPluginPostgresConfig,
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
}
