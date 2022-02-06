use {
    crate::{
        postgres_rpc_server_error::{PostgresRpcServerError, Result},
    },
    serde_derive::{Deserialize, Serialize},
    std::{fs::File, io::Read},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PostgresRpcServerConfig {
    pub host: Option<String>,
    pub user: Option<String>,
    pub port: Option<u16>,
    pub connection_str: Option<String>,
    pub use_ssl: Option<bool>,
    pub server_ca: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
}

impl PostgresRpcServerConfig {
    pub fn load_config_from_file(config_file: &str) -> Result<Self> {
        let mut file = File::open(config_file)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let result: serde_json::Result<PostgresRpcServerConfig> =
            serde_json::from_str(&contents);
        match result {
            Err(err) => {
                Err(PostgresRpcServerError::ConfigFileReadError {
                    msg: format!(
                        "The config file is not in the JSON format expected: {:?}",
                        err
                    ),
                })
            }
            Ok(config) => {
                Ok(config)
            }
        }
    }
}