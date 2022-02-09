use {
    crate::{
        postgres_rpc_server_config::PostgresRpcServerConfig,
        postgres_rpc_server_error::PostgresRpcServerError,
    },
    log::*,
    solana_sdk::{account::ReadableAccount, clock::Epoch, pubkey::Pubkey},
    std::{sync::Mutex},
    tokio_postgres::{
        Socket,
        tls::{NoTls, NoTlsStream,},
        Client, Connection, Statement
    }
    
};

const DEFAULT_POSTGRES_PORT: u16 = 5432;

impl Eq for DbAccountInfo {}

#[derive(Clone, PartialEq, Debug)]
pub struct DbAccountInfo {
    pub pubkey: Pubkey,
    pub lamports: i64,
    pub owner: Vec<u8>,
    pub executable: bool,
    pub rent_epoch: i64,
    pub data: Vec<u8>,
    pub slot: i64,
    pub write_version: i64,
}

impl ReadableAccount for DbAccountInfo {
    fn lamports(&self) -> u64 {
        self.lamports as u64
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn owner(&self) -> &Pubkey {
        &self.pubkey
    }

    fn executable(&self) -> bool {
        self.executable
    }

    fn rent_epoch(&self) -> Epoch {
        self.rent_epoch as u64
    }
}

struct PostgresSqlClientWrapper {
    client: Client,
    get_account_stmt: Statement,
}

pub struct SimplePostgresClient {
    client: Mutex<PostgresSqlClientWrapper>,
}

impl SimplePostgresClient {
     pub async fn connect_to_db <T> (
        config: &PostgresRpcServerConfig,
    ) -> Result<(Client, Connection<Socket, NoTlsStream>), PostgresRpcServerError> 
    {
        let port = config.port.unwrap_or(DEFAULT_POSTGRES_PORT);

        let connection_str = if let Some(connection_str) = &config.connection_str {
            connection_str.clone()
        } else {
            if config.host.is_none() || config.user.is_none() {
                let msg = format!(
                    "\"connection_str\": {:?}, or \"host\": {:?} \"user\": {:?} must be specified",
                    config.connection_str, config.host, config.user
                );
                return Err(PostgresRpcServerError::ConfigurationError { msg });
            }
            format!(
                "host={} user={} port={}",
                config.host.as_ref().unwrap(),
                config.user.as_ref().unwrap(),
                port
            )
        };

        let result = tokio_postgres::connect(&connection_str, NoTls).await;

        match result {
            Ok(result) => {
                Ok(result)
            }
            Err(err) => {
                let msg = format!(
                    "Error in connecting database \"connection_str\": {:?}, or \"host\": {:?} \"user\": {:?}: {}",
                    config.connection_str, config.host, config.user, err
                );
                Err(PostgresRpcServerError::DataStoreConnectionError {msg})
            }
        }
    }

    async fn build_get_account_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> Result<Statement, PostgresRpcServerError> {
        let stmt = "SELECT pubkey, slot, owner, lamports, executable, rent_epoch, data, write_version, updated_on FROM account AS acct \
            WHERE pubkey = $1";

        let stmt = client.prepare(stmt).await;

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

    pub fn get_account(
        &mut self,
        pubkey: &Pubkey,
    ) -> Result<DbAccountInfo, PostgresRpcServerError> {
        let client = self.client.get_mut().unwrap();
        let statement = &client.get_account_stmt;
        let client = &mut client.client;
        let pubkey_v = pubkey.to_bytes().to_vec();
        let result = client.query(statement, &[&pubkey_v]);
        match result {
            Err(error) => {
                let msg = format!(
                    "Failed load the account from the database. Account: {}, Error: ({:?})",
                    pubkey, error
                );
                Err(PostgresRpcServerError::DatabaseQueryError { msg })
            }
            Ok(result) => match result.len() {
                0 => {
                    let msg = format!(
                        "The account with key {} is not found from the database.",
                        pubkey
                    );
                    Err(PostgresRpcServerError::ObjectNotFound { msg })
                }
                1 => Ok(DbAccountInfo {
                    pubkey: Pubkey::new(result[0].get(0)),
                    lamports: result[0].get(1),
                    owner: result[0].get(2),
                    executable: result[0].get(3),
                    rent_epoch: result[0].get(4),
                    data: result[0].get(5),
                    slot: result[0].get(6),
                    write_version: result[0].get(7),
                }),
                cnt => {
                    let msg = format!(
                        "Found more than 1 accounts with the key {} count: {} from the database.",
                        pubkey, cnt
                    );
                    Err(PostgresRpcServerError::MoreThanOneObjectFound { msg })
                }
            },
        }
    }
}
