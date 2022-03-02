use {
    crate::{
        postgres_rpc_server_config::PostgresRpcServerConfig,
        postgres_rpc_server_error::PostgresRpcServerError,
    },
    log::*,
    solana_sdk::{
        account::ReadableAccount, clock::Epoch, commitment_config::CommitmentLevel, pubkey::Pubkey,
    },
    std::sync::Mutex,
    tokio_postgres::{
        tls::{NoTls, NoTlsStream},
        Client, Connection, Socket, Statement,
    },
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
    get_account_with_commitment_stmt: Statement,
    get_accounts_by_owner_stmt: Statement,
    get_accounts_by_token_owner_stmt: Statement,
    get_accounts_by_token_mint_stmt: Statement,
}

pub struct SimplePostgresClient {
    client: Mutex<PostgresSqlClientWrapper>,
}

fn get_commitment_level_str(commitment: CommitmentLevel) -> &'static str {
    match commitment {
        CommitmentLevel::Confirmed => "confirmed",
        CommitmentLevel::Finalized => "finalized",
        CommitmentLevel::Processed => "processed",
        _ => "unsupported",
    }
}

impl SimplePostgresClient {
    pub async fn connect_to_db(
        config: &PostgresRpcServerConfig,
    ) -> Result<(Client, Connection<Socket, NoTlsStream>), PostgresRpcServerError> {
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
            Ok(result) => Ok(result),
            Err(err) => {
                let msg = format!(
                    "Error in connecting database \"connection_str\": {:?}, or \"host\": {:?} \"user\": {:?}: {}",
                    config.connection_str, config.host, config.user, err
                );
                Err(PostgresRpcServerError::DataStoreConnectionError { msg })
            }
        }
    }

    /// This get the latest account from account table.
    async fn build_get_account_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> Result<Statement, PostgresRpcServerError> {
        let stmt = "SELECT pubkey, slot, owner, lamports, executable, rent_epoch, data, write_version, updated_on FROM account AS acct \
            WHERE pubkey = $1";
        info!("Preparing statement {}", stmt);
        let stmt = client.prepare(stmt).await;
        info!("Prepared statement, ok? {}", stmt.is_ok());

        match stmt {
            Err(err) => {
                return Err(PostgresRpcServerError::DataSchemaError {
                    msg: format!(
                        "Error in preparing for the accounts select by key for PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                        err, config.host, config.user, config
                    ),
                });
            }
            Ok(stmt) => Ok(stmt),
        }
    }

    /// This get the latest account from account table at certain commitment level.
    async fn build_get_account_with_commitment_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> Result<Statement, PostgresRpcServerError> {
        let stmt = "SELECT get_account_with_commitment_level($1, $2)";
        info!("Preparing statement {}", stmt);
        let stmt = client.prepare(stmt).await;
        info!("Prepared statement, ok? {}", stmt.is_ok());

        match stmt {
            Err(err) => {
                return Err(PostgresRpcServerError::DataSchemaError {
                    msg: format!(
                        "Error in preparing for the accounts select by key for PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                        err, config.host, config.user, config
                    ),
                });
            }
            Ok(stmt) => Ok(stmt),
        }
    }

    /// This get the latest slot from slot table at `processed` commitment level.
    async fn build_get_processed_slot_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> Result<Statement, PostgresRpcServerError> {
        let stmt = "SELECT s.* FROM slot s WHERE s.slot IN (SELECT max(s2.slot) FROM slot AS s2)";
        prepare_statement(stmt, client, config).await
    }

    /// This get the latest slot from slot table at `confirmed` commitment level.
    async fn build_get_confirmed_slot_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> Result<Statement, PostgresRpcServerError> {
        let stmt = "SELECT s.* FROM slot s WHERE s.slot IN \
            (SELECT max(s2.slot) FROM slot AS s2 AND s2.status in ('confirmed', 'finalized'))";
        prepare_statement(stmt, client, config).await
    }

    /// This get the latest slot from slot table at `finalized` commitment level.
    async fn build_get_finalized_slot_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> Result<Statement, PostgresRpcServerError> {
        let stmt = "SELECT s.* FROM slot s WHERE s.slot IN \
            (SELECT max(s2.slot) FROM slot AS s2 AND s2.status = 'finalized')";
        prepare_statement(stmt, client, config).await
    }

    async fn build_get_accounts_by_owner_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> Result<Statement, PostgresRpcServerError> {
        let stmt = "SELECT pubkey, slot, owner, lamports, executable, rent_epoch, data, write_version, updated_on FROM account AS acct \
            WHERE owner = $1";
        prepare_statement(stmt, client, config).await
    }

    async fn build_get_accounts_by_spl_token_owner_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> Result<Statement, PostgresRpcServerError> {
        let stmt = "SELECT pubkey, slot, owner, lamports, executable, rent_epoch, data, write_version, updated_on FROM account AS acct \
            JOIN  spl_token_owner_index AS owner_idx ON acct.pubkey = owner_idx.inner_key\
            WHERE owner_idx.owner_key = $1";
        prepare_statement(stmt, client, config).await
    }

    async fn build_get_accounts_by_spl_token_mint_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> Result<Statement, PostgresRpcServerError> {
        let stmt = "SELECT pubkey, slot, owner, lamports, executable, rent_epoch, data, write_version, updated_on FROM account AS acct \
            JOIN  spl_token_mint_index AS owner_idx ON acct.pubkey = owner_idx.inner_key\
            WHERE owner_idx.mint_key = $1";
        prepare_statement(stmt, client, config).await
    }

    pub async fn new(config: &PostgresRpcServerConfig) -> Result<Self, PostgresRpcServerError> {
        info!("Creating SimplePostgresClient...");
        let (mut client, connection) = Self::connect_to_db(config).await?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        let get_account_stmt = Self::build_get_account_stmt(&mut client, config).await?;
        let get_account_with_commitment_stmt =
            Self::build_get_account_with_commitment_stmt(&mut client, config).await?;

        let get_accounts_by_owner_stmt =
            Self::build_get_accounts_by_owner_stmt(&mut client, config).await?;

        let get_accounts_by_token_owner_stmt =
            Self::build_get_accounts_by_spl_token_owner_stmt(&mut client, config).await?;

        let get_accounts_by_token_mint_stmt =
            Self::build_get_accounts_by_spl_token_mint_stmt(&mut client, config).await?;

        info!("Created SimplePostgresClient.");
        Ok(Self {
            client: Mutex::new(PostgresSqlClientWrapper {
                client,
                get_account_stmt,
                get_account_with_commitment_stmt,
                get_accounts_by_owner_stmt,
                get_accounts_by_token_owner_stmt,
                get_accounts_by_token_mint_stmt,
            }),
        })
    }

    pub async fn get_account(
        &mut self,
        pubkey: &Pubkey,
    ) -> Result<DbAccountInfo, PostgresRpcServerError> {
        let client = self.client.get_mut().unwrap();
        let statement = &client.get_account_stmt;
        let client = &mut client.client;
        let pubkey_v = pubkey.to_bytes().to_vec();
        let result = client.query(statement, &[&pubkey_v]).await;
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
                    lamports: result[0].get(3),
                    owner: result[0].get(2),
                    executable: result[0].get(4),
                    rent_epoch: result[0].get(5),
                    data: result[0].get(6),
                    slot: result[0].get(1),
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

    pub async fn get_account_with_commitment(
        &mut self,
        pubkey: &Pubkey,
        commitment_level: CommitmentLevel,
    ) -> Result<DbAccountInfo, PostgresRpcServerError> {
        let client = self.client.get_mut().unwrap();
        let commitment_level = get_commitment_level_str(commitment_level);

        let statement = &client.get_account_with_commitment_stmt;
        let client = &mut client.client;
        let pubkey_v = pubkey.to_bytes().to_vec();
        let result = client
            .query(statement, &[&pubkey_v, &commitment_level])
            .await;
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
                    lamports: result[0].get(3),
                    owner: result[0].get(2),
                    executable: result[0].get(4),
                    rent_epoch: result[0].get(5),
                    data: result[0].get(6),
                    slot: result[0].get(1),
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

    pub async fn get_accounts_by_owner(
        &mut self,
        owner: &Pubkey,
    ) -> Result<Vec<DbAccountInfo>, PostgresRpcServerError> {
        let client = self.client.get_mut().unwrap();
        let statement = &client.get_accounts_by_owner_stmt;
        let client = &mut client.client;
        let pubkey_v = owner.to_bytes().to_vec();
        let result = client.query(statement, &[&pubkey_v]).await;
        load_results(result, owner)
    }

    pub async fn get_accounts_by_spl_token_owner(
        &mut self,
        owner: &Pubkey,
    ) -> Result<Vec<DbAccountInfo>, PostgresRpcServerError> {
        let client = self.client.get_mut().unwrap();
        let statement = &client.get_accounts_by_token_owner_stmt;
        let client = &mut client.client;
        let pubkey_v = owner.to_bytes().to_vec();
        let result = client.query(statement, &[&pubkey_v]).await;
        load_results(result, owner)
    }

    pub async fn get_accounts_by_spl_token_mint(
        &mut self,
        owner: &Pubkey,
    ) -> Result<Vec<DbAccountInfo>, PostgresRpcServerError> {
        let client = self.client.get_mut().unwrap();
        let statement = &client.get_accounts_by_token_mint_stmt;
        let client = &mut client.client;
        let pubkey_v = owner.to_bytes().to_vec();
        let result = client.query(statement, &[&pubkey_v]).await;
        load_results(result, owner)
    }
}

fn load_results(
    result: Result<Vec<postgres::Row>, postgres::Error>,
    owner: &Pubkey,
) -> Result<Vec<DbAccountInfo>, PostgresRpcServerError> {
    match result {
        Err(error) => {
            let msg = format!(
                "Failed load the account from the database. Account: {}, Error: ({:?})",
                owner, error
            );
            Err(PostgresRpcServerError::DatabaseQueryError { msg })
        }
        Ok(result) => {
            let results = result
                .into_iter()
                .map(|row| DbAccountInfo {
                    pubkey: Pubkey::new(row.get(0)),
                    lamports: row.get(3),
                    owner: row.get(2),
                    executable: row.get(4),
                    rent_epoch: row.get(5),
                    data: row.get(6),
                    slot: row.get(1),
                    write_version: row.get(7),
                })
                .collect();
            Ok(results)
        }
    }
}

async fn prepare_statement(
    stmt: &str,
    client: &mut Client,
    config: &PostgresRpcServerConfig,
) -> Result<Statement, PostgresRpcServerError> {
    info!("Preparing statement {}", stmt);
    let stmt = client.prepare(stmt).await;
    info!("Prepared statement, ok? {}", stmt.is_ok());
    match stmt {
        Err(err) => {
            return Err(PostgresRpcServerError::DataSchemaError {
                msg: format!(
                    "Error in preparing for the accounts select by token owner for PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                    err, config.host, config.user, config
                ),
            });
        }
        Ok(stmt) => Ok(stmt),
    }
}
