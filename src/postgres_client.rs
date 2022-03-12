pub mod postgres_client_account;

use {
    crate::{
        postgres_rpc_server_config::PostgresRpcServerConfig,
        postgres_rpc_server_error::PostgresRpcServerError,
    },
    chrono::naive::NaiveDateTime,
    log::*,
    solana_sdk::commitment_config::CommitmentLevel,
    std::sync::Mutex,
    tokio_postgres::{
        tls::{NoTls, NoTlsStream},
        Client, Connection, Socket, Statement,
    },
};

/// A Result type.
pub type ServerResult<T> = std::result::Result<T, PostgresRpcServerError>;

const DEFAULT_POSTGRES_PORT: u16 = 5432;

pub struct DbSlotInfo {
    pub slot: i64,
    pub parent: i64,
    pub status: String,
    pub updated_on: NaiveDateTime,
}
struct PostgresSqlClientWrapper {
    client: Client,
    get_account_stmt: Statement,
    get_account_with_commitment_stmt: Statement,
    get_accounts_by_owner_stmt: Statement,
    get_accounts_by_token_owner_stmt: Statement,
    get_accounts_by_token_mint_stmt: Statement,
    get_processed_slot_stmt: Statement,
    get_confirmed_slot_stmt: Statement,
    get_finalized_slot_stmt: Statement,
    get_account_with_commitment_and_slot_stmt: Statement,
}

pub struct SimplePostgresClient {
    client: Mutex<PostgresSqlClientWrapper>,
}

fn get_commitment_level_str(commitment: CommitmentLevel) -> &'static str {
    match commitment {
        CommitmentLevel::Confirmed => "confirmed",
        CommitmentLevel::Finalized => "rooted",
        CommitmentLevel::Processed => "processed",
        _ => "unsupported",
    }
}

impl SimplePostgresClient {
    pub async fn connect_to_db(
        config: &PostgresRpcServerConfig,
    ) -> ServerResult<(Client, Connection<Socket, NoTlsStream>)> {
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

    /// This get the latest slot from slot table at `processed` commitment level.
    async fn build_get_processed_slot_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> ServerResult<Statement> {
        let stmt = "SELECT s.* FROM slot s WHERE s.slot IN (SELECT max(s2.slot) FROM slot AS s2)";
        prepare_statement(stmt, client, config).await
    }

    /// This get the latest slot from slot table at `confirmed` commitment level.
    async fn build_get_confirmed_slot_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> ServerResult<Statement> {
        let stmt = "SELECT s.* FROM slot s WHERE s.slot IN \
            (SELECT max(s2.slot) FROM slot AS s2 WHERE s2.status in ('confirmed', 'rooted'))";
        prepare_statement(stmt, client, config).await
    }

    /// This get the latest slot from slot table at `finalized` commitment level.
    async fn build_get_finalized_slot_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> ServerResult<Statement> {
        let stmt = "SELECT s.* FROM slot s WHERE s.slot IN \
            (SELECT max(s2.slot) FROM slot AS s2 WHERE s2.status = 'rooted')";
        prepare_statement(stmt, client, config).await
    }

    pub async fn new(config: &PostgresRpcServerConfig) -> ServerResult<Self> {
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

        let get_processed_slot_stmt =
            Self::build_get_processed_slot_stmt(&mut client, config).await?;

        let get_confirmed_slot_stmt =
            Self::build_get_confirmed_slot_stmt(&mut client, config).await?;

        let get_finalized_slot_stmt =
            Self::build_get_finalized_slot_stmt(&mut client, config).await?;

        let get_account_with_commitment_and_slot_stmt =
            Self::build_get_account_with_commitment_and_slot_stmt(&mut client, config).await?;

        info!("Created SimplePostgresClient.");
        Ok(Self {
            client: Mutex::new(PostgresSqlClientWrapper {
                client,
                get_account_stmt,
                get_account_with_commitment_stmt,
                get_accounts_by_owner_stmt,
                get_accounts_by_token_owner_stmt,
                get_accounts_by_token_mint_stmt,
                get_processed_slot_stmt,
                get_confirmed_slot_stmt,
                get_finalized_slot_stmt,
                get_account_with_commitment_and_slot_stmt,
            }),
        })
    }

    pub async fn get_last_processed_slot(&mut self) -> ServerResult<DbSlotInfo> {
        let client = self.client.get_mut().unwrap();
        let statement = &client.get_processed_slot_stmt;
        let client = &mut client.client;
        let result = client.query(statement, &[]).await;
        load_single_slot(result)
    }

    pub async fn get_last_confirmed_slot(&mut self) -> ServerResult<DbSlotInfo> {
        let client = self.client.get_mut().unwrap();
        let statement = &client.get_confirmed_slot_stmt;
        let client = &mut client.client;
        let result = client.query(statement, &[]).await;
        load_single_slot(result)
    }

    pub async fn get_last_finalized_slot(&mut self) -> ServerResult<DbSlotInfo> {
        let client = self.client.get_mut().unwrap();
        let statement = &client.get_finalized_slot_stmt;
        let client = &mut client.client;
        let result = client.query(statement, &[]).await;
        load_single_slot(result)
    }
}

/// Load a single slot record
fn load_single_slot(
    result: Result<Vec<postgres::Row>, postgres::Error>,
) -> ServerResult<DbSlotInfo> {
    let mut slots = load_slot_results(result)?;
    match slots.len() {
        0 => {
            let msg = "The slot is not found from the database.".to_string();
            Err(PostgresRpcServerError::ObjectNotFound { msg })
        }
        1 => Ok(slots.remove(0)),
        cnt => {
            let msg = format!(
                "Found more than 1 slots while expecting one, count: {} from the database.",
                cnt
            );
            Err(PostgresRpcServerError::MoreThanOneObjectFound { msg })
        }
    }
}

/// Load a list of DbSlotInfo from a query result.
fn load_slot_results(
    result: Result<Vec<postgres::Row>, postgres::Error>,
) -> ServerResult<Vec<DbSlotInfo>> {
    match result {
        Err(error) => {
            let msg = format!(
                "Failed load the slots from the database. Error: ({:?})",
                error
            );
            Err(PostgresRpcServerError::DatabaseQueryError { msg })
        }
        Ok(result) => {
            let results = result
                .into_iter()
                .map(|row| DbSlotInfo {
                    slot: row.get(0),
                    parent: row.get(1),
                    status: row.get(2),
                    updated_on: row.get(3),
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
) -> ServerResult<Statement> {
    info!("Preparing statement {}", stmt);
    let result = client.prepare(stmt).await;
    info!("Prepared statement, ok? {}", result.is_ok());
    match result {
        Err(err) => {
            return Err(PostgresRpcServerError::DataSchemaError {
                msg: format!(
                    "Error in preparing for the accounts select by token owner for PostgreSQL database: {} host: {:?} user: {:?} config: {:?}, stmt: {}",
                    err, config.host, config.user, config, stmt
                ),
            });
        }
        Ok(statement) => Ok(statement),
    }
}
