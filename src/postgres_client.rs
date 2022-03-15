pub mod postgres_client_account;
pub mod postgres_client_slot;

use {
    crate::{
        postgres_rpc_server_config::PostgresRpcServerConfig,
        postgres_rpc_server_error::PostgresRpcServerError,
    },
    bb8::{Pool, RunError},
    bb8_postgres::PostgresConnectionManager,
    log::*,
    solana_sdk::commitment_config::CommitmentLevel,
    std::error,
    tokio::sync::RwLock,
    tokio_postgres::{tls::NoTls, Client, Statement},
};

/// A Result type.
pub type ServerResult<T> = std::result::Result<T, PostgresRpcServerError>;

const DEFAULT_POSTGRES_PORT: u16 = 5432;

struct PostgresSqlClientWrapper {
    client: Pool<PostgresConnectionManager<NoTls>>,
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
    client: RwLock<PostgresSqlClientWrapper>,
}

fn get_commitment_level_str(commitment: CommitmentLevel) -> &'static str {
    match commitment {
        CommitmentLevel::Confirmed => "confirmed",
        CommitmentLevel::Finalized => "rooted",
        CommitmentLevel::Processed => "processed",
        _ => "unsupported",
    }
}

impl<E> From<RunError<E>> for PostgresRpcServerError
where
    E: error::Error + 'static,
{
    fn from(err: RunError<E>) -> Self {
        match err {
            RunError::User(ref err) => {
                let msg = format!("Error in communicating to the database: {}", err);
                PostgresRpcServerError::DataStoreConnectionError { msg }
            }
            RunError::TimedOut => {
                let msg = "Timed out in communicating to the database".to_string();
                PostgresRpcServerError::DataStoreConnectionError { msg }
            }
        }
    }
}

impl SimplePostgresClient {
    pub async fn connect_to_db(
        config: &PostgresRpcServerConfig,
    ) -> ServerResult<Pool<PostgresConnectionManager<NoTls>>> {
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

        let connection_mgr =
            PostgresConnectionManager::new_from_stringlike(&connection_str, NoTls).unwrap();

        match Pool::builder().build(connection_mgr).await {
            Ok(pool) => Ok(pool),
            Err(err) => {
                let msg = format!(
                    "Error in connecting database \"connection_str\": {:?}, or \"host\": {:?} \"user\": {:?}: {}",
                    config.connection_str, config.host, config.user, err
                );
                Err(PostgresRpcServerError::DataStoreConnectionError { msg })
            }
        }
    }

    pub async fn new(config: &PostgresRpcServerConfig) -> ServerResult<Self> {
        info!("Creating SimplePostgresClient...");
        let pool = Self::connect_to_db(config).await?;

        let clone = pool.clone();
        let client = pool.get().await?;

        let get_account_stmt = Self::build_get_account_stmt(&client, config).await?;
        let get_account_with_commitment_stmt =
            Self::build_get_account_with_commitment_stmt(&client, config).await?;

        let get_accounts_by_owner_stmt =
            Self::build_get_accounts_by_owner_stmt(&client, config).await?;

        let get_accounts_by_token_owner_stmt =
            Self::build_get_accounts_by_spl_token_owner_stmt(&client, config).await?;

        let get_accounts_by_token_mint_stmt =
            Self::build_get_accounts_by_spl_token_mint_stmt(&client, config).await?;

        let get_processed_slot_stmt = Self::build_get_processed_slot_stmt(&client, config).await?;

        let get_confirmed_slot_stmt = Self::build_get_confirmed_slot_stmt(&client, config).await?;

        let get_finalized_slot_stmt = Self::build_get_finalized_slot_stmt(&client, config).await?;

        let get_account_with_commitment_and_slot_stmt =
            Self::build_get_account_with_commitment_and_slot_stmt(&client, config).await?;

        info!("Created SimplePostgresClient.");
        Ok(Self {
            client: RwLock::new(PostgresSqlClientWrapper {
                client: clone,
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
}

async fn prepare_statement(
    stmt: &str,
    client: &Client,
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
