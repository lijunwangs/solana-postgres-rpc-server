use std::str::FromStr;

pub mod postgres_client_account;
pub mod postgres_client_slot;

use {
    crate::{
        postgres_rpc_server_config::PostgresRpcServerConfig,
        postgres_rpc_server_error::PostgresRpcServerError,
    },
    async_trait::async_trait,
    bb8::{CustomizeConnection, Pool, PooledConnection, RunError},
    bb8_postgres::PostgresConnectionManager,
    log::*,
    solana_sdk::commitment_config::CommitmentLevel,
    std::{collections::HashMap, error, ops::Deref},
    tokio_postgres::{config::Config, 
        tls::{MakeTlsConnect, NoTls, TlsConnect}, Error, Client, Socket, Statement},
};

/// A Result type.
pub type ServerResult<T> = std::result::Result<T, PostgresRpcServerError>;

const DEFAULT_POSTGRES_PORT: u16 = 5432;

pub struct SimplePostgresClient {
    client: Client,
    prepared_statements: HashMap<PreparedQuery, Statement>,
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

#[derive(Debug, Eq, Hash, Ord, PartialOrd, PartialEq,)]
pub enum PreparedQuery {
    GetAccount,
    GetAccountWithCommitment,
    GetAccountsByOwner,
    GetAccountsByTokenOwner,
    GetAccountsByTokenMint,
    GetProcessedSlot,
    GetConfirmedSlot,
    GetFinalizedSlot,
    GetAccountWithCommitmentAndSlot,
}

async fn prepare_statement(
    stmt: &str,
    client: &Client,
    config: &PostgresRpcServerConfig,
) -> Result<Statement, Error> {
    info!("Preparing statement {}", stmt);
    let result = client.prepare(stmt).await;
    info!("Prepared statement, ok? {}", result.is_ok());
    match result {
        Err(err) => {
            error!(
                "Error in preparing for the accounts select by token owner for PostgreSQL database: {} host: {:?} user: {:?} config: {:?}, stmt: {}",
                err, config.host, config.user, config, stmt);
            Err(err)
        }
        Ok(statement) => Ok(statement),
    }
}

impl SimplePostgresClient {


    pub async fn prepare_statements(&mut self, config: &PostgresRpcServerConfig) -> Result<(), Error> {
        info!("Preparing statements ...");

        let statement = Self::build_get_account_stmt(&self.client, config).await?;
        self.prepared_statements.insert(PreparedQuery::GetAccount, statement);

        let statement =
            Self::build_get_account_with_commitment_stmt(&self.client, config).await?;
        self.prepared_statements.insert(PreparedQuery::GetAccountWithCommitment, statement);

        let statement =
            Self::build_get_accounts_by_owner_stmt(&self.client, config).await?;
        self.prepared_statements.insert(PreparedQuery::GetAccountsByOwner, statement);

        let statement =
            Self::build_get_accounts_by_spl_token_owner_stmt(&self.client, config).await?;
        self.prepared_statements.insert(PreparedQuery::GetAccountsByTokenOwner, statement);

        let statement =
            Self::build_get_accounts_by_spl_token_mint_stmt(&self.client, config).await?;
        self.prepared_statements.insert(PreparedQuery::GetAccountsByTokenMint, statement);

        let statement = Self::build_get_processed_slot_stmt(&self.client, config).await?;
        self.prepared_statements.insert(PreparedQuery::GetProcessedSlot, statement);

        let statement = Self::build_get_confirmed_slot_stmt(&self.client, config).await?;
        self.prepared_statements.insert(PreparedQuery::GetConfirmedSlot, statement);

        let statement = Self::build_get_finalized_slot_stmt(&self.client, config).await?;
        self.prepared_statements.insert(PreparedQuery::GetFinalizedSlot, statement);

        let statement =
            Self::build_get_account_with_commitment_and_slot_stmt(&self.client, config).await?;
        self.prepared_statements.insert(PreparedQuery::GetAccountWithCommitmentAndSlot, statement);


        info!("Prepared statements.");
        Ok(())
    }   


    pub fn new(client: Client) -> Self {
        info!("Creating SimplePostgresClient...");
        Self {
            client,
            prepared_statements: HashMap::default()
        }
    }

    pub fn get_prepared_query(&self, query: PreparedQuery) -> Option<&Statement> {
        self.prepared_statements.get(&query)
    }
}

impl Deref for SimplePostgresClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

#[derive(Debug)]
struct PostgreConnectionCustomizer {
    config: PostgresRpcServerConfig
}

#[async_trait]
impl<'a> CustomizeConnection<SimplePostgresClient, Error> for PostgreConnectionCustomizer {
    async fn on_acquire(&self, conn: &mut SimplePostgresClient) -> Result<(), Error> {
        conn.prepare_statements(&self.config).await
    }
}


pub struct SimplePostgresClientManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    inner: PostgresConnectionManager<Tls>,
}

impl<Tls> SimplePostgresClientManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    pub fn new(config: Config, tls: Tls) -> Self {
        Self {
            inner: PostgresConnectionManager::new(config, tls),
        }
    }
}

#[async_trait]
impl<Tls> bb8::ManageConnection for SimplePostgresClientManager<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Connection = SimplePostgresClient;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.inner.connect().await?;
        Ok(SimplePostgresClient::new(conn))
    }

    async fn is_valid(&self, conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        let client = &conn as &Client;
        client.simple_query("").await.map(|_| ())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        self.inner.has_broken(&mut conn.client)
    }
}


/// An asynchronous PosgreSQL client which has connection pooling
pub struct AsyncPooledPostgresClient {
    pool: Pool<SimplePostgresClientManager<NoTls>>,
}

impl AsyncPooledPostgresClient {
    pub async fn connect_to_db(
        config: &PostgresRpcServerConfig,
    ) -> ServerResult<Pool<SimplePostgresClientManager<NoTls>>> {
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

        let connection_config = Config::from_str(&connection_str).unwrap();

        let connection_mgr =
            SimplePostgresClientManager::new(connection_config, NoTls);

        match Pool::builder()
        .connection_customizer(Box::new(PostgreConnectionCustomizer { config: config.clone() }))
        .build(connection_mgr)
        .await {
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
        info!("Creating AsyncPooledPostgresClient...");
        let pool = Self::connect_to_db(config).await?;

        info!("Created AsyncPooledPostgresClient.");
        Ok(Self {
            pool
        })
    }    
}