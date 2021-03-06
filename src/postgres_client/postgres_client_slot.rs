use {
    crate::{
        postgres_client::{prepare_statement, AsyncPooledPostgresClient, PreparedQuery,
            ServerResult, SimplePostgresClient},
        postgres_rpc_server_config::PostgresRpcServerConfig,
        postgres_rpc_server_error::PostgresRpcServerError,
    },
    chrono::naive::NaiveDateTime,
    tokio_postgres::{error::Error, Client, Statement},
};

pub struct DbSlotInfo {
    pub slot: i64,
    pub parent: i64,
    pub status: String,
    pub updated_on: NaiveDateTime,
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

impl SimplePostgresClient {
    /// This get the latest slot from slot table at `processed` commitment level.
    pub async fn build_get_processed_slot_stmt(
        client: &Client,
        config: &PostgresRpcServerConfig,
    ) -> Result<Statement, Error> {
        let stmt = "SELECT s.* FROM slot s WHERE s.slot IN (SELECT max(s2.slot) FROM slot AS s2)";
        prepare_statement(stmt, client, config).await
    }

    /// This get the latest slot from slot table at `confirmed` commitment level.
    pub async fn build_get_confirmed_slot_stmt(
        client: &Client,
        config: &PostgresRpcServerConfig,
    ) -> Result<Statement, Error> {
        let stmt = "SELECT s.* FROM slot s WHERE s.slot IN \
            (SELECT max(s2.slot) FROM slot AS s2 WHERE s2.status in ('confirmed', 'rooted'))";
        prepare_statement(stmt, client, config).await
    }

    /// This get the latest slot from slot table at `finalized` commitment level.
    pub async fn build_get_finalized_slot_stmt(
        client: &Client,
        config: &PostgresRpcServerConfig,
    ) -> Result<Statement, Error> {
        let stmt = "SELECT s.* FROM slot s WHERE s.slot IN \
            (SELECT max(s2.slot) FROM slot AS s2 WHERE s2.status = 'rooted')";
        prepare_statement(stmt, client, config).await
    }
}

impl AsyncPooledPostgresClient {

    pub async fn get_last_processed_slot(&self) -> ServerResult<DbSlotInfo> {
        let client = self.pool.get().await?;
        let statement = client.get_prepared_query(PreparedQuery::GetProcessedSlot).unwrap();
        let result = client.query(statement, &[]).await;
        load_single_slot(result)
    }

    pub async fn get_last_confirmed_slot(&self) -> ServerResult<DbSlotInfo> {
        let client = self.pool.get().await?;
        let statement = client.get_prepared_query(PreparedQuery::GetConfirmedSlot).unwrap();
        let result = client.query(statement, &[]).await;
        load_single_slot(result)
    }

    pub async fn get_last_finalized_slot(&self) -> ServerResult<DbSlotInfo> {
        let client = self.pool.get().await?;
        let statement = client.get_prepared_query(PreparedQuery::GetFinalizedSlot).unwrap();
        let result = client.query(statement, &[]).await;
        load_single_slot(result)
    }
}
