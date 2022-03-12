use {
    crate::{
        postgres_client::{
            get_commitment_level_str, prepare_statement, ServerResult, SimplePostgresClient,
        },
        postgres_rpc_server_config::PostgresRpcServerConfig,
        postgres_rpc_server_error::PostgresRpcServerError,
    },
    chrono::naive::NaiveDateTime,
    log::*,
    solana_sdk::{
        account::ReadableAccount, clock::Epoch, commitment_config::CommitmentLevel, pubkey::Pubkey,
    },
    tokio_postgres::{types::FromSql, Client, Statement},
};

impl Eq for AccountInfo {}

#[derive(Clone, PartialEq, FromSql, Debug)]
#[postgres(name = "account")]
pub struct DbAccountInfo {
    pub pubkey: Vec<u8>,
    pub owner: Vec<u8>,
    pub lamports: i64,
    pub slot: i64,
    pub executable: bool,
    pub rent_epoch: i64,
    pub data: Vec<u8>,
    pub write_version: i64,
    pub updated_on: NaiveDateTime,
}

#[derive(Clone, PartialEq, Debug)]
pub struct AccountInfo {
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub lamports: i64,
    pub slot: i64,
    pub executable: bool,
    pub rent_epoch: i64,
    pub data: Vec<u8>,
    pub write_version: i64,
}

impl ReadableAccount for AccountInfo {
    fn lamports(&self) -> u64 {
        self.lamports as u64
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn owner(&self) -> &Pubkey {
        &self.owner
    }

    fn executable(&self) -> bool {
        self.executable
    }

    fn rent_epoch(&self) -> Epoch {
        self.rent_epoch as u64
    }
}

fn load_account_results(
    result: Result<Vec<postgres::Row>, postgres::Error>,
    owner: &Pubkey,
) -> ServerResult<Vec<AccountInfo>> {
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
                .map(|row| AccountInfo {
                    pubkey: Pubkey::new(row.get(0)),
                    lamports: row.get(3),
                    owner: Pubkey::new(row.get(2)),
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

impl SimplePostgresClient {
    /// This get the latest account from account table.
    pub async fn build_get_account_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> ServerResult<Statement> {
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
    pub async fn build_get_account_with_commitment_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> ServerResult<Statement> {
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

    /// This get the latest account from account table at certain commitment level.
    pub async fn build_get_account_with_commitment_and_slot_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> ServerResult<Statement> {
        let stmt = "SELECT get_account_with_commitment_level_and_slot($1, $2, $3)";
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

    pub async fn build_get_accounts_by_owner_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> ServerResult<Statement> {
        let stmt = "SELECT pubkey, slot, owner, lamports, executable, rent_epoch, data, write_version, updated_on FROM account AS acct \
            WHERE owner = $1 \
            AND slot <= $2";
        prepare_statement(stmt, client, config).await
    }

    pub async fn build_get_accounts_by_spl_token_owner_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> ServerResult<Statement> {
        let stmt = "SELECT acct.pubkey, acct.slot, acct.owner, acct.lamports, acct.executable, acct.rent_epoch, \
            acct.data, acct.write_version, acct.updated_on FROM account AS acct \
            JOIN  spl_token_owner_index AS owner_idx ON acct.pubkey = owner_idx.owner_key \
            WHERE owner_idx.owner_key = $1
            AND owner_idx.slot <= $2";
        prepare_statement(stmt, client, config).await
    }

    pub async fn build_get_accounts_by_spl_token_mint_stmt(
        client: &mut Client,
        config: &PostgresRpcServerConfig,
    ) -> ServerResult<Statement> {
        let stmt = "SELECT acct.pubkey, acct.slot, acct.owner, acct.lamports, acct.executable, acct.rent_epoch, \
            acct.data, acct.write_version, acct.updated_on FROM account AS acct \
            JOIN  spl_token_mint_index AS owner_idx ON acct.pubkey = owner_idx.mint_key \
            WHERE owner_idx.mint_key = $1
            AND owner_idx.slot <= $2";
        prepare_statement(stmt, client, config).await
    }

    /// Get the account with the set commitment at the slot
    /// so that the account is consistent at that slot or an older slot
    /// with the set commitment level.
    pub async fn get_account_with_commitment_and_slot(
        &mut self,
        pubkey: &Pubkey,
        commitment_level: CommitmentLevel,
        slot: i64,
    ) -> ServerResult<AccountInfo> {
        let client = self.client.get_mut().unwrap();
        let commitment_level = get_commitment_level_str(commitment_level);

        let statement = &client.get_account_with_commitment_and_slot_stmt;
        let client = &mut client.client;
        let pubkey_v = pubkey.to_bytes().to_vec();
        let result = client
            .query(statement, &[&pubkey_v, &commitment_level, &slot])
            .await;
        match result {
            Err(error) => {
                let msg = format!(
                    "Failed load the account from the database. Account: {}, Error: ({:?})",
                    pubkey, error
                );
                error!("{}", msg);
                Err(PostgresRpcServerError::DatabaseQueryError { msg })
            }
            Ok(result) => match result.len() {
                0 => {
                    let msg = format!(
                        "The account with key {} is not found from the database.",
                        pubkey
                    );
                    error!("{}", msg);
                    Err(PostgresRpcServerError::ObjectNotFound { msg })
                }
                1 => {
                    let account: DbAccountInfo = result[0].get(0);
                    info!("Loaded account {:?}", account);

                    Ok(AccountInfo {
                        pubkey: Pubkey::new(&account.pubkey),
                        lamports: account.lamports,
                        owner: Pubkey::new(&account.owner),
                        executable: account.executable,
                        rent_epoch: account.rent_epoch,
                        data: account.data.clone(),
                        slot: account.slot,
                        write_version: account.write_version,
                    })
                }
                cnt => {
                    let msg = format!(
                        "Found more than 1 accounts with the key {} count: {} from the database.",
                        pubkey, cnt
                    );
                    error!("{}", msg);
                    Err(PostgresRpcServerError::MoreThanOneObjectFound { msg })
                }
            },
        }
    }

    pub async fn get_accounts_by_owner(
        &mut self,
        slot: i64,
        owner: &Pubkey,
    ) -> ServerResult<Vec<AccountInfo>> {
        let client = self.client.get_mut().unwrap();
        let statement = &client.get_accounts_by_owner_stmt;
        let client = &mut client.client;
        let pubkey_v = owner.to_bytes().to_vec();
        let result = client.query(statement, &[&pubkey_v, &slot]).await;
        load_account_results(result, owner)
    }

    pub async fn get_accounts_by_spl_token_owner(
        &mut self,
        slot: i64,
        owner: &Pubkey,
    ) -> ServerResult<Vec<AccountInfo>> {
        let client = self.client.get_mut().unwrap();
        let statement = &client.get_accounts_by_token_owner_stmt;
        let client = &mut client.client;
        let pubkey_v = owner.to_bytes().to_vec();
        let result = client.query(statement, &[&pubkey_v, &slot]).await;
        load_account_results(result, owner)
    }

    pub async fn get_accounts_by_spl_token_mint(
        &mut self,
        slot: i64,
        owner: &Pubkey,
    ) -> ServerResult<Vec<AccountInfo>> {
        let client = self.client.get_mut().unwrap();
        let statement = &client.get_accounts_by_token_mint_stmt;
        let client = &mut client.client;
        let pubkey_v = owner.to_bytes().to_vec();
        let result = client.query(statement, &[&pubkey_v, &slot]).await;
        load_account_results(result, owner)
    }

    /// Get the latest account regardless its commitment level
    pub async fn get_account(&mut self, pubkey: &Pubkey) -> ServerResult<AccountInfo> {
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
                info!("{}", msg);
                Err(PostgresRpcServerError::DatabaseQueryError { msg })
            }
            Ok(result) => match result.len() {
                0 => {
                    let msg = format!(
                        "The account with key {} is not found from the database.",
                        pubkey
                    );
                    info!("{}", msg);
                    Err(PostgresRpcServerError::ObjectNotFound { msg })
                }
                1 => Ok(AccountInfo {
                    pubkey: Pubkey::new(result[0].get(0)),
                    lamports: result[0].get(3),
                    owner: Pubkey::new(result[0].get(2)),
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
                    info!("{}", msg);
                    Err(PostgresRpcServerError::MoreThanOneObjectFound { msg })
                }
            },
        }
    }

    pub async fn get_account_with_commitment(
        &mut self,
        pubkey: &Pubkey,
        commitment_level: CommitmentLevel,
    ) -> ServerResult<AccountInfo> {
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
                1 => Ok(AccountInfo {
                    pubkey: Pubkey::new(result[0].get(0)),
                    lamports: result[0].get(3),
                    owner: Pubkey::new(result[0].get(2)),
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
}
