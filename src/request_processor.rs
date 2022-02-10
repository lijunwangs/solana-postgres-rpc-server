use {
    crate::{
        postgres_client::SimplePostgresClient, rpc::OptionalContext, rpc_service::JsonRpcConfig,
    },
    jsonrpc_core::{futures::lock::Mutex, types::error, types::Error, Metadata, Result},
    log::*,
    solana_account_decoder::{UiAccount, UiAccountEncoding, UiDataSliceConfig, MAX_BASE58_BYTES},
    solana_client::{
        rpc_config::RpcAccountInfoConfig,
        rpc_filter::RpcFilterType,
        rpc_response::{Response as RpcResponse, *},
    },
    solana_sdk::{account::ReadableAccount, pubkey::Pubkey},
    std::sync::Arc,
};

#[derive(Clone)]
pub struct JsonRpcRequestProcessor {
    pub config: JsonRpcConfig,
    pub db_client: Arc<Mutex<SimplePostgresClient>>,
}

impl Metadata for JsonRpcRequestProcessor {}

fn encode_account<T: ReadableAccount>(
    account: &T,
    pubkey: &Pubkey,
    encoding: UiAccountEncoding,
    data_slice: Option<UiDataSliceConfig>,
) -> Result<UiAccount> {
    if (encoding == UiAccountEncoding::Binary || encoding == UiAccountEncoding::Base58)
        && account.data().len() > MAX_BASE58_BYTES
    {
        let message = format!("Encoded binary (base 58) data should be less than {} bytes, please use Base64 encoding.", MAX_BASE58_BYTES);
        Err(error::Error {
            code: error::ErrorCode::InvalidRequest,
            message,
            data: None,
        })
    } else {
        Ok(UiAccount::encode(
            pubkey, account, encoding, None, data_slice,
        ))
    }
}

#[allow(unused_variables)]
impl JsonRpcRequestProcessor {
    pub fn new(config: JsonRpcConfig, db_client: SimplePostgresClient) -> Self {
        Self {
            config,
            db_client: Arc::new(Mutex::new(db_client)),
        }
    }

    pub async fn get_account_info(
        &mut self,
        pubkey: &Pubkey,
        config: Option<RpcAccountInfoConfig>,
    ) -> Result<RpcResponse<Option<UiAccount>>> {
        info!("getting account_info is called... {}", pubkey);
        let mut client = self.db_client.lock().await;
        let result = client.get_account(pubkey).await;
        match result {
            Ok(account) => {
                let config = config.unwrap_or_default();
                let encoding = config.encoding.unwrap_or(UiAccountEncoding::Binary);
                let data_slice_config = config.data_slice;

                Ok(RpcResponse {
                    context: RpcResponseContext {
                        slot: account.slot as u64,
                    },
                    value: Some(UiAccount::encode(
                        &account.pubkey,
                        &account,
                        encoding,
                        None,
                        data_slice_config,
                    )),
                })
            }
            Err(err) => Err(Error::internal_error()),
        }
    }

    #[allow(unused_mut)]
    pub fn get_multiple_accounts(
        &self,
        pubkeys: Vec<Pubkey>,
        config: Option<RpcAccountInfoConfig>,
    ) -> Result<RpcResponse<Vec<Option<UiAccount>>>> {
        Err(Error::internal_error())
    }

    #[allow(unused_mut)]
    pub async fn get_program_accounts(
        &self,
        program_id: &Pubkey,
        config: Option<RpcAccountInfoConfig>,
        mut filters: Vec<RpcFilterType>,
        with_context: bool,
    ) -> Result<OptionalContext<Vec<RpcKeyedAccount>>> {
        info!("get_program_accounts is called... {}", program_id);
        let mut client = self.db_client.lock().await;
        let result = client.get_accounts_by_owner(program_id).await;
        match result {
            Ok(accounts) => {
                let config = config.unwrap_or_default();
                let encoding = config.encoding.unwrap_or(UiAccountEncoding::Binary);
                let data_slice_config = config.data_slice;
                let result = accounts
                    .into_iter()
                    .map(|account| {
                        Ok(RpcKeyedAccount {
                            pubkey: account.pubkey.to_string(),
                            account: encode_account(
                                &account,
                                &account.pubkey,
                                encoding,
                                data_slice_config,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(result).map(|result| OptionalContext::NoContext(result))
            }
            Err(err) => Err(Error::internal_error()),
        }
    }
}
