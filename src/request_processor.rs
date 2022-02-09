use {
    crate::{
        postgres_client::SimplePostgresClient, rpc::OptionalContext, rpc_service::JsonRpcConfig,
    },
    jsonrpc_core::{types::Error, Metadata, Result},
    log::*,
    solana_account_decoder::{UiAccount, UiAccountEncoding},
    solana_client::{
        rpc_config::RpcAccountInfoConfig,
        rpc_filter::RpcFilterType,
        rpc_response::{Response as RpcResponse, *},
    },
    solana_sdk::pubkey::Pubkey,
    std::sync::{Arc, RwLock},
};

#[derive(Clone)]
pub struct JsonRpcRequestProcessor {
    pub config: JsonRpcConfig,
    pub db_client: Arc<RwLock<SimplePostgresClient>>,
}

impl Metadata for JsonRpcRequestProcessor {}

#[allow(unused_variables)]
impl JsonRpcRequestProcessor {
    pub fn new(config: JsonRpcConfig, db_client: SimplePostgresClient) -> Self {
        Self {
            config,
            db_client: Arc::new(RwLock::new(db_client)),
        }
    }

    pub async fn get_account_info(
        &mut self,
        pubkey: &Pubkey,
        config: Option<RpcAccountInfoConfig>,
    ) -> Result<RpcResponse<Option<UiAccount>>> {
        info!("getting account_info is called... {}", pubkey);
        let result = self.db_client.write().unwrap().get_account(pubkey).await;
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
    pub fn get_program_accounts(
        &self,
        program_id: &Pubkey,
        config: Option<RpcAccountInfoConfig>,
        mut filters: Vec<RpcFilterType>,
        with_context: bool,
    ) -> Result<OptionalContext<Vec<RpcKeyedAccount>>> {
        Err(Error::internal_error())
    }
}
