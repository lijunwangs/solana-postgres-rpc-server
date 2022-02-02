use {
    crate::{rpc::OptionalContext, rpc_service::JsonRpcConfig},
    jsonrpc_core::{types::Error, Metadata, Result},
    solana_account_decoder::UiAccount,
    solana_client::{
        rpc_config::RpcAccountInfoConfig,
        rpc_filter::RpcFilterType,
        rpc_response::{Response as RpcResponse, *},
    },
    solana_sdk::pubkey::Pubkey,
};

#[derive(Clone)]
pub struct JsonRpcRequestProcessor {
    pub config: JsonRpcConfig,
}

impl Metadata for JsonRpcRequestProcessor {}

#[allow(unused_variables)]
impl JsonRpcRequestProcessor {
    pub fn new(config: JsonRpcConfig) -> Self {
        Self { config }
    }

    pub fn get_account_info(
        &self,
        pubkey: &Pubkey,
        config: Option<RpcAccountInfoConfig>,
    ) -> Result<RpcResponse<Option<UiAccount>>> {
        Err(Error::internal_error())
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
