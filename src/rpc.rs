use {
    crate::request_processor::JsonRpcRequestProcessor,
    jsonrpc_core::{futures::future, BoxFuture, Error, Result},
    jsonrpc_derive::rpc,
    log::*,
    serde::{Deserialize, Serialize},
    solana_account_decoder::UiAccount,
    solana_client::{
        rpc_config::*,
        rpc_filter::RpcFilterType,
        rpc_request::{MAX_GET_PROGRAM_ACCOUNT_FILTERS, MAX_MULTIPLE_ACCOUNTS},
        rpc_response::{Response as RpcResponse, *},
    },
    solana_sdk::pubkey::Pubkey,
};

pub const MAX_REQUEST_PAYLOAD_SIZE: usize = 50 * (1 << 10); // 50kB

/// Wrapper for rpc return types of methods that provide responses both with and without context.
/// Main purpose of this is to fix methods that lack context information in their return type,
/// without breaking backwards compatibility.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OptionalContext<T> {
    Context(RpcResponse<T>),
    NoContext(T),
}

fn verify_filter(input: &RpcFilterType) -> Result<()> {
    input
        .verify()
        .map_err(|e| Error::invalid_params(format!("Invalid param: {:?}", e)))
}

fn verify_pubkey(input: &str) -> Result<Pubkey> {
    input
        .parse()
        .map_err(|e| Error::invalid_params(format!("Invalid param: {:?}", e)))
}

pub mod rpc_accounts {
    use super::*;

    #[rpc]
    pub trait AccountsData {
        type Metadata;

        #[rpc(meta, name = "getAccountInfo")]
        fn get_account_info(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            config: Option<RpcAccountInfoConfig>,
        ) -> BoxFuture<Result<RpcResponse<Option<UiAccount>>>>;

        #[rpc(meta, name = "getMultipleAccounts")]
        fn get_multiple_accounts(
            &self,
            meta: Self::Metadata,
            pubkey_strs: Vec<String>,
            config: Option<RpcAccountInfoConfig>,
        ) -> BoxFuture<Result<RpcResponse<Vec<Option<UiAccount>>>>>;

        #[rpc(meta, name = "getProgramAccounts")]
        fn get_program_accounts(
            &self,
            meta: Self::Metadata,
            program_id_str: String,
            config: Option<RpcProgramAccountsConfig>,
        ) -> BoxFuture<Result<OptionalContext<Vec<RpcKeyedAccount>>>>;
    }

    pub struct AccountsDataImpl;

    impl AccountsData for AccountsDataImpl {
        type Metadata = JsonRpcRequestProcessor;

        fn get_account_info(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            config: Option<RpcAccountInfoConfig>,
        ) -> BoxFuture<Result<RpcResponse<Option<UiAccount>>>> {
            info!("get_account_info rpc request received: {:?}", pubkey_str);
            let pubkey = verify_pubkey(&pubkey_str);
            match pubkey {
                Err(err) => Box::pin(future::err(err)),
                Ok(pubkey) => Box::pin(async move { meta.get_account_info(&pubkey, config).await }),
            }
        }

        fn get_multiple_accounts(
            &self,
            meta: Self::Metadata,
            pubkey_strs: Vec<String>,
            config: Option<RpcAccountInfoConfig>,
        ) -> BoxFuture<Result<RpcResponse<Vec<Option<UiAccount>>>>> {
            info!(
                "get_multiple_accounts rpc request received: {:?}",
                pubkey_strs.len()
            );

            let max_multiple_accounts = meta
                .config
                .max_multiple_accounts
                .unwrap_or(MAX_MULTIPLE_ACCOUNTS);
            if pubkey_strs.len() > max_multiple_accounts {
                return Box::pin(future::err(Error::invalid_params(format!(
                    "Too many inputs provided; max {}",
                    max_multiple_accounts
                ))));
            }
            let mut pubkeys = Vec::new();

            for pubkey in pubkey_strs {
                match verify_pubkey(&pubkey) {
                    Err(err) => {
                        return Box::pin(future::err(err));
                    }
                    Ok(pubkey) => {
                        pubkeys.push(pubkey);
                    }
                }
            }
            Box::pin(async move { meta.get_multiple_accounts(pubkeys, config).await })
        }

        fn get_program_accounts(
            &self,
            meta: Self::Metadata,
            program_id_str: String,
            config: Option<RpcProgramAccountsConfig>,
        ) -> BoxFuture<Result<OptionalContext<Vec<RpcKeyedAccount>>>> {
            info!(
                "get_program_accounts rpc request received: {:?}",
                program_id_str
            );
            let result = verify_pubkey(&program_id_str);
            if let Err(err) = result {
                return Box::pin(future::err(err));
            }
            let program_id = result.unwrap();
            let (config, filters, with_context) = if let Some(config) = config {
                (
                    Some(config.account_config),
                    config.filters.unwrap_or_default(),
                    config.with_context.unwrap_or_default(),
                )
            } else {
                (None, vec![], false)
            };
            if filters.len() > MAX_GET_PROGRAM_ACCOUNT_FILTERS {
                let err = Error::invalid_params(format!(
                    "Too many filters provided; max {}",
                    MAX_GET_PROGRAM_ACCOUNT_FILTERS
                ));
                return Box::pin(future::err(err));
            }
            for filter in &filters {
                let result = verify_filter(filter);
                if let Err(err) = result {
                    return Box::pin(future::err(err));
                }
            }
            Box::pin(async move {
                meta.get_program_accounts(&program_id, config, filters, with_context)
                    .await
            })
        }
    }
}
