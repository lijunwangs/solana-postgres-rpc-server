use {
    crate::{
        postgres_client::SimplePostgresClient, rpc::OptionalContext, rpc_service::JsonRpcConfig,
    },
    jsonrpc_core::{futures::lock::Mutex, types::error, types::Error, Metadata, Result},
    log::*,
    solana_account_decoder::{
        parse_token::is_known_spl_token_id, UiAccount, UiAccountEncoding, UiDataSliceConfig,
        MAX_BASE58_BYTES,
    },
    solana_client::{
        rpc_config::RpcAccountInfoConfig,
        rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
        rpc_response::{Response as RpcResponse, *},
    },
    solana_runtime::inline_spl_token::{
        SPL_TOKEN_ACCOUNT_MINT_OFFSET, SPL_TOKEN_ACCOUNT_OWNER_OFFSET,
    },
    solana_sdk::{
        account::ReadableAccount,
        program_pack::Pack,
        pubkey::{Pubkey, PUBKEY_BYTES},
    },
    spl_token::state::Account as TokenAccount,
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

/// Analyze custom filters to determine if the result will be a subset of spl-token accounts by
/// owner.
/// NOTE: `optimize_filters()` should almost always be called before using this method because of
/// the strict match on `MemcmpEncodedBytes::Bytes`.
fn get_spl_token_owner_filter(program_id: &Pubkey, filters: &[RpcFilterType]) -> Option<Pubkey> {
    if !is_known_spl_token_id(program_id) {
        return None;
    }
    let mut data_size_filter: Option<u64> = None;
    let mut owner_key: Option<Pubkey> = None;
    let mut incorrect_owner_len: Option<usize> = None;
    for filter in filters {
        match filter {
            RpcFilterType::DataSize(size) => data_size_filter = Some(*size),
            RpcFilterType::Memcmp(Memcmp {
                offset: SPL_TOKEN_ACCOUNT_OWNER_OFFSET,
                bytes: MemcmpEncodedBytes::Bytes(bytes),
                ..
            }) => {
                if bytes.len() == PUBKEY_BYTES {
                    owner_key = Some(Pubkey::new(bytes));
                } else {
                    incorrect_owner_len = Some(bytes.len());
                }
            }
            _ => {}
        }
    }
    if data_size_filter == Some(TokenAccount::get_packed_len() as u64) {
        if let Some(incorrect_owner_len) = incorrect_owner_len {
            info!(
                "Incorrect num bytes ({:?}) provided for spl_token_owner_filter",
                incorrect_owner_len
            );
        }
        owner_key
    } else {
        debug!("spl_token program filters do not match by-owner index requisites");
        None
    }
}

/// Analyze custom filters to determine if the result will be a subset of spl-token accounts by
/// mint.
/// NOTE: `optimize_filters()` should almost always be called before using this method because of
/// the strict match on `MemcmpEncodedBytes::Bytes`.
fn get_spl_token_mint_filter(program_id: &Pubkey, filters: &[RpcFilterType]) -> Option<Pubkey> {
    if !is_known_spl_token_id(program_id) {
        return None;
    }
    let mut data_size_filter: Option<u64> = None;
    let mut mint: Option<Pubkey> = None;
    let mut incorrect_mint_len: Option<usize> = None;
    for filter in filters {
        match filter {
            RpcFilterType::DataSize(size) => data_size_filter = Some(*size),
            RpcFilterType::Memcmp(Memcmp {
                offset: SPL_TOKEN_ACCOUNT_MINT_OFFSET,
                bytes: MemcmpEncodedBytes::Bytes(bytes),
                ..
            }) => {
                if bytes.len() == PUBKEY_BYTES {
                    mint = Some(Pubkey::new(bytes));
                } else {
                    incorrect_mint_len = Some(bytes.len());
                }
            }
            _ => {}
        }
    }
    if data_size_filter == Some(TokenAccount::get_packed_len() as u64) {
        if let Some(incorrect_mint_len) = incorrect_mint_len {
            info!(
                "Incorrect num bytes ({:?}) provided for spl_token_mint_filter",
                incorrect_mint_len
            );
        }
        mint
    } else {
        debug!("spl_token program filters do not match by-mint index requisites");
        None
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

                Ok(result).map(OptionalContext::NoContext)
            }
            Err(err) => Err(Error::internal_error()),
        }
    }
}
