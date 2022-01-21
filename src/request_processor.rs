use {
    crate::rpc_service::JsonRpcConfig
};

#[derive(Clone)]
pub struct JsonRpcRequestProcessor {
    config: JsonRpcConfig
}

impl JsonRpcRequestProcessor {
    pub fn new(config: JsonRpcConfig) -> Self {
        Self {
            config
        }
    }
}