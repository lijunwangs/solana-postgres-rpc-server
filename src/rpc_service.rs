use {
    crate::{
        request_processor::JsonRpcRequestProcessor,
        rpc::rpc_accounts::{self, *},
    },
    jsonrpc_core::MetaIoHandler,
    log::*,
    std::{
        net::SocketAddr,
        sync::Arc,
        thread::{Builder, JoinHandle}
    },
};

pub struct JsonRpcService {
    thread_hdl: JoinHandle<()>,
}

#[derive(Debug, Default, Clone)]
pub struct JsonRpcConfig {
    pub max_multiple_accounts: Option<usize>,
    pub rpc_threads: usize,
    pub rpc_niceness_adj: i8,
}

/// Adds `adjustment` to the nice value of calling thread. Negative `adjustment` increases priority,
/// positive `adjustment` decreases priority. New thread inherits nice value from current thread
/// when created.
///
/// Fails on non-Linux systems for all `adjustment` values except of zero.
#[cfg(not(target_os = "linux"))]
fn renice_this_thread(adjustment: i8) -> Result<(), String> {
    if adjustment == 0 {
        Ok(())
    } else {
        Err(String::from(
            "Failed to change thread's nice value: only supported on Linux",
        ))
    }
}

#[allow(unused_variables)]
impl JsonRpcService {
    pub fn new(rpc_addr: SocketAddr, config: JsonRpcConfig) {
        info!("rpc bound to {:?}", rpc_addr);
        let rpc_threads = 1.max(config.rpc_threads);
        let rpc_niceness_adj = config.rpc_niceness_adj;

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(rpc_threads)
                .on_thread_start(move || renice_this_thread(rpc_niceness_adj).unwrap())
                .thread_name("sol-rpc-el")
                .enable_all()
                .build()
                .expect("Runtime"),
        );
        let request_processer = JsonRpcRequestProcessor::new(config);

        let thread_hdl = Builder::new()
            .name("solana-jsonrpc".to_string())
            .spawn(move || {
                renice_this_thread(rpc_niceness_adj).unwrap();
                let mut io = MetaIoHandler::default();
                io.extend_with(rpc_accounts::AccountsDataImpl.to_delegate());
            })
            .unwrap();
    }
}