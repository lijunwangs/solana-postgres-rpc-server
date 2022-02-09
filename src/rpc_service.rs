use {
    crate::{
        postgres_client::SimplePostgresClient,
        request_processor::JsonRpcRequestProcessor,
        rpc::{
            rpc_accounts::{self, *},
            MAX_REQUEST_PAYLOAD_SIZE,
        },
    },
    jsonrpc_core::MetaIoHandler,
    jsonrpc_http_server::{
        hyper, AccessControlAllowOrigin, DomainsValidation, RequestMiddleware,
        RequestMiddlewareAction, ServerBuilder,
    },
    log::*,
    solana_perf::thread::renice_this_thread,
    std::{
        net::SocketAddr,
        sync::Arc,
        thread::{self, Builder, JoinHandle},
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
struct RpcRequestMiddleware;

impl RpcRequestMiddleware {
    fn new() -> Self {
        Self {}
    }

    #[allow(dead_code)]
    fn internal_server_error() -> hyper::Response<hyper::Body> {
        hyper::Response::builder()
            .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
            .body(hyper::Body::empty())
            .unwrap()
    }
}

impl RequestMiddleware for RpcRequestMiddleware {
    fn on_request(&self, request: hyper::Request<hyper::Body>) -> RequestMiddlewareAction {
        trace!("request uri: {}", request.uri());
        request.into()
    }
}

#[allow(unused_variables)]
impl JsonRpcService {
    pub fn new(
        rpc_addr: SocketAddr,
        config: JsonRpcConfig,
        db_client: SimplePostgresClient,
    ) -> Self {
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
        let request_processor = JsonRpcRequestProcessor::new(config, db_client);

        let thread_hdl = Builder::new()
            .name("solana-jsonrpc".to_string())
            .spawn(move || {
                renice_this_thread(rpc_niceness_adj).unwrap();
                let mut io = MetaIoHandler::default();
                io.extend_with(rpc_accounts::AccountsDataImpl.to_delegate());
                let request_middleware = RpcRequestMiddleware::new();

                let server = ServerBuilder::with_meta_extractor(
                    io,
                    move |_req: &hyper::Request<hyper::Body>| request_processor.clone(),
                )
                .event_loop_executor(runtime.handle().clone())
                .threads(1)
                .cors(DomainsValidation::AllowOnly(vec![
                    AccessControlAllowOrigin::Any,
                ]))
                .cors_max_age(86400)
                .request_middleware(request_middleware)
                .max_request_body_size(MAX_REQUEST_PAYLOAD_SIZE)
                .start_http(&rpc_addr);

                if let Err(e) = server {
                    warn!(
                        "JSON RPC service unavailable error: {:?}. \n\
                           Also, check that port {} is not already in use by another application",
                        e,
                        rpc_addr.port()
                    );
                    return;
                }
                let server = server.unwrap();
                server.wait();
            })
            .unwrap();

        Self { thread_hdl }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
