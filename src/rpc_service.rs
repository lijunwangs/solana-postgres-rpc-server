use {
    crate::{
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

struct RpcRequestMiddleware;

impl RpcRequestMiddleware {
    fn new() -> Self {
        Self {}
    }

    fn redirect(location: &str) -> hyper::Response<hyper::Body> {
        hyper::Response::builder()
            .status(hyper::StatusCode::SEE_OTHER)
            .header(hyper::header::LOCATION, location)
            .body(hyper::Body::from(String::from(location)))
            .unwrap()
    }

    fn not_found() -> hyper::Response<hyper::Body> {
        hyper::Response::builder()
            .status(hyper::StatusCode::NOT_FOUND)
            .body(hyper::Body::empty())
            .unwrap()
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
    pub fn new(rpc_addr: SocketAddr, config: JsonRpcConfig) -> Self {
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
        let request_processor = JsonRpcRequestProcessor::new(config);

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
