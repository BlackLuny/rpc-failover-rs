use std::{sync::Arc, time::{Duration, Instant}};
use anyhow::Result;
use axum::{body::{Body, to_bytes}, extract::State, response::{IntoResponse, Response}, Json, Router};
use http::{header, HeaderValue, Method, Request, StatusCode};
use serde::Serialize;
use tokio::sync::Semaphore;
use crate::{auth::Authenticator, config::{Config, Protocol}, health::{self, Chain, ChainReport}, rpc,
    transport::{grpc_status, provider_http_status, Transport}, wire::{self, Failure, WireResponse}};

pub struct Gateway {
    pub config: Config,
    pub chains: Vec<Arc<Chain>>,
    pub transport: Transport,
    auth: Authenticator,
    permits: Semaphore,
}
#[derive(Serialize)]
pub struct StatusReport {
    pub service: &'static str,
    pub version: &'static str,
    pub ready: bool,
    pub chains: Vec<ChainReport>,
}

impl Gateway {
    pub fn new(config: Config) -> Result<Arc<Self>> {
        config.validate()?;
        let auth = Authenticator::new(&config.auth)?;
        let transport = Transport::new(&config)?;
        let chains = config.chains.iter().cloned().map(|c| Arc::new(Chain::new(c))).collect();
        let permits = Semaphore::new(config.limits.max_concurrent_requests);
        Ok(Arc::new(Self { config, auth, transport, chains, permits }))
    }
    pub fn router(self: &Arc<Self>) -> Router { Router::new().fallback(handle).with_state(self.clone()) }
    pub async fn probe_all(&self) { health::probe_all(&self.chains, &self.transport, &self.config.policy).await; }
    pub fn status(&self) -> StatusReport {
        let chains: Vec<_> = self.chains.iter().map(|c| c.report(&self.config.policy)).collect();
        StatusReport { service: "rpc-failover-rs", version: env!("CARGO_PKG_VERSION"),
            ready: chains.iter().all(|c| c.healthy_upstreams > 0), chains }
    }
    pub fn spawn_probes(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let state = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(state.config.policy.probe_interval_ms));
            interval.tick().await;
            loop { interval.tick().await; state.probe_all().await; }
        })
    }

    async fn execute(&self, request: Request<Body>, deadline: Instant, grpc: bool) -> Response {
        let (parts, body) = request.into_parts();
        let path = parts.uri.path();
        if matches!(path, "/healthz" | "/readyz" | "/status") {
            if parts.method != Method::GET { return wire::error_response(StatusCode::METHOD_NOT_ALLOWED, "GET required"); }
            let report = self.status();
            let status = if path == "/readyz" && !report.ready { StatusCode::SERVICE_UNAVAILABLE } else { StatusCode::OK };
            let mut response = (status, Json(report)).into_response();
            response.headers_mut().insert("cache-control", HeaderValue::from_static("no-store"));
            return response;
        }
        let (chain, suffix) = if grpc {
            let Some(chain) = self.chains.iter().find(|c| c.config.protocol == Protocol::TronGrpc) else { return wire::grpc_error("12", "unknown service"); };
            if !valid_grpc_path(path) || parts.method != Method::POST || parts.uri.query().is_some() {
                return wire::grpc_error("12", "read method not permitted");
            }
            (chain, path)
        } else {
            let rest = path.trim_start_matches('/');
            let (name, suffix) = rest.split_once('/').unwrap_or((rest, ""));
            let Some(chain) = self.chains.iter().find(|c| c.config.name == name) else { return wire::error_response(StatusCode::NOT_FOUND, "unknown chain"); };
            (chain, suffix)
        };
        let body = match to_bytes(body, self.config.limits.max_request_bytes).await {
            Ok(body) => body,
            Err(_) if grpc => return wire::grpc_error("8", "request body limit exceeded"),
            Err(_) => return wire::error_response(StatusCode::PAYLOAD_TOO_LARGE, "request body limit exceeded"),
        };
        let rpc_input = match chain.config.protocol {
            Protocol::Evm | Protocol::Solana => {
                if parts.method != Method::POST || !suffix.is_empty() || parts.uri.query().is_some() {
                    return wire::error_response(StatusCode::BAD_REQUEST, "JSON-RPC requires POST on the chain root");
                }
                match rpc::parse_input(&body, &chain.config, self.config.limits.max_batch_calls) {
                    Ok(input) => Some(input),
                    Err(message) => return wire::error_response(StatusCode::BAD_REQUEST, message),
                }
            },
            Protocol::Aptos => {
                if parts.method != Method::GET || !body.is_empty() || !rpc::valid_aptos_path(suffix, parts.uri.query()) {
                    return wire::error_response(StatusCode::BAD_REQUEST, "Aptos read path or query not permitted");
                }
                None
            },
            Protocol::TonConfig => {
                if parts.method != Method::GET || !suffix.is_empty() || !body.is_empty() || parts.uri.query().is_some() {
                    return wire::error_response(StatusCode::BAD_REQUEST, "TON configuration requires GET on the chain root");
                }
                None
            },
            Protocol::TronGrpc => {
                if !grpc || wire::grpc_payload(&body).is_err() {
                    return wire::grpc_error("3", "uncompressed unary gRPC request required");
                }
                None
            },
        };
        let policy = &self.config.policy;
        let mut attempts = 0usize;
        for endpoint in &chain.endpoints {
            let Some(generation) = chain.ensure(endpoint, &self.transport, policy, false).await else { continue; };
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() { break; }
            let budget = Duration::from_millis(policy.attempt_timeout_ms).min(remaining);
            attempts += 1;
            let start = Instant::now();
            let operation = async {
                let response = if grpc {
                    self.transport.grpc(&endpoint.config, suffix, body.clone(), budget).await?
                } else {
                    self.transport.http(&endpoint.config, parts.method.clone(), suffix, parts.uri.query(), body.clone()).await?
                };
                let head = self.validate_response(&response, chain, rpc_input.as_ref(), suffix, grpc)?;
                Ok::<_, Failure>((response, head))
            };
            match tokio::time::timeout(budget, operation).await.unwrap_or(Err(Failure::Timeout)) {
                Ok((response, head)) => {
                    endpoint.success(generation, start.elapsed(), head);
                    let mut response = response.into_response();
                    if let Ok(name) = HeaderValue::from_str(&endpoint.config.name) { response.headers_mut().insert("x-rpc-upstream", name); }
                    if let Ok(attempts) = HeaderValue::from_str(&attempts.to_string()) { response.headers_mut().insert("x-rpc-attempts", attempts); }
                    return response;
                },
                Err(failure) => {
                    // A caller's shorter deadline does not establish a provider fault.
                    if failure == Failure::Timeout && budget < Duration::from_millis(policy.attempt_timeout_ms) && Instant::now() >= deadline {
                        break;
                    }
                    endpoint.failure(generation, failure, policy);
                    tracing::warn!(chain = %chain.config.name, upstream = %endpoint.config.name, ?failure, "request failed; trying backup");
                },
            }
        }
        if Instant::now() >= deadline {
            return if grpc { wire::grpc_error("4", "deadline exceeded") }
                else { wire::error_response(StatusCode::GATEWAY_TIMEOUT, "request deadline exceeded") };
        }
        if grpc { wire::grpc_error("14", "no healthy upstream available") }
        else { wire::error_response(StatusCode::SERVICE_UNAVAILABLE, "no healthy upstream available") }
    }

    fn validate_response(&self, response: &WireResponse, chain: &Chain, input: Option<&rpc::RpcInput>, suffix: &str, grpc: bool) -> Result<Option<u64>, Failure> {
        if provider_http_status(response.status) { return Err(Failure::HttpStatus); }
        if grpc {
            let status = grpc_status(response)?;
            if matches!(status, 4 | 8 | 12 | 13 | 14 | 16) { return Err(Failure::GrpcProvider); }
            if status == 0 { wire::protobuf_fields(wire::grpc_payload(&response.body)?)?; }
            return Ok(None);
        }
        let reference = chain.reference_head(&self.config.policy);
        if let Some(input) = input {
            if response.status != StatusCode::OK { return Err(Failure::HttpStatus); }
            return rpc::validate_response(&response.body, input, &chain.config, reference).map(|v| v.head);
        }
        match chain.config.protocol {
            Protocol::Aptos => {
                // A valid JSON 404/400 is a caller/not-found result, not a dead node.
                health::validate_aptos(&response.body, &chain.config,
                    response.status.is_success() && suffix.trim_matches('/') == "v1", reference)
            },
            Protocol::TonConfig => {
                if !response.status.is_success() { return Err(Failure::HttpStatus); }
                health::validate_ton(&response.body, &chain.config)?;
                Ok(None)
            },
            _ => Err(Failure::Malformed),
        }
    }
}

pub fn valid_grpc_path(path: &str) -> bool {
    matches!(path, "/protocol.Wallet/GetNowBlock2" | "/protocol.Wallet/GetBlockByNum2" |
        "/protocol.Wallet/GetTransactionById" | "/protocol.Wallet/GetTransactionInfoById")
}

async fn handle(State(gateway): State<Arc<Gateway>>, request: Request<Body>) -> Response {
    let grpc = request.headers().get("content-type").and_then(|v| v.to_str().ok()).is_some_and(|v| v.starts_with("application/grpc"));
    if gateway.auth.authenticate(request.headers()).is_none() {
        if grpc { return wire::grpc_error("16", "authentication required"); }
        let mut response = wire::error_response(StatusCode::UNAUTHORIZED, "authentication required");
        response.headers_mut().insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Basic realm=\"RPC gateway\""));
        return response;
    }
    let _permit = match gateway.permits.try_acquire() {
        Ok(permit) => permit,
        Err(_) if grpc => return wire::grpc_error("8", "concurrency limit exceeded"),
        Err(_) => return wire::error_response(StatusCode::TOO_MANY_REQUESTS, "concurrency limit exceeded"),
    };
    let mut budget = Duration::from_millis(gateway.config.policy.total_timeout_ms);
    if grpc && request.headers().contains_key("grpc-timeout") {
        let Some(timeout) = wire::grpc_timeout(request.headers()) else { return wire::grpc_error("3", "invalid grpc-timeout"); };
        budget = budget.min(timeout);
    }
    if budget.is_zero() { return wire::grpc_error("4", "deadline exceeded"); }
    let deadline = Instant::now() + budget;
    match tokio::time::timeout(budget, gateway.execute(request, deadline, grpc)).await {
        Ok(response) => response,
        Err(_) if grpc => wire::grpc_error("4", "deadline exceeded"),
        Err(_) => wire::error_response(StatusCode::GATEWAY_TIMEOUT, "request deadline exceeded"),
    }
}
