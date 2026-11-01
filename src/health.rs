use std::{sync::{Arc, Mutex}, time::{Duration, Instant}};
use bytes::Bytes;
use futures_util::{stream, StreamExt};
use http::Method;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;
use crate::{config::{ChainConfig, Policy, Protocol, UpstreamConfig}, rpc,
    transport::{grpc_status, Transport}, wire::{self, Failure}};

pub struct Endpoint {
    pub config: UpstreamConfig,
    state: Mutex<EndpointState>,
    probe_lock: AsyncMutex<()>,
}
struct EndpointState {
    generation: u64,
    healthy: bool,
    checked: Option<Instant>,
    cooldown_until: Option<Instant>,
    head: Option<u64>,
    last_failure: Option<Failure>,
    successes: u64,
    failures: u64,
    latency_ms: u64,
}
pub struct Chain {
    pub config: ChainConfig,
    pub endpoints: Vec<Arc<Endpoint>>,
}

#[derive(Serialize)]
pub struct EndpointReport {
    pub name: String,
    pub healthy: bool,
    pub head: Option<u64>,
    pub checked_ago_ms: Option<u64>,
    pub cooldown_remaining_ms: u64,
    pub last_failure: Option<Failure>,
    pub successes: u64,
    pub failures: u64,
    pub latency_ms: u64,
}
#[derive(Serialize)]
pub struct ChainReport { pub name: String, pub protocol: Protocol, pub healthy_upstreams: usize, pub endpoints: Vec<EndpointReport> }

impl Endpoint {
    fn new(config: UpstreamConfig) -> Self {
        Self { config, state: Mutex::new(EndpointState { generation: 0, healthy: false,
            checked: None, cooldown_until: None, head: None, last_failure: None,
            successes: 0, failures: 0, latency_ms: 0 }), probe_lock: AsyncMutex::new(()) }
    }
    fn ticket(&self, policy: &Policy) -> Option<u64> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        (state.healthy && state.checked.is_some_and(|at| at.elapsed().as_millis() < u128::from(policy.max_probe_age_ms)))
            .then_some(state.generation)
    }
    pub fn success(&self, generation: u64, latency: Duration, head: Option<u64>) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.generation != generation { return; }
        state.successes += 1;
        state.latency_ms = latency.as_millis() as u64;
        if let Some(head) = head { state.head = Some(head); }
    }
    pub fn failure(&self, generation: u64, failure: Failure, policy: &Policy) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.generation != generation { return; }
        state.generation = state.generation.wrapping_add(1);
        state.healthy = false;
        state.cooldown_until = Some(Instant::now() + Duration::from_millis(policy.cooldown_ms));
        state.last_failure = Some(failure);
        state.failures += 1;
    }
    pub fn report(&self, policy: &Policy) -> EndpointReport {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        EndpointReport { name: self.config.name.clone(), healthy: state.healthy && state.checked.is_some_and(|at|
            at.elapsed().as_millis() < u128::from(policy.max_probe_age_ms)), head: state.head,
            checked_ago_ms: state.checked.map(|at| at.elapsed().as_millis() as u64),
            cooldown_remaining_ms: state.cooldown_until.map_or(0, |at| at.saturating_duration_since(Instant::now()).as_millis() as u64),
            last_failure: state.last_failure, successes: state.successes, failures: state.failures, latency_ms: state.latency_ms }
    }
}
impl Chain {
    pub fn new(config: ChainConfig) -> Self {
        let endpoints = config.upstreams.iter().cloned().map(|config| Arc::new(Endpoint::new(config))).collect();
        Self { config, endpoints }
    }
    pub fn reference_head(&self, policy: &Policy) -> Option<u64> {
        self.endpoints.iter().filter_map(|ep| {
            let state = ep.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.healthy && state.checked.is_some_and(|at| at.elapsed().as_millis() < u128::from(policy.max_probe_age_ms)) { state.head } else { None }
        }).max()
    }
    pub fn report(&self, policy: &Policy) -> ChainReport {
        let mut endpoints: Vec<_> = self.endpoints.iter().map(|ep| ep.report(policy)).collect();
        let highest = self.reference_head(policy);
        for endpoint in &mut endpoints {
            if endpoint.head.zip(highest).is_some_and(|(head, highest)| highest.saturating_sub(head) > self.config.max_head_lag) {
                endpoint.healthy = false;
            }
        }
        ChainReport { name: self.config.name.clone(), protocol: self.config.protocol,
            healthy_upstreams: endpoints.iter().filter(|ep| ep.healthy).count(), endpoints }
    }

    pub async fn ensure(&self, endpoint: &Endpoint, transport: &Transport, policy: &Policy, force_probe: bool) -> Option<u64> {
        if !force_probe {
            if let Some(ticket) = endpoint.ticket(policy) {
                let state = endpoint.state.lock().unwrap_or_else(|e| e.into_inner());
                let head = state.head;
                drop(state);
                if head.zip(self.reference_head(policy)).is_none_or(|(head, highest)| highest.saturating_sub(head) <= self.config.max_head_lag) {
                    return Some(ticket);
                }
            }
        }
        let _probe_guard = endpoint.probe_lock.lock().await;
        let ticket = {
            let state = endpoint.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.cooldown_until.is_some_and(|at| at > Instant::now()) { return None; }
            if !force_probe && state.healthy && state.checked.is_some_and(|at| at.elapsed().as_millis() < u128::from(policy.max_probe_age_ms)) {
                let head = state.head;
                let generation = state.generation;
                drop(state);
                if head.zip(self.reference_head(policy)).is_none_or(|(head, highest)| highest.saturating_sub(head) <= self.config.max_head_lag) { return Some(generation); }
                endpoint.failure(generation, Failure::Stale, policy);
                return None;
            }
            state.generation
        };
        let start = Instant::now();
        let result = tokio::time::timeout(Duration::from_millis(policy.attempt_timeout_ms),
            probe(transport, &self.config, &endpoint.config, self.reference_head(policy))).await
            .unwrap_or(Err(Failure::Timeout));
        match result {
            Ok(head) => {
                let mut state = endpoint.state.lock().unwrap_or_else(|e| e.into_inner());
                // A probe started before a newer failure cannot resurrect stale health.
                if state.generation != ticket { return None; }
                state.healthy = true;
                state.checked = Some(Instant::now());
                state.cooldown_until = None;
                state.last_failure = None;
                state.head = head;
                state.latency_ms = start.elapsed().as_millis() as u64;
                Some(ticket)
            },
            Err(failure) => {
                endpoint.failure(ticket, failure, policy);
                tracing::warn!(chain = %self.config.name, upstream = %endpoint.config.name, ?failure, "upstream probe failed");
                None
            },
        }
    }
}

pub async fn probe_all(chains: &[Arc<Chain>], transport: &Transport, policy: &Policy) {
    let mut tasks = Vec::new();
    for chain in chains {
        for endpoint in &chain.endpoints { tasks.push((chain.clone(), endpoint.clone())); }
    }
    stream::iter(tasks).for_each_concurrent(8, |(chain, endpoint)| {
        let transport = transport.clone();
        let policy = policy.clone();
        async move { chain.ensure(&endpoint, &transport, &policy, true).await; }
    }).await;
}

#[derive(Clone, Copy, Serialize)]
pub enum ProbeMethod {
    #[serde(rename = "eth_chainId")] EvmChainId,
    #[serde(rename = "eth_blockNumber")] EvmHead,
    #[serde(rename = "eth_getBlockByNumber")] EvmBlock,
    #[serde(rename = "eth_getLogs")] EvmLogs,
    #[serde(rename = "getGenesisHash")] SolanaGenesis,
    #[serde(rename = "getSlot")] SolanaSlot,
    #[serde(rename = "getBlock")] SolanaBlock,
}
#[derive(Serialize)]
pub struct ProbeRequest { pub jsonrpc: &'static str, pub id: u64, pub method: ProbeMethod, pub params: Value }
impl ProbeRequest {
    pub fn new(id: u64, method: ProbeMethod, params: Value) -> Self { Self { jsonrpc: "2.0", id, method, params } }
}
pub fn empty_params() -> Value { Value::Array(Vec::new()) }
pub fn probe_bytes<T: Serialize>(value: &T) -> Result<Bytes, Failure> {
    serde_json::to_vec(value).map(Bytes::from).map_err(|_| Failure::Malformed)
}

async fn probe(transport: &Transport, chain: &ChainConfig, endpoint: &UpstreamConfig, reference: Option<u64>) -> Result<Option<u64>, Failure> {
    match chain.protocol {
        Protocol::Evm => {
            let requests = [ProbeRequest::new(1, ProbeMethod::EvmChainId, empty_params()),
                ProbeRequest::new(2, ProbeMethod::EvmHead, empty_params()),
                ProbeRequest::new(3, ProbeMethod::EvmBlock, serde_json::to_value(("latest", false)).map_err(|_| Failure::Malformed)?)];
            let body = probe_bytes(&requests)?;
            let input = rpc::parse_input(&body, chain, 3).map_err(|_| Failure::Malformed)?;
            let response = transport.http(endpoint, Method::POST, "", None, body).await?;
            if !response.status.is_success() { return Err(Failure::HttpStatus); }
            require_probe_success(&response.body)?;
            rpc::validate_response(&response.body, &input, chain, reference).map(|v| v.head)
        },
        Protocol::Solana => {
            let genesis = ProbeRequest::new(1, ProbeMethod::SolanaGenesis, empty_params());
            rpc_probe(transport, chain, endpoint, &genesis, None).await?;
            let slot = ProbeRequest::new(2, ProbeMethod::SolanaSlot, empty_params());
            rpc_probe(transport, chain, endpoint, &slot, reference).await
        },
        Protocol::Aptos => {
            let response = transport.http(endpoint, Method::GET, "v1", None, Bytes::new()).await?;
            if !response.status.is_success() { return Err(Failure::HttpStatus); }
            validate_aptos(&response.body, chain, true, reference)
        },
        Protocol::TonConfig => {
            let response = transport.http(endpoint, Method::GET, "", None, Bytes::new()).await?;
            if !response.status.is_success() { return Err(Failure::HttpStatus); }
            validate_ton(&response.body, chain)?;
            Ok(None)
        },
        Protocol::TronGrpc => {
            let zero = transport.grpc(endpoint, "/protocol.Wallet/GetBlockByNum2", wire::grpc_frame(&[]), Duration::from_secs(3)).await?;
            if grpc_status(&zero)? != 0 { return Err(Failure::GrpcProvider); }
            let fields = wire::protobuf_fields(wire::grpc_payload(&zero.body)?)?;
            if Some(hex::encode(wire::pb_bytes(&fields, 3)?)).as_deref() != chain.genesis.as_deref() { return Err(Failure::WrongNetwork); }
            let latest = transport.grpc(endpoint, "/protocol.Wallet/GetNowBlock2", wire::grpc_frame(&[]), Duration::from_secs(3)).await?;
            if grpc_status(&latest)? != 0 { return Err(Failure::GrpcProvider); }
            let fields = wire::protobuf_fields(wire::grpc_payload(&latest.body)?)?;
            let header = wire::protobuf_fields(wire::pb_bytes(&fields, 2)?)?;
            let raw = wire::protobuf_fields(wire::pb_bytes(&header, 1)?)?;
            let number = wire::pb_number(&raw, 7)?;
            rpc::fresh_timestamp(wire::pb_number(&raw, 1)? / 1000, chain.max_head_age_secs)?;
            if number == 0 || reference.is_some_and(|highest| highest.saturating_sub(number) > chain.max_head_lag) { return Err(Failure::Stale); }
            Ok(Some(number))
        },
    }
}
async fn rpc_probe(transport: &Transport, chain: &ChainConfig, endpoint: &UpstreamConfig,
    request: &ProbeRequest, reference: Option<u64>) -> Result<Option<u64>, Failure> {
    let body = probe_bytes(request)?;
    let input = rpc::parse_input(&body, chain, 1).map_err(|_| Failure::Malformed)?;
    let response = transport.http(endpoint, Method::POST, "", None, body).await?;
    if !response.status.is_success() { return Err(Failure::HttpStatus); }
    require_probe_success(&response.body)?;
    rpc::validate_response(&response.body, &input, chain, reference).map(|v| v.head)
}
fn require_probe_success(body: &[u8]) -> Result<(), Failure> {
    let value: Value = serde_json::from_slice(body).map_err(|_| Failure::Malformed)?;
    let values = match &value { Value::Array(values) => values.as_slice(), other => std::slice::from_ref(other) };
    if values.is_empty() || values.iter().any(|v| v.get("error").is_some() || v.get("result").is_none()) { return Err(Failure::RpcProvider); }
    Ok(())
}

pub fn validate_aptos(body: &[u8], chain: &ChainConfig, root: bool, reference: Option<u64>) -> Result<Option<u64>, Failure> {
    let value: Value = serde_json::from_slice(body).map_err(|_| Failure::Malformed)?;
    if !root {
        if value.is_array() || value.is_object() { return Ok(None); }
        return Err(Failure::Malformed);
    }
    if value.get("chain_id").and_then(Value::as_u64) != chain.chain_id { return Err(Failure::WrongNetwork); }
    let head = value.get("ledger_version").and_then(Value::as_str).and_then(|v| v.parse::<u64>().ok()).ok_or(Failure::Malformed)?;
    let timestamp = value.get("ledger_timestamp").and_then(Value::as_str).and_then(|v| v.parse::<u64>().ok()).ok_or(Failure::Malformed)?;
    rpc::fresh_timestamp(timestamp / 1_000_000, chain.max_head_age_secs)?;
    if head == 0 || reference.is_some_and(|highest| highest.saturating_sub(head) > chain.max_head_lag) { return Err(Failure::Stale); }
    Ok(Some(head))
}
pub fn validate_ton(body: &[u8], chain: &ChainConfig) -> Result<(), Failure> {
    use base64::Engine;
    let value: Value = serde_json::from_slice(body).map_err(|_| Failure::Malformed)?;
    if value.pointer("/validator/zero_state/root_hash").and_then(Value::as_str) != chain.genesis.as_deref() { return Err(Failure::WrongNetwork); }
    let servers = value.get("liteservers").and_then(Value::as_array).ok_or(Failure::Malformed)?;
    if servers.len() < 2 || servers.iter().any(|s| {
        s.get("ip").and_then(Value::as_i64).is_none() ||
        !s.get("port").and_then(Value::as_u64).is_some_and(|port| (1..=65535).contains(&port)) ||
        s.pointer("/id/key").and_then(Value::as_str).and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok()).is_none_or(|key| key.len() != 32)
    }) { return Err(Failure::Malformed); }
    Ok(())
}
