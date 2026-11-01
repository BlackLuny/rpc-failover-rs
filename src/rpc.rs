use std::collections::HashSet;
use serde::Deserialize;
use serde_json::Value;
use crate::{config::{ChainConfig, Protocol}, wire::Failure};

#[derive(Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub id: Value,
    pub method: String,
    #[serde(default)] pub params: Value,
}
pub struct RpcInput { pub requests: Vec<RpcRequest>, pub batch: bool }

const EVM_READ: &[&str] = &[
    "eth_chainId", "eth_blockNumber", "eth_getBlockByNumber", "eth_getBlockByHash",
    "eth_getLogs", "eth_getTransactionReceipt", "eth_getTransactionByHash", "eth_call",
    "eth_getBalance", "eth_getCode", "eth_getStorageAt", "eth_getTransactionCount",
    "eth_getProof", "eth_gasPrice", "eth_feeHistory", "eth_maxPriorityFeePerGas",
    "eth_estimateGas", "net_version", "web3_clientVersion",
];
const SOLANA_READ: &[&str] = &[
    "getSlot", "getBlock", "getBlockHeight", "getSignatureStatuses", "getTransaction",
    "getGenesisHash", "getHealth", "getVersion", "getAccountInfo", "getMultipleAccounts",
    "getBalance", "getTokenAccountBalance", "getLatestBlockhash", "getBlocks",
    "getFirstAvailableBlock", "minimumLedgerSlot", "getSignaturesForAddress",
];

pub fn parse_input(bytes: &[u8], chain: &ChainConfig, limit: usize) -> Result<RpcInput, &'static str> {
    let value: Value = serde_json::from_slice(bytes).map_err(|_| "invalid JSON request")?;
    let batch = value.is_array();
    let entries = match value { Value::Array(v) => v, v @ Value::Object(_) => vec![v], _ => return Err("invalid JSON-RPC envelope") };
    if entries.is_empty() || entries.len() > limit { return Err("invalid JSON-RPC batch size"); }
    let mut ids = HashSet::new();
    let mut requests = Vec::with_capacity(entries.len());
    for entry in entries {
        let request: RpcRequest = serde_json::from_value(entry).map_err(|_| "JSON-RPC requires an ID and method")?;
        if request.jsonrpc != "2.0" || !(request.id.is_string() || request.id.as_i64().is_some() || request.id.as_u64().is_some()) {
            return Err("invalid JSON-RPC version or request ID");
        }
        if !ids.insert(request.id.to_string()) { return Err("duplicate JSON-RPC request ID"); }
        let defaults = if chain.protocol == Protocol::Evm { EVM_READ } else { SOLANA_READ };
        let method = request.method.as_str();
        if method.len() > 100 || !method.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') ||
            !(defaults.contains(&method) || chain.extra_read_methods.iter().any(|m| m == method)) ||
            method.starts_with("eth_send") || method.starts_with("personal_") || method.starts_with("admin_") ||
            method.starts_with("send") || method.starts_with("requestAirdrop") {
            return Err("RPC method is not permitted by the read-only policy");
        }
        if !request.params.is_null() && !request.params.is_array() && !request.params.is_object() { return Err("invalid JSON-RPC params"); }
        requests.push(request);
    }
    Ok(RpcInput { requests, batch })
}

pub struct RpcOutcome { pub head: Option<u64> }
pub fn validate_response(body: &[u8], input: &RpcInput, chain: &ChainConfig,
    reference_head: Option<u64>) -> Result<RpcOutcome, Failure> {
    let value: Value = serde_json::from_slice(body).map_err(|_| Failure::Malformed)?;
    let entries = match value {
        Value::Array(v) if input.batch => v,
        v @ Value::Object(_) if !input.batch => vec![v],
        _ => return Err(Failure::Malformed),
    };
    if entries.len() != input.requests.len() { return Err(Failure::Malformed); }
    let mut seen = HashSet::new();
    let mut head = None;
    for entry in entries {
        let id = entry.get("id").ok_or(Failure::Malformed)?;
        if !seen.insert(id.to_string()) || entry.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Err(Failure::Malformed);
        }
        let request = input.requests.iter().find(|r| &r.id == id).ok_or(Failure::Malformed)?;
        if let Some(error) = entry.get("error") {
            if entry.get("result").is_some() { return Err(Failure::Malformed); }
            let code = error.get("code").and_then(Value::as_i64).ok_or(Failure::Malformed)?;
            let message = error.get("message").and_then(Value::as_str).ok_or(Failure::Malformed)?;
            let caller_or_business = matches!(code, -32600 | -32602 | 3) ||
                (chain.protocol == Protocol::Solana && matches!(code, -32007 | -32009)) ||
                (code == -32000 && message.to_ascii_lowercase().contains("execution reverted"));
            if !caller_or_business { return Err(Failure::RpcProvider); }
            continue;
        }
        let result = entry.get("result").ok_or(Failure::Malformed)?;
        match request.method.as_str() {
            "eth_chainId" => if hex_number(result)? != chain.chain_id.ok_or(Failure::WrongNetwork)? { return Err(Failure::WrongNetwork); },
            "getGenesisHash" => if result.as_str() != chain.genesis.as_deref() { return Err(Failure::WrongNetwork); },
            "eth_blockNumber" => { head = Some(hex_number(result)?); },
            "getSlot" => { head = Some(result.as_u64().ok_or(Failure::Malformed)?); },
            "getBlockHeight" => { result.as_u64().ok_or(Failure::Malformed)?; },
            "eth_getBlockByNumber" => {
                if result.is_null() {
                    let requested = request.params.get(0).and_then(Value::as_str).and_then(parse_hex);
                    if requested.zip(reference_head).is_some_and(|(n, h)| n <= h) { return Err(Failure::RpcProvider); }
                } else {
                    let number = hex_number(result.get("number").ok_or(Failure::Malformed)?)?;
                    if request.params.get(0).and_then(Value::as_str) == Some("latest") {
                        head = Some(number);
                        fresh_timestamp(hex_number(result.get("timestamp").ok_or(Failure::Malformed)?)?, chain.max_head_age_secs)?;
                    } else if let Some(requested) = request.params.get(0).and_then(Value::as_str).and_then(parse_hex) {
                        if number != requested { return Err(Failure::Malformed); }
                    }
                }
            },
            "eth_getLogs" => if !result.is_array() { return Err(Failure::Malformed); },
            "getHealth" if result.as_str() != Some("ok") => return Err(Failure::RpcProvider),
            _ => {},
        }
    }
    if let Some(head) = head {
        if head == 0 || reference_head.is_some_and(|highest| highest.saturating_sub(head) > chain.max_head_lag) { return Err(Failure::Stale); }
    }
    Ok(RpcOutcome { head })
}

pub fn parse_hex(value: &str) -> Option<u64> {
    let raw = value.strip_prefix("0x")?;
    if raw.is_empty() { return None; }
    u64::from_str_radix(raw, 16).ok()
}
pub fn hex_number(value: &Value) -> Result<u64, Failure> {
    value.as_str().and_then(parse_hex).ok_or(Failure::Malformed)
}
pub fn fresh_timestamp(seconds: u64, max_age: u64) -> Result<(), Failure> {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|_| Failure::Stale)?.as_secs();
    if now.saturating_sub(seconds) > max_age || seconds.saturating_sub(now) > 30 { Err(Failure::Stale) } else { Ok(()) }
}

pub fn valid_aptos_path(suffix: &str, query: Option<&str>) -> bool {
    let path = suffix.trim_matches('/');
    let root = matches!(path, "v1" | "v1/transactions");
    let hash = path.strip_prefix("v1/transactions/by_hash/").is_some_and(|h|
        h.starts_with("0x") && h.len() == 66 && h[2..].bytes().all(|b| b.is_ascii_hexdigit()));
    let version = path.strip_prefix("v1/transactions/by_version/").is_some_and(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()));
    if !(root || hash || version) { return false; }
    query.is_none_or(|q| url::form_urlencoded::parse(q.as_bytes()).all(|(key, value)|
        matches!(key.as_ref(), "start" | "limit") && value.parse::<u64>().is_ok() &&
            (key != "limit" || value.parse::<u64>().is_ok_and(|v| (1..=1000).contains(&v)))))
}
