use std::{sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}}, time::Duration};
use axum::{body::{Body, to_bytes}, extract::State, response::Response, Router};
use base64::Engine;
use bytes::Bytes;
use http::{HeaderMap, Request, StatusCode};
use http_body_util::BodyExt;
use rpc_failover_rs::{auth::token_digest, config::Config, gateway::Gateway, wire};
use serde::Serialize;
use serde_json::Value;
use tower::ServiceExt;

const TOKEN: &str = "test-only-credential-with-more-than-32-bytes";
#[derive(Clone, Copy)]
enum Mode { Healthy, HttpError, RpcError, WrongId, WrongChain, Stale, Slow, Oversize, GrpcError, CallerError, ProbeError }
struct Mock {
    mode: Mutex<Mode>,
    calls: AtomicUsize,
    headers: Mutex<Vec<HeaderMap>>,
    probe_started: tokio::sync::Notify,
    release_probe: tokio::sync::Notify,
    delay_probe: std::sync::atomic::AtomicBool,
}
impl Mock {
    fn new(mode: Mode) -> Arc<Self> {
        Arc::new(Self { mode: Mutex::new(mode), calls: AtomicUsize::new(0), headers: Mutex::new(Vec::new()),
            probe_started: tokio::sync::Notify::new(), release_probe: tokio::sync::Notify::new(),
            delay_probe: std::sync::atomic::AtomicBool::new(false) })
    }
    fn set(&self, mode: Mode) { *self.mode.lock().unwrap() = mode; }
}
struct Server { url: String, task: tokio::task::JoinHandle<()> }
impl Drop for Server { fn drop(&mut self) { self.task.abort(); } }
async fn spawn_mock(state: Arc<Mock>) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, Router::new().fallback(mock).with_state(state)).await.unwrap(); });
    Server { url, task }
}
#[derive(Serialize)]
struct RpcResponse { jsonrpc: &'static str, id: Value, #[serde(skip_serializing_if = "Option::is_none")] result: Option<Value>, #[serde(skip_serializing_if = "Option::is_none")] error: Option<RpcError> }
#[derive(Serialize)]
struct RpcError { code: i64, message: &'static str }
fn value(raw: &str) -> Value { serde_json::from_str(raw).unwrap() }
fn now() -> u64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() }
fn json_response<T: Serialize>(data: &T) -> Response {
    Response::builder().header("content-type", "application/json").body(Body::from(serde_json::to_vec(data).unwrap())).unwrap()
}
async fn mock(State(state): State<Arc<Mock>>, request: Request<Body>) -> Response {
    state.calls.fetch_add(1, Ordering::Relaxed);
    state.headers.lock().unwrap().push(request.headers().clone());
    let mode = *state.mode.lock().unwrap();
    let path = request.uri().path().to_string();
    let get = request.method() == http::Method::GET;
    let body = to_bytes(request.into_body(), 1024 * 1024).await.unwrap();
    if path.starts_with("/protocol.Wallet/") {
        let zero = path.ends_with("GetBlockByNum2") && body.len() == 5;
        let probe = zero || path.ends_with("GetNowBlock2");
        if !probe && matches!(mode, Mode::Slow) { tokio::time::sleep(Duration::from_millis(180)).await; }
        if !probe && matches!(mode, Mode::GrpcError) { return wire::grpc_error("14", "test unavailable"); }
        let payload = if zero { [vec![26, 32], vec![0; 32]].concat() }
            else if probe {
                let mut raw = vec![8]; varint(now() * 1000, &mut raw);
                raw.push(56); varint(100, &mut raw);
                let mut header = vec![10, raw.len() as u8]; header.extend(raw);
                let mut block = vec![18, header.len() as u8]; block.extend(header); block
            } else {
                let mut data = vec![10]; varint(40_000, &mut data); data.extend(vec![0; 40_000]); data
            };
        let mut trailers = HeaderMap::new(); trailers.insert("grpc-status", "0".parse().unwrap());
        return rpc_failover_rs::wire::WireResponse { status: StatusCode::OK,
            headers: [(http::header::CONTENT_TYPE, "application/grpc".parse().unwrap())].into_iter().collect(),
            trailers: Some(trailers), body: wire::grpc_frame(&payload) }.into_response();
    }
    if path.contains("v1") {
        if matches!(mode, Mode::HttpError) { return Response::builder().status(503).body(Body::empty()).unwrap(); }
        return if path.ends_with("transactions") { json_response(&value("[{\"version\":\"99\"}]")) }
        else { json_response(&value(&format!("{{\"chain_id\":1,\"ledger_version\":\"100\",\"ledger_timestamp\":\"{}\"}}", now() * 1_000_000))) };
    }
    if get {
        let genesis = if matches!(mode, Mode::WrongChain) { "wrong" } else { "F6OpKZKqvqeFp6CQmFomXNMfMj2EnaUSOXN+Mh+wVWk=" };
        return json_response(&value(&format!(r#"{{"validator":{{"zero_state":{{"root_hash":"{genesis}"}}}},"liteservers":[{{"ip":16843009,"port":1234,"id":{{"key":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}}}},{{"ip":33686018,"port":1234,"id":{{"key":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}}}}]}}"#)));
    }
    let parsed: Value = serde_json::from_slice(&body).unwrap();
    let batch = parsed.is_array();
    let entries = if let Value::Array(items) = parsed { items } else { vec![parsed] };
    let probe = entries.iter().any(|v| v["method"] == "eth_chainId");
    if probe && state.delay_probe.swap(false, Ordering::SeqCst) {
        state.probe_started.notify_one(); state.release_probe.notified().await;
    }
    if !probe && matches!(mode, Mode::Slow) { tokio::time::sleep(Duration::from_millis(180)).await; }
    if !probe && matches!(mode, Mode::HttpError) { return Response::builder().status(503).body(Body::from("unavailable")).unwrap(); }
    if !probe && matches!(mode, Mode::Oversize) {
        let stream = futures_util::stream::iter([Ok::<_, std::io::Error>(Bytes::from(vec![b'x'; 2000]))]);
        return Response::new(Body::from_stream(stream));
    }
    let replies: Vec<_> = entries.into_iter().map(|entry| {
        let id = if !probe && matches!(mode, Mode::WrongId) { value("999") } else { entry["id"].clone() };
        let error = if matches!(mode, Mode::ProbeError) { Some(RpcError { code: -32602, message: "invalid health request" }) }
            else if !probe && matches!(mode, Mode::RpcError) { Some(RpcError { code: -32051, message: "API key disabled" }) }
            else if !probe && matches!(mode, Mode::CallerError) { Some(RpcError { code: -32602, message: "invalid params" }) } else { None };
        let result = if error.is_some() { None } else { Some(match entry["method"].as_str().unwrap() {
            "eth_chainId" => value(if matches!(mode, Mode::WrongChain) { "\"0x2\"" } else { "\"0x1\"" }),
            "eth_blockNumber" => value("\"0x64\""),
            "getGenesisHash" => value("\"5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d\""),
            "getSlot" => value("100"),
            "getBlockHeight" => value("1"),
            "eth_getBlockByNumber" => {
                let latest = entry["params"][0] == "latest";
                let timestamp = if latest && !matches!(mode, Mode::Stale) { now() } else { 1 };
                let number = if latest { "0x64" } else { entry["params"][0].as_str().unwrap_or("0x63") };
                value(&format!("{{\"number\":\"{number}\",\"timestamp\":\"0x{timestamp:x}\"}}"))
            },
            "eth_getLogs" => value("[]"),
            _ => Value::Null,
        }) };
        RpcResponse { jsonrpc: "2.0", id, result, error }
    }).collect();
    if batch { json_response(&replies) } else { json_response(&replies[0]) }
}
fn varint(mut value: u64, out: &mut Vec<u8>) { while value >= 128 { out.push((value as u8 & 127) | 128); value >>= 7; } out.push(value as u8); }
fn config_toml(first: &Server, second: &Server, protocol: &str) -> String {
    let identity = match protocol {
        "tron_grpc" => format!("genesis = \"{}\"", "0".repeat(64)),
        "ton_config" => "genesis = \"F6OpKZKqvqeFp6CQmFomXNMfMj2EnaUSOXN+Mh+wVWk=\"".to_owned(),
        "solana" => "genesis = \"5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d\"".to_owned(),
        _ => "chain_id = 1".to_owned(),
    };
    let raw = format!(r#"
listen = "127.0.0.1:0"
[auth]
keys = [{{id = "test", sha256 = "{}"}}]
[policy]
connect_timeout_ms = 40
attempt_timeout_ms = 80
total_timeout_ms = 350
cooldown_ms = 100
probe_interval_ms = 50
max_probe_age_ms = 5000
[[chains]]
name = "test"
protocol = "{protocol}"
{identity}
[[chains.upstreams]]
name = "primary"
url = "{}"
[[chains.upstreams]]
name = "backup"
url = "{}"
"#, token_digest(TOKEN.as_bytes()), first.url, second.url);
    raw
}
fn config(first: &Server, second: &Server, protocol: &str) -> Config { toml::from_str(&config_toml(first, second, protocol)).unwrap() }
async fn setup() -> (Arc<Gateway>, Arc<Mock>, Arc<Mock>, Server, Server) {
    let first = Mock::new(Mode::Healthy); let second = Mock::new(Mode::Healthy);
    let a = spawn_mock(first.clone()).await; let b = spawn_mock(second.clone()).await;
    let gateway = Gateway::new(config(&a, &b, "evm")).unwrap(); gateway.probe_all().await;
    assert!(gateway.status().ready);
    (gateway, first, second, a, b)
}
fn rpc_request(body: &str) -> Request<Body> {
    Request::builder().method("POST").uri("/test").header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::from(body.to_owned())).unwrap()
}
const LOGS: &str = r#"{"jsonrpc":"2.0","id":9007199254740993,"method":"eth_getLogs","params":[{}]}"#;

#[tokio::test]
async fn authentication_is_required_before_body_and_upstream_access() {
    let first = Mock::new(Mode::Healthy); let second = Mock::new(Mode::Healthy);
    let a = spawn_mock(first.clone()).await; let b = spawn_mock(second).await;
    let gateway = Gateway::new(config(&a, &b, "evm")).unwrap();
    for auth in [None, Some("Bearer wrong"), Some("Basic !!!!")] {
        let mut request = Request::builder().uri("/test").method("POST");
        if let Some(auth) = auth { request = request.header("authorization", auth); }
        let response = gateway.router().oneshot(request.body(Body::from(vec![0; 2 * 1024 * 1024])).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    assert_eq!(first.calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn http_and_rpc_errors_wrong_ids_and_timeouts_fail_over() {
    for mode in [Mode::HttpError, Mode::RpcError, Mode::WrongId, Mode::Slow] {
        let (gateway, first, _second, _a, _b) = setup().await;
        first.set(mode);
        let response = gateway.router().oneshot(rpc_request(LOGS)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-rpc-upstream"], "backup");
        let value: Value = serde_json::from_slice(&to_bytes(response.into_body(), 2048).await.unwrap()).unwrap();
        assert_eq!(value["id"].as_u64(), Some(9007199254740993));
        assert!(!gateway.status().chains[0].endpoints[0].healthy);
    }
}

#[tokio::test]
async fn credentials_are_not_forwarded_and_basic_auth_works() {
    let (gateway, first, _second, _a, _b) = setup().await;
    let credentials = base64::engine::general_purpose::STANDARD.encode(format!("bepusdt:{TOKEN}"));
    let request = Request::builder().method("POST").uri("/test").header("authorization", format!("Basic {credentials}"))
        .header("tron-pro-api-key", TOKEN).header("cookie", "private-cookie").body(Body::from(LOGS)).unwrap();
    assert_eq!(gateway.router().oneshot(request).await.unwrap().status(), StatusCode::OK);
    for headers in first.headers.lock().unwrap().iter() {
        assert!(!headers.contains_key("authorization")); assert!(!headers.contains_key("tron-pro-api-key")); assert!(!headers.contains_key("cookie"));
    }
}

#[tokio::test]
async fn mutations_notifications_and_duplicate_ids_never_reach_upstreams() {
    let (gateway, first, _second, _a, _b) = setup().await;
    let baseline = first.calls.load(Ordering::Relaxed);
    for request in [r#"{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":[]}"#,
        r#"{"jsonrpc":"2.0","method":"eth_blockNumber"}"#,
        r#"[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"},{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}]"#] {
        assert_eq!(gateway.router().oneshot(rpc_request(request)).await.unwrap().status(), StatusCode::BAD_REQUEST);
    }
    assert_eq!(first.calls.load(Ordering::Relaxed), baseline);
}

#[tokio::test]
async fn batches_preserve_ids_and_empty_log_results() {
    let (gateway, _, _, _a, _b) = setup().await;
    let request = r#"[{"jsonrpc":"2.0","id":"a","method":"eth_getLogs","params":[{}]},{"jsonrpc":"2.0","id":5,"method":"eth_blockNumber"}]"#;
    let response = gateway.router().oneshot(rpc_request(request)).await.unwrap();
    let value: Value = serde_json::from_slice(&to_bytes(response.into_body(), 2048).await.unwrap()).unwrap();
    assert_eq!(value[0]["id"], "a"); assert!(value[0]["result"].as_array().unwrap().is_empty()); assert_eq!(value[1]["id"], 5);
}

#[tokio::test]
async fn invalid_params_do_not_poison_provider_health() {
    let (gateway, first, _, _a, _b) = setup().await;
    first.set(Mode::CallerError);
    let response = gateway.router().oneshot(rpc_request(LOGS)).await.unwrap();
    assert_eq!(response.headers()["x-rpc-upstream"], "primary");
    assert!(gateway.status().chains[0].endpoints[0].healthy);
}

#[tokio::test]
async fn wrong_network_and_stale_heads_are_excluded() {
    for mode in [Mode::WrongChain, Mode::Stale, Mode::ProbeError] {
        let first = Mock::new(mode); let second = Mock::new(Mode::Healthy);
        let a = spawn_mock(first).await; let b = spawn_mock(second).await;
        let gateway = Gateway::new(config(&a, &b, "evm")).unwrap(); gateway.probe_all().await;
        assert_eq!(gateway.status().chains[0].healthy_upstreams, 1);
        let response = gateway.router().oneshot(rpc_request(LOGS)).await.unwrap();
        assert_eq!(response.headers()["x-rpc-upstream"], "backup");
    }
}

#[tokio::test]
async fn unavailable_network_and_oversized_request_fail_closed() {
    let first = spawn_mock(Mock::new(Mode::WrongChain)).await;
    let second = spawn_mock(Mock::new(Mode::WrongChain)).await;
    let gateway = Gateway::new(config(&first, &second, "evm")).unwrap(); gateway.probe_all().await;
    assert!(!gateway.status().ready);
    assert_eq!(gateway.router().oneshot(rpc_request(LOGS)).await.unwrap().status(), StatusCode::SERVICE_UNAVAILABLE);
    let request = Request::builder().uri("/readyz").header("authorization", format!("Bearer {TOKEN}")).body(Body::empty()).unwrap();
    assert_eq!(gateway.router().oneshot(request).await.unwrap().status(), StatusCode::SERVICE_UNAVAILABLE);
    let (gateway, first, _, _a, _b) = setup().await;
    let baseline = first.calls.load(Ordering::Relaxed);
    let body = "x".repeat(gateway.config.limits.max_request_bytes + 1);
    assert_eq!(gateway.router().oneshot(rpc_request(&body)).await.unwrap().status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(first.calls.load(Ordering::Relaxed), baseline);
}

#[tokio::test]
async fn authenticated_concurrency_is_bounded() {
    let first = Mock::new(Mode::Healthy); let a = spawn_mock(first.clone()).await;
    let b = spawn_mock(Mock::new(Mode::Healthy)).await;
    let mut config = config(&a, &b, "evm"); config.limits.max_concurrent_requests = 1;
    let gateway = Gateway::new(config).unwrap(); gateway.probe_all().await;
    let baseline = first.calls.load(Ordering::Relaxed); first.set(Mode::Slow);
    let other = gateway.clone(); let pending = tokio::spawn(async move { other.router().oneshot(rpc_request(LOGS)).await.unwrap() });
    for _ in 0..50 { if first.calls.load(Ordering::Relaxed) > baseline { break; } tokio::time::sleep(Duration::from_millis(1)).await; }
    assert_eq!(gateway.router().oneshot(rpc_request(LOGS)).await.unwrap().status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(pending.await.unwrap().status(), StatusCode::OK);
}

#[tokio::test]
async fn grpc_caller_deadline_does_not_poison_healthy_upstream() {
    let first = Mock::new(Mode::Healthy); let a = spawn_mock(first.clone()).await;
    let b = spawn_mock(Mock::new(Mode::Healthy)).await;
    let mut config = config(&a, &b, "tron_grpc"); config.policy.attempt_timeout_ms = 1000; config.policy.total_timeout_ms = 2500;
    let gateway = Gateway::new(config).unwrap(); gateway.probe_all().await; first.set(Mode::Slow);
    let request = Request::builder().method("POST").uri("/protocol.Wallet/GetTransactionInfoById")
        .header("content-type", "application/grpc").header("tron-pro-api-key", TOKEN).header("grpc-timeout", "20m")
        .body(Body::from(wire::grpc_frame(&[10, 0]))).unwrap();
    let response = gateway.router().oneshot(request).await.unwrap();
    assert_eq!(response.headers()["grpc-status"], "4");
    assert!(gateway.status().chains[0].endpoints[0].healthy);
}

#[tokio::test]
async fn historical_block_timestamps_are_not_treated_as_stale_heads() {
    let (gateway, _, _, _a, _b) = setup().await;
    let response = gateway.router().oneshot(rpc_request(r#"{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x1",false]}"#)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK); assert_eq!(response.headers()["x-rpc-upstream"], "primary");
}

#[tokio::test]
async fn oversized_chunked_responses_fail_over() {
    let first = Mock::new(Mode::Healthy); let second = Mock::new(Mode::Healthy);
    let a = spawn_mock(first.clone()).await; let b = spawn_mock(second).await;
    let mut cfg = config(&a, &b, "evm"); cfg.limits.max_response_bytes = 1000;
    let gateway = Gateway::new(cfg).unwrap(); gateway.probe_all().await; first.set(Mode::Oversize);
    let response = gateway.router().oneshot(rpc_request(LOGS)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK); assert_eq!(response.headers()["x-rpc-upstream"], "backup");
}

#[tokio::test]
async fn circuits_recover_and_restore_primary_priority() {
    let (gateway, first, _, _a, _b) = setup().await; first.set(Mode::RpcError);
    assert_eq!(gateway.router().oneshot(rpc_request(LOGS)).await.unwrap().headers()["x-rpc-upstream"], "backup");
    first.set(Mode::Healthy); tokio::time::sleep(Duration::from_millis(120)).await;
    gateway.probe_all().await;
    assert_eq!(gateway.router().oneshot(rpc_request(LOGS)).await.unwrap().headers()["x-rpc-upstream"], "primary");
}

#[tokio::test]
async fn old_probe_cannot_resurrect_newer_request_failure() {
    let (gateway, first, _, _a, _b) = setup().await;
    first.delay_probe.store(true, Ordering::SeqCst);
    let probe_gateway = gateway.clone();
    let probe = tokio::spawn(async move { probe_gateway.probe_all().await; });
    first.probe_started.notified().await;
    first.set(Mode::RpcError);
    assert_eq!(gateway.router().oneshot(rpc_request(LOGS)).await.unwrap().headers()["x-rpc-upstream"], "backup");
    first.release_probe.notify_one(); probe.await.unwrap();
    assert!(!gateway.status().chains[0].endpoints[0].healthy);
}

#[tokio::test]
async fn grpc_auth_failover_and_trailers_are_preserved() {
    let first = Mock::new(Mode::Healthy); let second = Mock::new(Mode::Healthy);
    let a = spawn_mock(first.clone()).await; let b = spawn_mock(second).await;
    let mut cfg = config(&a, &b, "tron_grpc");
    cfg.policy.connect_timeout_ms = 200;
    cfg.policy.attempt_timeout_ms = 1000;
    cfg.policy.total_timeout_ms = 2500;
    let gateway = Gateway::new(cfg).unwrap(); gateway.probe_all().await;
    assert!(gateway.status().ready, "{}", serde_json::to_string(&gateway.status()).unwrap()); first.set(Mode::GrpcError);
    let request = Request::builder().method("POST").uri("/protocol.Wallet/GetTransactionInfoById")
        .header("content-type", "application/grpc").header("tron-pro-api-key", TOKEN)
        .body(Body::from(wire::grpc_frame(&[10, 0]))).unwrap();
    let response = gateway.router().oneshot(request).await.unwrap();
    assert_eq!(response.headers()["x-rpc-upstream"], "backup");
    let body = response.into_body().collect().await.unwrap();
    assert_eq!(body.trailers().unwrap()["grpc-status"], "0");
    for headers in first.headers.lock().unwrap().iter() { assert!(!headers.contains_key("tron-pro-api-key")); }
}

#[test]
fn example_configuration_has_auth_and_two_upstreams_per_chain() {
    let cfg: Config = toml::from_str(include_str!("../config.example.toml")).unwrap();
    cfg.validate().unwrap(); assert_eq!(cfg.chains.len(), 11);
    assert!(cfg.chains.iter().all(|chain| chain.upstreams.len() >= 2));
}

#[test]
fn public_listeners_require_tls_and_missing_keys_fail_closed() {
    let mut cfg: Config = toml::from_str(include_str!("../config.example.toml")).unwrap();
    cfg.listen = "0.0.0.0:8547".parse().unwrap(); assert!(cfg.validate().is_err());
    cfg.listen = "127.0.0.1:8547".parse().unwrap(); cfg.auth.keys.clear(); assert!(cfg.validate().is_err());
}

#[test]
fn malformed_grpc_frames_and_timeout_values_are_rejected() {
    assert!(wire::grpc_payload(&[0, 0, 0, 0, 10]).is_err());
    assert!(wire::protobuf_fields(&[255; 12]).is_err());
    let mut headers = HeaderMap::new(); headers.insert("grpc-timeout", "200m".parse().unwrap());
    assert_eq!(wire::grpc_timeout(&headers), Some(Duration::from_millis(200)));
    headers.insert("grpc-timeout", "oops".parse().unwrap()); assert_eq!(wire::grpc_timeout(&headers), None);
}

#[tokio::test]
async fn aptos_rest_failover_preserves_queries_and_rejects_traversal() {
    let first = Mock::new(Mode::Healthy); let a = spawn_mock(first.clone()).await;
    let b = spawn_mock(Mock::new(Mode::Healthy)).await;
    let gateway = Gateway::new(config(&a, &b, "aptos")).unwrap(); gateway.probe_all().await;
    assert!(gateway.status().ready); first.set(Mode::HttpError);
    let request = Request::builder().uri("/test/v1/transactions?start=97&limit=1")
        .header("authorization", format!("Bearer {TOKEN}")).body(Body::empty()).unwrap();
    let response = gateway.router().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK); assert_eq!(response.headers()["x-rpc-upstream"], "backup");
    for path in ["/test/v1/../status", "/test/v1/%2e%2e/status", "/test/v1/transactions?limit=1000000"] {
        let request = Request::builder().uri(path).header("authorization", format!("Bearer {TOKEN}")).body(Body::empty()).unwrap();
        assert_eq!(gateway.router().oneshot(request).await.unwrap().status(), StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn ton_configuration_checks_network_and_fails_over() {
    let first = spawn_mock(Mock::new(Mode::WrongChain)).await;
    let second = spawn_mock(Mock::new(Mode::Healthy)).await;
    let gateway = Gateway::new(config(&first, &second, "ton_config")).unwrap(); gateway.probe_all().await;
    assert_eq!(gateway.status().chains[0].healthy_upstreams, 1);
    let request = Request::builder().uri("/test").header("authorization", format!("Bearer {TOKEN}")).body(Body::empty()).unwrap();
    let response = gateway.router().oneshot(request).await.unwrap();
    assert_eq!(response.headers()["x-rpc-upstream"], "backup");
    let config: Value = serde_json::from_slice(&to_bytes(response.into_body(), 2048).await.unwrap()).unwrap();
    assert_eq!(config["liteservers"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn solana_block_height_is_not_compared_to_slot_height() {
    let a = spawn_mock(Mock::new(Mode::Healthy)).await; let b = spawn_mock(Mock::new(Mode::Healthy)).await;
    let gateway = Gateway::new(config(&a, &b, "solana")).unwrap(); gateway.probe_all().await;
    assert!(gateway.status().ready);
    let response = gateway.router().oneshot(rpc_request(r#"{"jsonrpc":"2.0","id":1,"method":"getBlockHeight","params":[]}"#)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK); assert_eq!(response.headers()["x-rpc-upstream"], "primary");
    assert!(gateway.status().chains[0].endpoints[0].healthy);
}

#[tokio::test]
async fn native_https_authentication_and_http2_grpc_work_end_to_end() {
    use std::{io::Read, process::{Command, Stdio}};
    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard { fn drop(&mut self) { let _ = self.0.kill(); let _ = self.0.wait(); } }
    let folder = tempfile::tempdir().unwrap();
    let cert = folder.path().join("cert.pem"); let key = folder.path().join("key.pem");
    let generated = Command::new("openssl").args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
        "-addext", "basicConstraints=critical,CA:FALSE", "-keyout"])
        .arg(&key).arg("-out").arg(&cert).stdout(Stdio::null()).stderr(Stdio::null()).status().unwrap();
    assert!(generated.success());
    let first = spawn_mock(Mock::new(Mode::Healthy)).await;
    let second = spawn_mock(Mock::new(Mode::Healthy)).await;
    let mut config: toml::Value = toml::from_str(&config_toml(&first, &second, "evm")).unwrap();
    let mut tron: toml::Value = toml::from_str(&config_toml(&first, &second, "tron_grpc")).unwrap();
    tron["chains"][0]["name"] = toml::Value::String("tron".into());
    config["chains"].as_array_mut().unwrap().push(tron["chains"][0].clone());
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reserved.local_addr().unwrap(); drop(reserved);
    config["listen"] = toml::Value::String(address.to_string());
    config["policy"]["connect_timeout_ms"] = toml::Value::Integer(200);
    config["policy"]["attempt_timeout_ms"] = toml::Value::Integer(1000);
    config["policy"]["total_timeout_ms"] = toml::Value::Integer(2500);
    config["policy"]["probe_interval_ms"] = toml::Value::Integer(1000);
    config.as_table_mut().unwrap().insert("tls".into(), toml::Value::Table([
        ("cert_file".into(), toml::Value::String(cert.to_str().unwrap().into())),
        ("key_file".into(), toml::Value::String(key.to_str().unwrap().into())),
    ].into_iter().collect()));
    let path = folder.path().join("config.toml"); std::fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
    let error_path = folder.path().join("server.log"); let errors = std::fs::File::create(&error_path).unwrap();
    let mut child = ChildGuard(Command::new(env!("CARGO_BIN_EXE_rpc-failover-rs")).arg("serve").arg("--config").arg(path)
        .stdout(Stdio::null()).stderr(errors).spawn().unwrap());
    let pem = std::fs::read(&cert).unwrap();
    let client = reqwest::Client::builder().no_proxy().http2_prior_knowledge()
        .add_root_certificate(reqwest::Certificate::from_pem(&pem).unwrap())
        .timeout(Duration::from_millis(300)).build().unwrap();
    let base = format!("https://{address}");
    let mut ready = false;
    let mut last_error = String::new();
    for _ in 0..80 {
        if child.0.try_wait().unwrap().is_some() { break; }
        match client.get(format!("{base}/readyz")).bearer_auth(TOKEN).send().await {
            Ok(response) if response.status() == StatusCode::OK => { ready = true; break; },
            Ok(response) => { last_error = format!("{}", response.status()); },
            Err(error) => { last_error = format!("{error:?}"); },
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut log = String::new(); std::fs::File::open(&error_path).unwrap().read_to_string(&mut log).unwrap();
    assert!(ready, "TLS server failed to become ready: {last_error}; server log: {log}");
    assert_eq!(client.get(format!("{base}/status")).send().await.unwrap().status(), StatusCode::UNAUTHORIZED);
    let response = client.post(format!("{base}/test")).bearer_auth(TOKEN).body(LOGS).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK); assert_eq!(response.version(), http::Version::HTTP_2);
    let result: Value = response.json().await.unwrap(); assert_eq!(result["id"].as_u64(), Some(9007199254740993));
    // Use a real HTTP/2 client so trailers cross both server and proxy transports.
    let mut roots = rustls::RootCertStore::empty();
    let encoded = String::from_utf8(pem).unwrap().lines().filter(|s| !s.starts_with("---")).collect::<String>();
    roots.add(rustls::pki_types::CertificateDer::from(base64::engine::general_purpose::STANDARD.decode(encoded).unwrap())).unwrap();
    let tls = rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    let connector = hyper_rustls::HttpsConnectorBuilder::new().with_tls_config(tls).https_only().enable_http2().build();
    let h2 = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).http2_only(true)
        .build::<_, http_body_util::Full<Bytes>>(connector);
    let request = Request::builder().method("POST").uri(format!("{base}/protocol.Wallet/GetTransactionInfoById"))
        .header("content-type", "application/grpc").header("tron-pro-api-key", TOKEN)
        .body(http_body_util::Full::new(wire::grpc_frame(&[10, 0]))).unwrap();
    let response = h2.request(request).await.unwrap();
    assert!(!response.headers().contains_key("content-length"));
    let result = response.into_body().collect().await.unwrap();
    assert_eq!(result.trailers().unwrap()["grpc-status"], "0");
    assert!(result.to_bytes().len() > 16_384);
}
