use std::{collections::{BTreeMap, HashSet}, net::{IpAddr, SocketAddr}, path::Path};
use anyhow::{bail, Context, Result};
use http::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use url::Url;

#[derive(Clone, Copy, Debug, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Protocol { Evm, Solana, Aptos, TonConfig, TronGrpc }

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: SocketAddr,
    pub auth: AuthConfig,
    #[serde(default)] pub limits: Limits,
    #[serde(default)] pub policy: Policy,
    pub tls: Option<TlsConfig>,
    pub chains: Vec<ChainConfig>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig { pub keys: Vec<AuthKey> }
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthKey { pub id: String, pub sha256: String }
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig { pub cert_file: String, pub key_file: String }

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub max_batch_calls: usize,
    pub max_concurrent_requests: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self { max_request_bytes: 1024 * 1024, max_response_bytes: 16 * 1024 * 1024,
            max_batch_calls: 100, max_concurrent_requests: 32 }
    }
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub connect_timeout_ms: u64,
    pub attempt_timeout_ms: u64,
    pub total_timeout_ms: u64,
    pub cooldown_ms: u64,
    pub probe_interval_ms: u64,
    pub max_probe_age_ms: u64,
}
impl Default for Policy {
    fn default() -> Self {
        Self { connect_timeout_ms: 1500, attempt_timeout_ms: 3000, total_timeout_ms: 9000,
            cooldown_ms: 30_000, probe_interval_ms: 20_000, max_probe_age_ms: 60_000 }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    pub name: String,
    pub protocol: Protocol,
    pub chain_id: Option<u64>,
    pub genesis: Option<String>,
    #[serde(default = "default_lag")] pub max_head_lag: u64,
    #[serde(default = "default_age")] pub max_head_age_secs: u64,
    #[serde(default)] pub extra_read_methods: Vec<String>,
    pub upstreams: Vec<UpstreamConfig>,
}
fn default_lag() -> u64 { 64 }
fn default_age() -> u64 { 120 }

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamConfig {
    pub name: String,
    pub url: String,
    #[serde(default)] pub headers: BTreeMap<String, String>,
}

pub fn valid_name(value: &str) -> bool {
    !value.is_empty() && value.len() <= 64 && value.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path).context("cannot read configuration file")?;
        // TOML diagnostics otherwise quote the input, which may contain provider credentials.
        let parsed: Self = toml::from_str(&raw).map_err(|_| anyhow::anyhow!("invalid TOML configuration; check field names and types"))?;
        parsed.validate()?;
        Ok(parsed)
    }

    pub fn validate(&self) -> Result<()> {
        if self.auth.keys.is_empty() { bail!("at least one authentication key is required"); }
        let mut names = HashSet::new();
        let mut hashes = HashSet::new();
        for key in &self.auth.keys {
            if !valid_name(&key.id) || !names.insert(&key.id) { bail!("authentication key IDs must be valid and unique"); }
            if key.sha256.len() != 64 || hex::decode(&key.sha256).map_or(true, |v| v.len() != 32) {
                bail!("authentication digests must contain 64 hexadecimal characters");
            }
            if !hashes.insert(key.sha256.to_ascii_lowercase()) { bail!("authentication digests must be unique"); }
        }
        let private = match self.listen.ip() {
            IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
            IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
        };
        if !private && self.tls.is_none() { bail!("a public or wildcard listener requires TLS; otherwise bind a private address"); }
        if let Some(tls) = &self.tls {
            if tls.cert_file.is_empty() || tls.key_file.is_empty() { bail!("TLS needs a certificate and private key file"); }
        }
        let p = &self.policy;
        if p.connect_timeout_ms == 0 || p.attempt_timeout_ms < p.connect_timeout_ms ||
            p.total_timeout_ms < p.attempt_timeout_ms || p.total_timeout_ms > 120_000 ||
            p.cooldown_ms == 0 || p.probe_interval_ms == 0 || p.max_probe_age_ms < p.probe_interval_ms {
            bail!("invalid timeout, probe-age, or cooldown policy");
        }
        let l = &self.limits;
        if l.max_request_bytes == 0 || l.max_request_bytes > 16 * 1024 * 1024 ||
            l.max_response_bytes == 0 || l.max_response_bytes > 128 * 1024 * 1024 ||
            l.max_batch_calls == 0 || l.max_batch_calls > 1000 ||
            l.max_concurrent_requests == 0 || l.max_concurrent_requests > 1024 {
            bail!("invalid body, batch, or concurrency limits");
        }
        if self.chains.is_empty() { bail!("at least one chain is required"); }
        names.clear();
        let mut tron_count = 0;
        for chain in &self.chains {
            if !valid_name(&chain.name) || matches!(chain.name.as_str(), "healthz" | "readyz" | "status") || !names.insert(&chain.name) { bail!("chain names must be valid, unique, and not reserved health routes"); }
            if chain.upstreams.len() < 2 || chain.upstreams.len() > 16 { bail!("each chain requires between two and sixteen upstreams"); }
            if chain.max_head_age_secs == 0 || chain.max_head_lag == 0 { bail!("chain freshness limits must be positive"); }
            match chain.protocol {
                Protocol::Evm | Protocol::Aptos if chain.chain_id.is_none() => bail!("EVM and Aptos require chain_id"),
                Protocol::Solana | Protocol::TronGrpc | Protocol::TonConfig if chain.genesis.as_deref().is_none_or(str::is_empty) => bail!("Solana, TRON and TON require a pinned genesis identity"),
                _ => {},
            }
            if chain.protocol == Protocol::TronGrpc { tron_count += 1; }
            let mut endpoint_names = HashSet::new();
            let mut endpoint_urls = HashSet::new();
            for endpoint in &chain.upstreams {
                if !valid_name(&endpoint.name) || !endpoint_names.insert(&endpoint.name) { bail!("upstream names must be valid and unique within a chain"); }
                let url = Url::parse(&endpoint.url).map_err(|_| anyhow::anyhow!("invalid upstream URL"))?;
                if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() || url.fragment().is_some() {
                    bail!("upstreams require an absolute HTTP(S) URL without fragments");
                }
                if !url.username().is_empty() || url.password().is_some() { bail!("use environment-backed upstream headers, not credentials in upstream URLs"); }
                if url.scheme() == "http" && chain.protocol != Protocol::TronGrpc &&
                    !matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]" | "::1")) {
                    bail!("public HTTP upstreams are not allowed; use HTTPS");
                }
                if !endpoint_urls.insert(url.to_string()) { bail!("duplicate upstream URL"); }
                endpoint.resolved_headers()?;
            }
        }
        if tron_count > 1 { bail!("only one TRON gRPC chain may own native /protocol.Wallet routes"); }
        Ok(())
    }
}

impl UpstreamConfig {
    pub fn resolved_headers(&self) -> Result<HeaderMap> {
        let mut result = HeaderMap::new();
        for (name, value) in &self.headers {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| anyhow::anyhow!("invalid upstream header name"))?;
            if matches!(name.as_str(), "host" | "content-length" | "connection" | "transfer-encoding" | "te") {
                bail!("upstream framing and routing headers cannot be overridden");
            }
            let secret = matches!(name.as_str(), "authorization" | "x-api-key" | "api-key" | "tron-pro-api-key");
            let value = if let Some(variable) = value.strip_prefix("env:") {
                std::env::var(variable).map_err(|_| anyhow::anyhow!("required upstream header environment variable is missing"))?
            } else {
                if secret { bail!("sensitive upstream headers must reference env:VARIABLE"); }
                value.clone()
            };
            result.insert(name, HeaderValue::from_str(&value).map_err(|_| anyhow::anyhow!("invalid upstream header value"))?);
        }
        Ok(result)
    }
}
