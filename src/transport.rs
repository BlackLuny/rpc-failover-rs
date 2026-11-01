use std::time::Duration;
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use http::{Method, Request, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{client::legacy::{Client, connect::HttpConnector}, rt::TokioExecutor};
use reqwest::{redirect::Policy, Url};
use crate::{config::{Config, UpstreamConfig}, wire::{Failure, WireResponse}};

#[derive(Clone)]
pub struct Transport {
    http: reqwest::Client,
    grpc: Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
    max_bytes: usize,
}
impl Transport {
    pub fn new(config: &Config) -> Result<Self> {
        let connect_timeout = Duration::from_millis(config.policy.connect_timeout_ms);
        let http = reqwest::Client::builder().connect_timeout(connect_timeout)
            .redirect(Policy::none()).no_proxy().user_agent("rpc-failover-rs/0.1")
            .pool_idle_timeout(Duration::from_secs(60)).build()?;
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(connect_timeout));
        connector.set_nodelay(true);
        connector.enforce_http(false);
        let tls = HttpsConnectorBuilder::new().with_webpki_roots().https_or_http()
            .enable_http2().wrap_connector(connector);
        let grpc = Client::builder(TokioExecutor::new()).http2_only(true).build(tls);
        Ok(Self { http, grpc, max_bytes: config.limits.max_response_bytes })
    }

    pub async fn http(&self, endpoint: &UpstreamConfig, method: Method, suffix: &str,
        query: Option<&str>, body: Bytes) -> Result<WireResponse, Failure> {
        let mut url = Url::parse(&endpoint.url).map_err(|_| Failure::Malformed)?;
        if !suffix.is_empty() {
            let path = format!("{}/{}", url.path().trim_end_matches('/'), suffix.trim_start_matches('/'));
            url.set_path(&path);
        }
        if let Some(query) = query {
            if let Some(existing) = url.query() { url.set_query(Some(&format!("{existing}&{query}"))); }
            else { url.set_query(Some(query)); }
        }
        let mut request = self.http.request(method, url)
            .headers(endpoint.resolved_headers().map_err(|_| Failure::Malformed)?)
            .header("accept", "application/json");
        if !body.is_empty() { request = request.header("content-type", "application/json").body(body); }
        let mut response = request.send().await.map_err(classify_reqwest)?;
        if response.content_length().is_some_and(|len| len > self.max_bytes as u64) { return Err(Failure::TooLarge); }
        let status = response.status();
        let headers = response.headers().clone();
        let mut buffer = BytesMut::new();
        while let Some(chunk) = response.chunk().await.map_err(classify_reqwest)? {
            if chunk.len() > self.max_bytes.saturating_sub(buffer.len()) { return Err(Failure::TooLarge); }
            buffer.extend_from_slice(&chunk);
        }
        Ok(WireResponse { status, headers, trailers: None, body: buffer.freeze() })
    }

    pub async fn grpc(&self, endpoint: &UpstreamConfig, path: &str, body: Bytes,
        budget: Duration) -> Result<WireResponse, Failure> {
        let mut url = Url::parse(&endpoint.url).map_err(|_| Failure::Malformed)?;
        url.set_path(path);
        let mut request = Request::builder().method(Method::POST).uri(url.as_str())
            .version(http::Version::HTTP_2)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .header("grpc-timeout", format!("{}m", budget.as_millis().max(1)))
            .header("user-agent", "rpc-failover-rs/0.1")
            .body(Full::new(body)).map_err(|_| Failure::Malformed)?;
        request.headers_mut().extend(endpoint.resolved_headers().map_err(|_| Failure::Malformed)?);
        let response = self.grpc.request(request).await.map_err(|_| Failure::Transport)?;
        let status = response.status();
        let headers = response.headers().clone();
        let collected = Limited::new(response.into_body(), self.max_bytes).collect().await
            .map_err(|_| Failure::TooLarge)?;
        let trailers = collected.trailers().cloned();
        let body = collected.to_bytes();
        Ok(WireResponse { status, headers, trailers, body })
    }
}
fn classify_reqwest(error: reqwest::Error) -> Failure {
    if error.is_timeout() { Failure::Timeout } else { Failure::Transport }
}

pub fn grpc_status(response: &WireResponse) -> Result<u32, Failure> {
    if response.status != StatusCode::OK { return Err(Failure::HttpStatus); }
    let value = response.trailers.as_ref().and_then(|h| h.get("grpc-status"))
        .or_else(|| response.headers.get("grpc-status")).ok_or(Failure::Malformed)?;
    value.to_str().ok().and_then(|s| s.parse().ok()).ok_or(Failure::Malformed)
}

pub fn provider_http_status(status: StatusCode) -> bool {
    status.is_server_error() || status.is_redirection() ||
        matches!(status.as_u16(), 401 | 403 | 408 | 409 | 425 | 429)
}
