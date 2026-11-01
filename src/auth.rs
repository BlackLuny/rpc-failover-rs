use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use http::{header, HeaderMap};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use crate::config::AuthConfig;

#[derive(Clone)]
pub struct Authenticator { keys: Vec<(String, [u8; 32])> }

impl Authenticator {
    pub fn new(config: &AuthConfig) -> Result<Self> {
        let keys = config.keys.iter().map(|key| {
            let bytes: [u8; 32] = hex::decode(&key.sha256)?.try_into()
                .map_err(|_| anyhow::anyhow!("invalid authentication digest length"))?;
            Ok((key.id.clone(), bytes))
        }).collect::<Result<_>>()?;
        Ok(Self { keys })
    }

    pub fn authenticate(&self, headers: &HeaderMap) -> Option<&str> {
        if headers.get_all(header::AUTHORIZATION).iter().count() > 1 { return None; }
        let token = if let Some(auth) = headers.get(header::AUTHORIZATION) {
            let auth = auth.to_str().ok()?;
            let (scheme, value) = auth.split_once(' ')?;
            if scheme.eq_ignore_ascii_case("Bearer") { value.as_bytes().to_vec() }
            else if scheme.eq_ignore_ascii_case("Basic") {
                let decoded = STANDARD.decode(value).ok()?;
                let split = decoded.iter().position(|&b| b == b':')?;
                if split == 0 { return None; }
                decoded[split + 1..].to_vec()
            } else { return None; }
        } else {
            let values = headers.get_all("tron-pro-api-key");
            let first = values.iter().next()?;
            if values.iter().any(|v| v != first) { return None; }
            first.as_bytes().to_vec()
        };
        if token.len() < 32 || token.len() > 1024 { return None; }
        let digest: [u8; 32] = Sha256::digest(&token).into();
        let mut matched = None;
        for (id, expected) in &self.keys {
            if bool::from(digest.ct_eq(expected)) { matched = Some(id.as_str()); }
        }
        matched
    }
}

pub fn token_digest(token: &[u8]) -> String { hex::encode(Sha256::digest(token)) }
