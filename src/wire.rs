use std::{convert::Infallible, time::Duration};
use axum::{body::Body, response::Response};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, StatusCode};
use http_body_util::StreamBody;
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Failure {
    Transport, Timeout, HttpStatus, Malformed, WrongNetwork, Stale, TooLarge,
    RpcProvider, GrpcProvider, Unavailable,
}

pub struct WireResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub trailers: Option<HeaderMap>,
    pub body: Bytes,
}
impl WireResponse {
    pub fn into_response(self) -> Response {
        let body = if let Some(trailers) = self.trailers {
            // Do not advertise an exact data length for a gRPC stream. Some
            // HTTP/2 clients stop at Content-Length before consuming trailers.
            let frames = [Ok::<_, Infallible>(hyper::body::Frame::data(self.body)),
                Ok(hyper::body::Frame::trailers(trailers))];
            Body::new(StreamBody::new(futures_util::stream::iter(frames)))
        } else { Body::from(self.body) };
        let mut response = Response::new(body);
        *response.status_mut() = self.status;
        for name in ["content-type", "grpc-status", "grpc-message", "grpc-encoding"] {
            if let Some(value) = self.headers.get(name) { response.headers_mut().insert(
                http::HeaderName::from_static(name), value.clone()); }
        }
        response.headers_mut().insert("cache-control", HeaderValue::from_static("no-store"));
        response
    }
}

#[derive(Serialize)]
struct ErrorBody { error: &'static str }
pub fn error_response(status: StatusCode, message: &'static str) -> Response {
    let bytes = serde_json::to_vec(&ErrorBody { error: message }).unwrap_or_default();
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    response.headers_mut().insert("content-type", HeaderValue::from_static("application/json"));
    response.headers_mut().insert("cache-control", HeaderValue::from_static("no-store"));
    response
}

pub fn grpc_error(status: &'static str, message: &'static str) -> Response {
    let mut response = Response::new(Body::empty());
    response.headers_mut().insert("content-type", HeaderValue::from_static("application/grpc"));
    response.headers_mut().insert("grpc-status", HeaderValue::from_static(status));
    response.headers_mut().insert("grpc-message", HeaderValue::from_static(message));
    response
}

pub fn grpc_timeout(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get("grpc-timeout")?.to_str().ok()?;
    if value.len() < 2 || value.len() > 9 { return None; }
    let (digits, unit) = value.split_at(value.len() - 1);
    let number = digits.parse::<u64>().ok()?;
    match unit {
        "H" => Some(Duration::from_secs(number.checked_mul(3600)?)),
        "M" => Some(Duration::from_secs(number.checked_mul(60)?)),
        "S" => Some(Duration::from_secs(number)),
        "m" => Some(Duration::from_millis(number)),
        "u" => Some(Duration::from_micros(number)),
        "n" => Some(Duration::from_nanos(number)),
        _ => None,
    }
}

pub fn protobuf_fields(data: &[u8]) -> Result<Vec<(u32, ProtoValue<'_>)>, Failure> {
    fn varint(data: &[u8], offset: &mut usize) -> Result<u64, Failure> {
        let mut result = 0u64;
        for shift in (0..70).step_by(7) {
            let byte = *data.get(*offset).ok_or(Failure::Malformed)?;
            *offset += 1;
            if shift == 63 && byte > 1 { return Err(Failure::Malformed); }
            result |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 { return Ok(result); }
        }
        Err(Failure::Malformed)
    }
    let mut fields = Vec::new();
    let mut offset = 0;
    while offset < data.len() {
        let tag = varint(data, &mut offset)?;
        let field = u32::try_from(tag >> 3).map_err(|_| Failure::Malformed)?;
        if field == 0 { return Err(Failure::Malformed); }
        let value = match tag & 7 {
            0 => ProtoValue::Integer(varint(data, &mut offset)?),
            wire @ (1 | 2 | 5) => {
                let len = match wire {
                    1 => 8, 5 => 4,
                    _ => usize::try_from(varint(data, &mut offset)?).map_err(|_| Failure::Malformed)?,
                };
                let end = offset.checked_add(len).ok_or(Failure::Malformed)?;
                let bytes = data.get(offset..end).ok_or(Failure::Malformed)?;
                offset = end;
                ProtoValue::Bytes(bytes)
            },
            _ => return Err(Failure::Malformed),
        };
        fields.push((field, value));
    }
    Ok(fields)
}
pub enum ProtoValue<'a> { Integer(u64), Bytes(&'a [u8]) }
pub fn pb_bytes<'a>(fields: &[(u32, ProtoValue<'a>)], id: u32) -> Result<&'a [u8], Failure> {
    fields.iter().find_map(|(key, value)| match value {
        ProtoValue::Bytes(bytes) if *key == id => Some(*bytes), _ => None,
    }).ok_or(Failure::Malformed)
}
pub fn pb_number(fields: &[(u32, ProtoValue<'_>)], id: u32) -> Result<u64, Failure> {
    fields.iter().find_map(|(key, value)| match value {
        ProtoValue::Integer(number) if *key == id => Some(*number), _ => None,
    }).ok_or(Failure::Malformed)
}
pub fn grpc_payload(bytes: &[u8]) -> Result<&[u8], Failure> {
    if bytes.len() < 5 || bytes[0] != 0 { return Err(Failure::Malformed); }
    let len = u32::from_be_bytes(bytes[1..5].try_into().map_err(|_| Failure::Malformed)?) as usize;
    if bytes.len() != len + 5 { return Err(Failure::Malformed); }
    Ok(&bytes[5..])
}
pub fn grpc_frame(payload: &[u8]) -> Bytes {
    let mut result = Vec::with_capacity(payload.len() + 5);
    result.push(0);
    result.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    result.extend_from_slice(payload);
    result.into()
}
