use std::{sync::Arc, time::Duration};
use bytes::Bytes;
use http::Method;
use serde::Serialize;
use serde_json::Value;
use crate::{config::Protocol, gateway::Gateway, health::{Chain, Endpoint, ProbeMethod, ProbeRequest, probe_bytes},
    rpc, transport::grpc_status, wire::{self, Failure}};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LogFilter { from_block: String, to_block: String, topics: [&'static str; 1] }
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BlockOptions { encoding: &'static str, max_supported_transaction_version: u8, transaction_details: &'static str, rewards: bool }

pub async fn deep_probe(gateway: &Arc<Gateway>) {
    use futures_util::{stream, StreamExt};
    let mut tasks = Vec::new();
    for chain in &gateway.chains { for endpoint in &chain.endpoints { tasks.push((chain.clone(), endpoint.clone())); } }
    stream::iter(tasks).for_each_concurrent(4, |(chain, endpoint)| {
        let gateway = gateway.clone();
        async move {
            let Some(ticket) = chain.ensure(&endpoint, &gateway.transport, &gateway.config.policy, false).await else { return; };
            let result = tokio::time::timeout(Duration::from_secs(20), deep_endpoint(&gateway, &chain, &endpoint)).await.unwrap_or(Err(Failure::Timeout));
            if let Err(failure) = result {
                endpoint.failure(ticket, failure, &gateway.config.policy);
                tracing::warn!(chain = %chain.config.name, upstream = %endpoint.config.name, ?failure, "deep API verification failed");
            }
        }
    }).await;
}

async fn deep_endpoint(gateway: &Gateway, chain: &Chain, endpoint: &Endpoint) -> Result<(), Failure> {
    let report = endpoint.report(&gateway.config.policy);
    match chain.config.protocol {
        Protocol::Evm => {
            let head = report.head.ok_or(Failure::Malformed)?.saturating_sub(2);
            let block = format!("0x{head:x}");
            let filter = LogFilter { from_block: block.clone(), to_block: block.clone(),
                topics: ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"] };
            let requests = [
                ProbeRequest::new(10, ProbeMethod::EvmBlock, serde_json::to_value((block, false)).map_err(|_| Failure::Malformed)?),
                ProbeRequest::new(11, ProbeMethod::EvmLogs, serde_json::to_value([filter]).map_err(|_| Failure::Malformed)?),
            ];
            checked_rpc(gateway, chain, endpoint, probe_bytes(&requests)?).await?;
        },
        Protocol::Solana => {
            let slot = report.head.ok_or(Failure::Malformed)?.saturating_sub(64);
            let options = BlockOptions { encoding: "json", max_supported_transaction_version: 1, transaction_details: "full", rewards: false };
            let request = ProbeRequest::new(12, ProbeMethod::SolanaBlock, serde_json::to_value((slot, options)).map_err(|_| Failure::Malformed)?);
            checked_rpc(gateway, chain, endpoint, probe_bytes(&request)?).await?;
        },
        Protocol::Aptos => {
            let version = report.head.ok_or(Failure::Malformed)?.saturating_sub(3);
            let response = gateway.transport.http(&endpoint.config, Method::GET, "v1/transactions", Some(&format!("start={version}&limit=1")), Bytes::new()).await?;
            if !response.status.is_success() { return Err(Failure::HttpStatus); }
            let value: Value = serde_json::from_slice(&response.body).map_err(|_| Failure::Malformed)?;
            if value.as_array().is_none_or(|v| v.len() != 1) { return Err(Failure::Malformed); }
        },
        Protocol::TronGrpc => {
            let number = report.head.ok_or(Failure::Malformed)?.saturating_sub(1);
            let mut param = vec![8];
            write_varint(number, &mut param);
            for (method, body) in [
                ("/protocol.Wallet/GetBlockByNum2", param),
                ("/protocol.Wallet/GetTransactionById", [vec![10, 32], vec![0; 32]].concat()),
                ("/protocol.Wallet/GetTransactionInfoById", [vec![10, 32], vec![0; 32]].concat()),
            ] {
                let response = gateway.transport.grpc(&endpoint.config, method, wire::grpc_frame(&body), Duration::from_secs(5)).await?;
                if grpc_status(&response)? != 0 { return Err(Failure::GrpcProvider); }
                wire::protobuf_fields(wire::grpc_payload(&response.body)?)?;
            }
        },
        // The TON route serves the pinned configuration. Native ADNL is downstream
        // of this service and stays in the consumer's existing LiteServer pool.
        Protocol::TonConfig => {},
    }
    Ok(())
}
fn write_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 128 { output.push((value as u8 & 127) | 128); value >>= 7; }
    output.push(value as u8);
}
async fn checked_rpc(gateway: &Gateway, chain: &Chain, endpoint: &Endpoint, body: Bytes) -> Result<(), Failure> {
    let input = rpc::parse_input(&body, &chain.config, 100).map_err(|_| Failure::Malformed)?;
    let response = gateway.transport.http(&endpoint.config, Method::POST, "", None, body).await?;
    if !response.status.is_success() { return Err(Failure::HttpStatus); }
    let decoded: Value = serde_json::from_slice(&response.body).map_err(|_| Failure::Malformed)?;
    let entries = match &decoded { Value::Array(entries) => entries.as_slice(), other => std::slice::from_ref(other) };
    if entries.iter().any(|entry| entry.get("error").is_some() || entry.get("result").is_none()) { return Err(Failure::RpcProvider); }
    rpc::validate_response(&response.body, &input, &chain.config, chain.reference_head(&gateway.config.policy))?;
    Ok(())
}
