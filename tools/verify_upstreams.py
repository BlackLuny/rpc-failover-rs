#!/usr/bin/env python3
"""Read-only, deployment-host checks of BEpusdt's actual public API methods.

No credentials, wallet addresses, customer orders, or transaction hashes are used.
TRON's wire check needs curl built with HTTP/2; the Rust probe needs no curl.
"""
import argparse
import concurrent.futures
import datetime
import json
import pathlib
import re
import socket
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

EVM = {
    "ethereum": (1, ["https://ethereum.publicnode.com", "https://eth.drpc.org"]),
    "bsc": (56, ["https://bsc.publicnode.com", "https://1rpc.io/bnb"]),
    "polygon": (137, ["https://polygon.drpc.org", "https://polygon.publicnode.com"]),
    "arbitrum": (42161, ["https://arb1.arbitrum.io/rpc", "https://arbitrum-one-rpc.publicnode.com"]),
    "base": (8453, ["https://base-rpc.publicnode.com", "https://base.drpc.org"]),
    "xlayer": (196, ["https://rpc.xlayer.tech", "https://xlayerrpc.okx.com"]),
    "plasma": (9745, ["https://rpc.plasma.to", "https://plasma.gateway.tenderly.co"]),
}
OTHER = {
    "solana": ["https://api.mainnet-beta.solana.com", "https://solana-rpc.publicnode.com"],
    "aptos": ["https://fullnode.mainnet.aptoslabs.com", "https://aptos-rest.publicnode.com"],
    "ton": ["https://ton.org/global-config.json", "https://ton-blockchain.github.io/global.config.json"],
    "tron": ["https://tron-grpc.publicnode.com:443", "http://3.225.171.164:50051"],
}
TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def http(url, payload=None):
    headers = {"User-Agent": "rpc-failover-rs/0.1", "Accept": "application/json"}
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=12) as response:
        return json.load(response)


def rpc(url, method, params=None):
    data = http(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []})
    if "error" in data:
        raise RuntimeError("RPC error: " + str(data["error"])[:220])
    assert data.get("id") == 1 and "result" in data, "invalid response identity"
    return data["result"]


def varint(value):
    out = bytearray()
    while value >= 128:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)


def protobuf_fields(data):
    def read_varint(pos):
        value = 0
        for shift in range(0, 70, 7):
            if pos >= len(data):
                raise ValueError("truncated protobuf")
            b = data[pos]
            pos += 1
            value |= (b & 127) << shift
            if not b & 128:
                return value, pos
        raise ValueError("invalid protobuf varint")
    out, pos = {}, 0
    while pos < len(data):
        tag, pos = read_varint(pos)
        field, wire = tag >> 3, tag & 7
        if wire == 0:
            value, pos = read_varint(pos)
        elif wire == 2:
            size, pos = read_varint(pos)
            if pos + size > len(data):
                raise ValueError("truncated protobuf bytes")
            value, pos = data[pos:pos + size], pos + size
        elif wire in (1, 5):
            size = 8 if wire == 1 else 4
            value, pos = data[pos:pos + size], pos + size
        else:
            raise ValueError("unsupported protobuf wire type")
        out.setdefault(field, []).append(value)
    return out


def grpc(url, method, payload=b""):
    with tempfile.TemporaryDirectory() as folder:
        header_file = pathlib.Path(folder) / "headers"
        cmd = ["curl", "--silent", "--show-error", "--http2-prior-knowledge", "--max-time", "12",
               "--connect-timeout", "4", "--max-filesize", str(16 * 1024 * 1024),
               "-H", "content-type: application/grpc", "-H", "te: trailers",
               "-H", "user-agent: rpc-failover-rs/0.1", "-D", str(header_file),
               "--data-binary", "@-", url.rstrip("/") + "/protocol.Wallet/" + method]
        response = subprocess.run(cmd, input=b"\0" + struct.pack(">I", len(payload)) + payload,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
        if response.returncode:
            raise RuntimeError(response.stderr.decode(errors="replace")[:180])
        headers = header_file.read_text().lower()
        assert re.search(r"grpc-status:\s*0\s", headers + "\n"), headers[:220]
        raw = response.stdout
        assert len(raw) >= 5 and raw[0] == 0 and len(raw) == 5 + struct.unpack(">I", raw[1:5])[0], "invalid unary gRPC frame"
        return protobuf_fields(raw[5:])


def check(chain, url):
    start = time.monotonic()
    result = {"chain": chain, "url": url, "ok": False}
    try:
        if chain in EVM:
            chain_id, _ = EVM[chain]
            batch = http(url, [{"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
                               {"jsonrpc": "2.0", "id": 2, "method": "eth_blockNumber", "params": []}])
            assert isinstance(batch, list) and len(batch) == 2, "batch unsupported"
            by_id = {item["id"]: item for item in batch}
            assert not any("error" in item for item in batch), str(batch)[:220]
            assert int(by_id[1]["result"], 16) == chain_id, "wrong network"
            head = int(by_id[2]["result"], 16)
            block = rpc(url, "eth_getBlockByNumber", [hex(head - 2), False])
            assert int(block["number"], 16) == head - 2 and abs(time.time() - int(block["timestamp"], 16)) < 180, "stale block"
            logs = rpc(url, "eth_getLogs", [{"fromBlock": hex(head - 2), "toBlock": hex(head - 2), "topics": [TOPIC]}])
            assert isinstance(logs, list), "invalid logs"
            result.update(chain_id=chain_id, head=head, log_count=len(logs), methods=["batch", "eth_chainId", "eth_blockNumber", "eth_getBlockByNumber", "eth_getLogs"])
        elif chain == "solana":
            genesis = rpc(url, "getGenesisHash")
            slot = rpc(url, "getSlot", [{"commitment": "finalized"}])
            block = rpc(url, "getBlock", [slot - 64, {"encoding": "json", "maxSupportedTransactionVersion": 1, "transactionDetails": "full", "rewards": False}])
            assert block is None or isinstance(block, dict), "invalid block"
            result.update(genesis=genesis, head=slot, methods=["getGenesisHash", "getSlot", "getBlock"])
        elif chain == "aptos":
            info = http(url.rstrip("/") + "/v1")
            assert info["chain_id"] == 1, "wrong Aptos network"
            version = int(info["ledger_version"])
            assert abs(time.time() - int(info["ledger_timestamp"]) / 1000000) < 180, "stale ledger"
            transactions = http(url.rstrip("/") + "/v1/transactions?start=" + str(version - 3) + "&limit=1")
            assert isinstance(transactions, list) and len(transactions) == 1, "transactions unavailable"
            result.update(chain_id=1, head=version, methods=["GET /v1", "GET /v1/transactions"])
        elif chain == "ton":
            conf = http(url)
            assert isinstance(conf["liteservers"], list) and len(conf["liteservers"]) >= 2, "missing liteservers"
            result.update(genesis=conf["validator"]["zero_state"]["root_hash"], liteservers=len(conf["liteservers"]), methods=["GET global config"])
        elif chain == "tron":
            zero = grpc(url, "GetBlockByNum2")
            head = grpc(url, "GetNowBlock2")
            raw = protobuf_fields(protobuf_fields(head[2][0])[1][0])
            assert abs(time.time() - raw[1][0] / 1000) < 180, "stale Tron block"
            result.update(genesis=zero[3][0].hex(), head=raw[7][0], methods=["GetBlockByNum2", "GetNowBlock2", "gRPC trailers"])
        result["ok"] = True
    except Exception as error:
        result["error"] = type(error).__name__ + ": " + str(error)[:300]
    result["latency_ms"] = round((time.monotonic() - start) * 1000)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()
    jobs = [(chain, endpoint) for chain, (_, endpoints) in EVM.items() for endpoint in endpoints]
    jobs += [(chain, endpoint) for chain, endpoints in OTHER.items() for endpoint in endpoints]
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for result in pool.map(lambda pair: check(*pair), jobs):
            results.append(result)
            print(json.dumps(result), flush=True)
    counts = {chain: sum(r["ok"] for r in results if r["chain"] == chain) for chain, _ in jobs}
    report = {"observed_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "scope": "Read-only API methods from the deployment host; not an uptime guarantee",
              "healthy_addresses_per_chain": counts, "results": results}
    args.out.write_text(json.dumps(report, indent=2) + "\n")
    return 0 if all(count >= 2 for count in counts.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
