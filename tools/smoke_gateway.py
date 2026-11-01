#!/usr/bin/env python3
"""Authenticated read-only smoke checks, without placing tokens in argv or output."""
import argparse
import json
import pathlib
import re
import subprocess
import tempfile
import urllib.error
import urllib.request

from verify_upstreams import EVM, OTHER


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--token-file", required=True, type=pathlib.Path)
    args = parser.parse_args()
    if args.token_file.stat().st_mode & 0o077:
        parser.error("token-file requires mode 0600")
    token = args.token_file.read_text().strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{32,1024}", token):
        parser.error("use a keygen credential")
    origin = args.origin.rstrip("/")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(path, payload=None, credential=token):
        headers = {"Content-Type": "application/json"}
        if credential is not None:
            headers["Authorization"] = "Bearer " + credential
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(origin + path, data=data, headers=headers)
        with opener.open(req, timeout=12) as response:
            return response.status, json.load(response)

    for credential in (None, "incorrect-test-credential-that-must-be-rejected"):
        try:
            request("/status", credential=credential)
            raise AssertionError("invalid credentials were accepted")
        except urllib.error.HTTPError as error:
            assert error.code == 401
    _, report = request("/readyz")
    assert report["ready"] and len(report["chains"]) == len(EVM) + len(OTHER)
    for chain, (identity, _) in EVM.items():
        _, result = request("/" + chain, [{"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
                                          {"jsonrpc": "2.0", "id": 2, "method": "eth_blockNumber", "params": []}])
        replies = {item["id"]: item["result"] for item in result}
        assert int(replies[1], 16) == identity and int(replies[2], 16) > 0
        print(chain + ": authenticated batch OK")
    _, result = request("/solana", {"jsonrpc": "2.0", "id": 1, "method": "getGenesisHash", "params": []})
    assert result["result"] == "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
    _, result = request("/aptos//v1")
    assert result["chain_id"] == 1
    _, result = request("/ton")
    assert result["validator"]["zero_state"]["root_hash"] == "F6OpKZKqvqeFp6CQmFomXNMfMj2EnaUSOXN+Mh+wVWk="
    try:
        request("/polygon", {"jsonrpc": "2.0", "id": 1, "method": "eth_sendRawTransaction", "params": []})
        raise AssertionError("mutation was accepted")
    except urllib.error.HTTPError as error:
        assert error.code == 400
    with tempfile.TemporaryDirectory() as folder:
        payload = pathlib.Path(folder) / "request.bin"
        payload.write_bytes(b"\0\0\0\0\0")
        headers = pathlib.Path(folder) / "headers"
        cmd = ["curl", "--config", "-", "--silent", "--show-error", "--http2-prior-knowledge", "--max-time", "12",
               "-H", "content-type: application/grpc", "-H", "te: trailers", "-D", str(headers),
               "--data-binary", "@" + str(payload), origin + "/protocol.Wallet/GetNowBlock2"]
        for credential, expected in ((None, 16), (token, 0)):
            config = ("header = \"tron-pro-api-key: " + credential + "\"\n") if credential else ""
            result = subprocess.run(cmd, input=config.encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
            observed = headers.read_text().lower()
            assert result.returncode == 0 and re.search(r"grpc-status:\s*" + str(expected) + r"\s", observed + "\n"), (
                "gRPC smoke failed", expected, result.returncode, observed, result.stderr.decode(errors="replace"))
    print("Solana, Aptos, TON and native Tron gRPC: OK")
    print("Missing/wrong HTTP credentials, missing gRPC credentials and mutation: rejected")
    print("Healthy upstreams:", {chain["name"]: chain["healthy_upstreams"] for chain in report["chains"]})


if __name__ == "__main__":
    main()
