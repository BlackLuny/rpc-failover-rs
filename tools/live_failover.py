#!/usr/bin/env python3
"""Isolated runtime failure/recovery test with a local primary and public Polygon backup."""
import argparse
import hashlib
import http.server
import json
import pathlib
import secrets
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--report", type=pathlib.Path)
    args = parser.parse_args()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    backup = "https://polygon.publicnode.com"

    def call(url, payload=None, token=None):
        data = json.dumps(payload).encode() if payload is not None else None
        headers = {"Content-Type": "application/json", "User-Agent": "rpc-failover-rs/0.1"}
        if token:
            headers["Authorization"] = "Bearer " + token
        with opener.open(urllib.request.Request(url, data=data, headers=headers), timeout=8) as response:
            return json.load(response), response.headers

    request = {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}
    latest, _ = call(backup, request)
    head = latest["result"]
    broken = threading.Event()

    class Primary(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            if broken.is_set():
                self.send_response(503)
                self.end_headers()
                return
            payload = json.loads(raw)

            def reply(item):
                method = item["method"]
                result = "0x89" if method == "eth_chainId" else head
                if method == "eth_getBlockByNumber":
                    result = {"number": head, "timestamp": hex(int(time.time()))}
                return {"jsonrpc": "2.0", "id": item["id"], "result": result}

            response = [reply(item) for item in payload] if isinstance(payload, list) else reply(payload)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())

        def log_message(self, *values):
            pass

    primary = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Primary)
    thread = threading.Thread(target=primary.serve_forever, daemon=True)
    thread.start()
    reservation = socket.socket()
    reservation.bind(("127.0.0.1", 0))
    port = reservation.getsockname()[1]
    reservation.close()
    token = secrets.token_hex(32)
    try:
        with tempfile.TemporaryDirectory() as folder:
            root = pathlib.Path(folder)
            config = root / "config.toml"
            config.write_text(f'''listen = "127.0.0.1:{port}"
[auth]
keys = [{{id = "isolated", sha256 = "{hashlib.sha256(token.encode()).hexdigest()}"}}]
[policy]
connect_timeout_ms = 1500
attempt_timeout_ms = 3000
total_timeout_ms = 9000
cooldown_ms = 1000
probe_interval_ms = 500
max_probe_age_ms = 6000
[[chains]]
name = "polygon"
protocol = "evm"
chain_id = 137
[[chains.upstreams]]
name = "local-primary"
url = "http://127.0.0.1:{primary.server_port}"
[[chains.upstreams]]
name = "public-backup"
url = "{backup}"
''')
            with (root / "service.log").open("wb") as logs:
                child = subprocess.Popen([str(args.binary.resolve()), "serve", "--config", str(config)], stdout=logs, stderr=logs)
                try:
                    origin = f"http://127.0.0.1:{port}"
                    for _ in range(100):
                        try:
                            status, _ = call(origin + "/readyz", token=token)
                            if status["ready"]:
                                break
                        except (OSError, ValueError):
                            pass
                        if child.poll() is not None:
                            raise RuntimeError("isolated gateway exited")
                        time.sleep(0.1)
                    request["method"] = "eth_chainId"
                    _, first = call(origin + "/polygon", request, token)
                    assert first["x-rpc-upstream"] == "local-primary"
                    broken.set()
                    started = time.monotonic()
                    response, second = call(origin + "/polygon", request, token)
                    elapsed = round((time.monotonic() - started) * 1000)
                    assert response["result"] == "0x89" and second["x-rpc-upstream"] == "public-backup"
                    status, _ = call(origin + "/status", token=token)
                    failed = status["chains"][0]["endpoints"][0]
                    assert not failed["healthy"] and failed["failures"] > 0
                    broken.clear()
                    for _ in range(80):
                        time.sleep(0.1)
                        _, recovered = call(origin + "/polygon", request, token)
                        if recovered["x-rpc-upstream"] == "local-primary":
                            break
                    assert recovered["x-rpc-upstream"] == "local-primary"
                    report = {"scenario": "isolated local primary HTTP 503, public backup, cooldown and primary recovery",
                              "primary_selected": True, "backup_selected_on_failure": True, "failed_primary_excluded": True,
                              "primary_recovered": True, "failover_ms": elapsed, "production_configuration_changed": False}
                    if args.report:
                        args.report.write_text(json.dumps(report, indent=2) + "\n")
                    print(json.dumps(report))
                finally:
                    child.terminate()
                    try:
                        child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait()
    finally:
        primary.shutdown()
        primary.server_close()
        thread.join()


if __name__ == "__main__":
    main()
