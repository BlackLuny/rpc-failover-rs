# Verification — 2026-11-01

The final Rust source passed 22 integration tests and `cargo clippy --all-targets
--locked -- -D warnings` on Rust 1.96.1. Tests cover HTTP/RPC/gRPC failure, wrong
networks, stale heads, bounded bodies and deadlines, cooldown/recovery, concurrent
probe races, authentication/credential stripping, JSON-RPC batches and large IDs,
Aptos/TON/Solana behavior, and native HTTPS with HTTP/2 gRPC. The HTTPS test verifies
trailers after a response larger than an HTTP/2 data frame. gRPC responses with
trailers deliberately do not advertise Content-Length, so clients consume the
final gRPC status.

Three Python tests verify a consistent SQLite backup, config-only cutover and
rollback, preservation of orders created after cutover, refusal to overwrite
later operator changes, and no database writes on readiness/auth failure.

- `upstreams-2026-11-01.json`: independent deployment-host wire checks of 22 public addresses.
- `rust-deep-probe-2026-11-01.json`: all eleven chains had two healthy addresses, including actual block/log/transaction APIs.
- `live-failover-2026-11-01.json`: a local primary was made to return HTTP 503; the public backup answered, the failed primary was excluded, and it recovered after cooldown. The observed failover request took 10 ms; this is one measurement, not a latency guarantee.

The final static x86_64 Linux binary SHA-256 was
`b43ed323cdf43dfcf41b1c818568228f2159cf901ce03bfbdfed5dbd640615de`.
Its authenticated HTTP and native gRPC routes were checked on the deployment host,
including rejected missing/wrong credentials and mutations. BEpusdt's Basic Auth
request was also verified from inside its own container. After configuration
cutover, its active Tron scanner advanced throughout an observation of more than
two minutes without BEpusdt warnings. Other configured networks passed explicit
read-only RPC checks; idle business scanners were not forced to create orders.

Public nodes can temporarily rate-limit or fail. Transient failures and automatic
recovery were observed during testing; these reports are snapshots rather than
uptime promises. Rerun `probe --deep` from the intended deployment network.
X Layer's two example addresses share an operator. TON verification covers mainnet
configuration and LiteServer entries; native ADNL remains in BEpusdt's own pool.
