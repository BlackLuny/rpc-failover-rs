# RPC failover gateway

## Scope

A standalone Rust service for authenticated, read-only blockchain queries. Support
BEpusdt v1.24.3 without patching its binary: seven EVM networks, Solana JSON-RPC,
Aptos REST, TRON unary gRPC, and TON mainnet configuration retrieval. Each example
network must have at least two independently tested upstream addresses. TON's
ADNL/LiteServer traffic remains managed by BEpusdt's existing connection pool.

## Authentication and deployment

Every route requires a configured, high-entropy credential. Store SHA-256 digests,
compare in constant time, and support Bearer and Basic authentication. Accept the
same credential via `TRON-PRO-API-KEY` for BEpusdt's gRPC metadata interceptor.
Never forward these credentials to public upstreams. Optional upstream headers
use environment-variable references, not committed secrets. Do not log URLs,
request bodies, credentials, or upstream response bodies.

Default to a private listener. Provide native HTTPS and HTTP/2 for remote clients.
The current BEpusdt TRON client hardcodes plaintext gRPC; remote use of that client
requires a private tunnel/VPN, or a local proxy hop that forwards to this service
over TLS. The gateway itself supports TLS gRPC clients.

## Request and response rules

Only configured read methods and safe REST paths are accepted. Preserve JSON-RPC
IDs, batches, numeric wire values, bytes, and gRPC trailers. Reject mutations,
notifications, malformed/mismatched responses, oversized bodies, redirects, and
unknown routes. Bounds apply even without Content-Length. Authenticate before
reading request bodies or contacting upstreams. Bounded concurrency and total
request deadlines apply, including gRPC caller deadlines.

## Failover and health

Try endpoints in configured priority order, with bounded retries within the
request budget. HTTP errors, timeouts, malformed data, JSON-RPC provider errors,
and retryable gRPC errors open a temporary circuit. Invalid client parameters
and legitimate protocol results must not poison endpoint health. Periodic probes
and on-demand rechecks recover endpoints after cooldown; no lock guard crosses
network awaits except the deliberate asynchronous probe single-flight mutex.

Probe network identity, head progress, and freshness. Never accept an unknown or
wrong network as healthy. Relative head lag and stale block timestamps exclude
lagging nodes. Historical reads are not incorrectly rejected for old timestamps.
Health reports expose endpoint names and failure categories, not secrets.

## Verification and cutover

Test authentication, credential stripping, caller errors, batch identity,
HTTP/RPC/gRPC failures, deadline and size bounds, stale/wrong chains, failover,
cooldown and recovery using isolated local upstreams. Probe all configured public
upstreams from the deployment host, including BEpusdt's actual batch/block/log
methods. Run a staged instance, exercise failure and recovery without modifying
production nodes, then back up BEpusdt's SQLite database and change only its RPC
settings. Restart BEpusdt alone, verify authenticated HTTP/gRPC access and its
normal logs, and retain rollback records. Publish generic source, examples, and
sanitized verification evidence; exclude production configuration and credentials.
