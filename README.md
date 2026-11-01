# rpc-failover-rs

带认证的 Rust 区块链只读 RPC 容灾代理，支持 BEpusdt v1.24.3。

- 每条链配置多个上游，按顺序选择；失败后重试备用节点，冷却后探测恢复。
- 检查 HTTP 错误、JSON-RPC 错误、响应 ID、链身份、区块高度和最新区块时间。
- 保留 JSON-RPC 批量请求、原始数值、响应 ID，以及 gRPC 数据帧和 trailers。
- Bearer、HTTP Basic 和 Tron 的 `TRON-PRO-API-KEY` 认证；服务端保存 Token 的 SHA-256 摘要。
- 配置请求期限、连接期限、熔断时间、探测间隔、请求体大小和并发上限。
- HTTPS 和 HTTP/2；拒绝未配置 TLS 的公网或通配监听地址。

这是读取区块、转账和付款确认信息的代理。默认不开放交易广播、管理接口或无 ID 通知。
它不会创建订单、转账、签名或修改支付状态。

## 快速开始

```sh
cargo build --release --locked
cp config.example.toml config.local.toml
./target/release/rpc-failover-rs keygen --out /secure/rpc-gateway.token
```

`keygen --out` 以 `0600` 权限新建 Token 文件，只输出 SHA-256 摘要。将摘要填入：

```toml
[auth]
keys = [{ id = "bepusdt", sha256 = "这里填写 keygen 输出的 64 位十六进制摘要" }]
```

示例中的全零摘要不是可用凭据。不要将真实 Token、生产配置、TLS 私钥或供应商 API Key 提交到 Git。

```sh
./target/release/rpc-failover-rs check-config --config config.local.toml
./target/release/rpc-failover-rs probe --config config.local.toml --deep --report upstream-report.json
./target/release/rpc-failover-rs serve --config config.local.toml
```

`probe --deep` 会检查每个地址的链身份和实际扫描接口；任一链不足两个健康地址即返回非零。
探测只读，不使用客户的钱包、交易哈希、订单或凭据。

## 接口和网络

| 链 | 代理路径 | 上游协议 | BEpusdt 配置项 |
| --- | --- | --- | --- |
| Ethereum | `/ethereum` | JSON-RPC | `rpc_endpoint_ethereum` |
| BSC | `/bsc` | JSON-RPC | `rpc_endpoint_bsc` |
| Polygon | `/polygon` | JSON-RPC | `rpc_endpoint_polygon` |
| Arbitrum | `/arbitrum` | JSON-RPC | `rpc_endpoint_arbitrum` |
| Base | `/base` | JSON-RPC | `rpc_endpoint_base` |
| X Layer | `/xlayer` | JSON-RPC | `rpc_endpoint_xlayer` |
| Plasma | `/plasma` | JSON-RPC | `rpc_endpoint_plasma` |
| Solana | `/solana` | JSON-RPC | `rpc_endpoint_solana` |
| Aptos | `/aptos/v1/...` | REST GET | `rpc_endpoint_aptos`，URL 末尾保留 `/aptos/` |
| TON | `/ton` | 全局配置 GET | `rpc_global_config_url_ton` |
| Tron | `/protocol.Wallet/...` | unary gRPC / HTTP/2 | `rpc_endpoint_tron`，填写主机和端口 |

TON 路由对全局配置的下载提供认证和备用地址，并验证主网 zero-state 身份及 LiteServer 列表。
TON 原生 ADNL/LiteServer 流量由 BEpusdt 的现有连接池直连，不经过这个 HTTP 代理。

`/healthz`、`/readyz`、`/status` 也需要认证。`/readyz` 在任一链完全无可用上游时返回 503；
`/status` 给出节点名称、健康状态、冷却时间和错误类别，不返回 URL 或密钥。

## BEpusdt 接入和认证

HTTP 接口可通过 Basic Auth URL 接入，例如：

```text
http://bepusdt:TOKEN@PRIVATE_HOST:8547/polygon
http://bepusdt:TOKEN@PRIVATE_HOST:8547/aptos/
http://bepusdt:TOKEN@PRIVATE_HOST:8547/ton
```

标准客户端也可发送 `Authorization: Bearer TOKEN`。Token 不使用 URL 查询参数。

Tron 使用 HTTP/2 gRPC，不能填普通 HTTP JSON-RPC URL。将 `rpc_endpoint_tron` 设置为
代理的 `PRIVATE_HOST:8547`，并在 BEpusdt 的 TronGrid API Key 配置中填写本代理的 Token。
代理验证 `TRON-PRO-API-KEY` 元数据后，将其移除，不会把这个 Token 发给公共节点。

现有 BEpusdt v1.24.3 的 Tron 客户端固定使用明文 gRPC。它跨机器连接本服务时，应通过
VPN/私有隧道，或先连接同机代理，再由同机代理通过 TLS 连接远端服务。
支持 TLS 的标准 gRPC 客户端可直接连接本服务的 HTTPS 监听地址。

## 远程 HTTPS 部署

```toml
listen = "0.0.0.0:8547"

[tls]
cert_file = "/etc/rpc-failover-rs/fullchain.pem"
key_file = "/etc/rpc-failover-rs/privkey.pem"
```

保留 `[auth]` 配置，并使用客户端信任的证书。也可以仅监听私网/回环地址，由已有 HTTPS
反向代理终止 TLS；gRPC 路由必须保持 HTTP/2 和 trailers。

如上游供应商需要凭据，将完整 Header 值放在环境变量中：

```toml
[[chains.upstreams]]
name = "private-provider"
url = "https://rpc.example.com/"
headers = { authorization = "env:PROVIDER_AUTHORIZATION" }
```

`PROVIDER_AUTHORIZATION` 可由 systemd 的 root-only `EnvironmentFile` 提供。
本服务不会将客户端的 Authorization、Cookie 或 Tron API Key 透传到上游。

## 测试

```sh
cargo test --all-targets --locked
cargo clippy --all-targets --locked -- -D warnings
python3 -m unittest discover -s tests -p 'test_*.py'
python3 tools/verify_upstreams.py --out upstream-report.json
python3 tools/live_failover.py --binary ./target/release/rpc-failover-rs --report failover-report.json
python3 tools/smoke_gateway.py --origin http://127.0.0.1:8547 --token-file /secure/rpc-gateway.token
```

Python 脚本使用标准库；Tron wire 检查需要支持 HTTP/2 的 curl。Rust 的 `probe` 命令无需 curl。
本地故障测试不停止公共节点或生产网关，使用隔离的测试上游构造错误和恢复场景。

地址可用性会变化；示例地址和验证记录是特定时间的实测结果。接入前应在部署主机重新运行
`probe --deep`。配置、重启、备份和回滚步骤见 [运维说明](docs/operations.md)。

## 来源与许可

协议兼容性以 [BEpusdt v1.24.3](https://github.com/v03413/BEpusdt/tree/v1.24.3) 的调用方式为依据。
公链地址参考链和节点供应商的官方资料，并通过实际请求核对：

- [Polygon RPC 列表](https://docs.polygon.technology/pos/reference/rpc-endpoints)
- [PublicNode](https://publicnode.com/)
- [dRPC 网络列表](https://drpc.org/chainlist)
- [X Layer RPC](https://web3.okx.com/zh-hant/onchainos/dev-docs/xlayer/developer/rpc-endpoints/rpc-endpoints)
- [TON 主网配置](https://ton-blockchain.github.io/)
- [Tron gRPC](https://developers.tron.network/docs/api)

MIT，见 [LICENSE](LICENSE)。
