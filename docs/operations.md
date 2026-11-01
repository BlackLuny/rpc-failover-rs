# 部署与运维

## 安装与验证

使用 `cargo build --release --locked` 生成二进制；需要跨发行版部署时生成
`x86_64-unknown-linux-musl` 静态包。先上传临时路径，核对 SHA-256，再安装到
`/usr/local/bin/rpc-failover-rs`。不要覆盖正在运行的文件后才检查完整性。

创建无登录权限的 `rpc-failover` 系统用户和组。配置目录
`/etc/rpc-failover-rs` 使用 `root:rpc-failover`、`0750`，配置文件使用 `0640`。
复制 `deploy/rpc-failover-rs.service` 到 systemd。配置只有 Token 摘要；原始 Token 文件
使用 root 的 `0600` 权限保存，BEpusdt 的数据库及其备份同样属于敏感文件。

```sh
rpc-failover-rs keygen --out /root/rpc-gateway.token
# 将输出的 sha256 填入 config.toml，不要复制原始 Token 到日志或 Git。
rpc-failover-rs check-config --config /etc/rpc-failover-rs/config.toml
rpc-failover-rs probe --config /etc/rpc-failover-rs/config.toml --deep --report /root/upstreams.json
systemctl daemon-reload
systemctl enable --now rpc-failover-rs
```

每条链至少两个上游通过验证后才接入消费者。测试无凭据、错误凭据均被拒绝，
正确凭据可以读取 `/readyz` 和实际区块；Tron 还要测试原生 HTTP/2 gRPC 和 trailers。
公网部署必须配置受信任的 HTTPS 证书。私网明文仅用于受控网络；跨服务器优先 TLS 或 VPN。
只读检查并不需要真实客户的订单、钱包或交易。

## BEpusdt v1.24.3

先核对正在使用的数据库路径和容器名称。下例的 IP 仅为 Docker 默认桥接示例；
容器必须能访问该地址。确认监听地址属于主机，端口空闲。

```sh
python3 tools/bepusdt_config.py apply \
  --db /PATH/TO/bepusdt/data/sqlite.db \
  --token-file /root/rpc-gateway.token \
  --http-base http://172.17.0.1:8547 \
  --tron-target 172.17.0.1:8547 \
  --backup-dir /root/bepusdt-rpc-backup-UNIQUE
docker restart YOUR_BEPUSDT_CONTAINER
```

脚本检查认证后的服务状态，要求 11 条链各有两个健康上游；通过 SQLite backup API
备份正在写入 WAL 的数据库并检查完整性，再在事务中修改 12 个 RPC 配置项。
HTTP URL 自动加入 Basic Auth，Aptos 保留末尾斜线，Tron 单独设置 gRPC 地址和
`rpc_endpoint_tron_grid_api_key`。脚本不会重启服务，不修改订单、钱包或支付状态。
BEpusdt 会缓存配置，所以完成事务后只重启它自己的容器。

重启后检查 HTTP、gRPC 均能读取，BEpusdt 区块扫描高度持续前进且没有新的认证错误。
没有启用支付币种或没有待扫描业务时，对应扫描任务可能保持空闲，应另外做只读 RPC 验证。
TON 这里只验证全局配置下载和主网身份；原生 ADNL 连通性需从 BEpusdt 自身观察。

## 回滚

保留备份目录及 `rpc-settings.json`，两者含敏感配置，不上传到仓库。

```sh
python3 tools/bepusdt_config.py rollback \
  --db /PATH/TO/bepusdt/data/sqlite.db \
  --record /root/bepusdt-rpc-backup-UNIQUE/rpc-settings.json
docker restart YOUR_BEPUSDT_CONTAINER
```

回滚只恢复这次修改的 RPC 配置。若运维人员已再次修改配置，脚本会拒绝覆盖。
不要用旧数据库覆盖正在营业的数据库，否则会丢失备份之后的订单和支付变化。

## 故障与密钥轮换

`/status` 返回节点名、最近探测时间、区块高度、冷却时间和错误类别。HTTP 401/403/429、
服务端错误、错误链身份、过期区块、异常 JSON-RPC 和 gRPC 状态会切换上游。
所有上游不可用时返回 503 或 gRPC Unavailable；不会伪造成功结果。
冷却结束后自动重新验证，恢复的高优先级节点会重新参与选择。

增加密钥时在 `[auth].keys` 中保留旧摘要并加入新摘要，检查配置后重启本服务，
更新消费者凭据并验证，最后删除旧摘要并再次重启。TLS 证书和配置文件更新同样需要重启。
观察 `journalctl -u rpc-failover-rs`；日志仅记录链名、节点名和错误类别。

公共 RPC 会限流或改变服务；持续出现限流时配置自己的节点或付费供应商，
API Key 用环境变量提供。X Layer 两个示例地址属于同一运营方，地址冗余不能代表供应商独立。
