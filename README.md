# rh-searcher

Robinhood Chain 通用链上 Searcher：**跨 DEX / 跨池原子套利** + **Morpho Blue 清算**。

> 一个仓库、两套策略、两个进程、两个钱包、两个执行合约、共享底层设施。

## 技术栈

| 层 | 技术 |
|---|---|
| 生产热路径 | Go 1.26（go-ethereum / pgx / prometheus / cobra，日志用 `log/slog`） |
| 原子执行层 | Solidity 0.8.24 + Foundry |
| 研究层 | Python（历史回放、统计、参数实验，**不进入实时热路径**） |
| 持久化 | PostgreSQL |
| 监控 | Prometheus + Grafana |

MVP 范围：Robinhood Chain + V3-compatible 池 + WETH 循环套利 + Morpho Blue 清算 + GMGN 快速广播。
明确不做：Meme 狙击、三跳以上、跨链、CEX-DEX、v4 Hooks、Midnight、多链、Web 前端、Kafka/Redis/K8s。

## 架构

```
Robinhood WS / Feed
        │
   chain-ingestor
        ├──────────────┐
 DEX State Store  Morpho State Store
        │              │
 Arbitrage Engine  Liquidation Engine
        └──────┬───────┘
        Opportunity Bus
        Transaction Simulator
        Profit / Risk Verification
        Signer + Nonce Manager
        GMGN Fast RPC（发送专用）
        Receipt / PnL Reconciler
```

核心原则：

- **读取/发现/发送端分离**：Archive RPC（历史）、WS RPC（实时）、Sim RPC（模拟）、GMGN 快速 RPC（只发送）
- **发送端不进策略代码**：`Broadcaster` 接口隔离
- **数据源可替换**：`BlockSource` / `LogSource` 接口，回放与实盘共用同一策略引擎
- **Dry / Shadow / Live 隔离**；所有候选（含拒绝）落盘，不只是成交
- **禁止 Look-ahead**：observed 字段与执行结果严格分离
- **净收益 = 最终 WETH − 初始 WETH − Gas − DEX 费 − 滑点 − 安全边际**，不允许只看中间价差

## 目录

```
cmd/rh-indexer      链索引器（区块/日志 → 池状态 → checkpoint）
cmd/rh-arbitrage    套利引擎（默认 shadow）
cmd/rh-liquidator   清算引擎（M7 骨架）
cmd/rh-cli          运维工具（config-check / bench / balance）
internal/chain      BlockSource / LogSource / WS 自动重连 / 重组检测
internal/rpc        多 RPC 池、健康、广播（GMGN/Sequencer/Fallback）、基准
internal/signer     本地签名 + 独立 Nonce Manager
internal/simulation eth_call 模拟 + 成本核算
internal/dex        PoolAdapter / 池图 / 路由 / V3 本地报价
internal/arbitrage  候选发现 → 优化 → 评估 → 执行（shadow）
internal/morpho     市场/仓位/利息/健康度（M6 起）
internal/liquidation 清算候选（M7 起）
internal/storage    PostgreSQL + checkpoint（含迁移 SQL）
internal/telemetry  Prometheus 指标 + slog
contracts/          ArbitrageExecutor / LiquidationExecutor / BaseExecutor（Foundry）
configs/            robinhood.yaml / dexes.yaml / morpho.yaml（地址均为 M0 待核实占位）
research/           回放与统计（Python，后续填充）
```

## 快速开始

```bash
# 1. 本地基础设施
docker compose -f docker-compose.dev.yml up -d     # postgres + prometheus + grafana

# 2. 配置环境变量（不写入仓库）
export RH_ARCHIVE_RPC=... RH_READ_WS_RPC=wss://... RH_SIM_RPC=... RH_GMGN_RPC=...
export RH_POSTGRES_URL=postgres://rh:rh@localhost:5432/rh

# 3. 检查配置
go run ./cmd/rh-cli config-check
go run ./cmd/rh-cli bench                      # RPC 延迟基准

# 4. 合约
cd contracts && forge build && forge test       # 已通过 5 项安全测试

# 5. 运行（shadow 模式：发现 + 模拟 + 落盘，不发送）
make run-indexer
make run-arbitrage
```

完整命令见 `Makefile`。

## 实施阶段

| 阶段 | 内容 | 状态 |
|---|---|---|
| M0 | 链上事实确认 | ✅ **完成**（见 `deployments/M0-chain-facts.md`）：Chain ID 4663、aeWETH、Uniswap V3/V2 全家桶、Morpho Blue 地址全部实测确认；发现 Morpho 链上 0 市场 |
| M1 | 多 RPC 池、WS 重连、补块、签名、Nonce、Receipt 确认 | ✅ 骨架完成（轮询源 + 429 退避 + 自适应批次） |
| M2 | V3 Factory/Pool 索引 + 本地 Quote 验收 | 🔶 数学实现完成（双向精确报价 + TickMath 金标准）；真实状态恢复（bitmap 按需加载/gross 跟踪）完成，待长跑验证 |
| M3 | WETH→TOKEN→WETH 两池 Shadow 搜索 | 🔶 本地候选落盘完成（对数网格优化器 + 每跳记录）；链上模拟已接入（executeV3Cycle eth_call，分层 local_candidate/simulation_accepted），待积累数据 |
| M4 | ArbitrageExecutor + Foundry 测试 | 🔶 合约结构完成，真实 V3 Callback 语义已修（主动付款 + amountSpecified 恒正），14/14 本地通过，Forge CI 已修复（去除重复 forge install）；真链 fork 测试待接入 |
| M5 | Replay → Shadow → Canary → Live 上线门槛 | 未开始 |
| M6 | Morpho 市场/仓位索引 | ⏸ 推迟：链上 0 市场（见 M0 报告），等 CreateMarket 事件 |
| M7 | 清算机会计算 | ⏸ 推迟（依赖 M6） |
| M8 | LiquidationExecutor + Flash Loan | 🔶 合约草案保留 |

**开发顺序：先完成共享底座 → DEX 池状态 → 套利 Shadow → 套利执行 → Morpho 索引 → 清算执行。**
清算会复用套利约一半的基础能力（DEX 状态/报价/模拟），不要两个策略同时开写。

## Stream-first freshness canary（`rh-canary`）

M5 前置实验：验证 Alchemy Robinhood WSS 能否把 Swap 事件以 `state_lag <= 2` 的新鲜状态
送进本地报价评估——这是「继续 Robinhood / 换链 / 付费 RPC」决策的证据门槛。

- **摄取 stream-first**：`eth_subscribe(logs)`（topic0=Swap，全池订阅）+ 本地 WETH 池集过滤；
  实时路径不做逐块/批量 getLogs。
- **恢复 polling-recovery**：断线/启动缺口用 Alchemy HTTP `getLogs` 补齐，每次 ≤10 blocks
  （Alchemy 免费版限制），与订阅事件按 (block, tx, logIndex) 身份去重（`chain.LogCursor`）。
- **池宇宙**：完整 WETH 池集（一次性 bootstrap 产物，静态池保留——两池套利第二腿
  往往来自长期无 Swap 的池）。加载顺序：PG `dex_pools` → 宇宙文件 → 运行时自发现。
  静态池**永不因不活跃删除**；运行时新发现的池照常注册。
- **评估（token-group / head-batch）**：Swap(pool) → 定位 TOKEN → 一次批量刷新该
  TOKEN 全部 WETH 池到当前 head（Multicall3 `aggregate3`，slot0+liquidity 一次往返，
  tickBitmap 一次往返，bitmap word 跨 head 持久缓存）→ 本地一次算完全部 pair 组合。
  cache key=(headHash, poolAddress)：同一 head 同一池最多刷新一次，禁止 route 级重复 RPC。
  无 250ms 节流——同 token 连续事件由快照缓存吸收（重复评估零 RPC）。
- **交叉校验**：公共 Robinhood RPC 仅作 secondary（head 对照，不参与摄取）。

```bash
# 一次性：重建完整 WETH 池宇宙（公共 RPC 100k-block 批扫 Factory PoolCreated，
# 断点续扫 + 增量落盘；~10-30 分钟，视限速）
go run ./cmd/rh-cli pools bootstrap --out data/canary/weth-universe.jsonl
go run ./cmd/rh-cli pools count --file data/canary/weth-universe.jsonl   # 宇宙统计

export RH_STREAM_RPC=wss://robinhood-mainnet.g.alchemy.com/v2/{API_KEY}
export RH_ALCHEMY_RPC=https://robinhood-mainnet.g.alchemy.com/v2/{API_KEY}
make run-canary            # 默认 2h 或 1000 个 fresh 评估（-duration / -max-evals 可调）
```

输出：`data/canary/results-<ts>.jsonl`（事件行 + 候选行，含每次评估的
`rpc_calls/unique_pools/route_count/state_fetch_ms/local_quote_ms/total_eval_ms`）+
结束时 p50/p95/p99 汇总（`data/canary/summary-<ts>.txt`）。
门槛：`state_lag_blocks` p50<=0 / p95<=1 / p99<=2；`event_to_evaluation_ms` p50<250ms /
p95<750ms；评估 `total_eval_ms` p50<250ms / p95<750ms（实测 p50≈0 / p95≈380ms）。
Alpha 统计：`python3 scripts/alpha_stats.py data/canary/results-*.jsonl`（仅 lag<=2，
gross/net_1x..3x，按 token 与 pool-pair 分组）。

## 安全与运行隔离

- 钱包 A：套利（rh-arbitrage + ArbitrageExecutor）；钱包 B：清算（rh-liquidator + LiquidationExecutor）；钱包 C：Admin（只做 pause/withdraw/部署，不跑自动交易）
- 进程间不共享 Nonce / 热钱包 / 日亏损额度
- 私钥只从环境变量读取；`configs/` 内禁止出现任何密钥
- 每笔交易硬 `minProfit` + `deadline`；合约无任意外部 call 入口

## 监控

Prometheus 指标（`rh_` 前缀）：chain_head_lag、rpc_latency_ms、pool_state_age_ms、candidate_total、simulation_success_rate、broadcast_latency_ms、receipt_latency_ms、tx_revert_total、expected/actual profit、gas_loss、nonce_gap、wallet_balance。

## 参考

- [Robinhood Chain 文档](https://docs.robinhood.com/chain/connecting/)
- [Morpho Blue 文档](https://docs.morpho.org/learn/concepts/blue/)
- [Uniswap V3 Quoting](https://docs.uniswap.org/sdk/v3/guides/swaps/quoting)
- 旧项目 `rh-sniper` 保留为历史研究档案，不复用其实现（`engine.py` 91KB 单文件、subprocess 调 gmgn-cli、JSON 当数据库等做法已弃用）
