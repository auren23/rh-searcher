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
| M2 | V3 Factory/Pool 索引 + 本地 Quote 验收 | ✅ 事件解码修正（真实日志 fixture 测试）+ 双向精确报价（官方 SqrtPriceMath 移植，TickMath 金标准测试） |
| M3 | WETH→TOKEN→WETH 两池 Shadow 搜索 | ✅ 引擎 + 完整候选落盘（可重放 ID/路由 JSON） |
| M4 | ArbitrageExecutor + Foundry 测试 | ✅ **重写为直接调 V3 Pool**（executeV3Cycle），14/14 测试通过（两跳成功/权限/minProfit/回调验证/重入/重复池） |
| M5 | Replay → Shadow → Canary → Live 上线门槛 | 未开始 |
| M6 | Morpho 市场/仓位索引 | ⏸ 推迟：链上 0 市场（见 M0 报告），等 CreateMarket 事件 |
| M7 | 清算机会计算 | ⏸ 推迟（依赖 M6） |
| M8 | LiquidationExecutor + Flash Loan | 🔶 合约草案保留 |

**开发顺序：先完成共享底座 → DEX 池状态 → 套利 Shadow → 套利执行 → Morpho 索引 → 清算执行。**
清算会复用套利约一半的基础能力（DEX 状态/报价/模拟），不要两个策略同时开写。

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
