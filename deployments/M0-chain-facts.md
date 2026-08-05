# M0 链上事实确认报告（2026-08-05）

验收标准：所有地址有来源、部署区块；关键事实经链上 RPC 实测确认。

## 链基础

| 项 | 值 | 来源/验证 |
|---|---|---|
| Chain ID | **4663** (0x1237) | 官方文档 docs.robinhood.com/chain/connecting/ + `eth_chainId` 实测 |
| 类型 | Arbitrum Nitro | 官方文档（run-a-full-node 用 nitro-node） |
| 原生代币 | ETH (18 decimals) | 官方 |
| 公共 RPC | `https://rpc.mainnet.chain.robinhood.com` | 官方（限速，仅开发） |
| 公共 WS | `wss://rpc.mainnet.chain.robinhood.com/ws` | 官方文档 |
| Sequencer Feed | `wss://feed.mainnet.chain.robinhood.com` | 官方（低延迟发现，后续接入） |
| Sequencer Endpoint | `https://sequencer.mainnet.chain.robinhood.com` | 官方（备用发送端） |
| Explorer | `https://robinhoodchain.blockscout.com` | 官方 |
| 当前高度 | ~28,290,000 | eth_blockNumber 实测 |
| `debug_traceTransaction` | **不支持** | 公共 RPC 返回 method not available |

## 关键合约（全部经 eth_getCode / eth_call / blockscout verified 三重确认）

| 合约 | 地址 | 部署区块 | 验证方式 |
|---|---|---|---|
| **WETH (aeWETH)** | `0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73` | 2 | eip1967 proxy，实现 aeWETH `0xC6B81b429797E0f555440b70cD99e032D7AE947e`（verified：deposit/withdraw/approve/transfer/permit + bridgeMint/bridgeBurn）。**不是** mainnet WETH 地址 |
| **Morpho Blue** | `0x9d53d5e3bd5e8d4cbfa6db1ca238aea02e651010` | 286 | blockscout verified "Morpho"，15582 字节；从 MetaMorphoV1_1 vault 的 `MORPHO()` immutable 读出。**不在**标准地址 0xBBBB...FFCb（该地址无代码） |
| **Uniswap V3 Factory** | `0x1f7d7550b1b028f7571e69a784071f0205fd2efa` | 8930 | verified UniswapV3Factory (0.7.6)，24535 字节；feeAmountTickSpacing(3000)=60；getPool(WETH,..,10000) 返回实际池 |
| **Uniswap V2 Factory** | `0x8bceaa40b9acdfaedf85adf4ff01f5ad6517937f` | 8928 | verified UniswapV2Factory，30150 对；pair.factory() 确认 |
| **UniversalRouter** | `0x8876789976dEcBfCbBbe364623C63652db8C0904` | 18127 | verified UniversalRouter（**非 SwapRouter**，calldata 用 execute(commands, inputs) 命令编码） |
| **MixedRouteQuoterV2** | `0x7edd862aa08dD5Be664C21188E1A2A0E64e3A283` | 27077903 | verified；`uniswapV3Poolfactory()` 返回 V3 factory、`uniswapV2Poolfactory()` 返回 V2 factory（交叉验证） |

## 陷阱（务必记录）

1. **标准 mainnet 地址全部被占用/无效**：
   - `0xE592427A...`（mainnet SwapRouter）：链上是蜜罐合约（仅 recoverETH/transferTokensTo/changeOwner 等 5 个函数）
   - `0x1F98431c...`（mainnet V3 Factory）：链上仅 2109 字节 stub
   - `0x7a250d56...`（mainnet V2 Router）、`0xC02aaA39...`（mainnet WETH）、`0xA0b86991...`（USDC）、`0xcbB7C000...`（cbBTC）：**无代码**
   - blockscout 的 verified 名称可能来自 mainnet twin 数据（`verified_twin_address_hash`），**不可盲信**
2. **WETH 是 proxy**：aeWETH 实现是标准 ERC20 + deposit/withdraw，对执行合约兼容；但它是升级代理，M4 合约不应假设地址不可变
3. **init code hash 与 mainnet 不同**：Robinhood pool 部署字节码 keccak = `0x69d2e169bb75e98864b527160eb6afa342f1a44319deb279a17f42c4de48ab44`，mainnet = `0xf2b8b58f95b1471751302e520a0e7c410ce9846ed46020be253dbd25fbb6da11`（同 22142 字节）。实际 init code 需 archive 节点 + 内部交易提取；**MVP 池寻址用 factory.getPool()，不依赖 init code hash**
4. **Morpho 链上无活跃市场（重要结论）**：
   - `CreateMarket` 事件（topic `0x328c8b64...`）从区块 0 到现在 **0 次**
   - `market(0xfff517c4...)`（API 返回的 marketId）链上返回全零
   - MetaMorpho vault `0x8cb8AA35...` 的 totalAssets ≈ 0（份额历史存在但资产归零）
   - **Morpho Blue 是"已部署但闲置"**（部署于区块 286，从未创建市场）
   - Morpho API `blue-api.morpho.org/graphql?chainId=4663` 返回的数据**与链上状态不符**（asset 地址链上无代码），不可作为生产数据源，必须链上交叉验证
   - **影响**：清算策略（M6-M8）当前无目标市场，推迟到真实市场出现后再推进；M0 后聚焦套利（M2-M5）

## M0 遗留 TODO

- [ ] GMGN 快速 RPC 是否支持 robinhood 链（发送端点确认）——用 `gmgn-cli token info --chain robinhood` 实测
- [ ] 归档节点（供应商）：公共 RPC 不支持 archive/debug 方法
- [x] 稳定币主地址：mainnet 标准地址（USDC `0xA0b8...`、cbBTC `0xcbB7...`）链上确认无代码（与 Morpho API 数据矛盾 → API 不可信）
- [ ] Morpho 市场：等待真实 CreateMarket 事件（当前链上 0 市场）
- [x] init code hash：pool 部署字节码与 mainnet 不同（定制版 V3），Quoter 在该链 create2 寻址失败（quoteExactInputSingle(V2/V3) 全部 revert）→ **本地报价直读 slot0/liquidity，寻址用 factory.getPool()，链上验证走自己的执行合约**（不依赖 Quoter）
- [ ] V3 WETH 池清单与 TVL（第一版两池循环的基础）—— rh-indexer 跑通后自动积累
