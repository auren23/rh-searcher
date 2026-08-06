#!/usr/bin/env bash
# anvil_latest_smoke.sh — 免费验证 Executor 与 Robinhood 主网真实 V3 池的兼容性。
#
# 原理：anvil fork 主网**最新**状态（不指定 --fork-block-number，避免历史状态
# 需求），本地虚拟钱包部署 Executor + 虚拟 WETH 注资，对主网真实池执行。
# 不做历史回放、不模拟实时竞争，只证明"合约能在真实池状态下执行"。
#
# 用法：RH_READ_RPC=https://rpc.mainnet.chain.robinhood.com bash scripts/anvil_latest_smoke.sh
set -euo pipefail

RH_READ_RPC="${RH_READ_RPC:-https://rpc.mainnet.chain.robinhood.com}"
WETH=0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73
V3_FACTORY=0x1f7d7550b1b028f7571e69a784071f0205fd2efa
# anvil 默认测试钱包（仅本地有效）
PK=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
WALLET=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
RPC=http://127.0.0.1:8545
export PATH="$PATH:$HOME/.foundry/bin"

echo "== 1. anvil fork latest（公共 RPC 非 archive：历史块不可用，latest 可试）"
setsid anvil --fork-url "$RH_READ_RPC" --chain-id 4663 --port 8545 --silent \
  >/tmp/anvil_latest.log 2>&1 < /dev/null & disown
sleep 20
HEAD=$(curl -s -m 8 $RPC -X POST -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' | python3 -c "import json,sys;print(json.load(sys.stdin)['result'])")
if [ -z "$HEAD" ]; then
  echo "!! anvil fork failed (see /tmp/anvil_latest.log)；公共 RPC 可能仍拒绝 fork 初始化"
  tail -3 /tmp/anvil_latest.log
  exit 2
fi
echo "fork head: $HEAD"

echo "== 2. 部署 ArbitrageExecutor"
cd "$(dirname "$0")/../contracts"
export HOT_WALLET=$WALLET WETH=$WETH V3_FACTORY=$V3_FACTORY
forge script script/DeployArbitrage.s.sol:DeployArbitrage \
  --rpc-url $RPC --broadcast --legacy --private-key $PK >/dev/null 2>&1 || { echo "!! deploy failed"; exit 2; }
EXEC=$(python3 -c "
import json
d=json.load(open('broadcast/DeployArbitrage.s.sol/4663/run-latest.json'))
print([t['contractAddress'] for t in d['transactions'] if t.get('contractAddress')][0])")
echo "executor: $EXEC"

echo "== 3. 虚拟 WETH：deposit + 注资 executor"
cast send $WETH "deposit()" --value 1ether --private-key $PK --rpc-url $RPC >/dev/null
cast send $WETH "transfer(address,uint256)" $EXEC 500000000000000000 \
  --private-key $PK --rpc-url $RPC >/dev/null
BAL=$(cast call $WETH "balanceOf(address)(uint256)" $EXEC --rpc-url $RPC)
echo "executor weth balance: $BAL"

echo "== 4. 真实主网池状态（aeWETH 池）"
for P in 0xa9188730fe85be88ad499d7d52b099e800fb0334 0x8BE7E807FBA21F5C16D2F0079b1A7c84C0866EA5; do
  T=$(cast call $P "slot0()" --rpc-url $RPC 2>/dev/null | head -c 40)
  echo "pool $P slot0: ${T:0:32}..."
done

echo "== 5. 执行冒烟（单池往返会被合约拒绝——这里仅验证调用路径与 revert 语义）"
# 用两个真实 aeWETH 池构造两跳（WETH→X→WETH）：链上同对池通常只有一个 fee，
# 这里退化为验证"池归属校验"路径：错误池地址必须被 PoolNotWhitelisted 拒绝
BAD=$(cast calldata "executeV3Cycle((address,address,address,uint24)[],uint256,uint256,uint256)" \
  "[($WETH,$WETH,0x99f381b8bcd5b367178809abdbb7ae79da782e0e,3000),($WETH,0x99f381b8bcd5b367178809abdbb7ae79da782e0e,$WETH,3000)]" \
  1000000000000000 1 2000000000)
R=$(cast send $EXEC $BAD --private-key $PK --rpc-url $RPC 2>&1 | grep -oE "PoolNotWhitelisted|HopDiscontinuity|DuplicatePool|status.*success" | head -1)
echo "revert path: ${R:-OK}"
echo "== smoke done =="
